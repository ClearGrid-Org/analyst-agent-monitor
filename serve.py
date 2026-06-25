#!/usr/bin/env python3
"""
sl-agent-monitor — operational monitoring dashboard for cg-sl-agent.

Standalone web UI (no dependency on the agent's Python package). Reads
from the agent's Postgres (agent_runs / agent_spans / user_budget_limits
/ agent_bq_usage) and tails its log file.

Single page at localhost:8766, auto-refreshing tiles:
  • Live in-flight runs (with elapsed timer)
  • Last 30 completed runs (compact list)
  • 24-hour metrics tiles: questions, success%, p50/p95 latency, total cost,
    BigQuery bytes scanned
  • Top users today
  • Per-user budget headroom (from user_budget_limits + actual usage)
  • Live agent.log tail via Server-Sent Events

Run
---
    python serve.py                                 # localhost:8766
    python serve.py --host 0.0.0.0                  # all interfaces (gate with nginx + auth)
    python serve.py --log-file /custom/path.log     # custom log path

Access from a laptop (SSH tunnel):
    ssh -L 8766:127.0.0.1:8766 ubuntu@<vm-ip>
    # then open http://127.0.0.1:8766

Configuration
-------------
All connection vars are read from .env (or process env). See .env.example.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

# ─── Config — self-contained, no agent package dependency ────────────────
# Loads .env from the script's directory if python-dotenv is available.
# Falls back to bare os.environ if not.
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env", override=False)
except ImportError:
    pass

try:
    import psycopg
except ImportError:
    print("ERROR: pip install 'psycopg[binary]>=3.1'", file=sys.stderr)
    sys.exit(2)


def _env(name: str, default: str | None = None, required: bool = False) -> str:
    v = os.environ.get(name, default)
    if required and not v:
        print(f"ERROR: required env var {name} not set (see .env.example)",
              file=sys.stderr)
        sys.exit(2)
    return v or ""


DB_HOST     = _env("DB_HOST",     required=True)
DB_PORT     = _env("DB_PORT",     "5432")
DB_USER     = _env("DB_USER",     required=True)
DB_PASSWORD = _env("DB_PASSWORD", required=True)
DB_NAME     = _env("DB_NAME",     required=True)
DB_SSLMODE  = _env("DB_SSLMODE",  "prefer").strip()


# ─── DB helpers ──────────────────────────────────────────────────────────
def _conn():
    return psycopg.connect(
        host=DB_HOST, port=int(DB_PORT),
        user=DB_USER, password=DB_PASSWORD,
        dbname=DB_NAME, sslmode=DB_SSLMODE,
        autocommit=True,
    )


def _rows(sql: str, params: tuple = ()) -> list[dict]:
    with _conn() as c, c.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _jsonify(o: Any) -> Any:
    if isinstance(o, (datetime,)):
        return o.replace(tzinfo=timezone.utc).isoformat() if o.tzinfo is None else o.isoformat()
    if isinstance(o, (list, tuple)):
        return [_jsonify(x) for x in o]
    if isinstance(o, dict):
        return {k: _jsonify(v) for k, v in o.items()}
    if hasattr(o, "isoformat"):
        return o.isoformat()
    return o


# ─── Data fetchers ───────────────────────────────────────────────────────
def fetch_live_runs() -> list[dict]:
    """Currently in-flight requests — started but not finished, last 10 min."""
    return _rows("""
        select id, surface, channel, user_id, user_name,
               left(coalesce(question, ''), 120)               as question_preview,
               agent_name, tier, started_at,
               extract(epoch from (now() - started_at))::int   as elapsed_sec
        from agent_runs
        where finished_at is null
          and started_at > now() - interval '10 minutes'
        order by started_at desc
        limit 50
    """)


def fetch_recent_runs(limit: int = 30) -> list[dict]:
    """Last N completed runs — compact for the dashboard panel."""
    return _rows("""
        select id, surface, user_name,
               left(coalesce(question, ''), 100)               as question_preview,
               status, tier,
               round(duration_sec::numeric, 1)                 as duration_sec,
               coalesce(total_input_tokens, 0)
                 + coalesce(total_output_tokens, 0)            as total_tokens,
               round(coalesce(total_cost_usd, 0)::numeric, 4)  as cost_usd,
               started_at, finished_at,
               case
                   when status = 'error' or error_msg is not null then true
                   else false
               end                                              as errored,
               left(coalesce(error_msg, ''), 200)               as error_preview
        from agent_runs
        where finished_at is not null
        order by finished_at desc
        limit %s
    """, (limit,))


def fetch_metrics_24h() -> dict:
    rows = _rows("""
        with windowed as (
            select * from agent_runs
            where started_at > now() - interval '24 hours'
              and finished_at is not null
        )
        select
            count(*)                                              as total_runs,
            sum(case when status = 'ok' or (status is null and error_msg is null)
                     then 1 else 0 end)                            as ok_runs,
            sum(case when status = 'error' or error_msg is not null
                     then 1 else 0 end)                            as err_runs,
            percentile_cont(0.50) within group (order by duration_sec) as p50_sec,
            percentile_cont(0.95) within group (order by duration_sec) as p95_sec,
            sum(coalesce(total_input_tokens,0) + coalesce(total_output_tokens,0)) as total_tokens,
            sum(coalesce(total_cost_usd, 0))                       as total_cost_usd
        from windowed
    """)
    m = rows[0] if rows else {}
    # BQ bytes scanned in last 24h
    try:
        bq = _rows("""
            select coalesce(sum(bytes_billed), 0) as bytes_billed_24h
            from agent_bq_usage
            where timestamp > now() - interval '24 hours'
        """)
        m["bq_bytes_24h"] = int(bq[0]["bytes_billed_24h"] or 0) if bq else 0
    except Exception:
        m["bq_bytes_24h"] = None
    return _jsonify(m)


def fetch_top_users_today() -> list[dict]:
    return _rows("""
        select user_name,
               count(*)                                                as runs_today,
               sum(coalesce(total_input_tokens,0)
                 + coalesce(total_output_tokens,0))                    as tokens_today,
               round(sum(coalesce(total_cost_usd,0))::numeric, 4)      as cost_today
        from agent_runs
        where started_at >= current_date
          and user_name is not null
        group by user_name
        order by runs_today desc
        limit 10
    """)


def fetch_budget_status() -> list[dict]:
    """Per-user budget vs actual usage today and this month."""
    try:
        return _rows("""
            with today_usage as (
                select user_name,
                       sum(coalesce(total_input_tokens,0)
                         + coalesce(total_output_tokens,0))    as tokens_today,
                       sum(coalesce(total_cost_usd,0))         as cost_today_usd
                from agent_runs
                where started_at >= current_date
                group by user_name
            ),
            month_usage as (
                select user_name,
                       sum(coalesce(total_input_tokens,0)
                         + coalesce(total_output_tokens,0))    as tokens_mtd
                from agent_runs
                where started_at >= date_trunc('month', current_date)
                group by user_name
            )
            select b.user_pattern,
                   b.tokens_per_day, b.tokens_per_month,
                   coalesce(t.tokens_today, 0)                  as tokens_today,
                   coalesce(m.tokens_mtd, 0)                    as tokens_mtd,
                   round(coalesce(t.cost_today_usd, 0)::numeric, 4) as cost_today_usd,
                   case when b.tokens_per_day is not null and b.tokens_per_day > 0
                        then round((coalesce(t.tokens_today, 0)::numeric
                                    / b.tokens_per_day) * 100, 1)
                   end                                          as day_used_pct,
                   case when b.tokens_per_month is not null and b.tokens_per_month > 0
                        then round((coalesce(m.tokens_mtd, 0)::numeric
                                    / b.tokens_per_month) * 100, 1)
                   end                                          as month_used_pct
            from user_budget_limits b
            left join today_usage t  on t.user_name = b.user_pattern
            left join month_usage m  on m.user_name = b.user_pattern
            order by day_used_pct desc nulls last
        """)
    except Exception:
        return []


def fetch_agent_status(log_path: Path) -> dict:
    """
    Combine three signals to decide if the agent is up/quiet/down/unknown:

      1. agent_heartbeats table (most reliable — opt-in; agent must write it)
      2. Log file mtime (works without any agent change, but goes stale if
         the agent stops writing logs even though it's hung)
      3. Most recent agent_runs.started_at (only meaningful during traffic)

    Decision tree:
      • heartbeat < 60s old              → UP   (certain)
      • log written < 2 min ago          → UP   (likely)
      • log written 2-10 min ago         → QUIET (uncertain — possibly idle)
      • log written > 10 min ago AND
        no recent runs                   → DOWN (likely)
      • Nothing of these available       → UNKNOWN
    """
    now = datetime.now(timezone.utc)
    status = {
        "state":            "unknown",      # up | quiet | down | unknown
        "signal_used":      None,
        "heartbeat_at":     None,
        "heartbeat_age_s":  None,
        "log_mtime":        None,
        "log_age_s":        None,
        "last_run_at":      None,
        "last_run_age_s":   None,
    }

    # ── (1) Heartbeat table, if present ──────────────────────────────────
    try:
        rows = _rows("""
            select heartbeat_at, hostname, pid
            from agent_heartbeats
            order by heartbeat_at desc
            limit 1
        """)
        if rows:
            hb = rows[0]["heartbeat_at"]
            hb_aware = hb if hb.tzinfo else hb.replace(tzinfo=timezone.utc)
            status["heartbeat_at"]    = hb
            status["heartbeat_age_s"] = int((now - hb_aware).total_seconds())
            status["hostname"]        = rows[0].get("hostname")
            status["pid"]             = rows[0].get("pid")
    except Exception:
        pass  # table doesn't exist; that's OK, fall through to other signals

    # ── (2) Log file mtime ───────────────────────────────────────────────
    try:
        if log_path.exists():
            mtime = datetime.fromtimestamp(log_path.stat().st_mtime, tz=timezone.utc)
            status["log_mtime"] = mtime
            status["log_age_s"] = int((now - mtime).total_seconds())
    except Exception:
        pass

    # ── (3) Most recent agent_runs ───────────────────────────────────────
    try:
        rows = _rows("""
            select started_at
            from agent_runs
            order by started_at desc
            limit 1
        """)
        if rows:
            lr = rows[0]["started_at"]
            lr_aware = lr if lr.tzinfo else lr.replace(tzinfo=timezone.utc)
            status["last_run_at"]    = lr
            status["last_run_age_s"] = int((now - lr_aware).total_seconds())
    except Exception:
        pass

    # ── Decision tree ────────────────────────────────────────────────────
    if status["heartbeat_age_s"] is not None and status["heartbeat_age_s"] < 60:
        status["state"], status["signal_used"] = "up", "heartbeat"
    elif status["log_age_s"] is not None and status["log_age_s"] < 120:
        status["state"], status["signal_used"] = "up", "log"
    elif status["log_age_s"] is not None and status["log_age_s"] < 600:
        status["state"], status["signal_used"] = "quiet", "log"
    elif status["log_age_s"] is not None or status["last_run_age_s"] is not None:
        # Have at least one signal, but all are stale → likely down
        status["state"], status["signal_used"] = "down", "log+runs"
    # else stays 'unknown'

    return _jsonify(status)


def fetch_health() -> dict:
    last_err = _rows("""
        select id, started_at, user_name, left(error_msg, 300) as error_msg
        from agent_runs
        where error_msg is not null
        order by started_at desc
        limit 1
    """)
    last_ok = _rows("""
        select started_at
        from agent_runs
        where (status = 'ok' or (status is null and error_msg is null))
          and finished_at is not null
        order by finished_at desc
        limit 1
    """)
    err_rate_1h = _rows("""
        select
            count(*)                                              as total,
            sum(case when error_msg is not null then 1 else 0 end) as errors
        from agent_runs
        where started_at > now() - interval '1 hour'
          and finished_at is not null
    """)
    in_flight = _rows("""
        select count(*) as n
        from agent_runs
        where finished_at is null
          and started_at > now() - interval '10 minutes'
    """)
    return _jsonify({
        "last_error":           last_err[0] if last_err else None,
        "last_ok_at":           last_ok[0]["started_at"] if last_ok else None,
        "err_rate_1h":          err_rate_1h[0] if err_rate_1h else {},
        "in_flight_count":      int(in_flight[0]["n"]) if in_flight else 0,
    })


# ─── Log tail (SSE) ──────────────────────────────────────────────────────
def stream_log(handler: BaseHTTPRequestHandler, log_path: Path) -> None:
    """Server-Sent Events stream of the agent log file tail."""
    handler.send_response(200)
    handler.send_header("Content-Type", "text/event-stream")
    handler.send_header("Cache-Control", "no-cache")
    handler.send_header("X-Accel-Buffering", "no")
    handler.end_headers()

    if not log_path.exists():
        handler.wfile.write(f"data: (log not found at {log_path})\n\n".encode())
        handler.wfile.flush()
        return

    with open(log_path, "r", errors="replace") as f:
        # Seek to last ~5KB so the panel doesn't replay everything
        try:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 5000))
            f.readline()  # discard partial line
        except OSError:
            pass

        while True:
            line = f.readline()
            if line:
                payload = line.rstrip("\n").replace("\r", "")
                handler.wfile.write(f"data: {payload}\n\n".encode())
                try:
                    handler.wfile.flush()
                except BrokenPipeError:
                    return
            else:
                time.sleep(0.5)


# ─── HTTP handler ────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    log_path = Path("/var/log/cg-sl-agent/agent.log")

    def log_message(self, fmt, *args):  # mute access log
        return

    def _json(self, payload: Any, code: int = 200) -> None:
        body = json.dumps(_jsonify(payload), default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _html(self, body: bytes) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        try:
            if path in ("/", "/index.html"):
                self._html(INDEX_HTML.encode())
            elif path == "/api/live":
                self._json(fetch_live_runs())
            elif path == "/api/recent":
                self._json(fetch_recent_runs())
            elif path == "/api/metrics":
                self._json(fetch_metrics_24h())
            elif path == "/api/top_users":
                self._json(fetch_top_users_today())
            elif path == "/api/budgets":
                self._json(fetch_budget_status())
            elif path == "/api/health":
                self._json(fetch_health())
            elif path == "/api/agent_status":
                self._json(fetch_agent_status(self.log_path))
            elif path == "/api/logs/tail":
                stream_log(self, self.log_path)
            else:
                self.send_response(404)
                self.end_headers()
        except Exception as e:
            self.send_response(500)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(f"{type(e).__name__}: {e}".encode())


# ─── HTML/JS (single file, no build step) ────────────────────────────────
INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>cg-sl-agent — monitor</title>
<style>
  :root {
    --bg:#0e1116; --panel:#161a21; --panel2:#1d222b;
    --ink:#e6edf3; --muted:#8b949e; --border:#30363d;
    --ok:#3fb950; --warn:#d29922; --err:#f85149; --info:#58a6ff;
    --teal:#0f766e;
  }
  *{box-sizing:border-box}
  body{margin:0;font:13px/1.5 -apple-system,Segoe UI,Helvetica,sans-serif;
       background:var(--bg);color:var(--ink)}
  header{display:flex;align-items:center;gap:16px;padding:12px 16px;
         border-bottom:1px solid var(--border);background:var(--panel)}
  h1{font-size:14px;margin:0;color:var(--ink)}
  .dot{width:10px;height:10px;border-radius:50%;display:inline-block;
       vertical-align:middle;margin-right:6px}
  .dot.ok{background:var(--ok)} .dot.warn{background:var(--warn)} .dot.err{background:var(--err)}

  /* Agent up/down pill — more prominent than the health pill since it's
     the answer to "is the bot alive at all". */
  .agent-pill{
    display:inline-flex;align-items:center;gap:6px;
    padding:4px 10px;border-radius:999px;
    font-size:12px;font-weight:600;
    border:1px solid var(--border);background:var(--panel2);
  }
  .agent-pill .dot{margin:0}
  .agent-pill.up      {border-color:var(--ok);   background:#0e3018;color:var(--ok)}
  .agent-pill.up      .dot{background:var(--ok);box-shadow:0 0 0 3px rgba(63,185,80,.15)}
  .agent-pill.quiet   {border-color:var(--warn); background:#3a2806;color:var(--warn)}
  .agent-pill.quiet   .dot{background:var(--warn)}
  .agent-pill.down    {border-color:var(--err);  background:#3a1212;color:var(--err)}
  .agent-pill.down    .dot{background:var(--err);
    animation:pulse 1.2s ease-in-out infinite}
  .agent-pill.unknown {color:var(--muted)}
  .agent-pill.unknown .dot{background:var(--muted)}
  @keyframes pulse {
    0%, 100% { box-shadow: 0 0 0 0 rgba(248,81,73,.6); }
    50%      { box-shadow: 0 0 0 6px rgba(248,81,73,0); }
  }
  .agent-sub{font-size:11px;color:var(--muted);font-weight:400;margin-left:4px}
  .muted{color:var(--muted)} .mono{font-family:ui-monospace,Menlo,Consolas,monospace}
  .grid{display:grid;gap:12px;padding:12px;
        grid-template-columns: 2fr 1fr;
        grid-template-areas:
          "tiles tiles"
          "live  recent"
          "users budgets"
          "logs  logs";}
  .panel{background:var(--panel);border:1px solid var(--border);
         border-radius:8px;overflow:hidden}
  .panel h2{font-size:11px;margin:0;padding:8px 12px;color:var(--muted);
            text-transform:uppercase;letter-spacing:.05em;border-bottom:1px solid var(--border);
            background:var(--panel2);display:flex;justify-content:space-between;align-items:center}
  .tile-row{grid-area:tiles;display:grid;grid-template-columns:repeat(6,1fr);gap:12px}
  .tile{background:var(--panel);border:1px solid var(--border);border-radius:8px;
        padding:12px 14px}
  .tile .label{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em}
  .tile .value{font-size:22px;font-weight:600;margin-top:2px}
  .tile .sub{font-size:11px;color:var(--muted);margin-top:2px}
  .live{grid-area:live}.recent{grid-area:recent}
  .users{grid-area:users}.budgets{grid-area:budgets}.logs{grid-area:logs}
  table{width:100%;border-collapse:collapse;font-size:12px}
  th,td{padding:6px 10px;text-align:left;border-bottom:1px solid var(--border)}
  th{color:var(--muted);font-weight:500;font-size:11px;text-transform:uppercase;letter-spacing:.04em}
  tr:last-child td{border-bottom:none}
  tr.err td{color:var(--err)} tr.err td.q{color:#fb8985}
  .pill{display:inline-block;padding:1px 6px;border-radius:3px;font-size:10px;
        background:var(--panel2);color:var(--muted);font-family:monospace}
  .pill.ok{background:#0e3018;color:var(--ok)}
  .pill.err{background:#3a1212;color:var(--err)}
  .pill.warn{background:#3a2806;color:var(--warn)}
  .timer{font-family:monospace;color:var(--warn)}
  pre.log{margin:0;padding:10px;background:#0a0d12;height:240px;overflow-y:auto;
          font:11px/1.4 ui-monospace,Menlo,Consolas,monospace;color:var(--ink)}
  pre.log .err{color:var(--err)} pre.log .warn{color:var(--warn)}
  .bar{height:6px;background:var(--panel2);border-radius:3px;overflow:hidden;margin-top:4px}
  .bar > div{height:100%;background:var(--info);transition:width .3s}
  .bar > div.high{background:var(--warn)} .bar > div.over{background:var(--err)}
  td.q{max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .empty{padding:24px;text-align:center;color:var(--muted);font-style:italic}
</style>
</head>
<body>
  <header>
    <h1>cg-sl-agent · monitor</h1>
    <span id="agent-pill" class="agent-pill unknown" title="Agent process status">
      <span class="dot"></span><span class="agent-label">checking…</span>
    </span>
    <span id="health-pill" class="muted">…</span>
    <span class="muted" style="margin-left:auto">refresh every 5s · log tail live</span>
  </header>
  <div class="grid">
    <div class="tile-row" id="tile-row"></div>
    <div class="panel live"><h2>Live (in-flight) <span id="live-count" class="muted">0</span></h2>
      <div id="live-body"><div class="empty">No active requests.</div></div>
    </div>
    <div class="panel recent"><h2>Last 30 completed</h2>
      <div id="recent-body"><div class="empty">Loading…</div></div>
    </div>
    <div class="panel users"><h2>Top users today</h2>
      <div id="users-body"><div class="empty">Loading…</div></div>
    </div>
    <div class="panel budgets"><h2>Budget headroom</h2>
      <div id="budgets-body"><div class="empty">Loading…</div></div>
    </div>
    <div class="panel logs"><h2>Live log tail
        <span class="muted" id="log-source"></span></h2>
      <pre class="log" id="log"></pre>
    </div>
  </div>

<script>
const $ = id => document.getElementById(id);
const esc = s => (s ?? '').toString()
   .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

function fmtBytes(b) {
  if (b == null) return '—';
  const u = ['B','KB','MB','GB','TB']; let i = 0;
  while (b >= 1024 && i < u.length-1) { b /= 1024; i++; }
  return b.toFixed(b < 10 ? 2 : 1) + ' ' + u[i];
}
function fmtTime(s) {
  if (s == null) return '—';
  return s.toFixed ? s.toFixed(1)+'s' : s+'s';
}
function fmtUsd(v) {
  if (v == null) return '—';
  return '$' + Number(v).toFixed(v < 1 ? 4 : 2);
}
function ago(iso) {
  if (!iso) return '—';
  const d = new Date(iso); const s = (Date.now() - d.getTime()) / 1000;
  if (s < 60) return s.toFixed(0)+'s ago';
  if (s < 3600) return (s/60).toFixed(0)+'m ago';
  if (s < 86400) return (s/3600).toFixed(1)+'h ago';
  return (s/86400).toFixed(1)+'d ago';
}

async function refreshTiles() {
  const m = await fetch('/api/metrics').then(r => r.json());
  const okRate = m.total_runs > 0
      ? (100 * (m.ok_runs / m.total_runs)).toFixed(1) + '%' : '—';
  $('tile-row').innerHTML = `
    <div class="tile"><div class="label">Questions (24h)</div>
      <div class="value">${m.total_runs ?? 0}</div>
      <div class="sub">${m.err_runs ?? 0} errored</div></div>
    <div class="tile"><div class="label">Success rate (24h)</div>
      <div class="value">${okRate}</div>
      <div class="sub">${m.ok_runs ?? 0} ok / ${m.total_runs ?? 0}</div></div>
    <div class="tile"><div class="label">p50 latency</div>
      <div class="value">${fmtTime(m.p50_sec)}</div>
      <div class="sub">p95 ${fmtTime(m.p95_sec)}</div></div>
    <div class="tile"><div class="label">Cost (24h)</div>
      <div class="value">${fmtUsd(m.total_cost_usd)}</div>
      <div class="sub">${(m.total_tokens || 0).toLocaleString()} tokens</div></div>
    <div class="tile"><div class="label">BQ scanned (24h)</div>
      <div class="value">${fmtBytes(m.bq_bytes_24h)}</div>
      <div class="sub">billed bytes</div></div>
    <div class="tile"><div class="label">In-flight now</div>
      <div class="value" id="in-flight-tile">—</div>
      <div class="sub">started < 10 min ago</div></div>
  `;
}

async function refreshAgentStatus() {
  const s = await fetch('/api/agent_status').then(r => r.json());
  const pill = $('agent-pill');
  pill.classList.remove('up','quiet','down','unknown');
  pill.classList.add(s.state || 'unknown');

  const labels = {
    up:      'AGENT UP',
    quiet:   'AGENT QUIET',
    down:    'AGENT DOWN',
    unknown: 'AGENT STATUS UNKNOWN',
  };
  let sub = '';
  if (s.state === 'up' && s.signal_used === 'heartbeat')
    sub = `heartbeat ${s.heartbeat_age_s}s ago`;
  else if (s.state === 'up' && s.log_age_s != null)
    sub = `log ${s.log_age_s}s ago`;
  else if (s.state === 'quiet' && s.log_age_s != null)
    sub = `last log ${Math.round(s.log_age_s/60)}m ago — possibly idle`;
  else if (s.state === 'down')
    sub = `no activity for ${Math.round((s.log_age_s||0)/60)}m`;
  else if (s.state === 'unknown')
    sub = 'no log + no DB activity yet';

  pill.innerHTML = '<span class="dot"></span>'
    + '<span class="agent-label">' + (labels[s.state] || 'UNKNOWN') + '</span>'
    + '<span class="agent-sub">' + sub + '</span>';
}

async function refreshHealth() {
  const h = await fetch('/api/health').then(r => r.json());
  const total = (h.err_rate_1h && h.err_rate_1h.total) || 0;
  const errs  = (h.err_rate_1h && h.err_rate_1h.errors) || 0;
  let cls = 'ok', label = 'healthy';
  if (errs > 0 && total > 0 && (errs / total) > 0.25) { cls = 'err'; label = 'high error rate (1h)'; }
  else if (errs > 0) { cls = 'warn'; label = errs + ' err / ' + total + ' (1h)'; }
  $('health-pill').innerHTML = '<span class="dot '+cls+'"></span>' + label
      + (h.last_error ? ' · last err ' + ago(h.last_error.started_at) : '');
  if ($('in-flight-tile')) $('in-flight-tile').textContent = h.in_flight_count;
}

async function refreshLive() {
  const rows = await fetch('/api/live').then(r => r.json());
  $('live-count').textContent = rows.length;
  if (!rows.length) { $('live-body').innerHTML = '<div class="empty">No active requests.</div>'; return; }
  $('live-body').innerHTML = '<table><tr>'
    + '<th>Elapsed</th><th>User</th><th>Surface</th><th>Question</th>'
    + '</tr>' + rows.map(r =>
      '<tr><td><span class="timer">' + r.elapsed_sec + 's</span></td>'
      + '<td>' + esc(r.user_name || r.user_id || '—') + '</td>'
      + '<td><span class="pill">' + esc(r.surface || '—') + '</span></td>'
      + '<td class="q" title="' + esc(r.question_preview) + '">'
      + esc(r.question_preview) + '</td></tr>').join('') + '</table>';
}

async function refreshRecent() {
  const rows = await fetch('/api/recent').then(r => r.json());
  if (!rows.length) { $('recent-body').innerHTML = '<div class="empty">No runs yet.</div>'; return; }
  $('recent-body').innerHTML = '<table><tr>'
    + '<th></th><th>Question</th><th>Dur</th><th>Cost</th><th>When</th>'
    + '</tr>' + rows.map(r => {
      const errCls = r.errored ? ' err' : '';
      const statusPill = '<span class="pill ' + (r.errored ? 'err' : 'ok') + '">'
        + (r.errored ? 'err' : (r.status || 'ok')) + '</span>';
      return '<tr class="' + (r.errored ? 'err' : '') + '">'
        + '<td>' + statusPill + '</td>'
        + '<td class="q" title="' + esc(r.errored ? r.error_preview : r.question_preview) + '">'
          + esc(r.question_preview) + '</td>'
        + '<td>' + fmtTime(r.duration_sec) + '</td>'
        + '<td>' + fmtUsd(r.cost_usd) + '</td>'
        + '<td class="muted">' + ago(r.finished_at) + '</td></tr>';
    }).join('') + '</table>';
}

async function refreshUsers() {
  const rows = await fetch('/api/top_users').then(r => r.json());
  if (!rows.length) { $('users-body').innerHTML = '<div class="empty">No activity today.</div>'; return; }
  $('users-body').innerHTML = '<table><tr>'
    + '<th>User</th><th>Runs</th><th>Tokens</th><th>Cost</th>'
    + '</tr>' + rows.map(r =>
      '<tr><td>' + esc(r.user_name) + '</td>'
      + '<td>' + r.runs_today + '</td>'
      + '<td>' + Number(r.tokens_today).toLocaleString() + '</td>'
      + '<td>' + fmtUsd(r.cost_today) + '</td></tr>').join('') + '</table>';
}

async function refreshBudgets() {
  const rows = await fetch('/api/budgets').then(r => r.json());
  if (!rows.length) { $('budgets-body').innerHTML = '<div class="empty">No budget rows.</div>'; return; }
  $('budgets-body').innerHTML = '<table><tr>'
    + '<th>User pattern</th><th>Today</th><th>MTD</th>'
    + '</tr>' + rows.map(r => {
      const dayUsed = r.day_used_pct;
      const monthUsed = r.month_used_pct;
      const barCls = pct => pct == null ? '' : (pct >= 100 ? 'over' : (pct >= 80 ? 'high' : ''));
      const bar = (pct) => pct == null ? '<span class="muted">—</span>'
        : '<div class="bar"><div class="'+barCls(pct)+'" style="width:' + Math.min(100, pct) + '%"></div></div>'
          + '<span class="muted">'+pct+'%</span>';
      return '<tr><td>' + esc(r.user_pattern) + '</td>'
        + '<td>' + bar(dayUsed) + '</td>'
        + '<td>' + bar(monthUsed) + '</td></tr>';
    }).join('') + '</table>';
}

function startLogTail() {
  $('log-source').textContent = '— /var/log/cg-sl-agent/agent.log';
  const es = new EventSource('/api/logs/tail');
  const pane = $('log');
  es.onmessage = ev => {
    const line = ev.data;
    let cls = '';
    if (/\b(ERROR|Traceback|exception|failed)\b/i.test(line)) cls = 'err';
    else if (/\b(WARN|warning)\b/i.test(line)) cls = 'warn';
    const div = document.createElement('div');
    if (cls) div.className = cls;
    div.textContent = line;
    pane.appendChild(div);
    // Cap at ~500 lines so the browser doesn't grow forever
    while (pane.childNodes.length > 500) pane.removeChild(pane.firstChild);
    pane.scrollTop = pane.scrollHeight;
  };
  es.onerror = () => { /* reconnect attempted automatically */ };
}

async function refreshAll() {
  await Promise.allSettled([
    refreshAgentStatus(), refreshTiles(), refreshHealth(), refreshLive(),
    refreshRecent(), refreshUsers(), refreshBudgets()
  ]);
}
refreshAll(); startLogTail();
setInterval(refreshAll, 5000);
</script>
</body>
</html>
"""


# ─── CLI ─────────────────────────────────────────────────────────────────
def main() -> int:
    p = argparse.ArgumentParser(description="Operational monitor for cg-sl-agent.")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8766)
    p.add_argument("--log-file", default="/var/log/cg-sl-agent/agent.log",
                   help="Path to the agent log file for the live tail panel.")
    args = p.parse_args()

    Handler.log_path = Path(args.log_file)
    if not Handler.log_path.exists():
        print(f"WARN: log file {args.log_file} not found — log tail panel will say so",
              file=sys.stderr)

    print(f"\n  cg-sl-agent monitor → http://{args.host}:{args.port}", flush=True)
    print(f"  Log tail:             {Handler.log_path}", flush=True)
    print(f"  Tunnel from laptop:   ssh -L {args.port}:127.0.0.1:{args.port} ubuntu@<vm>\n",
          flush=True)

    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        srv.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
