#!/usr/bin/env python3
"""
analyst-agent-monitor — observability dashboard for cg-sl-agent.

Standalone web UI (no dependency on the agent's Python package). Reads the
agent's Postgres and tails its log file. Three views + a drill-down:

  • PULSE   — is everything okay right now? Ops tiles + trust tiles
              (feedback rate, raw-SQL fallback rate) + in-flight + budgets
              + live log tail.
  • RUNS    — every run, filterable by chips (errors / 👎 / raw-SQL / slow /
              surface / text search). Entry point for all debugging.
  • detail  — click a run → span waterfall, tool calls with verbatim SQL,
              conversation context, feedback. "Why did THIS happen?"
  • QUALITY — is the agent getting better? Feedback trend, raw-SQL fallback
              list (= the semantic-layer backlog), error taxonomy, slowest
              tools, token outliers.

Design rules (see README):
  - Provenance is DERIVED from telemetry (agent_queries), never from the
    model's self-reported source line.
  - Every aggregate links to the runs behind it (chips pre-filter RUNS).
  - Cost is COMPUTED from token counters × a price map — the DB stores no
    cost column, and we'd rather show a computed number than a fake one.

Run
---
    python serve.py                         # localhost:8766
    python serve.py --host 0.0.0.0          # only behind nginx + auth
    python serve.py --log-file /path.log    # custom agent log

Access from a laptop:  ssh -L 8766:127.0.0.1:8766 ubuntu@<vm-ip>
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

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
        print(f"ERROR: required env var {name} not set (see .env.example)", file=sys.stderr)
        sys.exit(2)
    return v or ""


# The agent API to health-probe for the status pill. A 200 from /health is a
# POSITIVE liveness signal ("I am alive and answering HTTP") — much stronger
# than inferring aliveness from log-file freshness.
AGENT_API_URL = _env("AGENT_API_URL", "http://127.0.0.1:8000").rstrip("/")

DB_HOST     = _env("DB_HOST", required=True)
DB_PORT     = _env("DB_PORT", "5432")
DB_USER     = _env("DB_USER", required=True)
DB_PASSWORD = _env("DB_PASSWORD", required=True)
DB_NAME     = _env("DB_NAME", required=True)
DB_SSLMODE  = _env("DB_SSLMODE", "prefer").strip()

# ─── Cost model ──────────────────────────────────────────────────────────
# The DB stores token counters, not dollars. USD per MILLION tokens.
# Default = claude-sonnet-4-5. Override via env if the agent model changes.
PRICE_IN        = float(_env("PRICE_INPUT_PER_M",       "3.0"))
PRICE_OUT       = float(_env("PRICE_OUTPUT_PER_M",      "15.0"))
PRICE_CACHE_W   = float(_env("PRICE_CACHE_WRITE_PER_M", "3.75"))
PRICE_CACHE_R   = float(_env("PRICE_CACHE_READ_PER_M",  "0.30"))

# SQL fragment computing a run's cost from agent_runs token columns.
COST_SQL = (
    f"(coalesce(input_tokens,0)*{PRICE_IN} + coalesce(output_tokens,0)*{PRICE_OUT}"
    f" + coalesce(cache_creation_input_tokens,0)*{PRICE_CACHE_W}"
    f" + coalesce(cache_read_input_tokens,0)*{PRICE_CACHE_R}) / 1e6"
)

# Feedback reactions → sentiment. Slack emoji names vary; be permissive.
_FB_UP   = "('+1','thumbsup','thumbs_up','raised_hands','heart','clap','tada')"
_FB_DOWN = "('-1','thumbsdown','thumbs_down','x','no_entry','confused')"


# ─── DB helpers ──────────────────────────────────────────────────────────
# A shared pool: opening a fresh TLS connection to the (remote) Postgres per
# query cost ~1.5-2s each — run-detail made 5 of them, Pulse 7, which is why
# every click took seconds. Pooled connections make a query a single
# round-trip (~100-300ms).
from psycopg.conninfo import make_conninfo
from psycopg_pool import ConnectionPool

_pool = ConnectionPool(
    make_conninfo(host=DB_HOST, port=int(DB_PORT), user=DB_USER,
                  password=DB_PASSWORD, dbname=DB_NAME, sslmode=DB_SSLMODE),
    min_size=1, max_size=8, kwargs={"autocommit": True}, open=True,
)


def _conn():
    """Back-compat handle for `make test` — returns a pooled connection."""
    return _pool.getconn()


def _rows(sql: str, params: tuple = ()) -> list[dict]:
    with _pool.connection() as c, c.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _parallel(**thunks) -> dict:
    """Run independent queries concurrently (each on its own pooled conn).

    A page that needs 7 queries pays one round-trip instead of seven.
    Failures come back as the exception object — callers decide the fallback.
    """
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=min(len(thunks), 8)) as ex:
        futs = {k: ex.submit(fn) for k, fn in thunks.items()}
        out = {}
        for k, f in futs.items():
            try:
                out[k] = f.result()
            except Exception as e:  # noqa: BLE001
                out[k] = e
        return out


def _jsonify(o: Any) -> Any:
    if isinstance(o, datetime):
        return o.replace(tzinfo=timezone.utc).isoformat() if o.tzinfo is None else o.isoformat()
    if isinstance(o, (list, tuple)):
        return [_jsonify(x) for x in o]
    if isinstance(o, dict):
        return {k: _jsonify(v) for k, v in o.items()}
    if hasattr(o, "isoformat"):
        return o.isoformat()
    return o


# ═══ PULSE ═══════════════════════════════════════════════════════════════
def fetch_pulse() -> dict:
    """Everything the front page needs in one payload."""
    # All 7 queries are independent — run them concurrently on pooled
    # connections so the page costs one round-trip, not seven.
    res = _parallel(
        m=lambda: _rows(f"""
            with windowed as (
                select * from agent_runs
                where ts > now() - interval '24 hours' and finished_at is not null
            )
            select count(*)                                                as total_runs,
                   sum(case when status='ok' or (status is null and error is null)
                            then 1 else 0 end)                             as ok_runs,
                   sum(case when status='error' or error is not null
                            then 1 else 0 end)                             as err_runs,
                   percentile_cont(0.5)  within group (order by duration_ms/1000.0) as p50_sec,
                   percentile_cont(0.95) within group (order by duration_ms/1000.0) as p95_sec,
                   sum(coalesce(input_tokens,0)+coalesce(output_tokens,0)) as total_tokens,
                   round(sum({COST_SQL})::numeric, 2)                      as total_cost_usd
            from windowed
        """),
        bq=lambda: _rows("""select coalesce(sum(bytes_billed),0) as b from agent_bq_usage
                            where ts > now() - interval '24 hours'"""),
        # Trust tile 1: 7-day feedback (reaction rows, action=added).
        fb=lambda: _rows(f"""
            select sum(case when reaction in {_FB_UP}   then 1 else 0 end) as ups,
                   sum(case when reaction in {_FB_DOWN} then 1 else 0 end) as downs
            from feedback
            where action = 'added' and ts > now() - interval '7 days'
        """),
        # Trust tile 2: raw-SQL share of data answers — DERIVED from
        # agent_queries; the model's own "source:" line lies sometimes.
        rs=lambda: _rows("""
            select count(distinct run_id) filter (where query_type = 'raw_sql')    as raw_runs,
                   count(distinct run_id) filter (where query_type is not null)     as data_runs
            from agent_queries
            where ts > now() - interval '7 days' and run_id is not null
        """),
        downs=lambda: _rows(f"""
            select f.ts, f.user_name, f.reaction,
                   left(coalesce(r.question, c.content, ''), 90) as question
            from feedback f
            left join agent_runs r
                   on r.thread_ts = f.thread_ts
                  and r.ts between f.ts - interval '30 minutes' and f.ts
            left join lateral (
                select content from conversations c
                where c.thread_ts = f.thread_ts and c.role = 'user'
                order by c.seq desc limit 1
            ) c on true
            where f.action = 'added' and f.reaction in {_FB_DOWN}
            order by f.ts desc limit 5
        """),
        inflight=lambda: _rows("""
            select id, surface, user_name, left(coalesce(question,''),100) as question,
                   extract(epoch from (now() - ts))::int as elapsed_sec
            from agent_runs
            where finished_at is null and ts > now() - interval '10 minutes'
            order by ts desc limit 20
        """),
        budgets=lambda: _rows("""
            with today_usage as (
                select user_id, max(user_name) as user_name,
                       sum(coalesce(input_tokens,0)+coalesce(output_tokens,0)) as t
                from agent_runs
                where ts >= current_date and user_id is not null
                group by user_id
            ),
            universe as (
                select user_id from today_usage
                union
                select user_id from user_budget_limits where user_id <> '*'
            )
            select un.user_id,
                   coalesce(tu.user_name, un.user_id)        as label,
                   coalesce(b.daily_token_limit,
                            (select daily_token_limit from user_budget_limits
                              where user_id = '*'))          as daily_token_limit,
                   (b.user_id is not null)                   as personal,
                   coalesce(tu.t, 0)                         as tokens_today,
                   case when coalesce(b.daily_token_limit,
                             (select daily_token_limit from user_budget_limits
                               where user_id = '*')) > 0
                        then round(100.0 * coalesce(tu.t,0)
                             / coalesce(b.daily_token_limit,
                               (select daily_token_limit from user_budget_limits
                                 where user_id = '*')), 1) end as pct
            from universe un
            left join user_budget_limits b on b.user_id = un.user_id
            left join today_usage tu      on tu.user_id = un.user_id
            order by pct desc nulls last
            limit 10
        """),
    )

    def ok(key):  # result or None if that query failed (optional table etc.)
        v = res[key]
        return None if isinstance(v, Exception) else v

    m = ok("m") or []
    out = dict(m[0]) if m else {}

    bq = ok("bq")
    out["bq_bytes_24h"] = int(bq[0]["b"] or 0) if bq else None

    fb = ok("fb")
    if fb:
        ups, downs = int(fb[0]["ups"] or 0), int(fb[0]["downs"] or 0)
        out["fb_ups_7d"], out["fb_downs_7d"] = ups, downs
        out["fb_down_pct_7d"] = round(100.0 * downs / (ups + downs), 1) if (ups + downs) else None
    else:
        out["fb_ups_7d"] = out["fb_downs_7d"] = out["fb_down_pct_7d"] = None

    rs = ok("rs")
    if rs:
        raw_n, data_n = int(rs[0]["raw_runs"] or 0), int(rs[0]["data_runs"] or 0)
        out["raw_sql_runs_7d"], out["data_runs_7d"] = raw_n, data_n
        out["raw_sql_pct_7d"] = round(100.0 * raw_n / data_n, 1) if data_n else None
    else:
        out["raw_sql_runs_7d"] = out["data_runs_7d"] = out["raw_sql_pct_7d"] = None

    out["recent_downs"] = ok("downs") or []
    out["in_flight"] = ok("inflight") or []
    out["budgets"] = ok("budgets") or []

    return _jsonify(out)


def fetch_agent_status(log_path: Path) -> dict:
    """API liveness by direct probe of GET {AGENT_API_URL}/health.

    A 200 means the API is up, full stop — no inference. On probe failure we
    fall back to the heartbeat table (if the agent ever opts in) as a weaker
    "process alive but HTTP broken?" hint. Last-run age is informational only.
    """
    import ssl
    import urllib.error
    import urllib.request

    # macOS framework Pythons ship no CA bundle → https probes fail with
    # CERTIFICATE_VERIFY_FAILED. Use certifi's bundle when available.
    try:
        import certifi
        ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        ssl_ctx = ssl.create_default_context()

    now = datetime.now(timezone.utc)
    st: dict[str, Any] = {"state": "down", "signal_used": "api-health",
                          "api_url": AGENT_API_URL, "api_latency_ms": None,
                          "api_error": None, "heartbeat_age_s": None,
                          "last_run_age_s": None}
    t0 = time.monotonic()
    try:
        req = urllib.request.Request(AGENT_API_URL + "/health",
                                     headers={"User-Agent": "analyst-agent-monitor"})
        with urllib.request.urlopen(req, timeout=3, context=ssl_ctx) as resp:
            st["api_latency_ms"] = int((time.monotonic() - t0) * 1000)
            if resp.status == 200:
                st["state"] = "up"
            else:
                st["api_error"] = f"HTTP {resp.status}"
    except urllib.error.HTTPError as e:
        st["api_latency_ms"] = int((time.monotonic() - t0) * 1000)
        st["api_error"] = f"HTTP {e.code}"
    except Exception as e:
        st["api_error"] = f"{type(e).__name__}: {e}"

    try:
        rows = _rows("select heartbeat_at from agent_heartbeats order by heartbeat_at desc limit 1")
        if rows:
            hb = rows[0]["heartbeat_at"]
            hb = hb if hb.tzinfo else hb.replace(tzinfo=timezone.utc)
            st["heartbeat_age_s"] = int((now - hb).total_seconds())
    except Exception:
        pass
    try:
        rows = _rows("select ts from agent_runs order by ts desc limit 1")
        if rows:
            lr = rows[0]["ts"]
            lr = lr if lr.tzinfo else lr.replace(tzinfo=timezone.utc)
            st["last_run_age_s"] = int((now - lr).total_seconds())
    except Exception:
        pass

    # Probe failed, but a fresh heartbeat says the process is alive → QUIET
    # (something between the process and HTTP is broken, or URL is wrong).
    if st["state"] == "down" and st["heartbeat_age_s"] is not None and st["heartbeat_age_s"] < 60:
        st["state"], st["signal_used"] = "quiet", "heartbeat"
    return _jsonify(st)


# ═══ RUNS (filterable list) ══════════════════════════════════════════════
def fetch_runs(q: dict) -> list[dict]:
    """Chip-filterable run list with derived quality badges."""
    where, params = ["1=1"], []
    f = (q.get("filter") or [""])[0]
    if f == "errors":
        where.append("(r.status = 'error' or r.error is not null)")
    elif f == "slow":
        where.append("r.duration_ms > 60000")
    elif f == "maxturns":
        where.append("r.max_turn_reached = true")
    if (q.get("surface") or [""])[0]:
        where.append("r.surface = %s"); params.append(q["surface"][0])
    if (q.get("user") or [""])[0]:
        where.append("r.user_name ilike %s"); params.append(f"%{q['user'][0]}%")
    if (q.get("q") or [""])[0]:
        where.append("r.question ilike %s"); params.append(f"%{q['q'][0]}%")

    # raw-SQL / feedback filters need the derived columns → filter after CTE.
    having = ""
    if f == "raw":
        having = "where raw_sql"
    elif f == "downs":
        having = "where fb_downs > 0"

    limit = min(int((q.get("limit") or ["100"])[0]), 500)
    params_all = tuple(params) + (limit,)
    return _rows(f"""
        with base as (
            select r.id, r.ts, r.finished_at, r.surface, r.user_name, r.status,
                   left(coalesce(r.question,''),110)            as question,
                   round((r.duration_ms/1000.0)::numeric,1)     as duration_sec,
                   coalesce(r.input_tokens,0)+coalesce(r.output_tokens,0) as total_tokens,
                   round(({COST_SQL})::numeric, 4)              as cost_usd,
                   r.max_turn_reached,
                   (r.error is not null or r.status='error')    as errored,
                   exists (select 1 from agent_queries aq
                           where aq.run_id = r.id and aq.query_type='raw_sql') as raw_sql,
                   (select count(*) from feedback fb
                     where fb.thread_ts = r.thread_ts and fb.action='added'
                       and fb.reaction in {_FB_UP})             as fb_ups,
                   (select count(*) from feedback fb
                     where fb.thread_ts = r.thread_ts and fb.action='added'
                       and fb.reaction in {_FB_DOWN})           as fb_downs
            from agent_runs r
            where {' and '.join(where)}
            order by r.ts desc
            limit 1000
        )
        select * from base {having} order by ts desc limit %s
    """, params_all)


# ═══ RUN DETAIL ══════════════════════════════════════════════════════════
def fetch_run_detail(run_id: int) -> dict:
    run = _rows(f"""
        select r.*, round(({COST_SQL})::numeric,4) as cost_usd,
               coalesce(r.input_tokens,0)+coalesce(r.output_tokens,0) as tokens_fresh
        from agent_runs r where r.id = %s
    """, (run_id,))
    if not run:
        return {"error": "run not found"}
    r = run[0]
    r.pop("final_answer_full", None)

    # The 4 sub-queries only need the run row (thread_ts) — run them together.
    thunks = {
        "spans": lambda: _rows("""
            select seq, span_type, span_name, started_at, duration_ms, status,
                   stop_reason, input_tokens, output_tokens, cache_read_input_tokens,
                   left(coalesce(input::text,''), 600)  as input_preview,
                   left(coalesce(output,''), 900)       as output_preview,
                   error
            from agent_spans where run_id = %s order by seq
        """, (run_id,)),
        "queries": lambda: _rows("""
            select tool_name, query_type, metric_name, rows_returned, bytes_billed,
                   duration_ms, status, error, left(coalesce(query_text,''), 2000) as query_text
            from agent_queries where run_id = %s order by id
        """, (run_id,)),
    }
    if r.get("thread_ts"):
        thunks["convo"] = lambda: _rows("""
            select seq, role, ts, left(content, 1500) as content
            from conversations where thread_ts = %s and channel = %s
            order by seq
        """, (r["thread_ts"], r.get("channel") or ""))
        thunks["fb"] = lambda: _rows("""
            select ts, user_name, reaction, action from feedback
            where thread_ts = %s order by ts
        """, (r["thread_ts"],))

    res = _parallel(**thunks)
    get = lambda k: [] if isinstance(res.get(k, []), Exception) else res.get(k, [])
    return _jsonify({"run": r, "spans": get("spans"), "queries": get("queries"),
                     "conversation": get("convo"), "feedback": get("fb")})


# ═══ QUALITY ═════════════════════════════════════════════════════════════
def fetch_quality() -> dict:
    res = _parallel(
        feedback_trend=lambda: _rows(f"""
            select date_trunc('week', ts)::date as week,
                   sum(case when reaction in {_FB_UP}   then 1 else 0 end) as ups,
                   sum(case when reaction in {_FB_DOWN} then 1 else 0 end) as downs
            from feedback where action='added' and ts > now() - interval '8 weeks'
            group by 1 order by 1
        """),
        # Every raw-SQL answer = a bug or a missing governed metric. This list
        # IS the semantic-layer backlog.
        fallbacks=lambda: _rows("""
            select q.ts, q.run_id, r.user_name,
                   left(coalesce(r.question,''),100) as question,
                   left(coalesce(q.query_text,''),400) as sql,
                   q.rows_returned, q.status
            from agent_queries q
            left join agent_runs r on r.id = q.run_id
            where q.query_type = 'raw_sql'
            order by q.ts desc limit 20
        """),
        error_taxonomy=lambda: _rows("""
            select left(coalesce(error,''),90) as error_head, count(*) as n, max(ts) as last_seen
            from agent_runs
            where error is not null and ts > now() - interval '30 days'
            group by 1 order by n desc limit 10
        """),
        slow_tools=lambda: _rows("""
            select span_name, count(*) as calls,
                   round((percentile_cont(0.5)  within group (order by duration_ms))::numeric)  as p50_ms,
                   round((percentile_cont(0.95) within group (order by duration_ms))::numeric)  as p95_ms
            from agent_spans
            where span_type='tool' and started_at > now() - interval '7 days'
            group by span_name order by p95_ms desc limit 12
        """),
        token_outliers=lambda: _rows("""
            select id, ts, user_name, left(coalesce(question,''),90) as question,
                   coalesce(input_tokens,0)+coalesce(output_tokens,0) as tokens,
                   turn_count, tool_calls_count
            from agent_runs
            where ts > now() - interval '7 days'
            order by coalesce(input_tokens,0)+coalesce(output_tokens,0) desc limit 10
        """),
        maxturns=lambda: _rows("""
            select count(*) as n from agent_runs
            where max_turn_reached = true and ts > now() - interval '7 days'
        """),
    )
    out: dict[str, Any] = {}
    for k in ("feedback_trend", "fallbacks", "error_taxonomy", "slow_tools", "token_outliers"):
        out[k] = [] if isinstance(res[k], Exception) else res[k]
    mt = res["maxturns"]
    out["maxturns"] = 0 if isinstance(mt, Exception) else mt[0]["n"]
    return _jsonify(out)


# ═══ Log tail (SSE) ══════════════════════════════════════════════════════
def stream_log(handler: BaseHTTPRequestHandler, log_path: Path) -> None:
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
        try:
            f.seek(0, 2); size = f.tell(); f.seek(max(0, size - 5000)); f.readline()
        except OSError:
            pass
        while True:
            line = f.readline()
            if line:
                handler.wfile.write(f"data: {line.rstrip()}\n\n".encode())
                try:
                    handler.wfile.flush()
                except BrokenPipeError:
                    return
            else:
                time.sleep(0.5)


# ═══ HTTP ════════════════════════════════════════════════════════════════
class Handler(BaseHTTPRequestHandler):
    log_path = Path("/var/log/cg-sl-agent/agent.log")

    def log_message(self, fmt, *args):
        return

    def _json(self, payload: Any, code: int = 200) -> None:
        body = json.dumps(_jsonify(payload), default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path, q = parsed.path, parse_qs(parsed.query)
        try:
            if path in ("/", "/index.html"):
                body = INDEX_HTML.encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                # The dashboard is a living page — never let a browser or an
                # intermediary proxy serve a stale copy of it.
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif path == "/api/pulse":
                self._json(fetch_pulse())
            elif path == "/api/agent_status":
                self._json(fetch_agent_status(self.log_path))
            elif path == "/api/runs":
                self._json(fetch_runs(q))
            elif re.fullmatch(r"/api/run/\d+", path):
                self._json(fetch_run_detail(int(path.rsplit("/", 1)[1])))
            elif path == "/api/quality":
                self._json(fetch_quality())
            elif path == "/api/logs/tail":
                stream_log(self, self.log_path)
            else:
                self.send_response(404); self.end_headers()
        except Exception as e:
            self._json({"error": f"{type(e).__name__}: {e}"}, code=500)


# ═══ Frontend (single file, no build) ════════════════════════════════════
INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>cg-sl-agent — monitor</title>
<style>
/* ── Design tokens ─────────────────────────────────────────────────────
   Dark-first, designed (not flipped). Mark colors #6478F0/#17A99A are the
   validated categorical pair (CVD ΔE 18.6 deutan on this surface); status
   colors are reserved for status and always ship with a text label. */
:root{
  --bg:#0B0E14; --panel:#10151D; --panel2:#161C26; --inset:#0C1017;
  --border:#1E2632; --border-2:#2A3442;
  --ink:#E9EDF4; --muted:#9AA3B2; --dim:#6B7686;
  --accent:#6478F0; --accent-soft:rgba(100,120,240,.12);
  --mark-llm:#6478F0; --mark-tool:#17A99A;
  --ok:#46C078;  --ok-soft:rgba(70,192,120,.12);
  --warn:#DBA43B;--warn-soft:rgba(219,164,59,.13);
  --err:#E5655C; --err-soft:rgba(229,101,92,.12);
  --info:var(--accent); --amber:var(--warn);
  --r:12px; --r-s:8px;
}
*{box-sizing:border-box}
html{scrollbar-color:var(--border-2) transparent}
::-webkit-scrollbar{width:10px;height:10px}
::-webkit-scrollbar-thumb{background:var(--border-2);border-radius:8px;border:2px solid var(--bg)}
::-webkit-scrollbar-track{background:transparent}
body{margin:0;background:var(--bg);color:var(--ink);
     font:13px/1.55 ui-sans-serif,-apple-system,"SF Pro Text","Segoe UI",Inter,Roboto,sans-serif;
     -webkit-font-smoothing:antialiased;text-rendering:optimizeLegibility}
button{font:inherit}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px;border-radius:6px}

/* ── Header ── */
header{display:flex;align-items:center;gap:16px;padding:0 20px;height:52px;
       border-bottom:1px solid var(--border);position:sticky;top:0;z-index:5;
       background:rgba(11,14,20,.85);backdrop-filter:blur(10px)}
h1{font-size:13.5px;font-weight:650;margin:0;letter-spacing:-.01em}
h1 .sub{color:var(--dim);font-weight:500;margin-left:2px}
nav{display:flex;gap:2px;margin-left:6px;padding:3px;background:var(--inset);
    border:1px solid var(--border);border-radius:10px}
nav button{background:none;border:0;color:var(--dim);padding:5px 14px;border-radius:7px;
           cursor:pointer;font-size:12.5px;font-weight:550;transition:color .15s,background .15s}
nav button:hover{color:var(--ink)}
nav button.active{background:var(--panel2);color:var(--ink);box-shadow:0 1px 2px rgba(0,0,0,.45)}
.pill{display:inline-flex;align-items:center;gap:7px;padding:4px 12px;border-radius:999px;
      font-size:11.5px;font-weight:650;letter-spacing:.01em;
      border:1px solid var(--border);background:var(--panel);color:var(--dim);
      font-variant-numeric:tabular-nums;transition:color .2s,border-color .2s}
.pill .dot{width:7px;height:7px;border-radius:50%;background:var(--dim)}
.pill.up{border-color:rgba(70,192,120,.35);color:var(--ok);background:var(--ok-soft)}
.pill.up .dot{background:var(--ok);box-shadow:0 0 0 3px rgba(70,192,120,.15)}
.pill.quiet{border-color:rgba(219,164,59,.35);color:var(--warn);background:var(--warn-soft)}
.pill.quiet .dot{background:var(--warn)}
.pill.down{border-color:rgba(229,101,92,.4);color:var(--err);background:var(--err-soft)}
.pill.down .dot{background:var(--err);animation:pulse 1.4s ease-in-out infinite}
@keyframes pulse{0%,100%{box-shadow:0 0 0 0 rgba(229,101,92,.45)}50%{box-shadow:0 0 0 5px rgba(229,101,92,0)}}
.hint{margin-left:auto;color:var(--dim);font-size:11.5px}

/* ── Layout ── */
main{padding:18px 24px 32px;max-width:1480px;margin:0 auto}
.cols{display:grid;grid-template-columns:1fr 1fr;gap:14px;align-items:start}
@media(max-width:1000px){.cols{grid-template-columns:1fr}}

/* ── Stat tiles ── */
/* min 144px lets all 9 tiles sit on ONE row inside the 1480 container —
   158px left an orphan tile wrapping alone on wide screens. */
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(144px,1fr));gap:12px;margin-bottom:16px}
.tile{background:var(--panel);border:1px solid var(--border);border-radius:var(--r);
      padding:13px 16px 12px;box-shadow:inset 0 1px 0 rgba(255,255,255,.025);
      transition:border-color .15s,transform .15s}
.tile .k{font-size:10.5px;letter-spacing:.08em;text-transform:uppercase;color:var(--dim);font-weight:600}
.tile .v{font-size:25px;font-weight:700;margin-top:4px;letter-spacing:-.02em;
         font-variant-numeric:tabular-nums;line-height:1.15}
.tile .s{font-size:11.5px;color:var(--dim);margin-top:3px;font-variant-numeric:tabular-nums}
.tile.link{cursor:pointer}
.tile.link:hover{border-color:var(--accent);transform:translateY(-1px)}
.tile.link .k::after{content:"↗";float:right;color:var(--accent);opacity:0;transition:opacity .15s}
.tile.link:hover .k::after{opacity:.9}
.tile.bad .v{color:var(--err)} .tile.warn .v{color:var(--warn)} .tile.good .v{color:var(--ok)}

/* ── Panels ── */
.panel{background:var(--panel);border:1px solid var(--border);border-radius:var(--r);
       margin-bottom:14px;overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.02)}
/* Panel titles must be findable at a glance — brighter and larger than the
   column headers inside them (th stays dim so the hierarchy holds). */
.panel h2{font-size:12px;letter-spacing:.06em;text-transform:uppercase;color:#CBD4E0;
          font-weight:650;margin:0;padding:12px 16px 10px;border-bottom:1px solid var(--border)}
/* Dim everything after the "—" — the annotation shouldn't shout like the title */
.panel h2 small{font-size:10.5px;color:var(--dim);font-weight:550;letter-spacing:.07em}
.panel .bd{padding:10px 16px 12px}

/* ── Tables ── */
table{width:100%;border-collapse:collapse;font-size:12.5px}
th{color:var(--dim);text-align:left;font-weight:600;font-size:10.5px;text-transform:uppercase;
   letter-spacing:.06em;padding:6px 10px;border-bottom:1px solid var(--border-2)}
td{padding:7px 10px;border-bottom:1px solid var(--border);vertical-align:top}
tr:last-child td{border-bottom:0}
tr.click{cursor:pointer}
tr.click:hover td{background:var(--accent-soft)}
.muted{color:var(--dim)} .num{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}

/* ── Badges (status → always tinted bg + text label, never color alone) ── */
.b{display:inline-block;padding:2px 8px;border-radius:6px;font-size:10px;font-weight:650;
   letter-spacing:.02em;margin-right:4px;font-variant-numeric:tabular-nums}
.b.ok{background:var(--ok-soft);color:var(--ok)}
.b.err{background:var(--err-soft);color:var(--err)}
.b.raw{background:var(--warn-soft);color:var(--warn)}
.b.down{background:var(--err-soft);color:var(--err)}
.b.up2{background:var(--ok-soft);color:var(--ok)}
.b.mx{background:var(--accent-soft);color:var(--accent)}

/* ── Filter chips ── */
.chips{display:flex;gap:7px;flex-wrap:wrap;align-items:center;padding:12px 16px}
.chip{border:1px solid var(--border);background:var(--inset);color:var(--muted);border-radius:999px;
      padding:5px 14px;font-size:12px;font-weight:550;cursor:pointer;
      transition:border-color .15s,color .15s,background .15s}
.chip:hover{border-color:var(--border-2);color:var(--ink)}
.chip.active{border-color:transparent;color:#fff;background:var(--accent)}
.chips input{background:var(--inset);border:1px solid var(--border);color:var(--ink);border-radius:8px;
             padding:5px 12px;font:inherit;font-size:12px;width:230px;transition:border-color .15s}
.chips input:focus{border-color:var(--accent);outline:none}

/* ── Log tail ── */
.log{font:11px/1.6 ui-monospace,"SF Mono",Menlo,monospace;max-height:280px;overflow-y:auto;
     padding:10px 16px;white-space:pre-wrap;background:var(--inset)}
.log .e{color:var(--err)} .log .w{color:var(--warn)}

/* ── Drawer (run detail) ── */
#detail{position:fixed;inset:0;background:rgba(5,8,13,.62);backdrop-filter:blur(3px);
        display:none;z-index:20}
#detail .sheet{position:absolute;right:0;top:0;bottom:0;width:min(900px,95vw);
               background:#0C1017;border-left:1px solid var(--border-2);
               overflow-y:auto;padding:20px 24px;animation:slidein .18s ease-out}
@keyframes slidein{from{transform:translateX(16px);opacity:.4}to{transform:none;opacity:1}}
.x{position:sticky;top:0;float:right;background:var(--panel2);border:1px solid var(--border);
   color:var(--ink);border-radius:8px;padding:5px 14px;cursor:pointer;font-size:12px;
   transition:border-color .15s}
.x:hover{border-color:var(--accent)}

/* ── Span waterfall — marks: validated llm/tool pair, 1px surface ring ── */
.wf{margin:2px 0}
.wf .row{display:flex;align-items:center;gap:10px;margin:4px 0}
.wf .lbl{width:220px;flex-shrink:0;font-size:12px;color:var(--muted);
         white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.wf .track{flex:1;height:14px;background:var(--inset);border-radius:4px;position:relative}
.wf .bar{position:absolute;height:100%;border-radius:3px;background:var(--mark-llm);
         min-width:2px;box-shadow:0 0 0 1px var(--panel)}
.wf .bar.tool{background:var(--mark-tool)} .wf .bar.err{background:var(--err)}
.wf .ms{width:74px;text-align:right;font-size:11px;color:var(--dim);flex-shrink:0;
        font-variant-numeric:tabular-nums}

/* ── Code blocks / conversation ── */
pre{background:var(--inset);border:1px solid var(--border);border-radius:var(--r-s);
    padding:10px 12px;font:11px/1.6 ui-monospace,"SF Mono",Menlo,monospace;
    white-space:pre-wrap;word-break:break-word;max-height:220px;overflow:auto}
.convo .m{margin:8px 0;padding:8px 12px;border-radius:10px;max-width:92%;font-size:12.5px}
.convo .u{background:var(--accent-soft);margin-left:auto}
.convo .a{background:var(--panel2)}

/* ── Feedback week columns (status semantics: 👍 good / 👎 bad) ── */
.spark{display:flex;align-items:flex-end;gap:6px;height:64px;padding:8px 4px 4px;
       border-bottom:1px solid var(--border)}
.spark .col{flex:1;display:flex;flex-direction:column;justify-content:flex-end;gap:2px}
.spark .u3{background:var(--ok);border-radius:3px 3px 0 0}
.spark .d3{background:var(--err);border-radius:0 0 3px 3px}
.spark .wk{font-size:10px;color:var(--dim);text-align:center;margin-top:4px}

@media (prefers-reduced-motion:reduce){
  *,*::before,*::after{animation:none!important;transition:none!important}
}
</style>
</head>
<body>
<header>
  <h1>cg-sl-agent <span class="sub">· monitor</span></h1>
  <span id="status" class="pill unknown"><span class="dot"></span>checking…</span>
  <nav>
    <button data-v="pulse" class="active">Pulse</button>
    <button data-v="runs">Runs</button>
    <button data-v="quality">Quality</button>
  </nav>
  <span class="hint">refresh 5s · click any tile or row to drill in</span>
</header>
<main>
  <section id="v-pulse"></section>
  <section id="v-runs" style="display:none"></section>
  <section id="v-quality" style="display:none"></section>
</main>
<div id="detail"><div class="sheet" id="sheet"></div></div>

<script>
const $ = s => document.querySelector(s);
const esc = s => (s??'').toString().replace(/[&<>"]/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const fmtN = v => v==null?'—':Number(v).toLocaleString();
const fmtUsd = v => v==null?'—':'$'+Number(v).toFixed(Number(v)<1?4:2);
const fmtGB = b => b==null?'—':(b/1e9).toFixed(2)+' GB';
const fmtS = v => v==null?'—':Number(v).toFixed(1)+'s';
const ago = iso => { if(!iso) return '—'; const s=(Date.now()-new Date(iso))/1000;
  if(s<60)return s.toFixed(0)+'s ago'; if(s<3600)return (s/60).toFixed(0)+'m ago';
  if(s<86400)return (s/3600).toFixed(1)+'h ago'; return (s/86400).toFixed(1)+'d ago'; };
const J = async p => { const r = await fetch(p); return r.json(); };

let view='pulse', runsFilter='', runsQ='';
document.querySelectorAll('nav button').forEach(b=>b.onclick=()=>{
  view=b.dataset.v;
  document.querySelectorAll('nav button').forEach(x=>x.classList.toggle('active',x===b));
  ['pulse','runs','quality'].forEach(v=>$('#v-'+v).style.display=v===view?'':'none');
  refresh(true);
});

// tiles that jump to a pre-filtered Runs view — "every aggregate links to evidence"
function gotoRuns(filter){ runsFilter=filter; view='runs';
  document.querySelectorAll('nav button').forEach(x=>x.classList.toggle('active',x.dataset.v==='runs'));
  ['pulse','runs','quality'].forEach(v=>$('#v-'+v).style.display=v==='runs'?'':'none');
  refresh(true); }

/* ── PULSE ── */
async function renderPulse(){
  const [m, st] = await Promise.all([J('/api/pulse'), J('/api/agent_status')]);
  const pill=$('#status'); pill.className='pill '+st.state;
  pill.title = st.api_url + (st.api_error ? ' — ' + st.api_error : '');
  pill.innerHTML='<span class="dot"></span>'+
    (st.state==='up' ? 'API UP · '+st.api_latency_ms+'ms'
     : st.state==='quiet' ? 'API UNREACHABLE · process alive'
     : 'API DOWN');
  const okPct = m.total_runs? (100*m.ok_runs/m.total_runs).toFixed(1) : null;
  const fbCls = m.fb_down_pct_7d==null?'':(m.fb_down_pct_7d>15?'bad':m.fb_down_pct_7d>5?'warn':'good');
  const rawCls = m.raw_sql_pct_7d==null?'':(m.raw_sql_pct_7d>10?'warn':'good');
  $('#v-pulse').innerHTML = `
  <div class="tiles">
    <div class="tile link" onclick="gotoRuns('')"><div class="k">questions 24h</div>
      <div class="v">${fmtN(m.total_runs)}</div><div class="s">${fmtN(m.total_tokens)} tokens</div></div>
    <div class="tile link ${m.err_runs>0?'warn':''}" onclick="gotoRuns('errors')"><div class="k">success 24h</div>
      <div class="v">${okPct==null?'—':okPct+'%'}</div><div class="s">${fmtN(m.err_runs)} errored</div></div>
    <div class="tile link" onclick="gotoRuns('slow')"><div class="k">latency p50 / p95</div>
      <div class="v">${fmtS(m.p50_sec)}</div><div class="s">p95 ${fmtS(m.p95_sec)}</div></div>
    <div class="tile"><div class="k">LLM cost 24h</div>
      <div class="v">${fmtUsd(m.total_cost_usd)}</div><div class="s">computed from tokens</div></div>
    <div class="tile"><div class="k">BQ scanned 24h</div>
      <div class="v">${fmtGB(m.bq_bytes_24h)}</div><div class="s">bytes billed</div></div>
    <div class="tile link ${fbCls}" onclick="gotoRuns('downs')"><div class="k">👎 rate 7d</div>
      <div class="v">${m.fb_down_pct_7d==null?'—':m.fb_down_pct_7d+'%'}</div>
      <div class="s">${fmtN(m.fb_ups_7d)} 👍 · ${fmtN(m.fb_downs_7d)} 👎</div></div>
    <div class="tile link ${rawCls}" onclick="gotoRuns('raw')"><div class="k">raw-SQL answers 7d</div>
      <div class="v">${m.raw_sql_pct_7d==null?'—':m.raw_sql_pct_7d+'%'}</div>
      <div class="s">${fmtN(m.raw_sql_runs_7d)} of ${fmtN(m.data_runs_7d)} data answers</div></div>
    <div class="tile"><div class="k">in-flight now</div>
      <div class="v">${m.in_flight.length}</div><div class="s">started &lt; 10 min ago</div></div>
  </div>
  <div class="cols">
    <div>
      <div class="panel"><h2>Live (in-flight)</h2><div class="bd">
        ${m.in_flight.length? '<table>'+m.in_flight.map(r=>
          `<tr class="click" onclick="openRun(${r.id})"><td>${esc(r.user_name||'—')}</td>
           <td>${esc(r.question)}</td><td class="num">${r.elapsed_sec}s</td></tr>`).join('')+'</table>'
        : '<div class="muted" style="text-align:center;padding:14px">No active requests.</div>'}
      </div></div>
      <div class="panel"><h2>Latest 👎 (7d)</h2><div class="bd">
        ${m.recent_downs.length? '<table>'+m.recent_downs.map(d=>
          `<tr><td class="muted">${ago(d.ts)}</td><td>${esc(d.user_name||'—')}</td>
           <td>${esc(d.question||'(question unknown)')}</td></tr>`).join('')+'</table>'
        : '<div class="muted" style="text-align:center;padding:14px">No thumbs-down this week 🎉</div>'}
      </div></div>
    </div>
    <div>
      <div class="panel"><h2>Live log tail</h2><div class="log" id="logbox">(connecting…)</div></div>
      <div class="panel"><h2>Budget headroom (today) <small>— everyone active, vs their effective cap</small></h2><div class="bd">
        ${m.budgets.length? '<table>'+m.budgets.map(b=>{
          const pct=b.pct==null?0:Math.min(Number(b.pct),100);
          const col=pct>=100?'var(--err)':pct>=80?'var(--warn)':'var(--info)';
          const cap=b.daily_token_limit==null?'no cap':(Number(b.daily_token_limit)/1e6)+'M';
          return `<tr><td style="width:30%;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:0" title="${esc(b.user_id)}">${esc(b.label)}
              ${b.personal?'':'<span class="muted" style="font-size:10.5px"> (default)</span>'}</td>
            <td style="width:44%"><div style="background:var(--inset);border-radius:3px;height:6px;min-width:120px">
              <div style="width:${pct}%;background:${col};height:100%;border-radius:3px;min-width:2px"></div></div></td>
            <td class="num" style="width:52px">${b.pct==null?'—':b.pct+'%'}</td>
            <td class="num muted" style="white-space:nowrap">${fmtN(b.tokens_today)} / ${cap}</td></tr>`;}).join('')+'</table>'
        : '<div class="muted" style="padding:10px">No budget rows.</div>'}
      </div></div>
    </div>
  </div>`;
  hookLog();
}

let logSrc=null;
function hookLog(){
  if(logSrc) return;
  const box=$('#logbox'); if(!box) return;
  box.textContent='';
  logSrc=new EventSource('/api/logs/tail');
  logSrc.onmessage=e=>{
    const box=$('#logbox'); if(!box) return;
    const d=document.createElement('div');
    if(/\b(ERROR|Traceback|failed)\b/i.test(e.data)) d.className='e';
    else if(/\bWARN/i.test(e.data)) d.className='w';
    d.textContent=e.data; box.appendChild(d);
    while(box.children.length>400) box.removeChild(box.firstChild);
    box.scrollTop=box.scrollHeight;
  };
}

/* ── RUNS ── */
async function renderRuns(){
  const chips=[['','all'],['errors','errors'],['downs','👎'],['raw','raw-SQL'],['slow','slow >60s'],['maxturns','max-turns']];
  const rows=await J('/api/runs?filter='+runsFilter+'&q='+encodeURIComponent(runsQ));
  $('#v-runs').innerHTML=`
  <div class="panel">
    <div class="chips">
      ${chips.map(([f,l])=>`<button class="chip ${runsFilter===f?'active':''}" onclick="runsFilter='${f}';refresh(true)">${l}</button>`).join('')}
      <input placeholder="search question… (enter)" value="${esc(runsQ)}"
             onkeydown="if(event.key==='Enter'){runsQ=this.value;refresh(true)}">
    </div>
    <table>
      <tr><th>when</th><th>user</th><th>question</th><th>badges</th>
          <th class="num">dur</th><th class="num">tokens</th><th class="num">cost</th></tr>
      ${rows.map(r=>`
        <tr class="click" onclick="openRun(${r.id})">
          <td class="muted" style="white-space:nowrap">${ago(r.ts)}</td>
          <td>${esc(r.user_name||'—')} <span class="muted">${r.surface||''}</span></td>
          <td>${esc(r.question)}</td>
          <td>
            ${r.errored?'<span class="b err">error</span>':'<span class="b ok">ok</span>'}
            ${r.raw_sql?'<span class="b raw">raw-SQL</span>':''}
            ${r.fb_downs>0?`<span class="b down">👎${r.fb_downs}</span>`:''}
            ${r.fb_ups>0?`<span class="b up2">👍${r.fb_ups}</span>`:''}
            ${r.max_turn_reached?'<span class="b mx">max-turns</span>':''}
          </td>
          <td class="num">${fmtS(r.duration_sec)}</td>
          <td class="num">${fmtN(r.total_tokens)}</td>
          <td class="num">${fmtUsd(r.cost_usd)}</td>
        </tr>`).join('')}
    </table>
    ${rows.length?'':'<div class="muted" style="text-align:center;padding:18px">No runs match.</div>'}
  </div>`;
}

/* ── RUN DETAIL (drawer) ── */
async function openRun(id){
  // Open instantly with a loading state — perceived speed matters as much as
  // real speed; the data fills in when the fetch lands.
  $('#sheet').innerHTML='<button class="x" onclick="$(\'#detail\').style.display=\'none\'">✕ close</button>'
    +'<h2 style="margin:4px 0">Run #'+id+'</h2><p class="muted">Loading…</p>';
  $('#detail').style.display='block';
  const d=await J('/api/run/'+id); const r=d.run||{};
  const total=Math.max(r.duration_ms||1,1);
  let t0=null; if(d.spans.length) t0=new Date(d.spans[0].started_at).getTime();
  $('#sheet').innerHTML=`
    <button class="x" onclick="$('#detail').style.display='none'">✕ close</button>
    <h2 style="margin:4px 0 2px">Run #${id} <span class="muted">· ${esc(r.user_name||'—')} · ${r.surface||''} · ${ago(r.ts)}</span></h2>
    <p style="margin:6px 0 10px"><b>${esc(r.question||'')}</b></p>
    <p class="muted" style="margin:0 0 12px">
      ${fmtS(r.duration_ms/1000)} · ${fmtN((r.input_tokens||0)+(r.output_tokens||0))} tokens
      (cache read ${fmtN(r.cache_read_input_tokens)}) · ${fmtUsd(r.cost_usd)} ·
      ${r.turn_count||0} turns · ${r.tool_calls_count||0} tool calls
      ${r.error?`<br><span style="color:var(--err)">error: ${esc(r.error)}</span>`:''}
    </p>
    <div class="panel"><h2>Span waterfall</h2><div class="bd wf">
      ${d.spans.map(s=>{
        const off=t0?Math.max(0,(new Date(s.started_at).getTime()-t0))/total*100:0;
        const w=Math.min(100-off,(s.duration_ms||0)/total*100);
        const cls=s.status!=='ok'?'err':(s.span_type==='tool'?'tool':'');
        return `<div class="row"><div class="lbl">${s.seq}. ${esc(s.span_name)}${s.stop_reason?' <span class=muted>('+s.stop_reason+')</span>':''}</div>
          <div class="track"><div class="bar ${cls}" style="left:${off}%;width:${w}%"></div></div>
          <div class="ms">${fmtN(s.duration_ms)} ms</div></div>`;}).join('')}
    </div></div>
    ${d.queries.length?`<div class="panel"><h2>Data queries (verbatim)</h2><div class="bd">
      ${d.queries.map(q=>`
        <p style="margin:6px 0 4px">
          <span class="b ${q.query_type==='raw_sql'?'raw':'ok'}">${esc(q.query_type||q.tool_name)}</span>
          ${q.metric_name?esc(q.metric_name)+' · ':''}${q.rows_returned!=null?q.rows_returned+' rows · ':''}
          ${fmtN(q.duration_ms)} ms ${q.error?`· <span style="color:var(--err)">${esc(q.error)}</span>`:''}
        </p><pre>${esc(q.query_text)}</pre>`).join('')}
    </div></div>`:''}
    <div class="cols">
      <div class="panel"><h2>Conversation</h2><div class="bd convo">
        ${d.conversation.map(c=>`<div class="m ${c.role==='user'?'u':'a'}"><span class="muted">${c.role}</span><br>${esc(c.content)}</div>`).join('')||'<span class="muted">no thread</span>'}
      </div></div>
      <div class="panel"><h2>Feedback on this thread</h2><div class="bd">
        ${d.feedback.length?'<table>'+d.feedback.map(f=>
          `<tr><td>${ago(f.ts)}</td><td>${esc(f.user_name||'—')}</td><td>:${esc(f.reaction)}: ${f.action}</td></tr>`).join('')+'</table>'
        :'<div class="muted" style="padding:8px">none</div>'}
      </div></div>
    </div>`;
  $('#detail').style.display='block';
}
$('#detail').onclick=e=>{ if(e.target.id==='detail') $('#detail').style.display='none'; };

/* ── QUALITY ── */
async function renderQuality(){
  const d=await J('/api/quality');
  const maxFb=Math.max(1,...d.feedback_trend.map(w=>Number(w.ups)+Number(w.downs)));
  $('#v-quality').innerHTML=`
  <div class="cols">
    <div>
      <div class="panel"><h2>Feedback per week (8w) <small>— 👍 up · 👎 down</small></h2><div class="bd">
        ${d.feedback_trend.length?`<div class="spark">${d.feedback_trend.map(w=>{
          const u=Number(w.ups),dn=Number(w.downs);
          return `<div class="col" title="${w.week}: ${u}👍 ${dn}👎">
            <div class="u3" style="height:${u/maxFb*46}px"></div>
            <div class="d3" style="height:${dn/maxFb*46}px"></div>
            <div class="wk">${w.week.slice(5)}</div></div>`;}).join('')}</div>`
        :'<div class="muted" style="padding:12px">No feedback yet — nudge users to 👍/👎 answers; it\'s the accuracy KPI.</div>'}
      </div></div>
      <div class="panel"><h2>Error taxonomy (30d)</h2><div class="bd">
        ${d.error_taxonomy.length?'<table>'+d.error_taxonomy.map(e=>
          `<tr><td class="num" style="width:36px">${e.n}×</td><td>${esc(e.error_head)}</td>
           <td class="muted num">${ago(e.last_seen)}</td></tr>`).join('')+'</table>'
        :'<div class="muted" style="padding:10px">No errors in 30 days.</div>'}
        <p class="muted" style="margin:6px 2px">max-turns hit ${d.maxturns}× this week</p>
      </div></div>
      <div class="panel"><h2>Slowest tools p95 (7d)</h2><div class="bd">
        <table><tr><th>tool</th><th class="num">calls</th><th class="num">p50</th><th class="num">p95</th></tr>
        ${d.slow_tools.map(t=>`<tr><td>${esc(t.span_name)}</td><td class="num">${t.calls}</td>
          <td class="num">${fmtN(t.p50_ms)} ms</td><td class="num">${fmtN(t.p95_ms)} ms</td></tr>`).join('')}
        </table>
      </div></div>
      <div class="panel"><h2>Token outliers (7d) <small>— context bloat detector</small></h2><div class="bd">
        <table><tr><th>question</th><th class="num">tokens</th><th class="num">turns</th><th class="num">tools</th></tr>
        ${d.token_outliers.map(o=>`<tr class="click" onclick="openRun(${o.id})">
          <td>${esc(o.question)}</td><td class="num">${fmtN(o.tokens)}</td>
          <td class="num">${o.turn_count}</td><td class="num">${o.tool_calls_count}</td></tr>`).join('')}
        </table>
      </div></div>
    </div>
    <div>
      <div class="panel"><h2>Raw-SQL fallbacks <small>— each = missing metric or bug</small></h2><div class="bd">
        ${d.fallbacks.length? d.fallbacks.map(f=>`
          <p style="margin:8px 0 3px"><span class="muted">${ago(f.ts)} · ${esc(f.user_name||'—')}</span><br>
          <b class="click" style="cursor:pointer" onclick="openRun(${f.run_id})">${esc(f.question||'(no question)')}</b></p>
          <pre>${esc(f.sql)}</pre>`).join('')
        :'<div class="muted" style="padding:12px">None — every answer used governed metrics 🎉</div>'}
      </div></div>
    </div>
  </div>`;
}

async function refresh(force){
  try{
    if(view==='pulse') await renderPulse();
    else if(view==='runs') await renderRuns();
    else await renderQuality();
  }catch(e){ console.error(e); }
}
refresh(true);
setInterval(()=>{ if(view==='pulse') refresh(); }, 5000);
</script>
</body>
</html>
"""


# ─── Entry point ─────────────────────────────────────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8766)
    ap.add_argument("--log-file", default="/var/log/cg-sl-agent/agent.log")
    args = ap.parse_args()
    Handler.log_path = Path(args.log_file)
    if not Handler.log_path.exists():
        print(f"WARN: log file {args.log_file} not found — log tail panel will say so")
    print(f"\n  cg-sl-agent monitor → http://{args.host}:{args.port}")
    print(f"  Views: Pulse · Runs (click a row for the waterfall) · Quality")
    print(f"  Tunnel from laptop:   ssh -L {args.port}:127.0.0.1:{args.port} ubuntu@<vm>\n")
    try:
        ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
