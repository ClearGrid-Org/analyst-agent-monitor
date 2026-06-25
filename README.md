# sl-agent-monitor

Operational monitoring dashboard for [`cg-sl-agent`](https://github.com/ClearGrid-Org/analyst-agent) — single-page web UI showing in-flight requests, recent runs, 24-hour metrics, per-user budget headroom, and a live log tail.

Standalone repo: no dependency on the agent's Python package. The monitor reads from the agent's Postgres (read-only is enough) and tails its log file. It can run on the same VM as the agent or on a separate one.

## What you see

- **Health pill** in the header — green / amber / red based on the last hour's error rate. Shows when the last error happened.
- **24-hour tiles** — question count, success rate, p50 / p95 latency, total LLM cost (USD), BigQuery bytes billed, in-flight count right now.
- **Live (in-flight)** panel — every request that's started but hasn't finished, with an elapsed-seconds timer.
- **Last 30 completed** — compact list with status pill, duration, cost, and time-ago. Errored rows are highlighted red.
- **Top users today** — distinct users by request count, tokens, and cost. Catches sudden hammering by one user.
- **Budget headroom** — per-user-pattern progress bars showing today's and MTD usage vs the limits in `user_budget_limits`. Blue → amber at ≥80% → red at ≥100%.
- **Live log tail** — Server-Sent Events stream of `/var/log/cg-sl-agent/agent.log`. ERROR lines red, WARN amber. Capped at 500 DOM rows.

Refreshes every 5 seconds. Stdlib-only on the server side (`http.server` + `psycopg`); no React, no build step.

## Schema dependency

Reads from the agent's Postgres tables:

| Table | Used for | Required? |
|---|---|---|
| `agent_runs` | All panels except budget bars and log tail | Yes |
| `agent_bq_usage` | BigQuery bytes-billed tile | Optional |
| `user_budget_limits` | Budget headroom panel | Optional |
| `agent_heartbeats` | Most reliable agent UP/DOWN signal | Optional (see below) |

If an optional table is missing the corresponding panel renders empty rather than erroring out the whole page.

## Agent UP / DOWN detection

The header pill shows one of four states:

| State | Meaning | How it's decided |
|---|---|---|
| 🟢 **AGENT UP** | Bot is alive and processing | `agent_heartbeats` < 60s old (preferred), OR log file written < 2 min ago |
| 🟡 **AGENT QUIET** | Possibly alive but idle | Log file written 2-10 min ago, no heartbeat |
| 🔴 **AGENT DOWN** | Bot is probably not running | Log + DB both stale >10 min — pill pulses red |
| ⚪ **UNKNOWN** | No signal available yet | Brand new install, no log file, no DB rows |

The heartbeat table is the most reliable signal. Without it, the monitor falls back to log-file mtime (good but can be fooled by a hung process holding the log open). To enable the heartbeat — one-line opt-in in the agent's startup:

```python
# In the agent's main entry point (e.g., cg_sl_agent/__main__.py)
from cg_sl_agent.store.heartbeat import start_heartbeat_thread
start_heartbeat_thread(agent_id="cg-sl-agent")
```

The helper auto-creates the table, runs a 30-second daemon thread that does an UPSERT into `agent_heartbeats`. If the agent process dies, no more writes land, and the monitor's pill flips to DOWN within ~60 seconds (one missed heartbeat + one refresh cycle).

## Quick start

```bash
git clone <repo-url> /opt/sl-agent-monitor
cd /opt/sl-agent-monitor

python3 -m venv .venv
source .venv/bin/activate
make install

cp .env.example .env
nano .env                        # fill in the 5 DB vars

make run                         # → http://127.0.0.1:8766
```

From a laptop:

```bash
ssh -L 8766:127.0.0.1:8766 ubuntu@<vm-ip>
# open http://127.0.0.1:8766 in your browser
```

## Configuration

All configuration is via env vars (loaded from `.env` next to `serve.py` if present). See `.env.example` for the full list.

| Var | Required | Default | Notes |
|---|---|---|---|
| `DB_HOST` | yes | — | Postgres host |
| `DB_PORT` | no | `5432` | |
| `DB_USER` | yes | — | Read-only access is enough |
| `DB_PASSWORD` | yes | — | URL-special chars are handled |
| `DB_NAME` | yes | — | Agent's database |
| `DB_SSLMODE` | no | `prefer` | `require` for managed Postgres; `disable` when going through Cloud SQL Auth Proxy |

Runtime flags:

| Flag | Default | Notes |
|---|---|---|
| `--host` | `127.0.0.1` | Use `0.0.0.0` only behind nginx + auth |
| `--port` | `8766` | |
| `--log-file` | `/var/log/cg-sl-agent/agent.log` | The file the live tail panel streams |

## Run as a service

```bash
# Adjust paths in deploy/sl-agent-monitor.service first
# (WorkingDirectory, EnvironmentFile, ExecStart)
make install-service
sudo journalctl -u sl-agent-monitor -f
```

To remove:

```bash
make uninstall-service
```

## Side by side with `serve_traces`

The agent ships its own trace viewer at port `8765`. They answer different questions:

| | `serve_traces` (in agent repo) | `serve.py` (this repo) |
|---|---|---|
| Question it answers | "What happened on THIS specific run?" | "What's happening across the bot right now?" |
| Drill-down | Full span tree per run | Compact 30-row recent list |
| Real-time | Recent list refreshes every 5s | All panels + live log SSE |
| Aggregate metrics | No | Yes — tiles, top users, budgets |

Tunnel both:

```bash
ssh -L 8765:127.0.0.1:8765 -L 8766:127.0.0.1:8766 ubuntu@<vm>
```

## Security note

The monitor exposes:

- Question text (truncated to 100-120 chars)
- User identifiers (Slack `user_name`, `user_id`)
- Token counts and cost
- Error messages (truncated to 300 chars)

That's all internal-only data. Keep this dashboard on `127.0.0.1` and access via SSH tunnel unless you put nginx + OAuth in front of it. The `--host 0.0.0.0` flag is intentionally not the default.

## Roadmap

Open ideas worth adding as the monitoring need grows:

- **Per-channel filter** — view only one Slack channel's traffic
- **Per-user historical view** — click a user, see their last N questions and outcomes
- **Cost graph** — sparkline of $ / hour for the last 24h
- **Alerts** — email or Slack ping when error rate > X% or any user crosses their budget
- **Trace deep-link** — click a row to jump to `/traces#run-123` on the trace viewer
- **Multi-instance support** — one dashboard fronting several agent deployments (prod, staging, dev)
