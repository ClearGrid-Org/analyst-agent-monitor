.PHONY: help venv install run run-public install-service uninstall-service test clean

PORT ?= 8766
LOG_FILE ?= /var/log/cg-sl-agent-api/agent.log
# Always the repo's own venv — never the caller's PATH. `make test` used to
# fail with "python: not found" on Ubuntu because it assumed an activated venv.
PY := ./.venv/bin/python

help:           ## Show this help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

venv:           ## Create the venv (once per machine).
	python3 -m venv .venv

install: venv   ## Create venv if needed + install deps into it.
	$(PY) -m pip install -r requirements.txt

run:            ## Launch the monitor on 127.0.0.1:$(PORT). SSH-tunnel to access.
	$(PY) serve.py --port $(PORT) --log-file $(LOG_FILE)

run-public:     ## Launch on 0.0.0.0:$(PORT) — only behind a reverse proxy + auth.
	$(PY) serve.py --host 0.0.0.0 --port $(PORT) --log-file $(LOG_FILE)

install-service: ## Install the systemd unit (requires sudo).
	sudo cp deploy/analyst-agent-monitor.service /etc/systemd/system/
	sudo systemctl daemon-reload
	sudo systemctl enable --now analyst-agent-monitor
	sudo systemctl status analyst-agent-monitor --no-pager

uninstall-service: ## Stop and remove the systemd unit.
	sudo systemctl disable --now analyst-agent-monitor || true
	sudo rm -f /etc/systemd/system/analyst-agent-monitor.service
	sudo systemctl daemon-reload

test:           ## Quick smoke test — verify env vars + DB reachable.
	@$(PY) -c "from serve import _conn; _conn().cursor().execute('select 1'); print('OK: DB reachable')"

clean:          ## Remove caches.
	find . -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete
