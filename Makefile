.PHONY: help install run run-public install-service uninstall-service test clean

PORT ?= 8766
LOG_FILE ?= /var/log/cg-sl-agent/agent.log

help:           ## Show this help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install:        ## Install Python deps into the current venv.
	pip install -r requirements.txt

run:            ## Launch the monitor on 127.0.0.1:$(PORT). SSH-tunnel to access.
	python serve.py --port $(PORT) --log-file $(LOG_FILE)

run-public:     ## Launch on 0.0.0.0:$(PORT) — only behind a reverse proxy + auth.
	python serve.py --host 0.0.0.0 --port $(PORT) --log-file $(LOG_FILE)

install-service: ## Install the systemd unit (requires sudo).
	sudo cp deploy/sl-agent-monitor.service /etc/systemd/system/
	sudo systemctl daemon-reload
	sudo systemctl enable --now sl-agent-monitor
	sudo systemctl status sl-agent-monitor --no-pager

uninstall-service: ## Stop and remove the systemd unit.
	sudo systemctl disable --now sl-agent-monitor || true
	sudo rm -f /etc/systemd/system/sl-agent-monitor.service
	sudo systemctl daemon-reload

test:           ## Quick smoke test — verify env vars + DB reachable.
	@python -c "from serve import _conn; _conn().cursor().execute('select 1'); print('OK: DB reachable')"

clean:          ## Remove caches.
	find . -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete
