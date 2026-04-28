#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p logs

port="${DASHBOARD_PORT:-8080}"
host="${DASHBOARD_HOST:-127.0.0.1}"

existing_listener="$(lsof -nP -iTCP:${port} -sTCP:LISTEN 2>/dev/null || true)"
if [ -n "$existing_listener" ]; then
  echo "dashboard port ${port} is already in use"
  echo "$existing_listener"
  exit 0
fi

if [ -f logs/dashboard.pid ]; then
  existing_pid="$(cat logs/dashboard.pid)"
  if kill -0 "$existing_pid" 2>/dev/null; then
    echo "dashboard already running with pid $existing_pid"
    exit 0
  fi
  rm -f logs/dashboard.pid
fi

nohup python3 -u main.py serve-dashboard --host "$host" --port "$port" >> logs/dashboard.log 2>&1 &
dashboard_pid=$!
echo "$dashboard_pid" > logs/dashboard.pid
sleep 1

if kill -0 "$dashboard_pid" 2>/dev/null; then
  echo "dashboard started with pid $dashboard_pid"
  exit 0
fi

rm -f logs/dashboard.pid
echo "dashboard failed to stay running; last log lines:"
tail -n 40 logs/dashboard.log || true
exit 1
