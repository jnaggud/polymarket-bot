#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p logs

if [ -f logs/bot.pid ]; then
  existing_pid="$(cat logs/bot.pid)"
  if kill -0 "$existing_pid" 2>/dev/null; then
    echo "bot already running with pid $existing_pid"
    exit 0
  fi
  rm -f logs/bot.pid
fi

nohup python3 -u main.py daemon --interval "${DAEMON_INTERVAL_SECONDS:-300}" >> logs/bot.log 2>&1 &
bot_pid=$!
echo "$bot_pid" > logs/bot.pid
sleep 1

if kill -0 "$bot_pid" 2>/dev/null; then
  echo "bot started with pid $bot_pid"
  exit 0
fi

rm -f logs/bot.pid
echo "bot failed to stay running; last log lines:"
tail -n 40 logs/bot.log || true
exit 1
