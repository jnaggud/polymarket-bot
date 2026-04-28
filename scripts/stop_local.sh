#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [ -f logs/bot.pid ]; then
  pid="$(cat logs/bot.pid)"
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" || true
  fi
  rm -f logs/bot.pid
fi

if [ -f logs/dashboard.pid ]; then
  pid="$(cat logs/dashboard.pid)"
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" || true
  fi
  rm -f logs/dashboard.pid
fi

echo "requested stop for bot and dashboard"
