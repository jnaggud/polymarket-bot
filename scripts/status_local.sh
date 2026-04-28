#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."

report_pid() {
  local name="$1"
  local pid_file="logs/${name}.pid"

  if [ ! -f "$pid_file" ]; then
    echo "${name}: stopped"
    return
  fi

  local pid
  pid="$(cat "$pid_file")"
  if kill -0 "$pid" 2>/dev/null; then
    echo "${name}: running (pid ${pid})"
    return
  fi

  echo "${name}: stale pid file (${pid})"
}

report_pid "bot"
report_pid "dashboard"

python3 - <<'PY'
import json
from pathlib import Path

status_path = Path("state/status.json")
if not status_path.exists():
    print("state: no status.json yet")
    raise SystemExit(0)

status = json.loads(status_path.read_text())
result = status.get("last_cycle_result", {})
print(
    "last_cycle:",
    f"started={status.get('last_cycle_started_at')}",
    f"completed={status.get('last_cycle_completed_at')}",
    f"queue={result.get('queue_count', 0)}",
    f"theses={result.get('thesis_count', 0)}",
    f"opened={result.get('opened_positions_count', 0)}",
    f"open_positions={result.get('marked_positions_count', 0)}",
    f"unrealized_pnl={result.get('unrealized_pnl_usdc', 0.0)}",
)
if status.get("last_error"):
    print("last_error:", status["last_error"])
PY
