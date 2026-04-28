#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."
python3 main.py daemon --interval "${DAEMON_INTERVAL_SECONDS:-300}"
