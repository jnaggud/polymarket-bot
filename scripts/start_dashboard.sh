#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."
python3 main.py serve-dashboard --host "${DASHBOARD_HOST:-127.0.0.1}" --port "${DASHBOARD_PORT:-8080}"
