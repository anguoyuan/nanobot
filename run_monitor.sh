#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -f .env ]; then
  echo "ERROR: .env not found. Copy .env.example to .env and fill in Gmail credentials." >&2
  exit 1
fi

set -a
. ./.env
set +a

exec .venv/bin/python -m payment_monitor.monitor
