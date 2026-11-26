#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
export PYTHONUNBUFFERED=1
export PYTHONUTF8=1
export PYTHONIOENCODING=utf-8
exec python3 -m pairqueue.verify "$@"
