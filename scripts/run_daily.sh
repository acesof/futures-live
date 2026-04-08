#!/usr/bin/env bash
# Daily execution wrapper — called by cron
# Scheduled at both 23:05 and 00:05 local to cover DST.
# Only the invocation at 21:xx UTC actually runs; the other exits silently.
set -euo pipefail

# UTC guard: only run if current UTC hour is 21
UTC_HOUR=$(date -u +%H)
if [ "$UTC_HOUR" != "21" ]; then
    exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/futures_$(date -u +%Y%m%d_%H%M%S).log"

mkdir -p "$LOG_DIR"

echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') — Starting futures executor daily run" | tee "$LOG_FILE"

# Source conda init for cron (conda not on PATH by default)
CONDA_BASE="/Users/acess/miniforge3"
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate base

cd "$PROJECT_DIR"

# Run
futures-executor run-once 2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}

echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') — Futures executor finished with exit code $EXIT_CODE" | tee -a "$LOG_FILE"

# Clean up old logs (keep 30 days)
find "$LOG_DIR" -name "futures_*.log" -mtime +30 -delete 2>/dev/null || true

exit $EXIT_CODE
