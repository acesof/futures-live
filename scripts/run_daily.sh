#!/usr/bin/env bash
# Daily execution wrapper — called by cron
# Runs at NY close (5pm ET). Scheduled at 23:10 and 00:10 local
# to cover DST (spring gap needs 23:xx) — only the one landing on 5pm ET runs.
set -euo pipefail

# Only run if it's currently 5pm (17:xx) US Eastern
ET_HOUR=$(TZ='America/New_York' date +%H)
if [ "$ET_HOUR" != "17" ]; then
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
