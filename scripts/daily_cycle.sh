#!/usr/bin/env bash
# Full daily cycle for futures_mini:
#   1. R-factory   : data ingest-futures-ibkr  (refresh canonical parquet)
#   2. futures-live: run-once                  (signals + broker orders)
#   3. futures-live: snapshot                  (canonical Snapshot JSON)
#   4. R-factory   : monitor run               (sim replay + capture + render)
#   5. R-factory   : monitor check → futures-executor notify on findings
#
# Scheduled by cron at 22:55 and 23:55 local (Mon-Fri). The ET_HOUR=16
# guard below skips the non-matching fire per DST season — CME close is
# 4 pm ET.
set -euo pipefail

# Only do real work at 4pm ET (CME close — daily bars are final).
ET_HOUR=$(TZ='America/New_York' date +%H)
if [ "$ET_HOUR" != "16" ]; then
    exit 0
fi

INSTRUMENT_SET="${INSTRUMENT_SET:-futures_mini}"
R_FACTORY_DIR="${R_FACTORY_DIR:-/Users/acess/projects/R-factory}"
FUTURES_LIVE_DIR="${FUTURES_LIVE_DIR:-/Users/acess/projects/futures-live}"
CONDA_BASE="${CONDA_BASE:-/Users/acess/miniforge3}"
LOG_DIR="${LOG_DIR:-$FUTURES_LIVE_DIR/logs}"
LOG_FILE="$LOG_DIR/daily_cycle_$(date -u +%Y%m%d_%H%M%S).log"
mkdir -p "$LOG_DIR"

echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') — daily_cycle start (set=$INSTRUMENT_SET)" | tee "$LOG_FILE"

# Conda base is the default runtime for futures-live (set up in run_daily.sh);
# installs both R-factory and futures-executor as editable.
# shellcheck disable=SC1090,SC1091
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate base

# 1. Refresh R-factory's canonical continuous-series parquet.
(cd "$R_FACTORY_DIR" && python -m algo_research_factory.cli data ingest-futures-ibkr \
    --instrument-set "$INSTRUMENT_SET" --yes) 2>&1 | tee -a "$LOG_FILE"

# 2. Execute the rebalance on IBKR (uses futures-live's own continuous series
#    for signal generation — intraday-synthesised today's bar).
(cd "$FUTURES_LIVE_DIR" && futures-executor run-once) 2>&1 | tee -a "$LOG_FILE"

# 3. Persist the canonical Snapshot JSON.
(cd "$FUTURES_LIVE_DIR" && futures-executor snapshot \
    --instrument-set "$INSTRUMENT_SET") 2>&1 | tee -a "$LOG_FILE"

# 4. Monitor: replay sim against R-factory parquet, capture, re-render.
(cd "$R_FACTORY_DIR" && python -m algo_research_factory.cli monitor run \
    --instrument-set "$INSTRUMENT_SET") 2>&1 | tee -a "$LOG_FILE"

# 5. Deterministic health checks → Signal on any warning/critical.
FINDINGS_FILE="$(mktemp -t monitor_findings)"
(cd "$R_FACTORY_DIR" && python -m algo_research_factory.cli monitor check \
    --instrument-set "$INSTRUMENT_SET" --severity warning \
    > "$FINDINGS_FILE") || CHECK_RC=$?
CHECK_RC="${CHECK_RC:-0}"
cat "$FINDINGS_FILE" | tee -a "$LOG_FILE"
if [ "$CHECK_RC" -ne 0 ]; then
    (cd "$FUTURES_LIVE_DIR" && futures-executor notify \
        --prefix "🔔 Monitor ($INSTRUMENT_SET)" < "$FINDINGS_FILE") \
        2>&1 | tee -a "$LOG_FILE" || true
fi
rm -f "$FINDINGS_FILE"

echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') — daily_cycle done" | tee -a "$LOG_FILE"

# Prune logs older than 30 days.
find "$LOG_DIR" -name "daily_cycle_*.log" -mtime +30 -delete 2>/dev/null || true
