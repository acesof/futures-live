#!/usr/bin/env bash
# Monitor-only post-close cycle for futures_mini:
#   1. R-factory:    data ingest-futures-ibkr (pick up today's final daily bar)
#   2. futures-live: snapshot               (canonical Snapshot JSON)
#   3. R-factory:    monitor run            (sim replay + capture + render)
#   4. R-factory:    monitor check → futures-executor notify on findings
#
# Fires ~30 min after CME close (17:30 ET) — gives IBKR time to publish
# today's completed daily bar. Scheduled by cron at 23:30 and 00:30 local
# (Mon-Fri) with the ET_HOUR=17 gate picking one per DST season.
#
# Trading happens in run_daily.sh at 16:55 ET (5 min before close) —
# that's a SEPARATE script; this one is monitor-only.
set -euo pipefail

# Only do real work at 5pm-hour ET — after CME close and bar
# publication.
ET_HOUR=$(TZ='America/New_York' date +%H)
if [ "$ET_HOUR" != "17" ]; then
    exit 0
fi

INSTRUMENT_SET="${INSTRUMENT_SET:-futures_mini}"
R_FACTORY_DIR="${R_FACTORY_DIR:-/Users/acess/projects/R-factory}"
FUTURES_LIVE_DIR="${FUTURES_LIVE_DIR:-/Users/acess/projects/futures-live}"
CONDA_BASE="${CONDA_BASE:-/Users/acess/miniforge3}"
LOG_DIR="${LOG_DIR:-$FUTURES_LIVE_DIR/logs}"
LOG_FILE="$LOG_DIR/monitor_cycle_$(date -u +%Y%m%d_%H%M%S).log"
mkdir -p "$LOG_DIR"

echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') — monitor_cycle start (set=$INSTRUMENT_SET)" | tee "$LOG_FILE"

# Conda base — both R-factory and futures-executor installed editable there.
# shellcheck disable=SC1090,SC1091
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate base

# 1. Refresh R-factory's canonical continuous-series parquet — now that
#    today's daily bar is finalised by IBKR.
(cd "$R_FACTORY_DIR" && python -m algo_research_factory.cli data ingest-futures-ibkr \
    --instrument-set "$INSTRUMENT_SET" --yes) 2>&1 | tee -a "$LOG_FILE"

# 2. Write the canonical snapshot (broker positions + today's fills from
#    audit.db + targets/close_prices persisted by run_daily.sh earlier).
(cd "$FUTURES_LIVE_DIR" && futures-executor snapshot \
    --instrument-set "$INSTRUMENT_SET") 2>&1 | tee -a "$LOG_FILE"

# 3. Monitor: replay sim against the freshly-ingested parquet, capture
#    today's row, regenerate the dashboard.
(cd "$R_FACTORY_DIR" && python -m algo_research_factory.cli monitor run \
    --instrument-set "$INSTRUMENT_SET") 2>&1 | tee -a "$LOG_FILE"

# 4. Health checks → Signal on any warning/critical.
FINDINGS_FILE="$(mktemp -t monitor_findings)"
(cd "$R_FACTORY_DIR" && python -m algo_research_factory.cli monitor check \
    --instrument-set "$INSTRUMENT_SET" --severity warning \
    > "$FINDINGS_FILE") || CHECK_RC=$?
CHECK_RC="${CHECK_RC:-0}"
cat "$FINDINGS_FILE" | tee -a "$LOG_FILE"
if [ "$CHECK_RC" -ne 0 ]; then
    # Exit code 1 = warning present, 2 = critical present (see
    # cmd_monitor_check). Pick emoji accordingly.
    if [ "$CHECK_RC" -ge 2 ]; then
        MONITOR_EMOJI="🚨"
    else
        MONITOR_EMOJI="⚠️"
    fi
    (cd "$FUTURES_LIVE_DIR" && futures-executor notify \
        --prefix "$MONITOR_EMOJI Futures Monitor ($INSTRUMENT_SET)" < "$FINDINGS_FILE") \
        2>&1 | tee -a "$LOG_FILE" || true
fi
rm -f "$FINDINGS_FILE"

echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') — monitor_cycle done" | tee -a "$LOG_FILE"

# Prune logs older than 30 days.
find "$LOG_DIR" -name "monitor_cycle_*.log" -mtime +30 -delete 2>/dev/null || true
