#!/usr/bin/env bash
# Heartbeat verifier for futures-live cron cycles.
#
# Single firing window: NY 19:00 ET Mon-Fri. Verifies:
#   1. monitor_cycle.sh ran (its log exists and ends with "done")
#   2. monitor.db has a row stamped with today's NY date for
#      instrument_set='futures_mini'
#
# CME futures are 24/5 with no weekend gap → no Sunday recovery
# window (per Choice 3 of PLAN_SHARED_RESILIENCE).
#
# Catches the orthogonal failure modes the per-script EXIT trap
# can't:
#   - cron didn't fire at all
#   - script aborted before the trap line ran (rare shell parse error)
#   - script exit was 0 but didn't actually persist the day's row
#
# Cron-DST-dual: two Vilnius-local entries per the (NY 19:00) window.
#
# See PLAN_SHARED_RESILIENCE.md (P5).
set -euo pipefail

INSTRUMENT_SET="${INSTRUMENT_SET:-futures_mini}"
R_FACTORY_DIR="${R_FACTORY_DIR:-/Users/acess/projects/R-factory}"
FUTURES_LIVE_DIR="${FUTURES_LIVE_DIR:-/Users/acess/projects/futures-live}"
CONDA_BASE="${CONDA_BASE:-/Users/acess/miniforge3}"
LOG_DIR="${LOG_DIR:-$FUTURES_LIVE_DIR/logs}"
NOTIFY_PROJECT_DIR="$FUTURES_LIVE_DIR"
NOTIFY_CMD="futures-executor notify"
# monitor.db is shared across instrument sets — column not directory.
MONITOR_DB="$R_FACTORY_DIR/artifacts/monitor/monitor.db"

# shellcheck source=/Users/acess/projects/R-factory/scripts/cron_lib.sh
source "$R_FACTORY_DIR/scripts/cron_lib.sh"

ET_HOUR=$(TZ='America/New_York' date +%H)
ET_DOW=$(TZ='America/New_York' date +%u)
NY_DATE=$(TZ='America/New_York' date +%Y-%m-%d)

# Single firing window — NY 19:00 ET Mon-Fri. Wrong-season fire
# silently exits 0.
case "$ET_DOW $ET_HOUR" in
    "1 19"|"2 19"|"3 19"|"4 19"|"5 19") ;;  # OK
    *) exit 0 ;;
esac

# conda init for cron (the notify cmd needs futures-executor on PATH).
# shellcheck disable=SC1090,SC1091
source "$CONDA_BASE/etc/profile.d/conda.sh" 2>/dev/null || true
conda activate base 2>/dev/null || true

# 1. Latest monitor_cycle log within 4-hour window (NY 15:00 - 19:00).
LOG_GLOB="monitor_cycle_*.log"
RECENT_LOGS=$(find "$LOG_DIR" -name "$LOG_GLOB" -mmin -240 2>/dev/null | sort)

if [ -z "$RECENT_LOGS" ]; then
    cron_lib_notify "🚨 Futures cron MISSED: monitor_cycle.sh" \
"No $LOG_GLOB found in $LOG_DIR within the last 4 hours.
NY date: $NY_DATE   NY DOW: $ET_DOW
Expected fire: NY 17:30 ET Mon-Fri (post-CME-close).
Probable cause: cron didn't fire (system reboot / launchd / crontab cleared)."
    exit 1
fi

LATEST_LOG=$(echo "$RECENT_LOGS" | tail -n 1)

if ! grep -qF "monitor_cycle done" "$LATEST_LOG"; then
    cron_lib_notify "🚨 Futures cycle INCOMPLETE: monitor_cycle.sh" \
"$LATEST_LOG exists but is missing the 'monitor_cycle done' marker.
NY date: $NY_DATE   NY DOW: $ET_DOW

Last 20 lines:
$(tail -n 20 "$LATEST_LOG")"
    exit 2
fi

# 2. Defense-in-depth: monitor.db has a row for today.
if [ -r "$MONITOR_DB" ]; then
    LATEST_RUN_DATE=$(sqlite3 "$MONITOR_DB" \
        "SELECT MAX(run_date) FROM portfolio_snapshots WHERE instrument_set='$INSTRUMENT_SET';" \
        2>/dev/null || echo "")
    if [ -z "$LATEST_RUN_DATE" ] || [ "$LATEST_RUN_DATE" != "$NY_DATE" ]; then
        cron_lib_notify "🚨 Futures Monitor DB STALE" \
"monitor_cycle log shows 'done' but monitor.db latest run_date='$LATEST_RUN_DATE' != today='$NY_DATE'.
Cycle ran but the capture row for today is missing.
Log: $LATEST_LOG
DB:  $MONITOR_DB"
        exit 3
    fi
fi

echo "$(cron_lib_ts) — heartbeat OK (monitor_cycle.sh, $LATEST_LOG)"
