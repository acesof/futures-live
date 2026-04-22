#!/usr/bin/env bash
# Install crontab entries for the full futures daily cycle
# (ingest-futures-ibkr → run-once → snapshot → monitor run → monitor check).
#
# Two slots per night (22:55 and 23:55 local) cover the DST-window shift
# of CME close (4 pm ET) relative to Vilnius local time. The ET_HOUR=16
# guard inside daily_cycle.sh ensures only the fire landing at 4 pm ET
# does work; the other exits 0 silently.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RUN_SCRIPT="$SCRIPT_DIR/daily_cycle.sh"

chmod +x "$RUN_SCRIPT"

CRON_ENTRY_1="55 22 * * 1-5 $RUN_SCRIPT"
CRON_ENTRY_2="55 23 * * 1-5 $RUN_SCRIPT"

# Replace any existing futures-live executor entries (either the legacy
# run_daily.sh or the current daily_cycle.sh); unrelated entries
# (forex-live, other projects) are preserved.
(
    crontab -l 2>/dev/null \
        | grep -vF "futures-live/scripts/run_daily.sh" \
        | grep -vF "futures-live/scripts/daily_cycle.sh" \
        || true
    echo "# Futures — 4pm ET close (CME), ET_HOUR=16 guard in script"
    echo "# 22:55 local covers spring DST gap; 23:55 covers rest of year"
    echo "$CRON_ENTRY_1"
    echo "$CRON_ENTRY_2"
) | crontab -

echo "Installed cron entries:"
echo "  $CRON_ENTRY_1"
echo "  $CRON_ENTRY_2"
echo ""
echo "Runs Mon-Fri at 22:55 and 23:55 local"
