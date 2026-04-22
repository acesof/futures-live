#!/usr/bin/env bash
# Install crontab entries for the futures workflow.
#
# Two roles, two scripts, two cron pairs:
#
#   run_daily.sh     (trading; ET_HOUR=16 gate at CME close−5min)
#     22:55 / 23:55 local — executes run-once and submits orders
#                            BEFORE the 4pm ET close.
#
#   monitor_cycle.sh (monitoring; ET_HOUR=17 gate, 30min post-close)
#     23:30 / 00:30 local — ingests today's final daily bar from
#                            IBKR, writes snapshot, runs monitor,
#                            pings Signal on findings.
#
# Each pair's dual slot covers the DST-window shift of ET relative
# to local time; the in-script ET_HOUR guard picks exactly one per
# night.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TRADING_SCRIPT="$SCRIPT_DIR/run_daily.sh"
MONITOR_SCRIPT="$SCRIPT_DIR/monitor_cycle.sh"

chmod +x "$TRADING_SCRIPT" "$MONITOR_SCRIPT"

TRADE_1="55 22 * * 1-5 $TRADING_SCRIPT"
TRADE_2="55 23 * * 1-5 $TRADING_SCRIPT"
MONITOR_1="30 23 * * 1-5 $MONITOR_SCRIPT"
MONITOR_2="30  0 * * 2-6 $MONITOR_SCRIPT"

# Scrub any existing futures-live entries (trading or monitor), then
# re-install the desired four. Unrelated crontab entries preserved.
(
    crontab -l 2>/dev/null \
        | grep -vF "futures-live/scripts/run_daily.sh" \
        | grep -vF "futures-live/scripts/daily_cycle.sh" \
        | grep -vF "futures-live/scripts/monitor_cycle.sh" \
        || true
    echo "# Futures — trade 5min before CME close (4pm ET, ET_HOUR=16 guard)"
    echo "$TRADE_1"
    echo "$TRADE_2"
    echo "# Futures — monitor 30min after CME close (5pm ET, ET_HOUR=17 guard)"
    echo "$MONITOR_1"
    echo "$MONITOR_2"
) | crontab -

echo "Installed cron entries:"
echo "  $TRADE_1"
echo "  $TRADE_2"
echo "  $MONITOR_1"
echo "  $MONITOR_2"
echo ""
echo "Trades Mon-Fri 22:55/23:55 local; monitor Mon-Fri 23:30 + Tue-Sat 00:30 local"
