#!/usr/bin/env bash
# Install crontab entries for the futures workflow.
#
# Three roles, three scripts, three cron pairs:
#
#   run_daily.sh       — trade 5min before CME close (NY 16:55 ET)
#   monitor_cycle.sh   — monitor 30min after CME close (NY 17:30 ET)
#   heartbeat_check.sh — verifier  (NY 19:00 ET Mon-Fri)
#
# Each pair's dual slot covers the DST-window shift of ET vs Vilnius
# local; the in-script et_gate_* filters the wrong-season fire.
#
# Block management routes through cron_lib.sh's `update_cron_block`
# helper (idempotent; preserves unrelated crontab entries; strips
# orphan FUT-prefixed comment headers from prior installs).
set -euo pipefail

R_FACTORY_DIR="${R_FACTORY_DIR:-/Users/acess/projects/R-factory}"

# shellcheck source=/Users/acess/projects/R-factory/scripts/cron_lib.sh
source "$R_FACTORY_DIR/scripts/cron_lib.sh"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TRADING_SCRIPT="$SCRIPT_DIR/run_daily.sh"
MONITOR_SCRIPT="$SCRIPT_DIR/monitor_cycle.sh"
HEARTBEAT_SCRIPT="$SCRIPT_DIR/heartbeat_check.sh"

chmod +x "$TRADING_SCRIPT" "$MONITOR_SCRIPT" "$HEARTBEAT_SCRIPT"

# Trading: 5min before CME close, ET_HOUR=16 guard.
TRADE_1="55 22 * * 1-5 $TRADING_SCRIPT"
TRADE_2="55 23 * * 1-5 $TRADING_SCRIPT"

# Monitor: 30min after CME close, ET_HOUR=17 guard.
MONITOR_1="30 23 * * 1-5 $MONITOR_SCRIPT"
MONITOR_2="30  0 * * 2-6 $MONITOR_SCRIPT"

# Heartbeat: NY 19:00 ET Mon-Fri (verifies monitor_cycle ran).
HEARTBEAT_1="0 2 * * 2-6 $HEARTBEAT_SCRIPT"
HEARTBEAT_2="0 1 * * 2-6 $HEARTBEAT_SCRIPT"

update_cron_block 'futures-live/scripts/(run_daily|daily_cycle|monitor_cycle|heartbeat_check)\.sh' "FUT" <<EOF
# FUT trade — 5min before CME close (NY 16:55 ET, ET_HOUR=16 guard)
$TRADE_1
$TRADE_2
# FUT monitor — 30min after CME close (NY 17:30 ET, ET_HOUR=17 guard)
$MONITOR_1
$MONITOR_2
# FUT heartbeat — NY 19:00 ET Mon-Fri (verifies monitor_cycle ran)
$HEARTBEAT_1
$HEARTBEAT_2
EOF

echo "Installed cron entries:"
echo "  $TRADE_1"
echo "  $TRADE_2"
echo "  $MONITOR_1"
echo "  $MONITOR_2"
echo "  $HEARTBEAT_1"
echo "  $HEARTBEAT_2"
echo ""
echo "Trades:    Mon-Fri 22:55 + 23:55 local"
echo "Monitor:   Mon-Fri 23:30 + Tue-Sat 00:30 local"
echo "Heartbeat: Tue-Sat 02:00 + 01:00 local"
