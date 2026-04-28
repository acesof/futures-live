#!/usr/bin/env bash
# Futures (IBKR) account onboarding — fresh tracking window, full pipeline reset.
#
# Mirror of forex-live/scripts/onboard_account.sh adapted for IBKR:
#   - No keychain rotation (IBKR auth is via TWS/IB Gateway login, not keychain)
#   - No bridge restart (no separate bridge process; ib_insync talks directly)
#   - All other phases parallel forex's flow
#
# Sequence:
#   0.  Sanity preflight: assert futures-live + R-factory are at post-leverage-
#       refactor level; assert IB Gateway port is reachable.
#   0.5 Re-onboard safety: refuse if monitor.db has > 1 captured run unless --force.
#   1.  Prompts: expected IBKR account ID + expected initial equity (account
#       currency). Recommend the user has switched account in TWS BEFORE running
#       this script — or confirm they want to use the currently-logged-in account.
#   2.  Verify broker: connect via the executor's BrokerConnection class
#       (same proven code path the daily cron uses); assert account ID
#       matches; print equity + configured-instrument positions.
#   3.  PAUSE — confirm before destructive steps.
#   4.  Wipe audit.db (unless --keep-audit).
#   5.  monitor reset --instrument-set futures_mini.
#   6.  futures-executor run-once (first live cycle; first writes to fresh audit.db).
#   7.  futures-executor snapshot.
#   8.  monitor run (stamps anchor equity + new operational fingerprint).
#
# Note: leftover paper-account warrants / expired contracts are NOT touched
# by this script. They don't affect futures-executor's run-once (it only
# trades configured instruments). Clean up via TWS manually if they
# clutter the dashboard.
#
# Refuses to run if monitor.db already has > 1 captured run unless --force.
# Writes a full transcript to logs/onboard_<timestamp>.log.
#
# Usage:
#   scripts/onboard_account.sh [--keep-audit] [--force]

set -euo pipefail

INSTRUMENT_SET="futures_mini"
FUTURES_LIVE_DIR="/Users/acess/projects/futures-live"
R_FACTORY_DIR="/Users/acess/projects/R-factory"
CONDA_BASE="/Users/acess/miniforge3"
IB_HOST="127.0.0.1"
IB_PORT="4002"      # paper account (4001 = live)

KEEP_AUDIT="false"
FORCE="false"

# --- Parse args ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --keep-audit)       KEEP_AUDIT="true";       shift ;;
        --force)            FORCE="true";            shift ;;
        -h|--help)
            grep '^#' "$0" | sed 's/^# \?//'
            exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

# --- Logging ---
LOG_DIR="$FUTURES_LIVE_DIR/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/onboard_$(date -u +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG_FILE") 2>&1

ts() { date -u '+%Y-%m-%d %H:%M:%S UTC'; }
phase() { echo; echo "=== [$1] $2 ==="; }
fail() { echo "[$(ts)] ERROR: $*" >&2; exit 2; }

echo "[$(ts)] onboard_account.sh starting (log: $LOG_FILE)"
echo "       --keep-audit=$KEEP_AUDIT --force=$FORCE"

# Conda activate (matches run_daily.sh / monitor_cycle.sh pattern). Both
# futures-executor + R-factory's CLI rely on this Python environment.
# shellcheck disable=SC1091
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate base
echo "       conda env: base"

# --- Phase 0: deploy preflight ---
phase 0 "deploy preflight"

if ! grep -q "target_sleeve_vol" "$FUTURES_LIVE_DIR/futures_executor/config/settings.yaml"; then
    fail "futures-live settings.yaml does not have 'target_sleeve_vol' — host hasn't pulled the leverage-refactor commit yet. Run 'cd $FUTURES_LIVE_DIR && git pull' first."
fi
if ! grep -q "replay_params_from_snapshot" "$R_FACTORY_DIR/algo_research_factory/src/monitor/capture.py" 2>/dev/null; then
    fail "R-factory capture.py missing 'replay_params_from_snapshot' — host hasn't pulled the monitor-snapshot-source fix. Run 'cd $R_FACTORY_DIR && git pull' first."
fi
if ! nc -z -w 2 "$IB_HOST" "$IB_PORT" 2>/dev/null; then
    fail "IB Gateway not reachable at $IB_HOST:$IB_PORT. Start TWS / IB Gateway first."
fi
echo "  futures-live settings.yaml: post-leverage-refactor"
echo "  R-factory capture.py:       post-monitor-snapshot-source fix"
echo "  IB Gateway:                 reachable at $IB_HOST:$IB_PORT"

# --- Phase 0.5: re-onboard safety ---
MONITOR_DB="$R_FACTORY_DIR/artifacts/monitor/monitor.db"
if [ -f "$MONITOR_DB" ]; then
    RUN_COUNT=$(sqlite3 "$MONITOR_DB" \
        "SELECT COUNT(*) FROM runs WHERE instrument_set='$INSTRUMENT_SET'" 2>/dev/null || echo 0)
    if [ "$RUN_COUNT" -gt 1 ] && [ "$FORCE" != "true" ]; then
        fail "monitor.db has $RUN_COUNT captured runs for $INSTRUMENT_SET. Re-onboarding will wipe that history. Pass --force to proceed."
    fi
    [ "$RUN_COUNT" -gt 1 ] && echo "  --force passed; will wipe $RUN_COUNT existing runs"
    [ "$RUN_COUNT" -le 1 ] && echo "  monitor.db has $RUN_COUNT runs for $INSTRUMENT_SET — safe to proceed"
fi

# --- Phase 1: prompts ---
phase 1 "expected IBKR account ID + expected initial equity"

read -rp "  Expected IBKR account ID (e.g. DUM258096 / DUH123456): " EXPECTED_ACCOUNT
[ -z "$EXPECTED_ACCOUNT" ] && fail "account_id required"
read -rp "  Expected initial equity in account currency (numeric, e.g. 1000000): " EXPECTED_EQUITY
case "$EXPECTED_EQUITY" in
    ''|*[!0-9.]*) fail "equity must be numeric" ;;
esac

# --- Phase 2: verify broker connection (read-only) ---
# Uses the executor's proven BrokerConnection class — same code path the
# daily cron has been running on every weekday without issue. Keeps the
# script's IBKR contact identical in shape to what the cron does (no
# custom ib_insync heredoc, no rapid reconnect).
phase 2 "verify IB Gateway connection — fetch accountId + positions"

PROBE_OUT="$(mktemp -t onboard_ib_probe)"
python - "$FUTURES_LIVE_DIR" "$EXPECTED_ACCOUNT" > "$PROBE_OUT" <<'PY'
"""Verify connect via the executor's BrokerConnection (proven path)."""
import json
import sys
from pathlib import Path

futures_live_dir = Path(sys.argv[1])
expected_account = sys.argv[2]

from futures_executor.config.loader import load_settings
from futures_executor.execution.broker import BrokerConnection

cfg = load_settings(futures_live_dir / "futures_executor" / "config")
broker = BrokerConnection(cfg.broker)
broker.connect()
try:
    accounts = list(broker.ib.managedAccounts())
    account = broker.get_account_info()
    positions = broker.get_positions()
    out = {
        "managedAccounts": accounts,
        "matched_expected": expected_account in accounts,
        "equity_account_currency": float(account.equity),
        "currency": account.currency,
        "positions_raw": [
            {
                "symbol": p.symbol,
                "localSymbol": p.local_symbol,
                "exchange": p.exchange,
                "position": float(p.position),
                "avgCost": float(p.avg_cost),
            }
            for p in positions
        ],
    }
    print(json.dumps(out, indent=2))
finally:
    broker.disconnect()
PY

cat "$PROBE_OUT"

ACTUAL_ACCOUNTS=$(jq -r '.managedAccounts[]' "$PROBE_OUT" 2>/dev/null | tr '\n' ' ')
MATCH=$(jq -r '.matched_expected' "$PROBE_OUT" 2>/dev/null)
ACTUAL_EQUITY=$(jq -r '.equity_account_currency // "?"' "$PROBE_OUT" 2>/dev/null)

if [ "$MATCH" != "true" ]; then
    fail "IB Gateway is logged into [$ACTUAL_ACCOUNTS] — none match expected '$EXPECTED_ACCOUNT'. Switch account in TWS / IB Gateway first."
fi
echo "  account match: $EXPECTED_ACCOUNT  ✓"
echo "  equity:        $ACTUAL_EQUITY  (expected ~$EXPECTED_EQUITY)"

# Surface non-trading-set positions (warrants etc.) for manual review via TWS.
# BrokerConnection.get_positions() filters to instruments configured in
# settings.yaml (i.e. the trading set). The full ib.positions() list we'd
# need to see warrants is NOT used here — keeping the script's connect
# pattern identical to the daily cron's. If you need to clean up the
# paper-account warrants, do so manually via TWS.
echo
echo "  Note: this probe shows configured-instrument positions only. Any"
echo "  paper-account leftovers (warrants, expired contracts, etc.) are"
echo "  ignored here and won't affect the futures-executor run-once. Clean"
echo "  up via TWS manually if they bother you on the dashboard."

# --- Phase 4: confirmation pause ---
phase 3 "confirm before destructive steps"

cat <<EOF
  About to perform IRREVERSIBLE state changes:
    - $([ "$KEEP_AUDIT" = "true" ] && echo "(skipped) keep audit.db" || echo "wipe audit.db")
    - reset monitor.db (wipes tracking window for $INSTRUMENT_SET)
    - run first 'futures-executor run-once' (places real orders if signals are non-flat)
    - stamp anchor equity ($ACTUAL_EQUITY in account currency) at tracking start today (UTC)

EOF
read -rp "  Type 'yes' to proceed: " CONFIRM
[ "$CONFIRM" != "yes" ] && { echo "[$(ts)] aborted by user before destructive phase"; exit 0; }

# --- Phase 5: wipe audit.db ---
phase 4 "audit.db handling"

# settings.yaml says `data/audit.db` (relative to project root)
AUDIT_DB="$FUTURES_LIVE_DIR/data/audit.db"
if [ "$KEEP_AUDIT" = "true" ]; then
    echo "  --keep-audit: leaving $AUDIT_DB untouched"
else
    if [ -f "$AUDIT_DB" ]; then
        BACKUP_PATH="${AUDIT_DB}.pre-onboard-$(date -u +%Y%m%d_%H%M%S)"
        mv "$AUDIT_DB" "$BACKUP_PATH"
        echo "  audit.db moved to $BACKUP_PATH (kept as backup)"
    else
        echo "  no audit.db at $AUDIT_DB; nothing to wipe"
    fi
    # Also clean up the stale 0-byte stub at repo root if present.
    if [ -f "$FUTURES_LIVE_DIR/audit.db" ] && [ ! -s "$FUTURES_LIVE_DIR/audit.db" ]; then
        rm -f "$FUTURES_LIVE_DIR/audit.db"
        echo "  removed stale 0-byte audit.db stub at repo root"
    fi
fi

# --- Phase 6: monitor reset ---
phase 5 "monitor reset"

cd "$R_FACTORY_DIR"
python -m algo_research_factory.cli monitor reset --instrument-set "$INSTRUMENT_SET"

# --- Phase 7: first cycle ---
phase 6 "first 'futures-executor run-once'"

# Re-verify IB Gateway is still reachable AFTER the user's confirmation pause
# (parallel to forex onboard's bridge re-check). Gateway sometimes drops on
# its own schedule.
if ! nc -z -w 2 "$IB_HOST" "$IB_PORT" 2>/dev/null; then
    fail "IB Gateway no longer reachable at $IB_HOST:$IB_PORT. Restart TWS, then manually run: futures-executor run-once && futures-executor snapshot --instrument-set $INSTRUMENT_SET && cd $R_FACTORY_DIR && python -m algo_research_factory.cli monitor run --instrument-set $INSTRUMENT_SET"
fi

cd "$FUTURES_LIVE_DIR"
futures-executor run-once

# --- Phase 8: snapshot ---
phase 7 "futures-executor snapshot"

futures-executor snapshot --instrument-set "$INSTRUMENT_SET"

# --- Phase 9: monitor run (anchor) ---
phase 8 "monitor run — stamp anchor equity + new operational fingerprint"

cd "$R_FACTORY_DIR"
python -m algo_research_factory.cli monitor run --instrument-set "$INSTRUMENT_SET"

# --- Done ---
phase OK "onboarding complete"
cat <<EOF
  Account:       $EXPECTED_ACCOUNT
  Anchor equity: $ACTUAL_EQUITY (account currency)
  Tracking from: today (UTC)
  Monitor DB:    $MONITOR_DB
  Dashboard:     $R_FACTORY_DIR/artifacts/monitor/$INSTRUMENT_SET/index.html
  Transcript:    $LOG_FILE

Next steps:
  - Daily cron will resume at the usual times (run_daily 16:55 ET / monitor_cycle 17:30 ET).
  - Day 1 dashboard equity lines will be flat (anchored), drift residuals
    accumulate from day 2 onwards.
EOF

rm -f "$PROBE_OUT"
