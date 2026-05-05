#!/usr/bin/env bash
# Daily futures trading wrapper — called by cron.
#
# Fires NY 16:55 ET (5 min before CME close) — synthesizes today's
# daily bar from 5-min intraday data, generates signals, and submits
# orders while the market is still live. CME closes at 17:00 ET
# Mon-Thu (Fri close at 16:00 ET on settlement day, but the in-bar
# generation handles either).
#
# Cron firing schedule (DST-dual): 22:55 / 23:55 Vilnius local;
# et_gate_hour 16 below silently exits the wrong-season fire.
#
# Resilience scaffolding (trap, pre-flight) lives in
# R-factory/scripts/cron_lib.sh — shared with monitor_cycle.sh +
# forex-live's daily_cycle.sh + friday_cycle.sh + sunday_recovery.sh.
# See PLAN_SHARED_RESILIENCE.md.
set -euo pipefail

SCRIPT_NAME="run_daily.sh"

INSTRUMENT_SET="${INSTRUMENT_SET:-futures_mini}"
R_FACTORY_DIR="${R_FACTORY_DIR:-/Users/acess/projects/R-factory}"
FUTURES_LIVE_DIR="${FUTURES_LIVE_DIR:-/Users/acess/projects/futures-live}"
CONDA_BASE="${CONDA_BASE:-/Users/acess/miniforge3}"
PROJECT_LOG_DIR="${PROJECT_LOG_DIR:-$FUTURES_LIVE_DIR}"
NOTIFY_PROJECT_DIR="$FUTURES_LIVE_DIR"
NOTIFY_CMD="futures-executor notify"

# shellcheck source=/Users/acess/projects/R-factory/scripts/cron_lib.sh
source "$R_FACTORY_DIR/scripts/cron_lib.sh"

# 1. Wrong-season-fire gate — only NY 16:00 hour (16:55 fire window).
et_gate_hour 16

# 2. Per-run log + 30-day prune. Keeping legacy `futures_*.log` basename
#    for log-rotation / heartbeat compat.
cron_lib_log_setup "futures"

# 3. Trap — Signal-alerts non-zero exit AFTER mark_started.
cron_lib_init
register_trap_alert

echo "$(cron_lib_ts) — Starting futures executor daily run (set=$INSTRUMENT_SET)" | tee "$LOG_FILE"

# Conda init for cron (PATH=/usr/bin:/bin in cron's default env).
# shellcheck disable=SC1090,SC1091
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate base

cd "$FUTURES_LIVE_DIR"

mark_started

# IB Gateway pre-flight removed 2026-05-05 — port-listening doesn't
# imply API-ready, and the executor's BrokerConnection.connect() now
# uses connect_ib with patient retry on TimeoutError. No JForex
# bridge in this stack — IBKR direct.

# 1. Refresh R-factory parquet with today's synthesized bar (futures
#    Option 2, 2026-05-05). At NY 16:55 ET (5 min before CME close)
#    the daily bar hasn't settled yet — synthesize from 5-min intraday
#    so step 2's run-once reads a parquet with today's in-progress bar.
#    monitor_cycle.sh later (post-close, 17:30 ET) re-ingests WITHOUT
#    --synthesize-eod so the IBKR-settled bar overwrites this one.
mark_step "data ingest-futures-ibkr --synthesize-eod"
(cd "$R_FACTORY_DIR" && python -m algo_research_factory.cli data ingest-futures-ibkr \
    --instrument-set "$INSTRUMENT_SET" --synthesize-eod --yes) 2>&1 | tee -a "$LOG_FILE"

# 2. Trade. Reads R-factory parquet (NOT IBKR fetch).
mark_step "futures-executor run-once"
futures-executor run-once 2>&1 | tee -a "$LOG_FILE"

echo "$(cron_lib_ts) — Futures executor finished" | tee -a "$LOG_FILE"
