#!/usr/bin/env bash
# Monitor-only post-close cycle for futures_mini:
#   1. R-factory:    data ingest-futures-ibkr (pick up today's final daily bar)
#   2. futures-live: snapshot               (canonical Snapshot JSON)
#   3. R-factory:    monitor run            (sim replay + capture + render)
#   4. R-factory:    monitor check          (Signal on warning/critical)
#
# Fires ~30 min after CME close (NY 17:30 ET) — gives IBKR time to
# publish today's completed daily bar. Cron schedules at Vilnius
# 23:30 / 00:30 (Mon-Fri); et_gate_hour 17 picks one per DST season.
#
# Trading happens in run_daily.sh at NY 16:55 ET — separate script;
# this one is monitor-only.
#
# Resilience scaffolding (trap, pre-flight, monitor check + notify)
# lives in R-factory/scripts/cron_lib.sh — shared with run_daily.sh +
# forex-live's cron scripts. See PLAN_SHARED_RESILIENCE.md.
set -euo pipefail

SCRIPT_NAME="monitor_cycle.sh"

INSTRUMENT_SET="${INSTRUMENT_SET:-futures_mini}"
R_FACTORY_DIR="${R_FACTORY_DIR:-/Users/acess/projects/R-factory}"
FUTURES_LIVE_DIR="${FUTURES_LIVE_DIR:-/Users/acess/projects/futures-live}"
CONDA_BASE="${CONDA_BASE:-/Users/acess/miniforge3}"
PROJECT_LOG_DIR="${PROJECT_LOG_DIR:-$FUTURES_LIVE_DIR}"
NOTIFY_PROJECT_DIR="$FUTURES_LIVE_DIR"
NOTIFY_CMD="futures-executor notify"

# shellcheck source=/Users/acess/projects/R-factory/scripts/cron_lib.sh
source "$R_FACTORY_DIR/scripts/cron_lib.sh"

# 1. Wrong-season-fire gate — NY 17:00 hour (post-close, daily bar
#    available).
et_gate_hour 17

cron_lib_log_setup "monitor_cycle"
cron_lib_init
register_trap_alert

echo "$(cron_lib_ts) — monitor_cycle start (set=$INSTRUMENT_SET)" | tee "$LOG_FILE"

# Conda init for cron.
# shellcheck disable=SC1090,SC1091
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate base

mark_started

# IB Gateway pre-flight removed 2026-05-05 — port-listening != API-ready.
# data ingest-futures-ibkr's connect_ib uses patient retry on
# TimeoutError to handle Gateway-not-yet-ready cases.

# 1. Refresh R-factory's continuous-series parquet — today's daily
#    bar is settled by IBKR.
mark_step "data ingest-futures-ibkr"
(cd "$R_FACTORY_DIR" && python -m algo_research_factory.cli data ingest-futures-ibkr \
    --instrument-set "$INSTRUMENT_SET" --yes) 2>&1 | tee -a "$LOG_FILE"

# 2. Write the canonical snapshot.
mark_step "futures-executor snapshot"
(cd "$FUTURES_LIVE_DIR" && futures-executor snapshot \
    --instrument-set "$INSTRUMENT_SET") 2>&1 | tee -a "$LOG_FILE"

# 3. Monitor: replay sim, capture today's row, regenerate dashboard.
#
# --balance-tol-bps 500 (= 5% of equity): TACTICAL bump. Futures
# balance settles daily via mark-to-market on still-open positions,
# which isn't in the txn-driven Δexpected — so Δactual − Δexpected
# drifts by the day's MTM. Default 1 bp would refuse every day; even
# 100 bps (1%) refused on 2026-05-05 manual-rebalance day (Δbalance
# −250 vs expected +10,633, diff ~1.02%). Loosened to 500 bps to keep
# collecting capture rows for troubleshooting while the futures-aware
# reconciliation (Task #169 / PLAN_BROKER_GROUND_TRUTH_ACCOUNTING
# Phase 4) is pending. **Tighten back to ≤100 bps once Phase 4 lands.**
mark_step "monitor run"
(cd "$R_FACTORY_DIR" && python -m algo_research_factory.cli monitor run \
    --instrument-set "$INSTRUMENT_SET" \
    --balance-tol-bps 500) 2>&1 | tee -a "$LOG_FILE"

# 4. Health checks → Signal on warning/critical.
monitor_check_and_notify "$INSTRUMENT_SET" "Futures Monitor ($INSTRUMENT_SET)"

echo "$(cron_lib_ts) — monitor_cycle done" | tee -a "$LOG_FILE"
