"""Safety rails for the futures executor: kill switch + daily-loss circuit breaker.

Mirrors `forex_executor.execution.safety` for the IBKR/futures side.
The pure circuit-breaker math lives in R-factory's
`algo_research_factory.src.safety.circuit_breaker`; this module wraps
it with the executor-side concerns (reference-equity persistence,
kill-switch activation, UTC-date rollover).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from algo_research_factory.src.safety.circuit_breaker import (
    CircuitBreakerDecision,
    evaluate_daily_loss_circuit,
)

logger = logging.getLogger(__name__)


def check_kill_switch(config) -> bool:
    """Return True if the kill switch file exists.

    Caller decides what to do (futures-live's `cmd_run_once` returns 1
    and notifies). Mirrors the existing inline check in cli.py — kept as
    a function here for parity with forex-live + so the breaker can call
    `activate_kill_switch` from the same module.
    """
    path = Path(config.safety.kill_switch_file)
    if path.exists():
        logger.critical(f"Kill switch active: {path}")
        return True
    return False


def activate_kill_switch(config) -> None:
    """Create the kill switch file."""
    path = Path(config.safety.kill_switch_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("Kill switch activated\n")
    logger.warning(f"Kill switch ACTIVATED: {path}")


def deactivate_kill_switch(config) -> None:
    """Remove the kill switch file."""
    path = Path(config.safety.kill_switch_file)
    if path.exists():
        path.unlink()
        logger.info(f"Kill switch deactivated: {path}")


# ---------------------------------------------------------------------------
# Daily-loss circuit breaker
# ---------------------------------------------------------------------------

def _load_reference_equity(path: Path, today: str) -> float | None:
    """Return today's persisted reference equity, or None if missing/stale.

    Stale = different `date` field (means a new UTC day rolled over;
    today's anchor will be re-seeded on this cycle).
    """
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        if data.get("date") != today:
            return None
        equity = data.get("equity")
        return float(equity) if equity is not None else None
    except Exception as e:
        logger.warning(f"Failed to parse reference_equity_file at {path}: {e}")
        return None


def _persist_reference_equity(path: Path, today: str, equity: float) -> None:
    """Write today's start-of-day equity. Called on the first cycle each UTC date."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"date": today, "equity": equity}, indent=2))
    logger.info(f"Reference equity for {today} seeded at {equity:.2f}")


def check_daily_loss_circuit(config, current_equity: float) -> CircuitBreakerDecision:
    """Evaluate the daily-loss circuit breaker for this cycle.

    Loads today's persisted reference_equity (seeds it on the first call
    of each UTC day). If the breaker trips, the kill switch is activated
    AS A SIDE EFFECT — caller should check `decision.should_trip` and
    exit without trading.

    Sticky: once tripped, manual reset is required. The threshold is set
    strictly below the worst historical day, so a trip means something
    is outside our envelope and a human should look.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ref_path = Path(config.safety.reference_equity_file)
    ref_equity = _load_reference_equity(ref_path, today)

    decision = evaluate_daily_loss_circuit(
        current_equity=current_equity,
        reference_equity=ref_equity,
        threshold_pct=config.safety.daily_loss_circuit_pct,
    )

    if ref_equity is None:
        # First call today — seed the anchor with the broker's current equity.
        _persist_reference_equity(ref_path, today, current_equity)

    if decision.should_trip:
        logger.error(f"Daily-loss circuit TRIPPED: {decision.reason}")
        activate_kill_switch(config)
    else:
        logger.info(f"Daily-loss circuit: {decision.reason}")

    return decision
