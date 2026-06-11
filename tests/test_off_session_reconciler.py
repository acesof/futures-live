"""Tests for AuditLog.reconcile_off_session — the cross-session half of
#228 A2 #4 (definitive fix 2026-06-11).

``reqExecutions`` cannot see fills from a prior trading day, so an
order left working at disconnect that fills off-cycle leaves its audit
row stuck at 'Submitted'. ``reconcile_off_session`` runs at cycle start
(before any new orders) and resolves those rows from broker truth:
working-order set membership + positions-by-contract-month inference.

Also covers the always-explicit "Working orders at cycle start" line in
``build_run_summary``.
"""
from __future__ import annotations

from futures_executor.config.loader import SignalSettings
from futures_executor.monitoring.audit import AuditLog
from futures_executor.monitoring.notifier import SignalNotifier


def _audit(tmp_path) -> AuditLog:
    return AuditLog(tmp_path / "audit.db")


def _insert_roll(
    audit: AuditLog,
    run_date: str,
    status: str = "Submitted",
    perm_id: int | None = 111,
    details: dict | None = None,
    event_type: str = "roll",
    symbol: str = "MES",
) -> int:
    audit.log_execution(
        run_date=run_date, symbol=symbol, event_type=event_type,
        quantity=20, status=status, perm_id=perm_id,
        details=details,
    )
    return audit._conn.execute("SELECT MAX(id) FROM executions").fetchone()[0]


def _row(audit: AuditLog, rid: int) -> dict:
    cur = audit._conn.execute("SELECT * FROM executions WHERE id=?", (rid,))
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, cur.fetchone()))


def test_filled_off_session_inferred_from_positions(tmp_path):
    """Roll from the last run, position now entirely on to_month →
    FilledOffSession."""
    audit = _audit(tmp_path)
    rid = _insert_roll(
        audit, "2026-06-10",
        details={"from_month": "20260618", "to_month": "20260918"},
    )
    counts = audit.reconcile_off_session(
        working_perm_ids=set(),
        positions_by_month={"MES": {"20260918": 20.0}},
        today="2026-06-11",
        last_run_date="2026-06-10",
    )
    assert counts == {
        "filled": 1, "cancelled": 0, "unknown": 0, "still_working": 0,
    }
    row = _row(audit, rid)
    assert row["status"] == "FilledOffSession"
    assert "filled off-session" in row["error"]


def test_cancelled_off_session_inferred_from_positions(tmp_path):
    """Roll from the last run, position still entirely on from_month →
    CancelledOffSession."""
    audit = _audit(tmp_path)
    rid = _insert_roll(
        audit, "2026-06-10",
        details={"from_month": "20260618", "to_month": "20260918"},
    )
    counts = audit.reconcile_off_session(
        working_perm_ids=set(),
        positions_by_month={"MES": {"20260618": 20.0}},
        today="2026-06-11",
        last_run_date="2026-06-10",
    )
    assert counts["cancelled"] == 1
    assert _row(audit, rid)["status"] == "CancelledOffSession"


def test_still_working_row_left_untouched(tmp_path):
    """perm_id present in the working-order set → the order is genuinely
    still live; the row must NOT be resolved (the open-order guard
    handles the trading side)."""
    audit = _audit(tmp_path)
    rid = _insert_roll(
        audit, "2026-06-10", perm_id=999,
        details={"from_month": "20260618", "to_month": "20260918"},
    )
    counts = audit.reconcile_off_session(
        working_perm_ids={999},
        positions_by_month={"MES": {"20260618": 20.0}},
        today="2026-06-11",
        last_run_date="2026-06-10",
    )
    assert counts["still_working"] == 1
    assert _row(audit, rid)["status"] == "Submitted"


def test_older_than_last_run_is_terminal_unknown(tmp_path):
    """Rows older than the immediately preceding run can't use position
    inference (intervening trades invalidate it) → TerminalUnknown,
    honest note appended."""
    audit = _audit(tmp_path)
    rid = _insert_roll(
        audit, "2026-06-08",
        details={"from_month": "20260618", "to_month": "20260918"},
    )
    counts = audit.reconcile_off_session(
        working_perm_ids=set(),
        positions_by_month={"MES": {"20260918": 20.0}},
        today="2026-06-11",
        last_run_date="2026-06-10",
    )
    assert counts["unknown"] == 1
    row = _row(audit, rid)
    assert row["status"] == "TerminalUnknown"
    assert "not verifiable" in row["error"]


def test_no_details_pre_fix_row_is_terminal_unknown(tmp_path):
    """Pre-fix rows (no from_month/to_month persisted; e.g. the May
    PendingSubmit batch with perm_id=None) → TerminalUnknown."""
    audit = _audit(tmp_path)
    rid = _insert_roll(
        audit, "2026-05-05", perm_id=None, status="PendingSubmit",
        event_type="adjustment", symbol="MCL",
    )
    counts = audit.reconcile_off_session(
        working_perm_ids=set(),
        positions_by_month={},
        today="2026-06-11",
        last_run_date="2026-06-10",
    )
    assert counts["unknown"] == 1
    assert _row(audit, rid)["status"] == "TerminalUnknown"


def test_current_day_active_rows_not_touched(tmp_path):
    """Rows from TODAY are this cycle's own work-in-progress — out of
    scope (run_date < today filter)."""
    audit = _audit(tmp_path)
    rid = _insert_roll(audit, "2026-06-11")
    counts = audit.reconcile_off_session(
        working_perm_ids=set(),
        positions_by_month={},
        today="2026-06-11",
        last_run_date="2026-06-10",
    )
    assert counts == {
        "filled": 0, "cancelled": 0, "unknown": 0, "still_working": 0,
    }
    assert _row(audit, rid)["status"] == "Submitted"


# ---------------------------------------------------------------------------
# Always-explicit "Working orders at cycle start" summary line
# ---------------------------------------------------------------------------


def _summary(records: list[dict]) -> str:
    notifier = SignalNotifier(SignalSettings())
    return notifier.build_run_summary(
        run_date="2026-06-11", equity=1_000_000.0,
        targets={"MES": 0.31}, records=records,
        n_orders=0, n_rolls=0, n_errors=0,
        total_commission=0.0, positions={"MES": 20},
    )


def test_summary_states_no_working_orders_explicitly():
    assert "Working orders at cycle start: none ✓" in _summary([])


def test_summary_flags_blocked_symbols():
    s = _summary([{
        "type": "open_order_skip", "symbol": "MES",
        "error": "working order(s) from a previous session still active",
        "status": "FAILED",
    }])
    assert "Working orders at cycle start: 1 symbol(s) BLOCKED" in s
    assert "cancel stuck order(s) in TWS" in s
