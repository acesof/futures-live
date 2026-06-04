"""Tests for #228 A2 #4 late-fill audit reconciler.

The reconciler runs at the start of each ``cmd_run_once`` cycle (after
broker connect, before sizing). It queries IBKR for executions since the
previous run's timestamp and reconciles them into ``audit.executions``:

  - Matching row with status IN ('Submitted','PreSubmitted','PendingSubmit')
    → UPDATE in-place to Filled with real fill data.
  - Matching row already Filled → skipped (same-cycle fills land here on
    second pass; idempotent).
  - No matching row → INSERT orphan with event_type='late_fill_orphan'.

These tests exercise the ``AuditLog.reconcile_late_fills`` method directly
against an in-memory-like temp DB; the CLI plumbing + broker.fetch
side is covered by smoke-tests in the broader suite.
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from futures_executor.execution.broker import _aggregate_fills_by_perm_id
from futures_executor.monitoring.audit import AuditLog


@pytest.fixture
def audit(tmp_path) -> AuditLog:
    """Fresh AuditLog backed by a temp SQLite file."""
    return AuditLog(tmp_path / "audit.db")


def _ex(
    perm_id: int = 1234567,
    exec_id: str = "0000e1a7.6a2d12bd.01.01",
    symbol: str = "MES",
    action: str = "BUY",
    quantity: int = 4,
    fill_price: float = 7544.25,
    commission: float = 2.48,
    realized_pnl: float = 0.0,
    time_iso: str = "2026-06-03T20:55:24+00:00",
) -> dict:
    """Build an execution dict in the reconciler's expected shape."""
    return {
        "perm_id": perm_id,
        "exec_id": exec_id,
        "symbol": symbol,
        "action": action,
        "quantity": quantity,
        "fill_price": fill_price,
        "commission": commission,
        "realized_pnl": realized_pnl,
        "time_iso": time_iso,
    }


# ---------------------------------------------------------------------------
# Schema migration
# ---------------------------------------------------------------------------


def test_schema_has_perm_id_column(audit):
    """The migration adds the perm_id column to executions."""
    cols = {row[1] for row in audit._conn.execute(
        "PRAGMA table_info(executions)"
    ).fetchall()}
    assert "perm_id" in cols, "perm_id column missing — migration didn't run"


def test_log_execution_accepts_perm_id_kwarg(audit):
    """log_execution accepts and stores perm_id."""
    audit.log_execution(
        run_date="2026-06-03", symbol="MES",
        event_type="adjustment", action="BUY", quantity=4,
        fill_price=7544.25, commission=2.48, status="Filled",
        perm_id=1234567,
    )
    row = audit._conn.execute(
        "SELECT perm_id FROM executions WHERE symbol='MES'"
    ).fetchone()
    assert row is not None and row[0] == 1234567


# ---------------------------------------------------------------------------
# Reconciler behaviour
# ---------------------------------------------------------------------------


def test_reconciler_skips_when_no_executions(audit):
    """Empty input → empty result, no DB writes."""
    result = audit.reconcile_late_fills([], "2026-06-04")
    assert result == {"updated": 0, "inserted_orphan": 0, "skipped": 0}


def test_reconciler_updates_pending_row_to_filled(audit):
    """Matching row with status=PreSubmitted → UPDATE to Filled with fill data."""
    # Pre-existing audit row from the prior cycle (Step-5 adjustment that
    # didn't fill within the 30s timeout, then was left working by Step 6).
    audit.log_execution(
        run_date="2026-06-03", symbol="MES",
        event_type="adjustment", action="BUY", quantity=4,
        target_contracts=22, current_contracts=18,
        fill_price=0.0, commission=0.0, status="PreSubmitted",
        perm_id=1234567,
    )
    # Late-fill execution from IBKR for the same perm_id.
    result = audit.reconcile_late_fills(
        [_ex(perm_id=1234567, fill_price=7544.25, commission=2.48,
             realized_pnl=0.0)],
        run_date="2026-06-04",
    )
    assert result == {"updated": 1, "inserted_orphan": 0, "skipped": 0}

    # The row is now Filled with the real fill data, but timestamp and
    # other context (target/current/quantity) are preserved.
    row = audit._conn.execute(
        "SELECT status, fill_price, commission, event_type, action, quantity, "
        "target_contracts, details FROM executions WHERE perm_id=?",
        (1234567,),
    ).fetchone()
    assert row[0] == "Filled"
    assert row[1] == pytest.approx(7544.25)
    assert row[2] == pytest.approx(2.48)
    assert row[3] == "adjustment"  # event_type preserved
    assert row[4] == "BUY"  # action preserved
    assert row[5] == 4  # quantity preserved
    assert row[6] == 22  # target_contracts preserved
    details = json.loads(row[7])
    assert details["exec_id"] == "0000e1a7.6a2d12bd.01.01"
    assert details["reconciled_in_run"] == "2026-06-04"


def test_reconciler_skips_already_filled_row(audit):
    """Matching row with status=Filled → skipped (same-cycle fill; idempotent)."""
    audit.log_execution(
        run_date="2026-06-03", symbol="MES",
        event_type="adjustment", action="BUY", quantity=4,
        fill_price=7544.25, commission=2.48, status="Filled",
        perm_id=1234567,
    )
    result = audit.reconcile_late_fills(
        [_ex(perm_id=1234567, fill_price=9999.99, commission=99.99)],
        run_date="2026-06-04",
    )
    assert result == {"updated": 0, "inserted_orphan": 0, "skipped": 1}

    # The Filled row is untouched — fill_price stays at 7544.25, not 9999.99.
    row = audit._conn.execute(
        "SELECT status, fill_price, commission FROM executions WHERE perm_id=?",
        (1234567,),
    ).fetchone()
    assert row[0] == "Filled"
    assert row[1] == pytest.approx(7544.25)
    assert row[2] == pytest.approx(2.48)


def test_reconciler_inserts_orphan_when_no_matching_row(audit):
    """No matching row → INSERT orphan with event_type='late_fill_orphan'."""
    result = audit.reconcile_late_fills(
        [_ex(perm_id=9999999, symbol="MCL", action="SELL",
             quantity=2, fill_price=92.34, commission=1.54,
             realized_pnl=886.92)],
        run_date="2026-06-04",
    )
    assert result == {"updated": 0, "inserted_orphan": 1, "skipped": 0}

    row = audit._conn.execute(
        "SELECT event_type, symbol, action, quantity, fill_price, "
        "commission, realized_pnl, status, perm_id, details "
        "FROM executions WHERE perm_id=?",
        (9999999,),
    ).fetchone()
    assert row[0] == "late_fill_orphan"
    assert row[1] == "MCL"
    assert row[2] == "SELL"
    assert row[3] == 2
    assert row[4] == pytest.approx(92.34)
    assert row[5] == pytest.approx(1.54)
    assert row[6] == pytest.approx(886.92)
    assert row[7] == "Filled"
    assert row[8] == 9999999
    details = json.loads(row[9])
    assert details["exec_id"] == "0000e1a7.6a2d12bd.01.01"
    assert details["reconciled_in_run"] == "2026-06-04"


def test_reconciler_idempotent_on_second_pass(audit):
    """Re-running on the same execution list produces all-skipped/no-op."""
    # First pass: one row gets updated, one gets inserted as orphan.
    audit.log_execution(
        run_date="2026-06-03", symbol="MES",
        event_type="adjustment", action="BUY", quantity=4,
        fill_price=0.0, commission=0.0, status="Submitted",
        perm_id=1111111,
    )
    executions = [
        _ex(perm_id=1111111, exec_id="exec-A", symbol="MES",
            fill_price=7544.25, commission=2.48),
        _ex(perm_id=2222222, exec_id="exec-B", symbol="MCL",
            action="SELL", quantity=2, fill_price=92.34, commission=1.54),
    ]
    r1 = audit.reconcile_late_fills(executions, "2026-06-04")
    assert r1 == {"updated": 1, "inserted_orphan": 1, "skipped": 0}

    # Second pass with the SAME inputs: matched row is now Filled
    # (skipped), orphan row exists (skipped — its perm_id is now found).
    r2 = audit.reconcile_late_fills(executions, "2026-06-04")
    assert r2 == {"updated": 0, "inserted_orphan": 0, "skipped": 2}

    # Total row count unchanged after second pass (no duplicate orphan).
    n = audit._conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0]
    assert n == 2


def test_reconciler_handles_missing_perm_id_as_orphan(audit):
    """Execution with perm_id=None falls through to the orphan path.

    This is a defensive edge case — ib_insync should always populate
    permId after submission ack, but a malformed input (or a fill with
    no parent order) should still produce a recoverable orphan row.
    """
    result = audit.reconcile_late_fills(
        [_ex(perm_id=None, exec_id="exec-orphan-no-pid")],
        run_date="2026-06-04",
    )
    assert result == {"updated": 0, "inserted_orphan": 1, "skipped": 0}

    row = audit._conn.execute(
        "SELECT event_type, perm_id FROM executions WHERE details LIKE '%exec-orphan-no-pid%'"
    ).fetchone()
    assert row[0] == "late_fill_orphan"
    assert row[1] is None


def test_reconciler_distinguishes_multiple_perm_ids(audit):
    """Distinct perm_ids → distinct match/orphan decisions, no cross-talk."""
    # Two pre-existing pending rows.
    audit.log_execution(
        run_date="2026-06-03", symbol="MES",
        event_type="adjustment", action="BUY", quantity=4,
        fill_price=0.0, commission=0.0, status="PreSubmitted",
        perm_id=111,
    )
    audit.log_execution(
        run_date="2026-06-03", symbol="MGC",
        event_type="adjustment", action="BUY", quantity=1,
        fill_price=0.0, commission=0.0, status="PreSubmitted",
        perm_id=222,
    )
    # Three executions: two match, one orphan.
    executions = [
        _ex(perm_id=111, exec_id="ex-mes", symbol="MES",
            fill_price=7544.25, commission=2.48),
        _ex(perm_id=222, exec_id="ex-mgc", symbol="MGC",
            quantity=1, fill_price=4527.0, commission=0.77),
        _ex(perm_id=333, exec_id="ex-orphan", symbol="MCL",
            action="SELL", quantity=2, fill_price=92.34, commission=1.54),
    ]
    result = audit.reconcile_late_fills(executions, "2026-06-04")
    assert result == {"updated": 2, "inserted_orphan": 1, "skipped": 0}

    # All three rows are now Filled (two updates + one orphan).
    rows = audit._conn.execute(
        "SELECT perm_id, symbol, event_type, status FROM executions "
        "ORDER BY perm_id"
    ).fetchall()
    assert rows == [
        (111, "MES", "adjustment",         "Filled"),
        (222, "MGC", "adjustment",         "Filled"),
        (333, "MCL", "late_fill_orphan",   "Filled"),
    ]


# ---------------------------------------------------------------------------
# get_last_run_timestamp helper
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Partial-fill aggregation (#228 A2 #4 trade-safety W1)
# ---------------------------------------------------------------------------


def test_aggregate_fills_single_perm_id_passthrough():
    """A single fill (no partials) passes through unchanged."""
    inp = [_ex(perm_id=1234567, fill_price=7544.25, quantity=4, commission=2.48)]
    out = _aggregate_fills_by_perm_id(inp)
    assert len(out) == 1
    assert out[0] == inp[0]


def test_aggregate_fills_multiple_partials_vwap_and_sums():
    """3 partials with same perm_id → 1 entry with VWAP price + summed everything."""
    partials = [
        {
            "perm_id": 1234567, "exec_id": "exec-A", "symbol": "MCL",
            "action": "SELL", "quantity": 1, "fill_price": 92.37,
            "commission": 0.77, "realized_pnl": 447.46,
            "time_iso": "2026-06-01T20:55:23.574+00:00",
        },
        {
            "perm_id": 1234567, "exec_id": "exec-B", "symbol": "MCL",
            "action": "SELL", "quantity": 2, "fill_price": 92.33,
            "commission": 1.54, "realized_pnl": 886.92,
            "time_iso": "2026-06-01T20:55:23.580+00:00",
        },
        {
            "perm_id": 1234567, "exec_id": "exec-C", "symbol": "MCL",
            "action": "SELL", "quantity": 1, "fill_price": 92.33,
            "commission": 0.77, "realized_pnl": 443.46,
            "time_iso": "2026-06-01T20:55:23.583+00:00",
        },
    ]
    out = _aggregate_fills_by_perm_id(partials)
    assert len(out) == 1
    agg = out[0]
    assert agg["perm_id"] == 1234567
    assert agg["quantity"] == 4
    expected_vwap = (1*92.37 + 2*92.33 + 1*92.33) / 4
    assert agg["fill_price"] == pytest.approx(expected_vwap)
    assert agg["commission"] == pytest.approx(0.77 + 1.54 + 0.77)
    assert agg["realized_pnl"] == pytest.approx(447.46 + 886.92 + 443.46)
    # Representative exec_id and latest time.
    assert agg["exec_id"] == "exec-A"
    assert agg["time_iso"] == "2026-06-01T20:55:23.583+00:00"
    # Symbol/action preserved.
    assert agg["symbol"] == "MCL"
    assert agg["action"] == "SELL"


def test_aggregate_fills_distinct_perm_ids_kept_separate():
    """Fills with different perm_ids are kept as separate entries."""
    fills = [
        _ex(perm_id=111, exec_id="a", symbol="MES", quantity=2,
            fill_price=7544.0, commission=1.2),
        _ex(perm_id=222, exec_id="b", symbol="MCL", quantity=4,
            fill_price=92.5, commission=2.0),
    ]
    out = _aggregate_fills_by_perm_id(fills)
    assert len(out) == 2
    by_pid = {e["perm_id"]: e for e in out}
    assert by_pid[111]["symbol"] == "MES" and by_pid[111]["quantity"] == 2
    assert by_pid[222]["symbol"] == "MCL" and by_pid[222]["quantity"] == 4


def test_aggregate_fills_perm_id_zero_or_none_treated_as_orphan_passthrough():
    """``perm_id`` of 0 or None bypasses aggregation — each is an independent
    orphan (no meaningful correlation key)."""
    fills = [
        _ex(perm_id=0, exec_id="orphan-a", symbol="MES", quantity=1),
        _ex(perm_id=None, exec_id="orphan-b", symbol="MCL", quantity=2),
        _ex(perm_id=111, exec_id="ok", symbol="MGC", quantity=1),
    ]
    out = _aggregate_fills_by_perm_id(fills)
    assert len(out) == 3
    # Distinct exec_ids preserved.
    assert {e["exec_id"] for e in out} == {"orphan-a", "orphan-b", "ok"}


def test_aggregate_fills_reconciler_integration_uses_vwap(audit):
    """Reconciler receives the AGGREGATED fill, so the audit row reflects
    VWAP + summed commission rather than just the first partial's data.

    This is the bug-injection test for trade-safety-reviewer W1: before
    aggregation, the audit row's fill_price/commission/realized_pnl
    were the first partial's. After aggregation, they're the totals.
    """
    # Pre-existing pending audit row.
    audit.log_execution(
        run_date="2026-06-01", symbol="MCL",
        event_type="adjustment", action="SELL", quantity=4,
        fill_price=0.0, commission=0.0, status="PreSubmitted",
        perm_id=1234567,
    )
    # Simulate the per-Fill output from reqExecutions: 3 partials.
    raw_fills = [
        {
            "perm_id": 1234567, "exec_id": "exec-A", "symbol": "MCL",
            "action": "SELL", "quantity": 1, "fill_price": 92.37,
            "commission": 0.77, "realized_pnl": 447.46,
            "time_iso": "2026-06-01T20:55:23.574+00:00",
        },
        {
            "perm_id": 1234567, "exec_id": "exec-B", "symbol": "MCL",
            "action": "SELL", "quantity": 2, "fill_price": 92.33,
            "commission": 1.54, "realized_pnl": 886.92,
            "time_iso": "2026-06-01T20:55:23.580+00:00",
        },
        {
            "perm_id": 1234567, "exec_id": "exec-C", "symbol": "MCL",
            "action": "SELL", "quantity": 1, "fill_price": 92.33,
            "commission": 0.77, "realized_pnl": 443.46,
            "time_iso": "2026-06-01T20:55:23.583+00:00",
        },
    ]
    aggregated = _aggregate_fills_by_perm_id(raw_fills)
    audit.reconcile_late_fills(aggregated, "2026-06-02")

    row = audit._conn.execute(
        "SELECT status, fill_price, commission, realized_pnl "
        "FROM executions WHERE perm_id=?",
        (1234567,),
    ).fetchone()
    assert row[0] == "Filled"
    expected_vwap = (1*92.37 + 2*92.33 + 1*92.33) / 4
    assert row[1] == pytest.approx(expected_vwap)
    assert row[2] == pytest.approx(0.77 + 1.54 + 0.77)
    assert row[3] == pytest.approx(447.46 + 886.92 + 443.46)


# ---------------------------------------------------------------------------
# get_last_run_timestamp helper
# ---------------------------------------------------------------------------


def test_get_last_run_timestamp_returns_none_on_fresh_db(audit):
    """A fresh install has no run_log rows — caller skips reconciliation."""
    assert audit.get_last_run_timestamp() is None


def test_get_last_run_timestamp_returns_most_recent(audit):
    """Most recent run_log timestamp wins when multiple exist."""
    audit.log_run(
        run_date="2026-06-02", equity=1_000_000.0, n_instruments=3,
        n_orders=2, n_rolls=0, n_errors=0, total_commission=4.0,
    )
    audit.log_run(
        run_date="2026-06-03", equity=1_001_000.0, n_instruments=3,
        n_orders=1, n_rolls=0, n_errors=0, total_commission=2.0,
    )
    ts = audit.get_last_run_timestamp()
    assert ts is not None
    assert ts.startswith("2026-")  # ISO format
