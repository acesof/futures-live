"""Tests for the fxf snapshot builder.

Mocks BrokerConnection (account + positions + ib.managedAccounts) and
uses real AuditLog tables pre-populated with execution rows. Verifies
the canonical Snapshot round-trips through
``Snapshot.from_json_dict``.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from algo_research_factory.src.monitor.snapshot_contract import Snapshot
from futures_executor.execution.broker import AccountInfo, BrokerPosition
from futures_executor.monitoring.snapshot import (
    build_snapshot,
    write_snapshot,
)


_TRACK_SINCE = "2026-03-01T00:00:00Z"
_INSTRUMENT_SET = "futures_mini"


def _seed_audit_db(db_path: Path, run_date: str) -> None:
    """Create audit.db with one executions row for today."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS executions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            run_date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            event_type TEXT NOT NULL,
            action TEXT,
            quantity INTEGER,
            target_contracts INTEGER,
            current_contracts INTEGER,
            target_signal REAL,
            fill_price REAL,
            bar_close REAL,
            slippage_ticks REAL,
            commission REAL,
            status TEXT,
            error TEXT,
            details TEXT
        );
        """)
        conn.execute(
            "INSERT INTO executions "
            "(timestamp, run_date, symbol, event_type, action, quantity, "
            " target_contracts, current_contracts, target_signal, fill_price, "
            " bar_close, slippage_ticks, commission, status, error, details) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (run_date + "T21:30:00+00:00", run_date, "MES", "adjustment",
             "BUY", 2, 2, 0, 0.25, 4520.50, 4520.00, 2.0, 0.62, "Filled", "", "{}"),
        )
        conn.commit()
    finally:
        conn.close()


def _make_config(tmp_path: Path) -> SimpleNamespace:
    """Minimal ExecutorConfig stub covering what snapshot.py reads."""
    return SimpleNamespace(
        audit=SimpleNamespace(db_path=str(tmp_path / "audit.db")),
        broker=SimpleNamespace(
            host="127.0.0.1", port=4001, client_id=1,
            readonly=False, timeout=30,
        ),
        execution=SimpleNamespace(
            portfolio_leverage=5.0,
            gross_exposure_cap=None,
            abs_threshold=1,
        ),
        vol_target=SimpleNamespace(
            enabled=True, target_sleeve_vol=0.30, vol_window=60,
            vol_floor=0.10, instrument_level=True,
        ),
        instruments=[
            SimpleNamespace(symbol="MES", portfolio_symbol="ES",
                            multiplier=5.0, exchange="CME",
                            margin=1500, delivery_buffer_days=0),
            SimpleNamespace(symbol="MCL", portfolio_symbol="CL",
                            multiplier=100.0, exchange="NYMEX",
                            margin=1000, delivery_buffer_days=0),
        ],
        monitor=SimpleNamespace(
            enabled=True,
            r_factory_artifacts_dir=str(tmp_path / "rf_artifacts"),
            r_factory_data_dir=str(tmp_path / "rf_data"),
            broker_id="ibkr-futures",
        ),
    )


def _fake_broker(equity=10_000.0, currency="USD",
                 positions=None, account_id="DU123456"):
    broker = MagicMock()
    broker.get_account_info.return_value = AccountInfo(
        equity=equity, buying_power=equity * 2, unrealized_pnl=0.0,
        realized_pnl=0.0, currency=currency,
    )
    broker.get_positions.return_value = positions or []
    broker.ib.managedAccounts.return_value = [account_id]
    return broker


def _write_targets(tmp_path: Path, run_date: str, targets: dict, is_v2: bool):
    with open(tmp_path / f"targets_{run_date}.json", "w") as f:
        json.dump({"targets": targets, "is_v2": is_v2}, f)


def _write_close_prices(tmp_path: Path, run_date: str, prices: dict):
    with open(tmp_path / f"close_prices_{run_date}.json", "w") as f:
        json.dump(prices, f)


def _write_dataset_manifest(tmp_path: Path, instrument_set: str, version: str):
    mdir = tmp_path / "rf_data" / "parquet" / instrument_set
    mdir.mkdir(parents=True, exist_ok=True)
    (mdir / "_manifest.json").write_text(
        json.dumps({"dataset_version": version, "updated_at": "", "instruments": {}})
    )


def _write_strategies_yaml(tmp_path: Path) -> Path:
    path = tmp_path / "strategies.yaml"
    path.write_text(
        "strategies:\n"
        "  - name: alpha\n    module_path: /tmp/a.py\n    params: {}\n    weight: 1.0\n    enabled: true\n"
    )
    return path


# ---------------------------------------------------------------------------

def test_build_snapshot_round_trips_and_computes_effective_fraction(tmp_path):
    config = _make_config(tmp_path)
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    _seed_audit_db(Path(config.audit.db_path), run_date)
    _write_targets(tmp_path, run_date, {"ES": 0.3, "CL": -0.2}, is_v2=True)
    _write_close_prices(tmp_path, run_date, {"ES": 4520.0, "CL": 78.0})
    _write_dataset_manifest(tmp_path, _INSTRUMENT_SET, "2026-04-22-abcdef012345")
    strategies_yaml = _write_strategies_yaml(tmp_path)

    # Broker has 2 long MES @ avg_cost 22600 (= 4520 × 5 × 1 contract),
    # giving notional_usd = 2 × 5 × 4520 = 45_200. At equity 10_000 the
    # effective fraction should be 4.52.
    positions = [
        BrokerPosition(
            symbol="MES", con_id=1, contract_month="202506",
            local_symbol="MESM6", exchange="CME", position=2.0,
            avg_cost=22_600.0, multiplier=5.0,
        ),
    ]
    broker = _fake_broker(equity=10_000.0, positions=positions)

    snap = build_snapshot(
        config=config, broker=broker,
        instrument_set=_INSTRUMENT_SET,
        tracking_since_iso=_TRACK_SINCE,
        run_date=run_date,
        strategies_yaml_path=strategies_yaml,
    )

    assert snap.schema_version == 1
    assert snap.instrument_set == _INSTRUMENT_SET
    assert snap.broker_id == "ibkr-futures"
    assert snap.account.currency == "USD"
    assert snap.account.equity == 10_000.0
    assert snap.account.account_id == "DU123456"
    assert snap.portfolio_leverage == 5.0

    # Positions: one MES long mapped to portfolio symbol ES.
    assert len(snap.positions) == 1
    pos = snap.positions[0]
    assert pos.instrument == "ES"
    assert pos.side == "LONG"
    assert pos.amount == 2.0
    # effective_fraction = 2 × 5 × 4520 / 10_000 = 4.52
    assert pos.effective_fraction == pytest.approx(4.52)

    # Fill from audit.db: 2 contracts BUY, fill 4520.50 vs bar close 4520.00,
    # multiplier 5 → slippage_amount = +1 × 0.50 × 5 × 2 = +5.00 USD.
    assert len(snap.fills_today) == 1
    fill = snap.fills_today[0]
    assert fill.instrument == "ES"
    assert fill.side == "BUY"
    assert fill.slippage_amount == pytest.approx(5.00)

    # transactions_since contains the same single row.
    assert len(snap.transactions_since) == 1
    tx = snap.transactions_since[0]
    assert tx.instrument == "ES"
    # Commission sign-flipped: audit stored 0.62 → stored as -0.62 (paid).
    assert tx.commission_amount == pytest.approx(-0.62)

    assert snap.targets == {"ES": 0.3, "CL": -0.2}
    assert snap.is_v2 is True
    assert snap.dataset_version == "2026-04-22-abcdef012345"

    # Round-trip through JSON.
    restored = Snapshot.from_json_dict(snap.to_json_dict())
    assert restored == snap


def test_build_snapshot_handles_missing_targets_gracefully(tmp_path):
    config = _make_config(tmp_path)
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    _seed_audit_db(Path(config.audit.db_path), run_date)
    _write_dataset_manifest(tmp_path, _INSTRUMENT_SET, "vtest")
    strategies_yaml = _write_strategies_yaml(tmp_path)

    snap = build_snapshot(
        config=config, broker=_fake_broker(),
        instrument_set=_INSTRUMENT_SET,
        tracking_since_iso=_TRACK_SINCE,
        run_date=run_date,
        strategies_yaml_path=strategies_yaml,
    )
    assert snap.targets == {}
    # is_v2 still comes from config, unaffected by missing targets file.
    assert snap.is_v2 is True


def test_build_snapshot_disabled_position_is_short(tmp_path):
    """Short position → side="SHORT"; effective_fraction uses |position|."""
    config = _make_config(tmp_path)
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    _seed_audit_db(Path(config.audit.db_path), run_date)
    _write_targets(tmp_path, run_date, {}, is_v2=False)
    _write_close_prices(tmp_path, run_date, {"CL": 75.0})
    _write_dataset_manifest(tmp_path, _INSTRUMENT_SET, "vtest")
    strategies_yaml = _write_strategies_yaml(tmp_path)

    positions = [
        BrokerPosition(
            symbol="MCL", con_id=2, contract_month="202507",
            local_symbol="MCLN7", exchange="NYMEX", position=-3.0,
            avg_cost=7_500.0, multiplier=100.0,
        ),
    ]
    broker = _fake_broker(equity=20_000.0, positions=positions)
    snap = build_snapshot(
        config=config, broker=broker,
        instrument_set=_INSTRUMENT_SET,
        tracking_since_iso=_TRACK_SINCE,
        run_date=run_date,
        strategies_yaml_path=strategies_yaml,
    )
    pos = snap.positions[0]
    assert pos.instrument == "CL"
    assert pos.side == "SHORT"
    assert pos.amount == 3.0
    # 3 × 100 × 75 / 20000 = 1.125
    assert pos.effective_fraction == pytest.approx(1.125)


def test_eur_account_applies_usd_to_eur_fx_conversion(tmp_path, monkeypatch):
    """EUR-base account holding USD-denominated futures: effective_fraction
    must scale USD notional by (1/eurusd) before dividing by EUR equity.
    """
    config = _make_config(tmp_path)
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    _seed_audit_db(Path(config.audit.db_path), run_date)
    _write_targets(tmp_path, run_date, {}, is_v2=True)
    _write_close_prices(tmp_path, run_date, {"ES": 4520.0})
    _write_dataset_manifest(tmp_path, _INSTRUMENT_SET, "vtest")
    strategies_yaml = _write_strategies_yaml(tmp_path)

    # Patch EUR/USD fetch to a deterministic 1.10
    from futures_executor.monitoring import snapshot as snap_module
    monkeypatch.setattr(snap_module, "_eurusd_spot", lambda _broker: 1.10)

    positions = [
        BrokerPosition(
            symbol="MES", con_id=1, contract_month="202506",
            local_symbol="MESM6", exchange="CME", position=10.0,
            avg_cost=22_600.0, multiplier=5.0,
        ),
    ]
    broker = _fake_broker(
        equity=1_000_000.0, currency="EUR", positions=positions,
    )
    snap = build_snapshot(
        config=config, broker=broker,
        instrument_set=_INSTRUMENT_SET,
        tracking_since_iso=_TRACK_SINCE,
        run_date=run_date,
        strategies_yaml_path=strategies_yaml,
    )
    pos = snap.positions[0]
    # notional_usd = 10 × 5 × 4520          = 226_000 USD
    # notional_eur = 226_000 / 1.10         = 205_454.55 EUR
    # effective_fraction = 205_454.55 / 1_000_000 = 0.20545
    assert pos.effective_fraction == pytest.approx(0.20545, rel=1e-4)
    assert snap.account.currency == "EUR"


def test_write_snapshot_creates_canonical_path(tmp_path):
    config = _make_config(tmp_path)
    run_date = "2026-05-01"
    _seed_audit_db(Path(config.audit.db_path), run_date)
    _write_targets(tmp_path, run_date, {}, is_v2=True)
    _write_dataset_manifest(tmp_path, _INSTRUMENT_SET, "vtest")
    strategies_yaml = _write_strategies_yaml(tmp_path)

    snap = build_snapshot(
        config=config, broker=_fake_broker(),
        instrument_set=_INSTRUMENT_SET,
        tracking_since_iso=_TRACK_SINCE,
        run_date=run_date,
        strategies_yaml_path=strategies_yaml,
    )
    path = write_snapshot(snap, Path(config.monitor.r_factory_artifacts_dir))
    assert path.exists()
    assert path.name == f"{run_date}.json"
    assert path.parent.name == "snapshots"
    assert path.parent.parent.name == _INSTRUMENT_SET

    reloaded = Snapshot.read(path)
    assert reloaded.run_date == run_date
    assert reloaded.instrument_set == _INSTRUMENT_SET
    assert reloaded.broker_id == "ibkr-futures"


# ----- realized_pnl migration + read coverage (2026-04-29) -----

def test_audit_db_migration_adds_realized_pnl_column_idempotently(tmp_path):
    """Pre-2026-04-29 audit.db lacks the realized_pnl column. Opening it via
    AuditLog should add the column, populating NULL on existing rows. Re-opening
    must be a no-op (idempotent) so daily startup doesn't churn.
    """
    from futures_executor.monitoring.audit import AuditLog
    db_path = tmp_path / "old_audit.db"

    # Seed an OLD-shape executions table (no realized_pnl column).
    _seed_audit_db(db_path, "2026-04-28")

    # First open: migration runs, column added.
    AuditLog(db_path).close()
    cols_after_first = {
        row[1] for row in
        sqlite3.connect(str(db_path)).execute("PRAGMA table_info(executions)").fetchall()
    }
    assert "realized_pnl" in cols_after_first

    # Second open: SCHEMA's CREATE-IF-NOT-EXISTS skips, _migrate sees column
    # already present and does nothing. Validates the no-op path.
    AuditLog(db_path).close()


def test_row_to_transaction_reads_realized_pnl_with_usd_to_account_conversion(tmp_path):
    """Snapshot's _row_to_transaction must surface IBKR's CommissionReport.realizedPNL
    (USD on this account) into TransactionSnap.realized_pnl_amount in account
    currency. Today's GC partial-close is the canonical case — this is what
    fixes monitor's balance-jump check on partial-close days.
    """
    from futures_executor.monitoring.audit import AuditLog
    from futures_executor.monitoring.snapshot import _row_to_transaction

    db_path = tmp_path / "audit.db"
    audit = AuditLog(db_path)
    audit.log_execution(
        run_date="2026-04-28", symbol="MGC", event_type="adjustment",
        action="SELL", quantity=1, fill_price=4680.6, bar_close=4682.6,
        commission=0.97, realized_pnl=-1746.0, status="Filled",
    )
    audit.close()

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM executions").fetchone()

    # USD→EUR conversion = 1 / 1.10 ≈ 0.9091
    tx = _row_to_transaction(
        row, usd_to_account=1 / 1.10, exec_to_portfolio={"MGC": "GC"},
    )
    assert tx.instrument == "GC"
    assert tx.realized_pnl_amount == pytest.approx(-1746.0 / 1.10, rel=1e-6)
    # Commission negated + converted (signed-as-cost convention).
    assert tx.commission_amount == pytest.approx(-0.97 / 1.10, rel=1e-6)
    conn.close()


def test_row_to_transaction_handles_null_realized_pnl_pre_migration(tmp_path):
    """Rows logged before the migration have NULL realized_pnl. Reader must
    fall back to 0.0 (matching the old "always zero" behavior) rather than
    crashing on the None.
    """
    from futures_executor.monitoring.snapshot import _row_to_transaction

    # Build a row directly via raw insert simulating pre-migration state —
    # AuditLog.__init__ runs the migration so we'd never get NULL via that
    # path on a fresh DB. Use a hand-built sqlite3.Row instead.
    db_path = tmp_path / "raw.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE executions (
            id INTEGER PRIMARY KEY, timestamp TEXT, run_date TEXT, symbol TEXT,
            event_type TEXT, action TEXT, quantity INTEGER,
            fill_price REAL, commission REAL, realized_pnl REAL
        );
    """)
    conn.execute(
        "INSERT INTO executions (timestamp, run_date, symbol, event_type, action, "
        "quantity, fill_price, commission, realized_pnl) VALUES "
        "(?, ?, ?, ?, ?, ?, ?, ?, NULL)",
        ("2026-04-28T21:30:00+00:00", "2026-04-28", "MGC", "adjustment",
         "SELL", 1, 4680.6, 0.97),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM executions").fetchone()
    tx = _row_to_transaction(row, usd_to_account=1.0, exec_to_portfolio={"MGC": "GC"})
    assert tx.realized_pnl_amount == 0.0
    conn.close()
