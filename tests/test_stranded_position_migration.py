"""Tests for OrderManager.migrate_stranded_positions — #228 buffer-advance
migration gap fix.

When ``contract_resolver.resolve()`` performs a ``delivery_buffer_days``
pointer-swap, any position held on the abandoned contract is silently
stranded: ``compute_position_diff`` aggregates by symbol so the strand
is invisible, and the scheduled-roll path's ``front_qty != 0`` check
looks at the NEW front (= 0). ``migrate_stranded_positions`` closes
that gap by detecting any ``contract_month != pair.front.expiry_str``
position and migrating it via the existing ``_execute_roll``
calendar-spread machinery, with a ``skip_symbols`` failure path that
preserves the miss-trade risk class.

See plan: ``/Users/acess/.claude/plans/wise-whistling-flamingo.md``
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from futures_executor.config.loader import (
    ExecutionSettings,
    ExecutorConfig,
    InstrumentSettings,
    RollSettings,
    SafetySettings,
    VolTargetSettings,
)
from futures_executor.data.contract_resolver import (
    ContractPair,
    ResolvedContract,
)
from futures_executor.execution.broker import BrokerPosition, FillInfo
from futures_executor.execution.order_manager import OrderManager


# ---------------------------------------------------------------------------
# Fakes — minimal stand-ins. Mirrors the pattern in
# tests/test_order_manager_close_contract.py and extends it with
# place_spread_order / get_fill_info + a configurable Trade status.
# ---------------------------------------------------------------------------


@dataclass
class _FakeContract:
    """Stand-in for ib_insync.Contract."""
    conId: int = 0
    symbol: str = ""
    exchange: str = ""
    localSymbol: str = ""


@dataclass
class _FakeOrderStatus:
    status: str = "Filled"


@dataclass
class _FakeTrade:
    orderStatus: _FakeOrderStatus

    def isDone(self) -> bool:
        return self.orderStatus.status in (
            "Filled", "Cancelled", "Rejected", "Inactive",
        )


class _FakeIB:
    """``qualifyContracts`` fake — returns the input contract by default.
    Set ``qualify_fails=True`` to simulate IBKR returning an empty list."""

    def __init__(self, qualify_fails: bool = False):
        self._fail = qualify_fails

    def qualifyContracts(self, *contracts):
        if self._fail:
            return []
        return list(contracts)

    def sleep(self, *_args, **_kw):  # used by broker.get_fill_info wait-loop
        return None


class _FakeBroker:
    """Fake ``BrokerConnection`` — records ``place_spread_order`` calls
    so tests can assert the exact legs/quantity. Returns a configurable
    Trade + FillInfo."""

    def __init__(
        self,
        positions: list[BrokerPosition],
        ib: _FakeIB | None = None,
        spread_status: str = "Filled",
        fill_price: float = 4500.0,
        commission: float = 0.5,
    ):
        self._positions = positions
        self.ib = ib or _FakeIB()
        self.spread_orders: list[dict] = []
        self._spread_status = spread_status
        self._fill_price = fill_price
        self._commission = commission

    def get_positions(self) -> list[BrokerPosition]:
        return list(self._positions)

    def place_spread_order(
        self,
        symbol: str,
        exchange: str,
        currency: str,
        front_con_id: int,
        next_con_id: int,
        quantity: int,
    ):
        self.spread_orders.append({
            "symbol": symbol, "exchange": exchange, "currency": currency,
            "front_con_id": front_con_id, "next_con_id": next_con_id,
            "quantity": quantity,
        })
        return _FakeTrade(orderStatus=_FakeOrderStatus(status=self._spread_status))

    def get_fill_info(self, _trade) -> FillInfo:
        return FillInfo(
            order_id=0, symbol="MGC",
            action="SELL" if self._spread_status == "Filled" else "?",
            quantity=1,
            avg_fill_price=self._fill_price,
            commission=self._commission,
            realized_pnl=0.0,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config() -> ExecutorConfig:
    return ExecutorConfig(
        instruments=[InstrumentSettings(
            symbol="MGC", exchange="COMEX", portfolio_symbol="GC",
            multiplier=10.0, margin=800.0, delivery_buffer_days=33,
        )],
        roll=RollSettings(),
        execution=ExecutionSettings(),
        vol_target=VolTargetSettings(),
        safety=SafetySettings(),
    )


def _resolved(
    expiry_str: str = "20260828",
    local_symbol: str = "MGCQ6",
    con_id: int = 732156883,
) -> ResolvedContract:
    return ResolvedContract(
        symbol="MGC", con_id=con_id, exchange="COMEX", currency="USD",
        expiry=date(
            int(expiry_str[:4]), int(expiry_str[4:6]), int(expiry_str[6:8]),
        ),
        expiry_str=expiry_str,
        multiplier=10.0,
        local_symbol=local_symbol,
        min_tick=0.10,
        contract=_FakeContract(
            conId=con_id, symbol="MGC", exchange="COMEX",
            localSymbol=local_symbol,
        ),
    )


def _pair_post_advance() -> ContractPair:
    """Post-buffer-advance MGC pair: front=MGCQ6 (August, 65d out),
    next=MGCV6 (October)."""
    return ContractPair(
        symbol="MGC",
        front=_resolved("20260828", "MGCQ6", 732156883),
        next=_resolved("20261029", "MGCV6", 800000001),
        days_to_expiry=65,
        roll_due=False,
        hard_deadline=False,
        tradable_now=True,
    )


def _stranded_pos(position: float = 1.0) -> BrokerPosition:
    """A position on MGCM6 (June, the contract MGC just abandoned)."""
    return BrokerPosition(
        symbol="MGC", con_id=712565978, contract_month="20260626",
        local_symbol="MGCM6", exchange="COMEX",
        position=position, avg_cost=4500.0, multiplier=10.0,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_field_default_buffer_advanced_from_is_none():
    """``ContractPair.buffer_advanced_from`` defaults to ``None`` so
    existing construction sites (``order_manager.py:404`` post-roll,
    ``test_order_manager_close_contract.py`` fakes) keep working."""
    pair = _pair_post_advance()
    assert pair.buffer_advanced_from is None


def test_long_strand_migrated_via_calendar_spread(tmp_path, monkeypatch):
    """+1 on MGCM6, pair.front = MGCQ6 → BAG combo placed with
    front_con_id=MGCM6, next_con_id=MGCQ6, quantity=+1; record marked
    ``migration_roll`` Filled; ``active_contracts['MGC'] = 20260828``."""
    monkeypatch.chdir(tmp_path)  # so state.json lands in tmp_path/data/
    broker = _FakeBroker(positions=[_stranded_pos(+1)])
    pairs = {"MGC": _pair_post_advance()}
    om = OrderManager(broker, _make_config())

    records, skip = om.migrate_stranded_positions(pairs, broker.get_positions())

    assert len(records) == 1
    rec = records[0]
    assert rec["type"] == "migration_roll"
    assert rec["status"] == "Filled"
    assert rec["from_month"] == "20260626"  # abandoned MGCM6
    assert rec["to_month"] == "20260828"    # new front MGCQ6
    assert rec["quantity"] == 1
    assert skip == set()

    # The BAG combo legs + sign convention.
    assert len(broker.spread_orders) == 1
    so = broker.spread_orders[0]
    assert so["front_con_id"] == 712565978   # MGCM6 (close leg)
    assert so["next_con_id"] == 732156883    # MGCQ6 (open leg)
    assert so["quantity"] == 1
    assert so["exchange"] == "COMEX"
    assert so["currency"] == "USD"

    # State updated.
    from futures_executor.state import load_executor_state
    state = load_executor_state()
    assert state.get("active_contracts", {}).get("MGC") == "20260828"


def test_short_strand_passes_negative_quantity(tmp_path, monkeypatch):
    """−1 on MGCM6 → ``quantity=-1`` passed through to
    ``place_spread_order`` (per broker.py:262-266 convention: negative
    = BUY front / SELL next, the short-close direction)."""
    monkeypatch.chdir(tmp_path)
    broker = _FakeBroker(positions=[_stranded_pos(-1)])
    pairs = {"MGC": _pair_post_advance()}
    om = OrderManager(broker, _make_config())

    records, skip = om.migrate_stranded_positions(pairs, broker.get_positions())

    assert len(records) == 1
    assert records[0]["status"] == "Filled"
    assert records[0]["quantity"] == -1
    assert broker.spread_orders[0]["quantity"] == -1
    assert skip == set()


def test_no_strand_is_no_op(tmp_path, monkeypatch):
    """Position on pair.front (MGCQ6) is NOT stranded → empty records,
    empty skip, no spread order. Also covers the empty-broker case."""
    monkeypatch.chdir(tmp_path)
    pos_on_front = BrokerPosition(
        symbol="MGC", con_id=732156883, contract_month="20260828",
        local_symbol="MGCQ6", exchange="COMEX",
        position=+1, avg_cost=4500.0, multiplier=10.0,
    )
    broker = _FakeBroker(positions=[pos_on_front])
    pairs = {"MGC": _pair_post_advance()}
    om = OrderManager(broker, _make_config())

    records, skip = om.migrate_stranded_positions(pairs, broker.get_positions())

    assert records == []
    assert skip == set()
    assert broker.spread_orders == []


def test_combo_rejected_adds_to_skip_no_state_write(tmp_path, monkeypatch):
    """Spread order returns non-Filled status → record carries that
    status, symbol in ``skip``, state.json untouched. Preserves the
    miss-trade-not-wrong-trade risk class: the rest of
    ``execute_rebalance`` will skip this symbol entirely."""
    monkeypatch.chdir(tmp_path)
    broker = _FakeBroker(
        positions=[_stranded_pos(+1)],
        spread_status="Cancelled",
    )
    pairs = {"MGC": _pair_post_advance()}
    om = OrderManager(broker, _make_config())

    records, skip = om.migrate_stranded_positions(pairs, broker.get_positions())

    assert len(records) == 1
    assert records[0]["type"] == "migration_roll"
    assert records[0]["status"] == "Cancelled"
    assert "MGC" in skip

    # State NOT updated (the stranded position is still on the broker).
    from futures_executor.state import load_executor_state
    state = load_executor_state()
    assert "MGC" not in state.get("active_contracts", {})


def test_qualify_fails_blocked_no_spread_order(tmp_path, monkeypatch):
    """``qualifyContracts`` returns empty for the stranded ``con_id`` →
    record ``migration_blocked``, symbol in ``skip``, NO spread order
    attempted."""
    monkeypatch.chdir(tmp_path)
    broker = _FakeBroker(
        positions=[_stranded_pos(+1)],
        ib=_FakeIB(qualify_fails=True),
    )
    pairs = {"MGC": _pair_post_advance()}
    om = OrderManager(broker, _make_config())

    records, skip = om.migrate_stranded_positions(pairs, broker.get_positions())

    assert len(records) == 1
    assert records[0]["type"] == "migration_blocked"
    assert records[0]["status"] == "FAILED"
    assert "qualifyContracts failed" in records[0]["error"]
    assert "MGC" in skip
    assert broker.spread_orders == []


def test_zero_position_strand_ignored(tmp_path, monkeypatch):
    """A row with ``position=0`` on a non-front contract is NOT treated
    as stranded (flat / closed-out leftover)."""
    monkeypatch.chdir(tmp_path)
    broker = _FakeBroker(positions=[_stranded_pos(0)])
    pairs = {"MGC": _pair_post_advance()}
    om = OrderManager(broker, _make_config())

    records, skip = om.migrate_stranded_positions(pairs, broker.get_positions())

    assert records == []
    assert skip == set()
    assert broker.spread_orders == []
