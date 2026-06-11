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
        working_orders: dict[str, list[str]] | None = None,
        working_orders_raises: bool = False,
    ):
        self._positions = positions
        self.ib = ib or _FakeIB()
        self.spread_orders: list[dict] = []
        self._spread_status = spread_status
        self._fill_price = fill_price
        self._commission = commission
        self._working_orders = working_orders or {}
        self._working_orders_raises = working_orders_raises

    def get_positions(self) -> list[BrokerPosition]:
        return list(self._positions)

    def get_working_orders_by_symbol(self) -> dict[str, list[str]]:
        if self._working_orders_raises:
            raise RuntimeError("simulated reqAllOpenOrders failure")
        return dict(self._working_orders)

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

    def get_fill_info(self, _trade, timeout: float = 30.0) -> FillInfo:
        # `timeout` kwarg accepted for forward-compat with real BrokerConnection
        # signature; ignored by the fake (no real I/O wait). 2026-06-09 bumped
        # spread-roll timeout to 90s in `_execute_roll`.
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


def test_submitted_left_working_does_not_write_state_and_skips(
    tmp_path, monkeypatch,
):
    """[Definitive fix 2026-06-11 — INVERTS the 06-10 policy.] A
    migration_roll left working (Submitted) must NOT write state.json.
    The 06-10 optimistic write trusted ``pair.front`` — exactly the
    value that is wrong when state was stale to begin with; it locked a
    wrong-direction migration into state.json. Left-working orders are
    now resolved by the NEXT cycle: ``reconcile_active_contracts``
    adopts the held month if the order filled off-cycle, and the
    open-order guard skips the symbol if it is still working.

    Symbol stays in ``skip`` (position in flux); state.json untouched.
    """
    monkeypatch.chdir(tmp_path)
    broker = _FakeBroker(
        positions=[_stranded_pos(+1)],
        spread_status="Submitted",
    )
    pairs = {"MGC": _pair_post_advance()}
    om = OrderManager(broker, _make_config())

    records, skip = om.migrate_stranded_positions(pairs, broker.get_positions())

    assert len(records) == 1
    assert records[0]["type"] == "migration_roll"
    assert records[0]["status"] == "Submitted"
    # Position is in flux → excluded from this cycle's sizing.
    assert "MGC" in skip

    # State.json NOT updated — broker truth resolves it next cycle.
    from futures_executor.state import load_executor_state
    state = load_executor_state()
    assert "MGC" not in state.get("active_contracts", {}), (
        "Submitted (left-working) migration_roll must NOT write "
        f"state.json, got {state.get('active_contracts')}"
    )


def test_backward_migration_refused_no_order(tmp_path, monkeypatch):
    """[Definitive fix 2026-06-11 — the 06-10 incident shape.] Broker
    holds a LATER expiry (Sep) than the resolver's front (Aug) — the
    roll already happened and state/front is stale. Migration must be
    REFUSED before any order is placed: rolling back in time is never
    correct. (2026-06-10: this exact shape filled 20 MES contracts
    backward, MESU6 → MESM6.)"""
    monkeypatch.chdir(tmp_path)
    later_pos = BrokerPosition(
        symbol="MGC", con_id=900000001, contract_month="20261029",
        local_symbol="MGCV6", exchange="COMEX",
        position=+1, avg_cost=4500.0, multiplier=10.0,
    )
    broker = _FakeBroker(positions=[later_pos])
    pairs = {"MGC": _pair_post_advance()}  # front=MGCQ6 (Aug 2026)
    om = OrderManager(broker, _make_config())

    records, skip = om.migrate_stranded_positions(pairs, broker.get_positions())

    assert len(records) == 1
    assert records[0]["type"] == "migration_refused_backward"
    assert records[0]["status"] == "FAILED"
    assert "MGC" in skip
    # The invariant of last resort: NO order reaches the broker.
    assert broker.spread_orders == []
    # State.json untouched.
    from futures_executor.state import load_executor_state
    assert "MGC" not in load_executor_state().get("active_contracts", {})


def test_exclude_param_suppresses_migration(tmp_path, monkeypatch):
    """A symbol already skipped by an upstream guard (working order /
    adoption / ambiguous) must not get a migration placed on top."""
    monkeypatch.chdir(tmp_path)
    broker = _FakeBroker(positions=[_stranded_pos(+1)])
    pairs = {"MGC": _pair_post_advance()}
    om = OrderManager(broker, _make_config())

    records, skip = om.migrate_stranded_positions(
        pairs, broker.get_positions(), exclude={"MGC"},
    )

    assert records == []
    assert skip == set()
    assert broker.spread_orders == []


# ---------------------------------------------------------------------------
# reconcile_active_contracts — broker-truth adoption (definitive fix
# 2026-06-11)
# ---------------------------------------------------------------------------


def test_reconcile_adopts_later_held_month(tmp_path, monkeypatch):
    """Broker holds a LATER month (Oct) than resolver front (Aug) → the
    roll already happened; adopt the held month into state.json, skip
    the symbol one cycle, place NO order."""
    monkeypatch.chdir(tmp_path)
    later_pos = BrokerPosition(
        symbol="MGC", con_id=900000001, contract_month="20261029",
        local_symbol="MGCV6", exchange="COMEX",
        position=+1, avg_cost=4500.0, multiplier=10.0,
    )
    broker = _FakeBroker(positions=[later_pos])
    pairs = {"MGC": _pair_post_advance()}  # front=MGCQ6 (Aug 2026)
    om = OrderManager(broker, _make_config())

    records, skip = om.reconcile_active_contracts(pairs, broker.get_positions())

    assert len(records) == 1
    assert records[0]["type"] == "contract_adoption"
    assert records[0]["status"] == "ADOPTED"
    assert records[0]["from_month"] == "20260828"
    assert records[0]["to_month"] == "20261029"
    assert skip == {"MGC"}
    assert broker.spread_orders == []

    from futures_executor.state import load_executor_state
    assert load_executor_state()["active_contracts"]["MGC"] == "20261029"


def test_reconcile_consistent_position_is_noop(tmp_path, monkeypatch):
    """Held month == pair.front → nothing to reconcile."""
    monkeypatch.chdir(tmp_path)
    pos_on_front = BrokerPosition(
        symbol="MGC", con_id=732156883, contract_month="20260828",
        local_symbol="MGCQ6", exchange="COMEX",
        position=+1, avg_cost=4500.0, multiplier=10.0,
    )
    broker = _FakeBroker(positions=[pos_on_front])
    pairs = {"MGC": _pair_post_advance()}
    om = OrderManager(broker, _make_config())

    records, skip = om.reconcile_active_contracts(pairs, broker.get_positions())

    assert records == []
    assert skip == set()


def test_reconcile_leaves_earlier_strand_for_migration(tmp_path, monkeypatch):
    """Held month EARLIER than front (the legitimate buffer-advance
    strand) is NOT adopted — it must flow to migrate_stranded_positions
    for a forward migration."""
    monkeypatch.chdir(tmp_path)
    broker = _FakeBroker(positions=[_stranded_pos(+1)])  # MGCM6 (June)
    pairs = {"MGC": _pair_post_advance()}  # front=MGCQ6 (Aug)
    om = OrderManager(broker, _make_config())

    records, skip = om.reconcile_active_contracts(pairs, broker.get_positions())

    assert records == []
    assert skip == set()
    from futures_executor.state import load_executor_state
    assert "MGC" not in load_executor_state().get("active_contracts", {})


def test_reconcile_multi_month_ambiguous_refuses(tmp_path, monkeypatch):
    """Positions on 2+ months for one symbol (mid-roll partial) →
    ambiguous; refuse to trade the symbol, FAILED record, no state
    write."""
    monkeypatch.chdir(tmp_path)
    broker = _FakeBroker(positions=[
        _stranded_pos(+1),  # MGCM6
        BrokerPosition(
            symbol="MGC", con_id=732156883, contract_month="20260828",
            local_symbol="MGCQ6", exchange="COMEX",
            position=+2, avg_cost=4500.0, multiplier=10.0,
        ),
    ])
    pairs = {"MGC": _pair_post_advance()}
    om = OrderManager(broker, _make_config())

    records, skip = om.reconcile_active_contracts(pairs, broker.get_positions())

    assert len(records) == 1
    assert records[0]["type"] == "contract_ambiguous"
    assert records[0]["status"] == "FAILED"
    assert skip == {"MGC"}
    from futures_executor.state import load_executor_state
    assert "MGC" not in load_executor_state().get("active_contracts", {})


# ---------------------------------------------------------------------------
# Open-order guard (definitive fix 2026-06-11)
# ---------------------------------------------------------------------------


def test_open_order_guard_skips_symbol(tmp_path, monkeypatch):
    """A working order from a previous session → symbol skipped with a
    loud FAILED record (routes to notify_error + exit 1)."""
    monkeypatch.chdir(tmp_path)
    broker = _FakeBroker(
        positions=[],
        working_orders={"MGC": ["orderId=276 permId=123 secType=BAG "
                                "status=Submitted filled=1.0 remaining=19.0"]},
    )
    pairs = {"MGC": _pair_post_advance()}
    om = OrderManager(broker, _make_config())

    records, skip = om._skip_symbols_with_working_orders(pairs)

    assert len(records) == 1
    assert records[0]["type"] == "open_order_skip"
    assert records[0]["status"] == "FAILED"
    assert "previous session" in records[0]["error"]
    assert skip == {"MGC"}


def test_open_order_guard_clean_scan_is_noop(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    broker = _FakeBroker(positions=[])
    pairs = {"MGC": _pair_post_advance()}
    om = OrderManager(broker, _make_config())

    records, skip = om._skip_symbols_with_working_orders(pairs)

    assert records == []
    assert skip == set()


def test_open_order_guard_scan_failure_fail_closed(tmp_path, monkeypatch):
    """If the open-order scan itself raises we cannot prove no order is
    working → ALL symbols excluded (miss a trade, never wrong trade)."""
    monkeypatch.chdir(tmp_path)
    broker = _FakeBroker(positions=[], working_orders_raises=True)
    pairs = {"MGC": _pair_post_advance()}
    om = OrderManager(broker, _make_config())

    records, skip = om._skip_symbols_with_working_orders(pairs)

    assert len(records) == 1
    assert records[0]["type"] == "open_order_scan_failed"
    assert records[0]["status"] == "FAILED"
    assert skip == {"MGC"}


def test_empty_sp_exchange_falls_back_to_pair_front(tmp_path, monkeypatch):
    """[#228 cascade fix 2026-06-10] ``sp.exchange`` from BrokerPosition is
    sometimes empty (IBKR API quirk on futures). The synthetic stranded
    ResolvedContract must use a non-empty exchange — fallback chain:
    ``qualified[0].exchange`` → ``sp.exchange`` → ``pair.front.exchange``.

    On 2026-06-09 cron, all three were involved in producing the BAG legs'
    empty ``exchange`` field, triggering ``Error 321: Missing order
    exchange`` on IBKR. This regression confirms the fallback works.
    """
    monkeypatch.chdir(tmp_path)
    # Build a stranded position with empty exchange (the broker-side bug).
    sp = BrokerPosition(
        symbol="MGC", con_id=12345, contract_month="20260626",
        local_symbol="MGCM6", exchange="",  # ← empty, the bug shape
        position=1.0, avg_cost=4500.0, multiplier=10.0,
        unrealized_pnl=0.0, realized_pnl=0.0, market_price=4500.0,
    )
    broker = _FakeBroker(positions=[sp], spread_status="Filled")
    pairs = {"MGC": _pair_post_advance()}  # pair.front.exchange = "COMEX"
    om = OrderManager(broker, _make_config())

    records, skip = om.migrate_stranded_positions(pairs, broker.get_positions())

    assert len(records) == 1
    assert records[0]["type"] == "migration_roll"
    # Spread order constructed and returned Filled — confirms the
    # exchange fallback chain produced a non-empty value. Pre-fix,
    # ``stranded_resolved.exchange = sp.exchange = ''`` would have
    # propagated into place_spread_order's BAG construction.
    assert records[0]["status"] == "Filled"
    # State.json reflects the migration.
    from futures_executor.state import load_executor_state
    assert load_executor_state().get("active_contracts", {}).get("MGC") == (
        pairs["MGC"].front.expiry_str
    )


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
