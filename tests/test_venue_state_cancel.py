"""Tests for the #228 A2 venue-state-conditioned cancel + reconcile skip.

Closes the false-CRITICAL noise from the 30s cancel-on-timer: on an open
venue a working market order will fill ASAP, so cancelling it defeats
the purpose. Only cancel when the venue has actually closed (would
otherwise orphan-fill at reopen — the Memorial-Day failure mode).

The fix uses ``ContractPair.current_session_end`` (stamped at resolve
time from ib_insync's ``tradingSessions()``) so the cancel-time check is
local — no IBKR round-trip mid-run.

See plan: ``/Users/acess/.claude/plans/wise-whistling-flamingo.md``.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from futures_executor.config.loader import (
    ExecutionSettings,
    ExecutorConfig,
    InstrumentSettings,
    RollSettings,
    SafetySettings,
    VolTargetSettings,
)
from futures_executor.data.contract_resolver import ContractPair, ResolvedContract
from futures_executor.execution.broker import BrokerPosition
from futures_executor.execution.order_manager import OrderManager, SizingResult


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


@dataclass
class _FakeContract:
    conId: int = 0
    symbol: str = ""
    exchange: str = ""
    localSymbol: str = ""


class _FakeIB:
    def qualifyContracts(self, *contracts):
        return list(contracts)

    def sleep(self, *_args, **_kw):
        return None


class _FakeBroker:
    """Minimal broker fake — just enough for _venue_still_open and
    _reconcile's skip path."""

    is_connected = True

    def __init__(self, positions: list[BrokerPosition] | None = None):
        self._positions = positions or []
        self.ib = _FakeIB()
        self.placed_market: list[tuple] = []  # for assertions

    def get_positions(self) -> list[BrokerPosition]:
        return list(self._positions)

    def get_positions_by_symbol(self) -> dict[str, BrokerPosition]:
        out: dict[str, BrokerPosition] = {}
        for p in self._positions:
            if p.symbol in out:
                out[p.symbol].position += p.position
            else:
                out[p.symbol] = p
        return out

    def place_market_order(self, contract, action, qty):
        """Record the call; return a dummy "trade" sentinel."""
        self.placed_market.append((contract, action, qty))
        return object()


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


def _resolved() -> ResolvedContract:
    return ResolvedContract(
        symbol="MGC", con_id=732156883, exchange="COMEX", currency="USD",
        expiry=date(2026, 8, 28), expiry_str="20260828",
        multiplier=10.0, local_symbol="MGCQ6", min_tick=0.10,
        contract=_FakeContract(
            conId=732156883, symbol="MGC", exchange="COMEX",
            localSymbol="MGCQ6",
        ),
    )


def _pair(session_end: datetime | None) -> ContractPair:
    return ContractPair(
        symbol="MGC", front=_resolved(), next=None,
        days_to_expiry=65, roll_due=False, hard_deadline=False,
        tradable_now=True, current_session_end=session_end,
    )


# ---------------------------------------------------------------------------
# _venue_still_open helper tests
# ---------------------------------------------------------------------------


def test_venue_still_open_true_when_session_end_in_future():
    """``now < pair.current_session_end`` → True (do NOT cancel)."""
    future = datetime.now(timezone.utc) + timedelta(minutes=10)
    pair = _pair(session_end=future)
    om = OrderManager(_FakeBroker(), _make_config())
    assert om._venue_still_open(pair) is True


def test_venue_still_open_false_when_session_end_in_past():
    """``now > pair.current_session_end`` → False (cancel — venue
    closed, the order would orphan-fill at reopen otherwise)."""
    past = datetime.now(timezone.utc) - timedelta(minutes=10)
    pair = _pair(session_end=past)
    om = OrderManager(_FakeBroker(), _make_config())
    assert om._venue_still_open(pair) is False


def test_venue_still_open_fails_open_when_session_end_none():
    """No session info (gate fail-OPEN path) → also fail-OPEN at cancel
    time. Returning True means "don't cancel" — a working order is left
    to fill on the venue, next run reconciles. Matches the design rule
    "miss a trade, never place a wrong trade.\""""
    pair = _pair(session_end=None)
    om = OrderManager(_FakeBroker(), _make_config())
    assert om._venue_still_open(pair) is True


def test_venue_still_open_accepts_injected_now():
    """``now_utc`` injection lets tests pin the clock — same data, two
    answers depending on injected ``now``."""
    pair_end = datetime(2026, 5, 28, 21, 0, tzinfo=timezone.utc)  # 17:00 ET
    pair = _pair(session_end=pair_end)
    om = OrderManager(_FakeBroker(), _make_config())
    # 16:55 ET → before close → still open.
    assert om._venue_still_open(
        pair, now_utc=datetime(2026, 5, 28, 20, 55, tzinfo=timezone.utc),
    ) is True
    # 17:05 ET → after close → cancel.
    assert om._venue_still_open(
        pair, now_utc=datetime(2026, 5, 28, 21, 5, tzinfo=timezone.utc),
    ) is False


# ---------------------------------------------------------------------------
# _reconcile skip integration
# ---------------------------------------------------------------------------


def test_reconcile_skips_symbols_in_pending_at_disconnect():
    """When a symbol is left with a working order at end of Step 6,
    reconcile must NOT issue a corrective order — that would double-fill
    once both the original and the corrective land."""
    # Broker reports MGC=0; target=1. Without the skip set, reconcile
    # would compute delta=+1 and place a corrective BUY. With the skip,
    # the symbol is excluded from mismatch detection and no corrective
    # order is placed.
    broker = _FakeBroker(positions=[])  # MGC absent → actual_qty=0
    pairs = {"MGC": _pair(session_end=None)}
    sizing = {"MGC": SizingResult(
        symbol="MGC", target_signal=0.5, target_contracts=1,
        notional_per_contract=45_000.0, multiplier=10.0,
        last_price=4500.0,
    )}
    target_contracts = {"MGC": 1}

    om = OrderManager(broker, _make_config())
    records = om._reconcile(
        target_contracts, pairs, sizing,
        pending_at_disconnect={"MGC"},
    )

    assert broker.placed_market == []
    mgc_records = [r for r in records if r.get("symbol") == "MGC"]
    assert mgc_records == []


def test_reconcile_no_skip_arg_preserves_existing_behavior():
    """``pending_at_disconnect`` is an optional kwarg with default
    ``None`` — existing call sites that don't pass it (legacy /
    pre-A2 paths if any) behave unchanged."""
    broker = _FakeBroker(positions=[])
    pairs = {"MGC": _pair(session_end=None)}
    sizing: dict[str, SizingResult] = {}
    target_contracts: dict[str, int] = {}  # no mismatch

    om = OrderManager(broker, _make_config())
    # Call without the kwarg.
    records = om._reconcile(target_contracts, pairs, sizing)
    assert records == []
