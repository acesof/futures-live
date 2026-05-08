"""Tests for OrderManager._resolve_close_contract — task #214 defense-in-depth.

Covers the contract-selection logic for close/reduce/reconcile orders so
that they target the contract that ACTUALLY HOLDS the position rather
than blindly using ``pair.front``. The 2026-05-08 incident showed the
failure mode: state.json drifted from broker truth (cli.py state-overwrite
bug, since fixed in commit 95b2a1b), and ``_execute_adjustment`` used
``pair.front`` which pointed at the wrong contract — opening a SHORT in
MCLM6 when the intent was to close a LONG in MCLN6.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from futures_executor.config.loader import (
    ExecutionSettings,
    ExecutorConfig,
    InstrumentSettings,
    RollSettings,
    SafetySettings,
    VolTargetSettings,
)
from futures_executor.execution.broker import BrokerPosition
from futures_executor.execution.order_manager import OrderManager


# ---------------------------------------------------------------------------
# Fakes — minimal stand-ins for ib_insync + BrokerConnection
# ---------------------------------------------------------------------------


@dataclass
class _FakeContract:
    """Stand-in for ib_insync.Contract."""
    conId: int = 0
    symbol: str = ""
    exchange: str = ""
    localSymbol: str = ""


class _FakeIB:
    """qualifyContracts fake — returns the input contract by default."""

    def __init__(self, qualified_local_symbol_for_conid: dict[int, str] | None = None,
                 qualify_fails: bool = False):
        self._map = qualified_local_symbol_for_conid or {}
        self._fail = qualify_fails

    def qualifyContracts(self, *contracts):
        if self._fail:
            return []
        out = []
        for c in contracts:
            if hasattr(c, "conId") and c.conId in self._map:
                c.localSymbol = self._map[c.conId]
            out.append(c)
        return out


class _FakeBroker:
    """Fake BrokerConnection exposing only what _resolve_close_contract uses."""

    def __init__(self, positions: list[BrokerPosition], ib: _FakeIB | None = None):
        self._positions = positions
        self.ib = ib or _FakeIB()

    def get_positions(self) -> list[BrokerPosition]:
        return list(self._positions)


@dataclass
class _FakeContractInfo:
    """Stand-in for ContractInfo — only the bits ContractPair needs."""
    contract: _FakeContract
    local_symbol: str = ""
    expiry_str: str = ""
    multiplier: float = 100.0


@dataclass
class _FakePair:
    """Stand-in for ContractPair."""
    symbol: str
    front: _FakeContractInfo
    next: Any = None
    roll_due: bool = False
    hard_deadline: bool = False
    days_to_expiry: int = 30


def _make_config() -> ExecutorConfig:
    """Minimal config sufficient for OrderManager construction."""
    return ExecutorConfig(
        instruments=[InstrumentSettings(symbol="MCL", exchange="NYMEX",
                                        portfolio_symbol="CL",
                                        multiplier=100.0, margin=1000.0)],
        roll=RollSettings(),
        execution=ExecutionSettings(),
        vol_target=VolTargetSettings(),
        safety=SafetySettings(),
    )


def _pos(local_symbol: str, con_id: int, position: float,
         symbol: str = "MCL", exchange: str = "NYMEX") -> BrokerPosition:
    return BrokerPosition(
        symbol=symbol,
        con_id=con_id,
        contract_month="20260618" if local_symbol.endswith("N6") else "20260518",
        local_symbol=local_symbol,
        exchange=exchange,
        position=position,
        avg_cost=100.0,
        multiplier=100.0,
    )


def _pair_with_front(local_symbol: str, con_id: int = 661016544) -> _FakePair:
    front_info = _FakeContractInfo(
        contract=_FakeContract(conId=con_id, symbol="MCL",
                               exchange="NYMEX", localSymbol=local_symbol),
        local_symbol=local_symbol,
        expiry_str="20260518" if local_symbol.endswith("M6") else "20260618",
    )
    return _FakePair(symbol="MCL", front=front_info)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_resolves_to_position_contract_when_single_match():
    """Single existing position → use its con_id, not pair.front."""
    # Position is in MCLN6 (back-month, conId=661016559); pair.front is
    # MCLM6 (front-month, conId=661016544). The close should target
    # MCLN6 — exactly the case that was broken on 2026-05-08.
    broker = _FakeBroker(
        positions=[_pos("MCLN6", con_id=661016559, position=+7)],
        ib=_FakeIB(qualified_local_symbol_for_conid={661016559: "MCLN6"}),
    )
    pair = _pair_with_front("MCLM6", con_id=661016544)

    om = OrderManager(broker, _make_config())
    contract, source = om._resolve_close_contract("MCL", pair)

    assert source == "position"
    assert contract.conId == 661016559
    assert contract.localSymbol == "MCLN6"


def test_falls_back_to_pair_front_when_no_existing_position():
    """No existing position → genuine new open; use pair.front."""
    broker = _FakeBroker(positions=[])
    pair = _pair_with_front("MCLM6", con_id=661016544)

    om = OrderManager(broker, _make_config())
    contract, source = om._resolve_close_contract("MCL", pair)

    assert source == "pair.front"
    assert contract is pair.front.contract
    assert contract.conId == 661016544


def test_warns_and_falls_back_on_split_position():
    """Position split across multiple contracts → log error, fall back to
    pair.front + flag the SPLIT marker so callers / readers see it."""
    broker = _FakeBroker(positions=[
        _pos("MCLN6", con_id=661016559, position=+7),
        _pos("MCLM6", con_id=661016544, position=-7),
    ])
    pair = _pair_with_front("MCLM6", con_id=661016544)

    om = OrderManager(broker, _make_config())
    contract, source = om._resolve_close_contract("MCL", pair)

    assert source == "pair.front+SPLIT"
    assert contract is pair.front.contract


def test_zero_position_treated_as_no_match():
    """A row with position=0 does not count as a holding."""
    broker = _FakeBroker(positions=[
        _pos("MCLN6", con_id=661016559, position=0),  # stale row, flat
    ])
    pair = _pair_with_front("MCLM6", con_id=661016544)

    om = OrderManager(broker, _make_config())
    contract, source = om._resolve_close_contract("MCL", pair)

    assert source == "pair.front"
    assert contract.conId == 661016544


def test_falls_back_on_qualify_failure():
    """Defensive: if qualifyContracts can't resolve our con_id (rare but
    possible during contract-roll boundary), fall back to pair.front."""
    broker = _FakeBroker(
        positions=[_pos("MCLN6", con_id=661016559, position=+7)],
        ib=_FakeIB(qualify_fails=True),
    )
    pair = _pair_with_front("MCLM6", con_id=661016544)

    om = OrderManager(broker, _make_config())
    contract, source = om._resolve_close_contract("MCL", pair)

    assert source == "pair.front"
    assert contract is pair.front.contract


def test_position_contract_takes_precedence_when_pair_front_disagrees(caplog):
    """Sanity-check the warning path: when pair.front and the
    position-holding contract disagree, we log a clear message + use the
    position-holding contract. Different-symbol case won't happen in
    practice, but the log helps operators trace the decision."""
    broker = _FakeBroker(
        positions=[_pos("MCLN6", con_id=661016559, position=+7)],
        ib=_FakeIB(qualified_local_symbol_for_conid={661016559: "MCLN6"}),
    )
    pair = _pair_with_front("MCLM6", con_id=661016544)

    om = OrderManager(broker, _make_config())
    with caplog.at_level("WARNING"):
        contract, source = om._resolve_close_contract("MCL", pair)

    assert source == "position"
    assert contract.localSymbol == "MCLN6"
    # Warning text should mention both local symbols so operators can grep
    # for either.
    msgs = " ".join(r.message for r in caplog.records)
    assert "MCLN6" in msgs and "MCLM6" in msgs
