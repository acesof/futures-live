"""Tests for futures_executor.execution.broker.BrokerConnection.

Mocks ib_insync IB; exercises the data-extraction surfaces (account info,
position filtering, fill info — including the realizedPNL sentinel filter
added 2026-04-29).

Phase 1 of strategy attribution reads broker equity at the top of every
cycle; these tests pin the extraction shapes so Phase 1 can rely on them.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from futures_executor.config.loader import BrokerSettings
from futures_executor.execution.broker import (
    AccountInfo,
    BrokerConnection,
    BrokerPosition,
    FillInfo,
)


@pytest.fixture
def broker():
    """BrokerConnection with a mocked ib_insync.IB."""
    settings = BrokerSettings()
    b = BrokerConnection(settings)
    b.ib = MagicMock()
    b._connected = True  # skip real connect()
    return b


def _account_item(tag: str, value: str, currency: str = "USD"):
    """Mimic an ib_insync AccountValue."""
    item = SimpleNamespace()
    item.tag = tag
    item.value = value
    item.currency = currency
    return item


# ---------------------------------------------------------------------------
# AccountInfo extraction
# ---------------------------------------------------------------------------

def test_get_account_info_extracts_all_four_tags(broker):
    broker.ib.accountSummary.return_value = [
        _account_item("NetLiquidation", "1053527.79", "EUR"),
        _account_item("BuyingPower", "5267638.95", "EUR"),
        _account_item("UnrealizedPnL", "-1132.50", "EUR"),
        _account_item("RealizedPnL", "0.0", "EUR"),
    ]

    info = broker.get_account_info()

    assert isinstance(info, AccountInfo)
    assert info.equity == pytest.approx(1053527.79)
    assert info.buying_power == pytest.approx(5267638.95)
    assert info.unrealized_pnl == pytest.approx(-1132.50)
    assert info.realized_pnl == pytest.approx(0.0)
    assert info.currency == "EUR"


def test_get_account_info_ignores_unknown_tags(broker):
    """ib's accountSummary returns dozens of tags; we only pick the 4 we use."""
    broker.ib.accountSummary.return_value = [
        _account_item("NetLiquidation", "10000.0", "USD"),
        _account_item("AccruedDividend", "5.0", "USD"),
        _account_item("DayTradesRemaining", "3", "USD"),
        _account_item("Currency", "USD", "USD"),
    ]

    info = broker.get_account_info()
    assert info.equity == pytest.approx(10000.0)
    assert info.buying_power == 0.0  # no BuyingPower row → default


def test_get_account_info_currency_taken_from_NetLiquidation_row(broker):
    """Currency field is sourced from NetLiquidation specifically; other rows'
    currencies are ignored."""
    broker.ib.accountSummary.return_value = [
        _account_item("NetLiquidation", "1000.0", "EUR"),
        _account_item("BuyingPower", "5000.0", "USD"),  # different ccy — ignored
    ]

    info = broker.get_account_info()
    assert info.currency == "EUR"


def test_get_account_info_empty_summary_returns_defaults(broker):
    broker.ib.accountSummary.return_value = []
    info = broker.get_account_info()
    assert info.equity == 0.0
    assert info.currency == "USD"


# ---------------------------------------------------------------------------
# Positions
# ---------------------------------------------------------------------------

def _position(sec_type: str, symbol: str, position: float, avg_cost: float = 100.0,
              local_symbol: str | None = None, contract_month: str = "202506",
              con_id: int = 1, multiplier: str = "5"):
    """Mimic an ib_insync Position."""
    contract = SimpleNamespace(
        secType=sec_type, symbol=symbol, conId=con_id,
        lastTradeDateOrContractMonth=contract_month,
        localSymbol=local_symbol or f"{symbol}{contract_month[-2:]}",
        exchange="CME", multiplier=multiplier,
    )
    pos = SimpleNamespace()
    pos.contract = contract
    pos.position = position
    pos.avgCost = avg_cost
    return pos


def test_get_positions_filters_to_FUT_and_CONTFUT(broker):
    """Only FUT/CONTFUT positions returned. Stock, FOREX, etc. dropped."""
    broker.ib.positions.return_value = [
        _position("FUT", "ES", 3.0),
        _position("CONTFUT", "MES", -1.0),
        _position("STK", "AAPL", 100.0),       # filtered
        _position("CASH", "EUR.USD", 50000.0),  # filtered
    ]

    positions = broker.get_positions()
    assert len(positions) == 2
    assert {p.symbol for p in positions} == {"ES", "MES"}


def test_get_positions_extracts_BrokerPosition_fields_correctly(broker):
    broker.ib.positions.return_value = [
        _position("FUT", "GC", 1.0, avg_cost=4855.20, multiplier="10"),
    ]
    p = broker.get_positions()[0]
    assert isinstance(p, BrokerPosition)
    assert p.symbol == "GC"
    assert p.position == 1.0
    assert p.avg_cost == pytest.approx(4855.20)
    assert p.multiplier == 10.0


def test_get_positions_handles_missing_multiplier(broker):
    """contract.multiplier may be None; default to 1.0."""
    pos = _position("FUT", "ES", 1.0, multiplier=None)
    pos.contract.multiplier = None  # type: ignore
    broker.ib.positions.return_value = [pos]

    p = broker.get_positions()[0]
    assert p.multiplier == 1.0


def test_get_positions_by_symbol_aggregates_across_contract_months(broker):
    """Two positions on the same root symbol but different months sum."""
    broker.ib.positions.return_value = [
        _position("FUT", "ES", 2.0, contract_month="202506", con_id=1),
        _position("FUT", "ES", 1.0, contract_month="202509", con_id=2),
    ]
    by_sym = broker.get_positions_by_symbol()
    assert "ES" in by_sym
    assert by_sym["ES"].position == pytest.approx(3.0)


def test_get_positions_by_symbol_keeps_separate_symbols(broker):
    broker.ib.positions.return_value = [
        _position("FUT", "ES", 3.0),
        _position("FUT", "GC", 1.0),
    ]
    by_sym = broker.get_positions_by_symbol()
    assert set(by_sym) == {"ES", "GC"}


# ---------------------------------------------------------------------------
# FillInfo extraction (incl. realizedPNL sentinel filter)
# ---------------------------------------------------------------------------

def _fill(shares: float, price: float, commission: float = 0.0, realized_pnl: float | None = 0.0):
    """Mimic an ib_insync Fill."""
    f = SimpleNamespace()
    f.execution = SimpleNamespace(shares=shares, price=price)
    f.commissionReport = SimpleNamespace(commission=commission, realizedPNL=realized_pnl)
    return f


def _trade(fills: list, order_id: int = 1, action: str = "SELL", symbol: str = "MGC", is_done: bool = True):
    """Mimic an ib_insync Trade."""
    t = SimpleNamespace()
    t.fills = fills
    t.order = SimpleNamespace(orderId=order_id, action=action)
    t.contract = SimpleNamespace(symbol=symbol)
    t.orderStatus = SimpleNamespace(status="Filled")
    t.isDone = MagicMock(return_value=is_done)
    return t


def test_get_fill_info_single_fill_extracts_basic_fields(broker):
    trade = _trade(fills=[_fill(shares=1.0, price=4680.6, commission=0.97, realized_pnl=-1746.0)])
    info = broker.get_fill_info(trade)

    assert isinstance(info, FillInfo)
    assert info.order_id == 1
    assert info.symbol == "MGC"
    assert info.action == "SELL"
    assert info.quantity == pytest.approx(1.0)
    assert info.avg_fill_price == pytest.approx(4680.6)
    assert info.commission == pytest.approx(0.97)
    assert info.realized_pnl == pytest.approx(-1746.0)


def test_get_fill_info_multi_fill_computes_volume_weighted_average_price(broker):
    """Two partial fills at different prices → avg_fill_price is volume-weighted."""
    fills = [
        _fill(shares=1.0, price=100.0, commission=0.5, realized_pnl=0.0),
        _fill(shares=2.0, price=101.0, commission=1.0, realized_pnl=0.0),
    ]
    info = broker.get_fill_info(_trade(fills))

    # (1×100 + 2×101) / 3 = 100.667
    assert info.avg_fill_price == pytest.approx((100.0 + 2 * 101.0) / 3)
    assert info.quantity == pytest.approx(3.0)
    assert info.commission == pytest.approx(1.5)


def test_get_fill_info_filters_realized_pnl_sentinel(broker):
    """Opening fills report sys.float_info.max (≈1.79e+308) as 'not
    applicable'. Must NOT be summed — would poison realized_pnl with
    junk. Sentinel filter is the line `if abs(pnl) < 1e30`."""
    fills = [
        _fill(shares=1.0, price=100.0, realized_pnl=1.7976931348623157e+308),  # sentinel
        _fill(shares=1.0, price=100.0, realized_pnl=-50.0),
    ]
    info = broker.get_fill_info(_trade(fills))
    # Sentinel filtered → only the -50.0 contributes
    assert info.realized_pnl == pytest.approx(-50.0)


def test_get_fill_info_handles_None_realized_pnl(broker):
    fills = [
        _fill(shares=1.0, price=100.0, realized_pnl=None),
        _fill(shares=1.0, price=100.0, realized_pnl=10.0),
    ]
    info = broker.get_fill_info(_trade(fills))
    assert info.realized_pnl == pytest.approx(10.0)  # None skipped


def test_get_fill_info_no_fills_returns_zero_values(broker):
    """Trade reports no fills (cancelled before fill?) — safe defaults, no crash."""
    info = broker.get_fill_info(_trade(fills=[]))
    assert info.quantity == 0
    assert info.avg_fill_price == 0.0
    assert info.commission == 0.0
    assert info.realized_pnl == 0.0


def test_get_fill_info_all_negative_realized_pnl_sums_correctly(broker):
    """Two closing fills both losing → sum is more negative."""
    fills = [
        _fill(shares=1.0, price=100.0, realized_pnl=-30.0),
        _fill(shares=1.0, price=100.0, realized_pnl=-40.0),
    ]
    info = broker.get_fill_info(_trade(fills))
    assert info.realized_pnl == pytest.approx(-70.0)


# ---------------------------------------------------------------------------
# Connection state
# ---------------------------------------------------------------------------

def test_is_connected_returns_false_after_disconnect():
    settings = BrokerSettings()
    b = BrokerConnection(settings)
    b.ib = MagicMock()
    b._connected = True
    b.ib.isConnected.return_value = True
    assert b.is_connected is True

    b.disconnect()
    assert b.is_connected is False
    b.ib.disconnect.assert_called_once()


def test_is_connected_false_when_underlying_ib_dropped():
    """Even if our flag says connected, if ib.isConnected() returns False,
    is_connected reports False — handles silent drops."""
    settings = BrokerSettings()
    b = BrokerConnection(settings)
    b.ib = MagicMock()
    b._connected = True
    b.ib.isConnected.return_value = False
    assert b.is_connected is False


def test_double_connect_is_noop():
    """Calling connect when already connected must NOT re-invoke ib.connect."""
    settings = BrokerSettings()
    b = BrokerConnection(settings)
    b.ib = MagicMock()
    b._connected = True
    b.connect()
    b.ib.connect.assert_not_called()
