"""IB Gateway broker connection and order execution."""

import logging
from dataclasses import dataclass, field
from typing import Any

from ib_insync import (
    IB,
    ComboLeg,
    Contract,
    MarketOrder,
    LimitOrder,
    Trade,
)

from futures_executor.config.loader import BrokerSettings

logger = logging.getLogger(__name__)


@dataclass
class AccountInfo:
    equity: float = 0.0
    buying_power: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    currency: str = "USD"


@dataclass
class BrokerPosition:
    symbol: str
    con_id: int
    contract_month: str
    local_symbol: str
    exchange: str
    position: float  # signed: +long, -short
    avg_cost: float
    multiplier: float
    # Phase β (2026-05-01): broker-truth P/L per position. Sourced from
    # ib.portfolio() (PortfolioItem) which carries unrealizedPNL +
    # realizedPNL automatically — no reqPnLSingle subscription needed.
    # Account currency, native sign (positive = profit). Default 0.0
    # for callers that build BrokerPosition without a portfolio() lookup
    # (e.g. tests, or get_positions_by_symbol's aggregation path).
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    market_price: float = 0.0  # current mark; useful for dashboard sanity-check

    @property
    def display_contract(self) -> str:
        """Human-readable contract display with symbol and expiry."""
        if self.local_symbol and self.contract_month:
            return f"{self.local_symbol} ({self.contract_month})"
        return self.local_symbol or self.contract_month or "-"


@dataclass
class FillInfo:
    order_id: int
    symbol: str
    action: str  # BUY or SELL
    quantity: float
    avg_fill_price: float
    commission: float = 0.0
    # Sum of realizedPNL across constituent fills, in account currency.
    # IBKR populates `CommissionReport.realizedPNL` only on the closing leg
    # of a position — opening trades report 0/NaN. Per ib_insync docs the
    # value is already in account currency, no conversion needed. Sign
    # convention: positive = profit on close, negative = loss.
    realized_pnl: float = 0.0


class BrokerConnection:
    """Manages connection to IB Gateway and provides order/position interfaces."""

    def __init__(self, settings: BrokerSettings):
        self.settings = settings
        self.ib = IB()
        self._connected = False

    def connect(self) -> None:
        """Connect to IB Gateway."""
        if self._connected:
            return
        logger.info(
            f"Connecting to IB Gateway at {self.settings.host}:{self.settings.port} "
            f"(clientId={self.settings.client_id})"
        )
        self.ib.connect(
            self.settings.host,
            self.settings.port,
            clientId=self.settings.client_id,
            readonly=self.settings.readonly,
            timeout=self.settings.timeout,
        )
        self._connected = True
        logger.info("Connected to IB Gateway")

    def disconnect(self) -> None:
        """Disconnect from IB Gateway."""
        if self._connected:
            self.ib.disconnect()
            self._connected = False
            logger.info("Disconnected from IB Gateway")

    @property
    def is_connected(self) -> bool:
        return self._connected and self.ib.isConnected()

    def get_account_info(self) -> AccountInfo:
        """Fetch account equity, buying power, PnL."""
        summary = self.ib.accountSummary()
        info = AccountInfo()
        for item in summary:
            if item.tag == "NetLiquidation":
                info.equity = float(item.value)
                info.currency = item.currency
            elif item.tag == "BuyingPower":
                info.buying_power = float(item.value)
            elif item.tag == "UnrealizedPnL":
                info.unrealized_pnl = float(item.value)
            elif item.tag == "RealizedPnL":
                info.realized_pnl = float(item.value)
        return info

    def get_positions(self) -> list[BrokerPosition]:
        """Fetch all current futures positions, enriched with broker-truth
        unrealized + realized P/L from ib.portfolio().

        ib.positions() carries position size + avg_cost; ib.portfolio()
        carries unrealizedPNL + realizedPNL + marketPrice. We zip them
        on conId so each BrokerPosition has the full P/L picture.
        Filters out non-futures (warrants, stocks) — clean by
        construction, no dashboard contamination from unrelated
        instruments held in the same account.
        """
        # Build conId-keyed map of portfolio items for the P/L join.
        # ib.portfolio() returns PortfolioItem with marketPrice +
        # unrealizedPNL + realizedPNL in account currency. Empty list
        # before IBKR's first portfolio update arrives — handle by
        # defaulting to 0.0 (caller sees a snapshot with no MTM info,
        # not a crash).
        portfolio_by_conid: dict[int, "object"] = {}
        try:
            for item in self.ib.portfolio():
                portfolio_by_conid[item.contract.conId] = item
        except Exception as e:
            logger.warning(
                f"ib.portfolio() lookup failed: {e}; per-position "
                "unrealized + realized will default to 0.0"
            )

        positions = self.ib.positions()
        result = []
        for pos in positions:
            c = pos.contract
            if c.secType not in ("FUT", "CONTFUT"):
                continue
            pi = portfolio_by_conid.get(c.conId)
            unrl = float(pi.unrealizedPNL) if pi is not None else 0.0
            rlz = float(pi.realizedPNL) if pi is not None else 0.0
            mkt = float(pi.marketPrice) if pi is not None else 0.0
            result.append(BrokerPosition(
                symbol=c.symbol,
                con_id=c.conId,
                contract_month=c.lastTradeDateOrContractMonth or "",
                local_symbol=c.localSymbol or c.lastTradeDateOrContractMonth or "",
                exchange=c.exchange,
                position=float(pos.position),
                avg_cost=float(pos.avgCost),
                multiplier=float(c.multiplier) if c.multiplier else 1.0,
                unrealized_pnl=unrl,
                realized_pnl=rlz,
                market_price=mkt,
            ))
        return result

    def get_positions_by_symbol(self) -> dict[str, BrokerPosition]:
        """Positions keyed by root symbol. If multiple months, sums position."""
        positions = self.get_positions()
        by_symbol: dict[str, BrokerPosition] = {}
        for pos in positions:
            if pos.symbol in by_symbol:
                # Sum positions across contract months (shouldn't happen normally)
                by_symbol[pos.symbol].position += pos.position
            else:
                by_symbol[pos.symbol] = pos
        return by_symbol

    def place_market_order(
        self, contract: Contract, action: str, quantity: int
    ) -> Trade:
        """Place a market order. action = 'BUY' or 'SELL'."""
        qualified = self.ib.qualifyContracts(contract)
        if not qualified:
            raise ValueError(f"Failed to qualify contract: {contract}")

        order = MarketOrder(action, abs(quantity))
        logger.info(
            f"Placing {action} {abs(quantity)} {contract.symbol} "
            f"({contract.lastTradeDateOrContractMonth}) @ MARKET"
        )
        trade = self.ib.placeOrder(qualified[0], order)
        self.ib.sleep(1)  # allow fill to propagate
        return trade

    def place_spread_order(
        self,
        symbol: str,
        exchange: str,
        currency: str,
        front_con_id: int,
        next_con_id: int,
        quantity: int,
    ) -> Trade:
        """Place a calendar spread (roll) order via BAG contract.

        Sells front, buys next (for long rolls). For short rolls,
        pass negative quantity.

        Args:
            quantity: positive = roll long position (sell front, buy next)
                     negative = roll short position (buy front, sell next)
        """
        if quantity == 0:
            raise ValueError("Spread quantity cannot be zero")

        # IB combo leg actions are interpreted relative to the BAG order side.
        # Use BUY orders for both directions so the leg actions below map
        # directly to the desired fills:
        #   positive quantity -> SELL front, BUY next
        #   negative quantity -> BUY front, SELL next
        if quantity > 0:
            front_action = "SELL"
            next_action = "BUY"
            order_action = "BUY"
            order_qty = abs(quantity)
        else:
            front_action = "BUY"
            next_action = "SELL"
            order_action = "BUY"
            order_qty = abs(quantity)

        spread = Contract()
        spread.symbol = symbol
        spread.secType = "BAG"
        spread.exchange = exchange
        spread.currency = currency
        spread.comboLegs = [
            ComboLeg(
                conId=front_con_id, ratio=1,
                action=front_action, exchange=exchange,
            ),
            ComboLeg(
                conId=next_con_id, ratio=1,
                action=next_action, exchange=exchange,
            ),
        ]

        order = MarketOrder(order_action, order_qty)
        logger.info(
            f"Placing calendar spread: {front_action} front (conId={front_con_id}) + "
            f"{next_action} next (conId={next_con_id}), qty={order_qty}"
        )
        trade = self.ib.placeOrder(spread, order)
        self.ib.sleep(2)  # spreads may take a moment
        return trade

    def get_fill_info(self, trade: Trade, timeout: float = 30) -> FillInfo:
        """Extract fill details from a completed trade.

        Waits up to `timeout` seconds for the order to fill before
        extracting price/commission data.
        """
        # Wait for fill to propagate (1s sleep in place_market_order is not enough)
        elapsed = 0.0
        while not trade.isDone() and elapsed < timeout:
            self.ib.sleep(0.5)
            elapsed += 0.5

        if not trade.isDone():
            logger.warning(
                f"Trade {trade.order.orderId} not done after {timeout}s "
                f"(status={trade.orderStatus.status})"
            )

        fills = trade.fills
        total_qty = sum(f.execution.shares for f in fills) if fills else 0
        avg_price = (
            sum(f.execution.shares * f.execution.price for f in fills) / total_qty
            if total_qty > 0 else 0.0
        )
        commission = sum(f.commissionReport.commission for f in fills
                        if f.commissionReport.commission) if fills else 0.0
        # CommissionReport.realizedPNL is set only on closing fills; for opening
        # fills IBKR returns sys.float_info.max as a sentinel for "not applicable".
        # Filter the sentinel out so we don't sum 1.7976931348623157e+308.
        realized_pnl = 0.0
        if fills:
            for f in fills:
                pnl = f.commissionReport.realizedPNL
                if pnl is not None and abs(pnl) < 1e30:
                    realized_pnl += pnl

        return FillInfo(
            order_id=trade.order.orderId,
            symbol=trade.contract.symbol,
            action=trade.order.action,
            quantity=total_qty,
            avg_fill_price=avg_price,
            commission=commission,
            realized_pnl=realized_pnl,
        )

    def cancel_order(self, trade: Trade, timeout: float = 10) -> bool:
        """Cancel an order. Returns True if cancelled/inactive."""
        if trade.isDone():
            return True
        self.ib.cancelOrder(trade.order)
        elapsed = 0.0
        while not trade.isDone() and elapsed < timeout:
            self.ib.sleep(0.5)
            elapsed += 0.5
        cancelled = trade.orderStatus.status in ("Cancelled", "Inactive")
        if not cancelled:
            logger.warning(
                f"Order {trade.order.orderId} not cancelled after {timeout}s "
                f"(status={trade.orderStatus.status})"
            )
        return cancelled

    def cancel_all_open(self, timeout: float = 10) -> int:
        """Cancel all open orders. Returns count cancelled."""
        trades = self.ib.openTrades()
        if not trades:
            return 0
        for t in trades:
            if not t.isDone():
                self.ib.cancelOrder(t.order)
        self.ib.sleep(min(timeout, 5))
        n = sum(1 for t in trades if t.orderStatus.status in ("Cancelled", "Inactive"))
        logger.info(f"Cancelled {n}/{len(trades)} open orders")
        return n

    def reconnect(self) -> bool:
        """Reconnect if disconnected. Returns True if connected."""
        if self.is_connected:
            return True
        logger.info("Reconnecting to IB Gateway...")
        try:
            self.ib.disconnect()
        except Exception:
            pass
        try:
            self.ib = IB()
            self.ib.connect(
                self.settings.host,
                self.settings.port,
                clientId=self.settings.client_id,
                readonly=self.settings.readonly,
                timeout=self.settings.timeout,
            )
            self._connected = True
            logger.info("Reconnected to IB Gateway")
            return True
        except Exception as e:
            logger.error(f"Reconnect failed: {e}")
            self._connected = False
            return False

    def sleep(self, seconds: float) -> None:
        """IB-aware sleep (processes messages while waiting)."""
        self.ib.sleep(seconds)
