"""Position diff, contract sizing, margin cap, roll execution, and rebalance logic.

Sizing matches R-factory execution_sim.py:
  target_contracts = round(sized_pos × equity × leverage / (price × multiplier))

Both V1 and V2 produce sized positions, so sizing is unified.
"""

import logging
from dataclasses import dataclass

import numpy as np

from futures_executor.config.loader import (
    ExecutionSettings,
    ExecutorConfig,
    InstrumentSettings,
    SafetySettings,
)
from futures_executor.data.contract_resolver import ContractPair
from futures_executor.execution.broker import BrokerConnection, BrokerPosition

logger = logging.getLogger(__name__)


@dataclass
class PositionDelta:
    """Difference between target and current positions for one instrument."""

    symbol: str
    current_contracts: int
    target_contracts: int
    delta: int  # target - current (signed)
    action: str  # "BUY", "SELL", or "HOLD"
    is_reversal: bool  # flipping sign (long→short or vice versa)
    needs_roll: bool  # roll required before/during rebalance


@dataclass
class SizingResult:
    """Contract sizing output for one instrument."""

    symbol: str
    target_signal: float  # sized position (fraction of capital)
    target_contracts: int  # signed
    notional_per_contract: float
    multiplier: float
    last_price: float


def compute_contract_size(
    signal: float,
    equity: float,
    last_price: float,
    multiplier: float,
    config: ExecutionSettings,
) -> SizingResult:
    """Convert a sized position to a number of contracts.

    Unified formula (both V1 and V2 produce sized positions):
        n = round(sized_pos × equity × portfolio_leverage / notional_per_contract)

    Matches R-factory execution_sim.py contract conversion.
    """
    notional_per_contract = last_price * multiplier

    if notional_per_contract <= 0:
        return SizingResult(
            symbol="",
            target_signal=signal,
            target_contracts=0,
            notional_per_contract=notional_per_contract,
            multiplier=multiplier,
            last_price=last_price,
        )

    raw = signal * equity * config.portfolio_leverage / notional_per_contract

    target = round(raw)

    return SizingResult(
        symbol="",
        target_signal=signal,
        target_contracts=target,
        notional_per_contract=notional_per_contract,
        multiplier=multiplier,
        last_price=last_price,
    )


def apply_margin_cap(
    sizing: dict[str, SizingResult],
    instruments: list[InstrumentSettings],
    available_margin: float,
) -> dict[str, SizingResult]:
    """Scale down contracts proportionally if total margin exceeds budget.

    Matches R-factory execution_sim.py _apply_margin_cap().
    """
    margin_map = {i.symbol: i.margin for i in instruments}
    # Also map portfolio symbols
    for i in instruments:
        if i.portfolio_symbol:
            margin_map[i.portfolio_symbol] = i.margin

    total_required = 0.0
    for sym, sz in sizing.items():
        margin_per = margin_map.get(sym, 0.0)
        total_required += abs(sz.target_contracts) * margin_per

    if total_required <= available_margin or total_required < 1e-10:
        return sizing

    scale = available_margin / total_required
    logger.warning(
        f"Margin cap: required ${total_required:,.0f} > "
        f"available ${available_margin:,.0f}, scaling by {scale:.3f}"
    )

    for sym, sz in sizing.items():
        scaled = sz.target_contracts * scale
        sz.target_contracts = int(np.fix(scaled))

    return sizing


def compute_position_diff(
    target_contracts: int,
    current_contracts: int,
    symbol: str,
    needs_roll: bool = False,
    abs_threshold: int = 1,
    rel_threshold: float = 0.15,
) -> PositionDelta:
    """Compute the delta between target and current positions.

    Dual-threshold execution filter:
      1. Absolute: abs(delta) >= abs_threshold  (noise filter)
      2. Relative: abs(delta) / max(abs(current), 1) >= rel_threshold  (risk control)
    Both must pass to trade.  Sign flips always trade regardless of thresholds.
    """
    delta = target_contracts - current_contracts

    # Reversal = flipping sign (long→short or short→long)
    is_reversal = (
        current_contracts > 0
        and target_contracts < 0
        or current_contracts < 0
        and target_contracts > 0
    )

    # Determine if delta passes thresholds
    passes_abs = abs(delta) >= abs_threshold
    denom = max(abs(current_contracts), 1)
    passes_rel = abs(delta) / denom >= rel_threshold

    # Always trade on sign flip; otherwise need both thresholds
    if is_reversal:
        pass  # keep delta as-is
    elif passes_abs and passes_rel:
        pass  # keep delta as-is
    else:
        delta = 0

    if delta > 0:
        action = "BUY"
    elif delta < 0:
        action = "SELL"
    else:
        action = "HOLD"

    return PositionDelta(
        symbol=symbol,
        current_contracts=current_contracts,
        target_contracts=target_contracts,
        delta=delta,
        action=action,
        is_reversal=is_reversal,
        needs_roll=needs_roll,
    )


def enforce_safety_limits(
    deltas: dict[str, PositionDelta],
    current_positions: dict[str, BrokerPosition],
    safety: SafetySettings,
) -> dict[str, PositionDelta]:
    """Enforce per-instrument and total position limits.

    Clamps target positions to stay within safety bounds.
    Returns modified deltas dict.
    """
    max_per = safety.max_position_contracts
    max_total = safety.max_total_contracts

    # First pass: clamp per-instrument
    for sym, d in deltas.items():
        if abs(d.target_contracts) > max_per:
            clamped = max_per if d.target_contracts > 0 else -max_per
            logger.warning(
                f"{sym}: clamped target from {d.target_contracts} "
                f"to {clamped} (max_per={max_per})"
            )
            d.target_contracts = clamped
            d.delta = d.target_contracts - d.current_contracts
            d.action = "BUY" if d.delta > 0 else ("SELL" if d.delta < 0 else "HOLD")

    # Second pass: check total exposure
    total_target = sum(abs(d.target_contracts) for d in deltas.values())
    if total_target > max_total:
        # Scale all targets proportionally
        scale = max_total / total_target
        logger.warning(
            f"Total target contracts {total_target} exceeds max {max_total}, "
            f"scaling by {scale:.2f}"
        )
        for d in deltas.values():
            scaled = int(d.target_contracts * scale)
            d.target_contracts = scaled
            d.delta = d.target_contracts - d.current_contracts
            d.action = "BUY" if d.delta > 0 else ("SELL" if d.delta < 0 else "HOLD")

    return deltas


class OrderManager:
    """Orchestrates position adjustments: sizing, rolls, and order execution."""

    def __init__(self, broker: BrokerConnection, config: ExecutorConfig):
        self.broker = broker
        self.config = config

    def execute_rebalance(
        self,
        target_signals: dict[str, float],
        contract_pairs: dict[str, ContractPair],
        equity: float,
    ) -> list[dict]:
        """Execute full rebalance cycle with verification and reconciliation.

        1. Size targets (sized_pos → contracts) — unified for V1/V2
        2. Apply margin cap
        3. Compute diffs vs current positions
        4. Execute orders (rolls first, then adjustments)
        5. Cancel any unfilled orders
        6. Reconcile: re-read positions, place corrective orders if needed

        Returns list of dicts with execution details for audit logging.
        """
        all_positions = self.broker.get_positions()
        current_positions: dict[str, BrokerPosition] = {}
        for pos in all_positions:
            if pos.symbol in current_positions:
                current_positions[pos.symbol].position += pos.position
            else:
                current_positions[pos.symbol] = pos
        records = []

        # Step 1: Size each instrument
        sizing: dict[str, SizingResult] = {}
        for symbol, signal in target_signals.items():
            pair = contract_pairs.get(symbol)
            if pair is None:
                logger.warning(f"{symbol}: no contract pair, skipping")
                continue

            last_price = self._get_last_price(pair)
            if last_price <= 0:
                logger.warning(f"{symbol}: invalid last price {last_price}, skipping")
                continue

            result = compute_contract_size(
                signal=signal,
                equity=equity,
                last_price=last_price,
                multiplier=pair.front.multiplier,
                config=self.config.execution,
            )
            result.symbol = symbol
            sizing[symbol] = result

            logger.info(
                f"{symbol}: sized_pos={signal:+.6f} → "
                f"target={result.target_contracts} contracts "
                f"(notional/ct=${result.notional_per_contract:,.0f})"
            )

        # Step 2: Margin cap
        available_margin = equity * self.config.execution.margin_cap
        sizing = apply_margin_cap(
            sizing,
            self.config.instruments,
            available_margin,
        )

        # Build target_contracts map for reconciliation
        target_contracts: dict[str, int] = {
            sym: sz.target_contracts for sym, sz in sizing.items()
        }

        # Step 3: Compute position diffs
        deltas: dict[str, PositionDelta] = {}
        for symbol, sz in sizing.items():
            current = current_positions.get(symbol)
            current_qty = int(current.position) if current else 0
            pair = contract_pairs[symbol]

            delta = compute_position_diff(
                target_contracts=sz.target_contracts,
                current_contracts=current_qty,
                symbol=symbol,
                needs_roll=pair.roll_due,
                abs_threshold=self.config.execution.abs_threshold,
                rel_threshold=self.config.execution.rel_threshold,
            )
            deltas[symbol] = delta

        # Step 4: Enforce safety limits
        deltas = enforce_safety_limits(
            deltas,
            current_positions,
            self.config.safety,
        )
        # Update targets after safety clamping
        for sym, d in deltas.items():
            target_contracts[sym] = d.target_contracts

        # Step 5: Execute — rolls first, then adjustments
        pending_trades: list[tuple[str, "Trade"]] = []

        for symbol, delta in deltas.items():
            pair = contract_pairs[symbol]
            sz = sizing.get(symbol)

            def _enrich(rec: dict, _sz=sz, _delta=delta, _sym=symbol) -> dict:
                rec.setdefault("symbol", _sym)
                if _sz:
                    rec["bar_close"] = _sz.last_price
                    rec["target_contracts"] = _sz.target_contracts
                    rec["target_signal"] = _sz.target_signal
                rec["current_contracts"] = _delta.current_contracts
                return rec

            # Handle rolls via calendar spread
            rolled = False
            if delta.needs_roll and pair.next is not None:
                front_qty = sum(
                    int(p.position)
                    for p in all_positions
                    if p.symbol == symbol and p.contract_month == pair.front.expiry_str
                )
                if front_qty != 0:
                    roll_record, roll_trade = self._execute_roll(
                        symbol,
                        pair,
                        front_qty,
                    )
                    if roll_record:
                        records.append(_enrich(roll_record))
                    if roll_trade and not roll_trade.isDone():
                        pending_trades.append((symbol, roll_trade))
                    # If roll didn't fill, skip adjustment for this symbol
                    if roll_trade and roll_trade.orderStatus.status != "Filled":
                        logger.warning(
                            f"{symbol}: roll not filled "
                            f"(status={roll_trade.orderStatus.status}), "
                            f"skipping adjustment"
                        )
                        continue
                    rolled = True

            # After successful roll, adjustments must target the new contract
            if rolled:
                pair = ContractPair(
                    symbol=pair.symbol,
                    front=pair.next,
                    next=None,
                    roll_due=False,
                    hard_deadline=False,
                    days_to_expiry=pair.days_to_expiry,
                )

            # Handle position adjustments
            if delta.action == "HOLD":
                logger.debug(f"{symbol}: no adjustment needed")
                continue

            if delta.is_reversal:
                rev_records = self._execute_reversal(symbol, pair, delta)
                for rec in rev_records:
                    records.append(_enrich(rec))
            else:
                record, trade = self._execute_adjustment(symbol, pair, delta)
                if record:
                    records.append(_enrich(record))
                if trade and not trade.isDone():
                    pending_trades.append((symbol, trade))

        # Step 6: Cancel unfilled orders
        for symbol, trade in pending_trades:
            if not trade.isDone():
                logger.warning(
                    f"{symbol}: order {trade.order.orderId} not filled "
                    f"(status={trade.orderStatus.status}), cancelling"
                )
                self.broker.cancel_order(trade)

        # Step 7: Reconcile — re-read positions vs targets
        reconcile_records = self._reconcile(
            target_contracts,
            contract_pairs,
            sizing,
        )
        records.extend(reconcile_records)

        return records

    def _reconcile(
        self,
        target_contracts: dict[str, int],
        contract_pairs: dict[str, ContractPair],
        sizing: dict[str, SizingResult],
    ) -> list[dict]:
        """Re-read positions, place corrective orders for any mismatches."""
        records = []

        # Reconnect if needed
        if not self.broker.is_connected:
            if not self.broker.reconnect():
                logger.error("Cannot reconcile — reconnect failed")
                return [
                    {
                        "type": "reconcile_error",
                        "symbol": "",
                        "error": "Reconnect failed, positions unverified",
                        "status": "FAILED",
                    }
                ]

        actual_positions = self.broker.get_positions_by_symbol()

        mismatches = []
        for symbol, target in target_contracts.items():
            actual = actual_positions.get(symbol)
            actual_qty = int(actual.position) if actual else 0
            if actual_qty != target:
                mismatches.append((symbol, actual_qty, target))

        if not mismatches:
            logger.info("Reconciliation: all positions match targets")
            return records

        # Place corrective orders
        for symbol, actual_qty, target in mismatches:
            delta = target - actual_qty
            if delta == 0:
                continue

            action = "BUY" if delta > 0 else "SELL"
            qty = abs(delta)
            pair = contract_pairs.get(symbol)
            if pair is None:
                continue

            sz = sizing.get(symbol)

            logger.warning(
                f"RECONCILE {symbol}: actual={actual_qty} target={target}, "
                f"correcting {action} {qty}"
            )

            try:
                trade = self.broker.place_market_order(
                    pair.front.contract,
                    action,
                    qty,
                )
                fill = self.broker.get_fill_info(trade)
                rec = {
                    "type": "reconcile",
                    "symbol": symbol,
                    "action": action,
                    "quantity": qty,
                    "fill_price": fill.avg_fill_price,
                    "commission": fill.commission,
                    "status": trade.orderStatus.status,
                    "target_contracts": target,
                    "current_contracts": actual_qty,
                }
                if sz:
                    rec["bar_close"] = sz.last_price

                if trade.orderStatus.status == "Filled":
                    logger.info(
                        f"RECONCILE {symbol}: corrected — "
                        f"{action} {qty} @ {fill.avg_fill_price}"
                    )
                else:
                    logger.error(
                        f"RECONCILE {symbol}: correction not filled "
                        f"(status={trade.orderStatus.status})"
                    )
                    rec["error"] = f"Correction not filled: {trade.orderStatus.status}"

                records.append(rec)

            except Exception as e:
                logger.error(f"RECONCILE {symbol}: correction failed: {e}")
                records.append(
                    {
                        "type": "reconcile",
                        "symbol": symbol,
                        "action": action,
                        "quantity": qty,
                        "error": str(e),
                        "status": "FAILED",
                        "target_contracts": target,
                        "current_contracts": actual_qty,
                    }
                )

        # Final verification
        if self.broker.is_connected:
            final = self.broker.get_positions_by_symbol()
            still_wrong = []
            for symbol, target in target_contracts.items():
                actual = final.get(symbol)
                actual_qty = int(actual.position) if actual else 0
                if actual_qty != target:
                    still_wrong.append(f"{symbol}: actual={actual_qty} target={target}")

            if still_wrong:
                msg = "POSITIONS STILL MISMATCHED AFTER RECONCILIATION: " + "; ".join(
                    still_wrong
                )
                logger.critical(msg)
                records.append(
                    {
                        "type": "reconcile_failed",
                        "symbol": "",
                        "error": msg,
                        "status": "FAILED",
                    }
                )
            else:
                logger.info("Final verification: all positions correct")

        return records

    def _execute_roll(
        self,
        symbol: str,
        pair: ContractPair,
        current_qty: int,
    ) -> tuple[dict | None, "Trade | None"]:
        """Execute a contract roll via calendar spread order.

        Returns (record, trade) — trade is needed for pending tracking.
        """
        if pair.next is None:
            logger.error(f"{symbol}: roll needed but no next contract available")
            return None, None

        logger.info(
            f"{symbol}: rolling {pair.front.local_symbol} → "
            f"{pair.next.local_symbol}, qty={current_qty}"
        )

        try:
            trade = self.broker.place_spread_order(
                symbol=symbol,
                exchange=pair.front.exchange,
                currency=pair.front.currency,
                front_con_id=pair.front.con_id,
                next_con_id=pair.next.con_id,
                quantity=current_qty,
            )

            fill = self.broker.get_fill_info(trade)
            record = {
                "type": "roll",
                "symbol": symbol,
                "from_month": pair.front.expiry_str,
                "to_month": pair.next.expiry_str,
                "quantity": current_qty,
                "fill_price": fill.avg_fill_price,
                "commission": fill.commission,
                "status": trade.orderStatus.status,
            }
            logger.info(
                f"{symbol}: roll — "
                f"{pair.front.local_symbol} → {pair.next.local_symbol}, "
                f"status={trade.orderStatus.status}"
            )
            return record, trade

        except Exception as e:
            logger.error(f"{symbol}: roll failed: {e}")
            return {
                "type": "roll",
                "symbol": symbol,
                "from_month": pair.front.expiry_str,
                "to_month": pair.next.expiry_str,
                "quantity": current_qty,
                "error": str(e),
                "status": "FAILED",
            }, None

    def _execute_reversal(
        self,
        symbol: str,
        pair: ContractPair,
        delta: PositionDelta,
    ) -> list[dict]:
        """Execute a position reversal: close current, then open new direction.

        Two separate orders to avoid partial fill issues on the reversal.
        """
        records = []
        contract = pair.front.contract

        # Step 1: Close existing position
        close_action = "SELL" if delta.current_contracts > 0 else "BUY"
        close_qty = abs(delta.current_contracts)

        logger.info(
            f"{symbol}: reversal — closing {delta.current_contracts} "
            f"({close_action} {close_qty})"
        )

        try:
            trade = self.broker.place_market_order(
                contract,
                close_action,
                close_qty,
            )
            fill = self.broker.get_fill_info(trade)
            records.append(
                {
                    "type": "close",
                    "symbol": symbol,
                    "action": close_action,
                    "quantity": close_qty,
                    "fill_price": fill.avg_fill_price,
                    "commission": fill.commission,
                    "status": trade.orderStatus.status,
                }
            )
        except Exception as e:
            logger.error(f"{symbol}: close leg of reversal failed: {e}")
            records.append(
                {
                    "type": "close",
                    "symbol": symbol,
                    "action": close_action,
                    "quantity": close_qty,
                    "error": str(e),
                    "status": "FAILED",
                }
            )
            return records  # Don't open new side if close failed

        # Step 2: Open new direction
        open_action = "BUY" if delta.target_contracts > 0 else "SELL"
        open_qty = abs(delta.target_contracts)

        if open_qty == 0:
            return records

        logger.info(
            f"{symbol}: reversal — opening {delta.target_contracts} "
            f"({open_action} {open_qty})"
        )

        try:
            trade = self.broker.place_market_order(
                contract,
                open_action,
                open_qty,
            )
            fill = self.broker.get_fill_info(trade)
            records.append(
                {
                    "type": "open",
                    "symbol": symbol,
                    "action": open_action,
                    "quantity": open_qty,
                    "fill_price": fill.avg_fill_price,
                    "commission": fill.commission,
                    "status": trade.orderStatus.status,
                }
            )
        except Exception as e:
            logger.error(f"{symbol}: open leg of reversal failed: {e}")
            records.append(
                {
                    "type": "open",
                    "symbol": symbol,
                    "action": open_action,
                    "quantity": open_qty,
                    "error": str(e),
                    "status": "FAILED",
                }
            )

        return records

    def _execute_adjustment(
        self,
        symbol: str,
        pair: ContractPair,
        delta: PositionDelta,
    ) -> tuple[dict | None, "Trade | None"]:
        """Execute a simple position adjustment (increase or decrease)."""
        contract = pair.front.contract
        action = delta.action
        qty = abs(delta.delta)

        if qty == 0:
            return None, None

        logger.info(f"{symbol}: {action} {qty} contracts")

        try:
            trade = self.broker.place_market_order(contract, action, qty)
            fill = self.broker.get_fill_info(trade)
            return {
                "type": "adjustment",
                "symbol": symbol,
                "action": action,
                "quantity": qty,
                "fill_price": fill.avg_fill_price,
                "commission": fill.commission,
                "status": trade.orderStatus.status,
            }, trade
        except Exception as e:
            logger.error(f"{symbol}: adjustment failed: {e}")
            return {
                "type": "adjustment",
                "symbol": symbol,
                "action": action,
                "quantity": qty,
                "error": str(e),
                "status": "FAILED",
            }, None

    def _get_last_price(self, pair: ContractPair) -> float:
        """Get last traded price for the front contract.

        Tries reqMktData snapshot first, falls back to last historical bar.
        """
        try:
            ticker = self.broker.ib.reqMktData(
                pair.front.contract,
                "",
                True,
                False,
            )
            self.broker.ib.sleep(2)

            if ticker.last and ticker.last > 0:
                return float(ticker.last)
            if ticker.close and ticker.close > 0:
                return float(ticker.close)

            # Fallback: last historical bar
            bars = self.broker.ib.reqHistoricalData(
                pair.front.contract,
                endDateTime="",
                durationStr="1 D",
                barSizeSetting="1 day",
                whatToShow="TRADES",
                useRTH=False,
                formatDate=1,
            )
            if bars:
                return float(bars[-1].close)

        except Exception as e:
            logger.warning(f"{pair.symbol}: failed to get last price: {e}")

        return 0.0
