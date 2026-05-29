"""Position diff, contract sizing, margin cap, roll execution, and rebalance logic.

Sizing matches R-factory execution_sim.py:
  target_contracts = round(sized_pos × equity × leverage / (price × multiplier))

Both V1 and V2 produce sized positions, so sizing is unified.
"""

import logging
from dataclasses import dataclass
from datetime import datetime as _dt

import numpy as np
from ib_insync import Contract as _IBContract

from futures_executor.config.loader import (
    ExecutionSettings,
    ExecutorConfig,
    InstrumentSettings,
    SafetySettings,
)
from futures_executor.data.contract_resolver import ContractPair, ResolvedContract
from futures_executor.execution.broker import BrokerConnection, BrokerPosition
from futures_executor.state import (
    load_executor_state,
    save_executor_state,
    set_active_contract,
)

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

        # [#228] Migrate any position held on a contract != pair.front
        # (the buffer-advance gap) BEFORE Step 1 sizing. The scheduled-
        # roll path can't see these because compute_position_diff is
        # symbol-aggregated and the roll's front_qty check looks at the
        # NEW front. Without this, a position carried into the buffer
        # window strands silently. Failed migrations populate
        # skip_symbols so the rest of this method excludes them.
        migration_records, skip_symbols = self.migrate_stranded_positions(
            contract_pairs, all_positions,
        )
        records: list[dict] = list(migration_records)
        if migration_records:
            # Re-fetch so the aggregation below sees post-migration truth.
            all_positions = self.broker.get_positions()

        current_positions: dict[str, BrokerPosition] = {}
        for pos in all_positions:
            if pos.symbol in current_positions:
                current_positions[pos.symbol].position += pos.position
            else:
                current_positions[pos.symbol] = pos

        # Step 1: Size each instrument
        sizing: dict[str, SizingResult] = {}
        for symbol, signal in target_signals.items():
            pair = contract_pairs.get(symbol)
            if pair is None:
                logger.warning(f"{symbol}: no contract pair, skipping")
                continue

            # [#228] If migrate_stranded_positions failed/blocked for this
            # symbol, exclude it from sizing → deltas → placement →
            # reconcile (same semantics as the venue-closed-skip below).
            if symbol in skip_symbols:
                logger.warning(
                    f"{symbol}: skipped — stranded-position migration "
                    "failed or was blocked; will retry next run."
                )
                continue

            # [#228] Tradability gate: if the venue is closed at fire time
            # (holiday / early-close / out-of-session), do NOT rebalance.
            # The backtest has no daily bar for closed venues so the sim
            # doesn't rebalance either — live must match. Skipping here
            # also keeps the symbol out of sizing → deltas → placement →
            # reconcile (reconcile only iterates target_contracts, which
            # is built from `sizing`). So no queued order, no orphan, no
            # reconcile_failed CRITICAL — Memorial-Day failure mode dies.
            if not pair.tradable_now:
                logger.warning(
                    f"{symbol}: venue closed at fire time "
                    f"(target_signal={signal:+.6f}); skipping rebalance "
                    f"— sim has no bar on this day either."
                )
                records.append({
                    "type": "venue_closed_skip",
                    "symbol": symbol,
                    "target_signal": float(signal),
                    "status": "SKIPPED",
                })
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
                    state = load_executor_state()
                    _save = set_active_contract(state, symbol, pair.next.expiry_str)
                    save_executor_state(_save)
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

            # Defense-in-depth: when correcting an existing position, use
            # the position-holding contract (not pair.front).
            corr_contract, corr_src = self._resolve_close_contract(symbol, pair)
            corr_local = getattr(corr_contract, "localSymbol", "") or corr_contract.symbol

            logger.warning(
                f"RECONCILE {symbol}: actual={actual_qty} target={target}, "
                f"correcting {action} {qty} on {corr_local} "
                f"(contract source: {corr_src})"
            )

            try:
                trade = self.broker.place_market_order(
                    corr_contract,
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
                    "realized_pnl": fill.realized_pnl,
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

    def migrate_stranded_positions(
        self,
        contract_pairs: dict[str, ContractPair],
        all_positions: list[BrokerPosition],
    ) -> tuple[list[dict], set[str]]:
        """Auto-migrate positions stranded on a contract != pair.front.

        Triggered by the `delivery_buffer_days` advance in
        ``contract_resolver.resolve()``: when the resolver swaps
        ``pair.front`` to the next contract, any existing position on
        the abandoned contract is invisible to ``compute_position_diff``
        (symbol-aggregated) and the scheduled-roll path (which checks
        ``front_qty`` on the NEW front). Without this method, those
        positions strand silently. (#228)

        For each stranded position, builds a synthetic ContractPair
        ``(front=stranded, next=pair.front)`` and runs the existing
        ``_execute_roll`` calendar-spread machinery. On Fill, updates
        ``active_contracts`` state. On failure, adds the symbol to
        ``skip_symbols`` so the rest of ``execute_rebalance`` excludes
        it from sizing/reconcile — preserves the miss-trade risk class
        (a failed migration emits Signal + audit, no further order
        placement on the still-stranded symbol).

        Returns ``(records, skip_symbols)``.
        """
        records: list[dict] = []
        skip: set[str] = set()

        for symbol, pair in contract_pairs.items():
            front_expiry = pair.front.expiry_str
            stranded = [
                p for p in all_positions
                if p.symbol == symbol
                and p.position != 0
                and p.contract_month
                and p.contract_month != front_expiry
            ]
            for sp in stranded:
                # Qualify the stranded contract — mirror of the pattern
                # in ``_resolve_close_contract`` (we need a real qualified
                # ``Contract`` for the BAG-combo close leg).
                try:
                    qualified = self.broker.ib.qualifyContracts(
                        _IBContract(conId=sp.con_id, exchange=sp.exchange)
                    )
                except Exception as e:
                    logger.critical(
                        f"{symbol}: qualifyContracts raised for stranded "
                        f"con_id={sp.con_id}: {e}"
                    )
                    qualified = []
                if not qualified:
                    msg = (
                        f"qualifyContracts failed for stranded "
                        f"{sp.local_symbol} (con_id={sp.con_id})"
                    )
                    logger.critical(f"{symbol}: {msg}")
                    records.append({
                        "type": "migration_blocked",
                        "symbol": symbol,
                        "from_month": sp.contract_month,
                        "to_month": pair.front.expiry_str,
                        "error": msg,
                        "status": "FAILED",
                    })
                    skip.add(symbol)
                    continue

                # Parse stranded expiry_str into a ``date`` (same logic
                # as ``contract_resolver._to_resolved`` parsing).
                try:
                    if len(sp.contract_month) == 8:
                        stranded_exp = _dt.strptime(
                            sp.contract_month, "%Y%m%d"
                        ).date()
                    else:
                        stranded_exp = _dt.strptime(
                            sp.contract_month, "%Y%m"
                        ).date()
                except ValueError:
                    msg = f"could not parse contract_month={sp.contract_month!r}"
                    logger.critical(f"{symbol}: {msg}")
                    records.append({
                        "type": "migration_blocked",
                        "symbol": symbol,
                        "from_month": sp.contract_month,
                        "to_month": pair.front.expiry_str,
                        "error": msg,
                        "status": "FAILED",
                    })
                    skip.add(symbol)
                    continue

                stranded_resolved = ResolvedContract(
                    symbol=sp.symbol,
                    con_id=sp.con_id,
                    exchange=sp.exchange,
                    currency=pair.front.currency,
                    expiry=stranded_exp,
                    expiry_str=sp.contract_month,
                    multiplier=sp.multiplier,
                    local_symbol=sp.local_symbol,
                    min_tick=pair.front.min_tick,
                    contract=qualified[0],
                )

                # Synthetic pair: front = stranded (close leg of the
                # BAG combo), next = current pair.front (open leg = the
                # migration target).
                synthetic = ContractPair(
                    symbol=symbol,
                    front=stranded_resolved,
                    next=pair.front,
                    days_to_expiry=0,
                    roll_due=True,
                    hard_deadline=True,
                    tradable_now=pair.tradable_now,
                )

                logger.warning(
                    f"{symbol}: STRANDED position on {sp.local_symbol} "
                    f"(qty={int(sp.position)}); migrating to "
                    f"{pair.front.local_symbol} via calendar spread."
                )
                record, _trade = self._execute_roll(
                    symbol, synthetic, current_qty=int(sp.position),
                )

                if record is None:
                    msg = "_execute_roll returned None (no next contract)"
                    logger.critical(f"{symbol}: {msg}")
                    records.append({
                        "type": "migration_blocked",
                        "symbol": symbol,
                        "from_month": sp.contract_month,
                        "to_month": pair.front.expiry_str,
                        "error": msg,
                        "status": "FAILED",
                    })
                    skip.add(symbol)
                    continue

                # Reclassify so cli.py / notifier / audit can distinguish
                # the buffer-triggered migration from a scheduled roll.
                record["type"] = "migration_roll"
                records.append(record)

                if record.get("status") == "Filled":
                    state = load_executor_state()
                    state = set_active_contract(
                        state, symbol, pair.front.expiry_str,
                    )
                    save_executor_state(state)
                else:
                    logger.error(
                        f"{symbol}: migration_roll did not fill "
                        f"(status={record.get('status')}); excluding "
                        "from subsequent sizing."
                    )
                    skip.add(symbol)

        return records, skip

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
                "realized_pnl": fill.realized_pnl,
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
        Step 1 (close) targets the contract that ACTUALLY HOLDS the
        position (via ``_resolve_close_contract``). Step 2 (open new
        direction) is a fresh open and uses ``pair.front`` (the
        front-month picker), which is the right semantic for a new
        open.
        """
        records = []

        # Step 1: Close existing position — use position-holding contract.
        close_contract, close_src = self._resolve_close_contract(symbol, pair)
        close_action = "SELL" if delta.current_contracts > 0 else "BUY"
        close_qty = abs(delta.current_contracts)
        close_local = getattr(close_contract, "localSymbol", "") or close_contract.symbol

        logger.info(
            f"{symbol}: reversal — closing {delta.current_contracts} "
            f"({close_action} {close_qty}) on {close_local} "
            f"(contract source: {close_src})"
        )

        try:
            trade = self.broker.place_market_order(
                close_contract,
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
                    "realized_pnl": fill.realized_pnl,
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

        # Step 2: Open new direction — pair.front is correct for fresh opens.
        open_contract = pair.front.contract
        open_action = "BUY" if delta.target_contracts > 0 else "SELL"
        open_qty = abs(delta.target_contracts)

        if open_qty == 0:
            return records

        logger.info(
            f"{symbol}: reversal — opening {delta.target_contracts} "
            f"({open_action} {open_qty}) on {pair.front.local_symbol}"
        )

        try:
            trade = self.broker.place_market_order(
                open_contract,
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
                    "realized_pnl": fill.realized_pnl,
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

    def _resolve_close_contract(
        self,
        symbol: str,
        pair: ContractPair,
    ) -> tuple["Contract", str]:
        """Pick the contract for a close/reduce/reconcile order.

        Defense-in-depth (task #214). The only safe contract for a close
        or reduce is the one that ACTUALLY HOLDS the position. Falling
        back to ``pair.front`` (front-month picker) silently opens new
        positions if state.json drifts from broker truth — the
        2026-05-08 incident's failure mode (cli.py state-overwrite bug
        was the proximate cause; this function is the wear-belt-and-
        suspenders backstop).

        Returns ``(contract, source_label)`` where source_label is one
        of:

          - ``"position"``       single existing position matched, used
                                 its ``con_id``
          - ``"pair.front"``     no existing position (genuine new open)
                                 OR qualifyContracts failed
          - ``"pair.front+SPLIT"`` split state across 2+ contracts —
                                 fell back to pair.front and logged loud
                                 warning; operator must intervene
        """
        from ib_insync import Contract

        all_positions = self.broker.get_positions()
        matches = [
            p for p in all_positions
            if p.symbol == symbol and abs(p.position) > 0
        ]

        if len(matches) == 1:
            pos = matches[0]
            contract = Contract(conId=pos.con_id, exchange=pos.exchange or "")
            qualified = self.broker.ib.qualifyContracts(contract)
            if not qualified:
                logger.error(
                    f"{symbol}: failed to qualify conId={pos.con_id}; "
                    f"falling back to pair.front ({pair.front.local_symbol})"
                )
                return pair.front.contract, "pair.front"
            if pos.local_symbol != pair.front.local_symbol:
                logger.warning(
                    f"{symbol}: trading on {pos.local_symbol} "
                    f"(pair.front would be {pair.front.local_symbol}) — "
                    f"position-holding contract takes precedence to avoid "
                    f"opening a wrong-contract leg"
                )
            return qualified[0], "position"

        if len(matches) > 1:
            legs = ", ".join(
                f"{p.local_symbol}={p.position:+.0f}" for p in matches
            )
            logger.error(
                f"{symbol}: SPLIT POSITION across {len(matches)} contracts "
                f"({legs}) — this should not happen; investigate via "
                f"`close_rogue_mcl_legs.py`-style cleanup. Falling back to "
                f"pair.front ({pair.front.local_symbol}); the next order "
                f"may worsen the split until manually resolved."
            )
            return pair.front.contract, "pair.front+SPLIT"

        # No existing position → genuine new open; pair.front is correct.
        return pair.front.contract, "pair.front"

    def _execute_adjustment(
        self,
        symbol: str,
        pair: ContractPair,
        delta: PositionDelta,
    ) -> tuple[dict | None, "Trade | None"]:
        """Execute a simple position adjustment (increase or decrease)."""
        action = delta.action
        qty = abs(delta.delta)

        if qty == 0:
            return None, None

        contract, source = self._resolve_close_contract(symbol, pair)
        local = getattr(contract, "localSymbol", "") or contract.symbol
        logger.info(f"{symbol}: {action} {qty} on {local} (contract source: {source})")

        try:
            trade = self.broker.place_market_order(contract, action, qty)
            fill = self.broker.get_fill_info(trade)
            return {
                "type": "adjustment",
                "symbol": symbol,
                "action": action,
                "quantity": qty,
                "fill_price": fill.avg_fill_price,
                "realized_pnl": fill.realized_pnl,
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
