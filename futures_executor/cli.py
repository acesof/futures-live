"""Futures executor CLI — run-once, status, flatten, roll-status."""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from futures_executor.config.loader import load_settings, load_strategies

logger = logging.getLogger("futures_executor")

CONFIG_DIR = Path(__file__).parent / "config"
STATE_FILE = Path("data/executor_state.json")


def _setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _load_state() -> dict:
    """Load persistent executor state (last rebalance date, etc.)."""
    import json
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def _save_state(state: dict):
    import json
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def _check_kill_switch(config) -> bool:
    """Check if kill switch file exists."""
    path = Path(config.safety.kill_switch_file)
    if path.exists():
        logger.critical(f"Kill switch active: {path}")
        return True
    return False


def cmd_run_once(args):
    """Execute one rebalance cycle."""
    from futures_executor.data.bar_fetcher import BarFetcher
    from futures_executor.data.continuous_series import ContinuousSeries
    from futures_executor.data.contract_resolver import ContractResolver
    from futures_executor.execution.broker import BrokerConnection
    from futures_executor.execution.order_manager import OrderManager
    from futures_executor.monitoring.audit import AuditLog
    from futures_executor.monitoring.notifier import SignalNotifier
    from futures_executor.strategy.aggregator import compute_aggregate_targets

    config = load_settings(CONFIG_DIR)
    strategies_config = load_strategies(CONFIG_DIR)
    notifier = SignalNotifier(config.signal)
    audit = AuditLog(config.audit.db_path)
    state = _load_state()
    today = date.today().isoformat()

    # Kill switch check
    if _check_kill_switch(config):
        notifier.notify_kill_switch()
        return 1

    # Connect to IBKR
    broker = BrokerConnection(config.broker)
    try:
        broker.connect()
    except Exception as e:
        logger.critical(f"Failed to connect to IB Gateway: {e}")
        notifier.notify_error("BROKER", str(e))
        return 1

    try:
        # Build symbol mapping: execution symbol ↔ portfolio symbol
        # Strategies reference portfolio symbols (NQ, ES, ...),
        # but we trade micro contracts (MNQ, MES, ...).
        exec_to_portfolio = {}  # MNQ → NQ
        portfolio_to_exec = {}  # NQ → MNQ
        for inst in config.instruments:
            p_sym = inst.portfolio_symbol or inst.symbol
            exec_to_portfolio[inst.symbol] = p_sym
            portfolio_to_exec[p_sym] = inst.symbol

        # Account info
        account = broker.get_account_info()
        logger.info(
            f"Account equity: ${account.equity:,.2f}, "
            f"buying power: ${account.buying_power:,.2f}"
        )

        if account.equity <= 0:
            logger.critical("Account equity is zero or negative")
            return 1

        # Resolve contracts
        resolver = ContractResolver(broker.ib, config.roll)
        contract_pairs = resolver.resolve_all(config.instruments)

        if not contract_pairs:
            logger.critical("No contracts resolved")
            return 1

        # Daily evaluation — thresholds decide whether to trade
        order_mgr = OrderManager(broker, config)

        # Fetch bars + build continuous series
        bar_fetcher = BarFetcher(broker.ib, Path(config.data.bar_history_dir))
        continuous = ContinuousSeries(Path(config.data.continuous_dir))

        for symbol, pair in contract_pairs.items():
            lookback = config.data.lookback_bars
            if lookback > 365:
                # IBKR rejects day durations > 365; convert to years
                years = (lookback // 365) + 1
                duration_str = f"{years} Y"
            else:
                duration_str = f"{lookback} D"
            front_bars = bar_fetcher.fetch_and_cache(
                pair.front.contract, symbol,
                duration=duration_str,
            )
            next_bars = None
            if pair.next:
                next_bars = bar_fetcher.fetch_bars(
                    pair.next.contract, duration="5 D",
                )
            continuous.update(symbol, front_bars, pair, next_bars)

        # Build MarketData and compute signals
        # Import R-factory MarketData interface
        sys.path.insert(0, config.rfactory_path)
        from algo_research_factory.src.strategy.interface import MarketData

        # Load continuous series into MarketData format
        # Strategies see portfolio symbols (NQ, ES, ...) as instrument names
        market_data = _build_market_data(
            continuous, contract_pairs, config, exec_to_portfolio,
        )

        # Compute aggregate target signals (keyed by portfolio symbol)
        portfolio_targets, is_v2 = compute_aggregate_targets(
            market_data,
            strategies_config.strategies,
            config,
        )

        # Map signals back to execution symbols (NQ→MNQ, ES→MES, ...)
        targets = {}
        for p_sym, signal in portfolio_targets.items():
            e_sym = portfolio_to_exec.get(p_sym, p_sym)
            targets[e_sym] = signal

        logger.info(
            f"{'V2' if is_v2 else 'V1'} target signals: "
            + ", ".join(f"{k}={v:+.6f}" for k, v in targets.items())
        )

        # Execute rebalance
        if args.dry_run:
            logger.info("DRY RUN — no orders will be placed")
            _print_dry_run(targets, contract_pairs, account.equity, config, broker)
            return 0

        records = order_mgr.execute_rebalance(
            target_signals=targets,
            contract_pairs=contract_pairs,
            equity=account.equity,
        )

        # Audit logging
        n_orders = 0
        n_rolls = 0
        n_errors = 0
        total_commission = 0.0

        for rec in records:
            event_type = rec.get("type", "unknown")
            status = rec.get("status", "")
            commission = rec.get("commission", 0.0) or 0.0

            if status == "FAILED":
                n_errors += 1
                notifier.notify_error(
                    rec.get("symbol", "?"),
                    rec.get("error", "unknown error"),
                )
            else:
                if event_type == "roll":
                    n_rolls += 1
                    notifier.notify_roll(
                        rec["symbol"],
                        rec.get("from_month", ""),
                        rec.get("to_month", ""),
                        rec.get("quantity", 0),
                        status,
                    )
                else:
                    n_orders += 1

            total_commission += commission

            audit.log_execution(
                run_date=today,
                symbol=rec.get("symbol", ""),
                event_type=event_type,
                action=rec.get("action"),
                quantity=rec.get("quantity"),
                target_contracts=rec.get("target_contracts"),
                current_contracts=rec.get("current_contracts"),
                target_signal=rec.get("target_signal"),
                fill_price=rec.get("fill_price"),
                bar_close=rec.get("bar_close"),
                commission=commission,
                status=status,
                error=rec.get("error"),
            )

        # Update state
        state["last_run_date"] = today
        _save_state(state)

        # Run summary
        final_positions = broker.get_positions_by_symbol()
        pos_map = {
            sym: int(p.position) for sym, p in final_positions.items()
        }

        audit.log_run(
            run_date=today, equity=account.equity,
            n_instruments=len(contract_pairs),
            n_orders=n_orders, n_rolls=n_rolls, n_errors=n_errors,
            total_commission=total_commission,
            last_rebalance_date=state.get("last_run_date"),
        )

        summary = notifier.build_run_summary(
            run_date=today, equity=account.equity,
            targets=targets, records=records,
            n_orders=n_orders, n_rolls=n_rolls, n_errors=n_errors,
            total_commission=total_commission,
            positions=pos_map,
        )
        print(summary)
        notifier.send(summary)

        logger.info(
            f"Run complete: {n_orders} orders, {n_rolls} rolls, "
            f"{n_errors} errors, ${total_commission:.2f} commission"
        )

        return 1 if n_errors > 0 else 0

    finally:
        broker.disconnect()
        audit.close()


def cmd_status(args):
    """Show connection status, account info, positions, and contract details."""
    config = load_settings(CONFIG_DIR)

    from futures_executor.data.contract_resolver import ContractResolver
    from futures_executor.execution.broker import BrokerConnection

    broker = BrokerConnection(config.broker)
    try:
        broker.connect()
    except Exception as e:
        print(f"Connection failed: {e}")
        return 1

    try:
        account = broker.get_account_info()
        print(f"\nAccount:")
        print(f"  Equity:       ${account.equity:>12,.2f}")
        print(f"  Buying Power: ${account.buying_power:>12,.2f}")
        print(f"  Unrealized:   ${account.unrealized_pnl:>12,.2f}")
        print(f"  Realized:     ${account.realized_pnl:>12,.2f}")

        positions = broker.get_positions_by_symbol()
        print(f"\nPositions ({len(positions)}):")
        if positions:
            for sym, pos in sorted(positions.items()):
                print(
                    f"  {sym:6s}  {pos.position:+6.0f} contracts  "
                    f"({pos.contract_month})  avg={pos.avg_cost:.2f}"
                )
        else:
            print("  (none)")

        resolver = ContractResolver(broker.ib, config.roll)
        pairs = resolver.resolve_all(config.instruments)
        print(f"\nContracts ({len(pairs)}):")
        for sym, pair in sorted(pairs.items()):
            roll_flag = " [ROLL DUE]" if pair.roll_due else ""
            hard_flag = " [HARD DEADLINE]" if pair.hard_deadline else ""
            next_sym = pair.next.local_symbol if pair.next else "N/A"
            print(
                f"  {sym:6s}  front={pair.front.local_symbol:8s} "
                f"(exp {pair.front.expiry}, {pair.days_to_expiry}d)  "
                f"next={next_sym}"
                f"{roll_flag}{hard_flag}"
            )

        state = _load_state()
        last_run = state.get("last_run_date", "(never)")
        print(f"\nLast run: {last_run}")

    finally:
        broker.disconnect()

    return 0


def cmd_flatten(args):
    """Flatten all positions (close everything)."""
    config = load_settings(CONFIG_DIR)

    from futures_executor.execution.broker import BrokerConnection
    from futures_executor.monitoring.audit import AuditLog
    from futures_executor.monitoring.notifier import SignalNotifier

    broker = BrokerConnection(config.broker)
    audit = AuditLog(config.audit.db_path)
    notifier = SignalNotifier(config.signal)
    today = date.today().isoformat()

    try:
        broker.connect()
    except Exception as e:
        print(f"Connection failed: {e}")
        return 1

    try:
        positions = broker.get_positions()
        if not positions:
            print("No positions to flatten.")
            return 0

        if not args.confirm:
            print("Positions to flatten:")
            for pos in positions:
                print(f"  {pos.symbol}: {pos.position:+.0f} ({pos.contract_month})")
            print("\nAdd --confirm to execute.")
            return 0

        for pos in positions:
            action = "SELL" if pos.position > 0 else "BUY"
            qty = int(abs(pos.position))

            from ib_insync import Contract
            contract = Contract(conId=pos.con_id)
            qualified = broker.ib.qualifyContracts(contract)
            if not qualified:
                logger.error(f"Failed to qualify {pos.symbol} (conId={pos.con_id})")
                continue

            trade = broker.place_market_order(qualified[0], action, qty)
            fill = broker.get_fill_info(trade)

            audit.log_execution(
                run_date=today,
                symbol=pos.symbol,
                event_type="flatten",
                action=action,
                quantity=qty,
                fill_price=fill.avg_fill_price,
                commission=fill.commission,
                status=trade.orderStatus.status,
            )
            print(
                f"  {pos.symbol}: {action} {qty} @ {fill.avg_fill_price:.2f} "
                f"[{trade.orderStatus.status}]"
            )

        notifier.send(f"FLATTEN executed — all positions closed ({today})")

    finally:
        broker.disconnect()
        audit.close()

    return 0


def cmd_roll_status(args):
    """Show roll status for all instruments."""
    config = load_settings(CONFIG_DIR)

    from futures_executor.data.contract_resolver import ContractResolver
    from futures_executor.execution.broker import BrokerConnection

    broker = BrokerConnection(config.broker)
    try:
        broker.connect()
    except Exception as e:
        print(f"Connection failed: {e}")
        return 1

    try:
        resolver = ContractResolver(broker.ib, config.roll)
        pairs = resolver.resolve_all(config.instruments)

        print(f"\nRoll Status ({date.today()}):")
        print(f"{'Symbol':8s} {'Front':10s} {'Expiry':12s} {'Days':>5s} "
              f"{'Next':10s} {'Roll?':6s} {'Volume OK?':>10s}")
        print("-" * 70)

        for sym, pair in sorted(pairs.items()):
            vol_ok = ""
            if pair.roll_due and pair.next:
                vol_ok = "YES" if resolver.check_next_volume(pair) else "NO"

            next_sym = pair.next.local_symbol if pair.next else "N/A"
            roll_str = "YES" if pair.roll_due else "no"
            if pair.hard_deadline:
                roll_str = "HARD"

            print(
                f"{sym:8s} {pair.front.local_symbol:10s} "
                f"{str(pair.front.expiry):12s} {pair.days_to_expiry:5d} "
                f"{next_sym:10s} {roll_str:6s} {vol_ok:>10s}"
            )

    finally:
        broker.disconnect()

    return 0


def cmd_audit(args):
    """Show recent execution history."""
    config = load_settings(CONFIG_DIR)

    from futures_executor.monitoring.audit import AuditLog

    audit = AuditLog(config.audit.db_path)

    runs = audit.get_run_history(n=args.days)
    print(f"\nRun History (last {args.days} runs):")
    print(f"{'Date':12s} {'Equity':>14s} {'Orders':>7s} {'Rolls':>6s} "
          f"{'Errors':>7s} {'Commission':>12s}")
    print("-" * 65)
    for r in runs:
        print(
            f"{r['run_date']:12s} ${r['equity']:13,.2f} "
            f"{r['n_orders']:7d} {r['n_rolls']:6d} "
            f"{r['n_errors']:7d} ${r['total_commission']:11.2f}"
        )

    audit.close()
    return 0


def cmd_slippage(args):
    """Show per-fill slippage detail (FXE-style)."""
    config = load_settings(CONFIG_DIR)

    from futures_executor.monitoring.audit import AuditLog

    audit = AuditLog(config.audit.db_path)
    rows = audit.get_slippage_report(args.limit)
    audit.close()

    if not rows:
        print("No filled orders with slippage data yet.")
        return 0

    print(
        f"{'Timestamp':<22} {'Symbol':<8} {'Side':<5} "
        f"{'BarClose':>12} {'Fill':>12} {'Slippage':>12}"
    )
    print("-" * 75)
    for r in rows:
        bar = r["bar_close"] or 0
        fill = r["fill_price"] or 0
        slip = r["slippage_ticks"] or 0
        print(
            f"{r['timestamp'][:19]:<22} {r['symbol']:<8} {r['action']:<5} "
            f"{bar:>12.4f} {fill:>12.4f} {slip:>+12.4f}"
        )

    # Summary
    slips = [r["slippage_ticks"] for r in rows if r["slippage_ticks"] is not None]
    if slips:
        print(f"\nSummary ({len(slips)} fills):")
        print(f"  Slippage: mean={sum(slips)/len(slips):+.4f}  max={max(slips, key=abs):+.4f}")

    return 0


def _build_market_data(continuous, contract_pairs, config, exec_to_portfolio=None):
    """Build a MarketData-compatible object from continuous series.

    Loads each instrument's continuous parquet series and stacks into
    the MarketData format expected by R-factory strategies.
    Uses portfolio symbols (NQ, ES, ...) as instrument_names so strategies
    see the same names as in backtesting.
    """
    import numpy as np

    sys.path.insert(0, config.rfactory_path)
    from algo_research_factory.src.strategy.interface import MarketData

    exec_symbols = sorted(contract_pairs.keys())
    all_series = {}
    for symbol in exec_symbols:
        df = continuous.load(symbol)
        if df is not None and not df.empty:
            all_series[symbol] = df

    if not all_series:
        raise ValueError("No continuous series data available")

    # Align dates across all instruments
    # Use intersection of dates
    date_sets = [set(df["date"].dt.date) for df in all_series.values()]
    common_dates = sorted(set.intersection(*date_sets))

    if len(common_dates) < 50:
        raise ValueError(
            f"Only {len(common_dates)} common dates across instruments "
            f"(need at least 50)"
        )

    n_bars = len(common_dates)
    n_inst = len(exec_symbols)

    open_arr = np.zeros((n_bars, n_inst))
    high_arr = np.zeros((n_bars, n_inst))
    low_arr = np.zeros((n_bars, n_inst))
    close_arr = np.zeros((n_bars, n_inst))
    volume_arr = np.full((n_bars, n_inst), np.nan)

    for j, symbol in enumerate(exec_symbols):
        df = all_series[symbol]
        df = df.copy()
        df["_date"] = df["date"].dt.date
        df = df[df["_date"].isin(set(common_dates))]
        df = df.sort_values("_date").reset_index(drop=True)

        open_arr[:, j] = df["open"].values[:n_bars]
        high_arr[:, j] = df["high"].values[:n_bars]
        low_arr[:, j] = df["low"].values[:n_bars]
        close_arr[:, j] = df["close"].values[:n_bars]
        if "volume" in df.columns:
            volume_arr[:, j] = df["volume"].values[:n_bars]

    dates_arr = np.array(common_dates, dtype="datetime64[D]")

    # Use portfolio symbols as instrument names so strategies see NQ, ES, etc.
    if exec_to_portfolio:
        instrument_names = [exec_to_portfolio.get(s, s) for s in exec_symbols]
    else:
        instrument_names = list(exec_symbols)

    return MarketData(
        open=open_arr,
        high=high_arr,
        low=low_arr,
        close=close_arr,
        volume=volume_arr,
        dates=dates_arr,
        instrument_names=instrument_names,
    )


def _print_dry_run(targets, contract_pairs, equity, config, broker):
    """Print what would happen without executing."""
    from futures_executor.execution.order_manager import (
        apply_margin_cap,
        compute_contract_size,
        compute_position_diff,
    )

    current_positions = broker.get_positions_by_symbol()

    is_v2 = config.vol_target.instrument_level
    mode = "V2 (instrument-level)" if is_v2 else "V1 (strategy-level)"
    print(f"\nDry Run [{mode}] — equity=${equity:,.2f}, leverage={config.execution.portfolio_leverage}")
    print(f"{'Symbol':8s} {'Sized Pos':>10s} {'Target':>7s} {'Current':>8s} "
          f"{'Delta':>6s} {'Notional/ct':>14s} {'Margin':>8s}")
    print("-" * 72)

    # Size all instruments first (for margin cap)
    sizing = {}
    from futures_executor.execution.order_manager import OrderManager
    om = OrderManager(broker, config)

    for symbol, signal in sorted(targets.items()):
        pair = contract_pairs.get(symbol)
        if pair is None:
            continue

        last_price = om._get_last_price(pair)

        sz = compute_contract_size(
            signal=signal,
            equity=equity,
            last_price=last_price,
            multiplier=pair.front.multiplier,
            config=config.execution,
        )
        sz.symbol = symbol
        sizing[symbol] = sz

    # Apply margin cap
    available_margin = equity * config.execution.margin_cap
    sizing = apply_margin_cap(sizing, config.instruments, available_margin)

    margin_map = {i.symbol: i.margin for i in config.instruments}
    for i in config.instruments:
        if i.portfolio_symbol:
            margin_map[i.portfolio_symbol] = i.margin

    total_margin = 0.0
    for symbol in sorted(targets.keys()):
        sz = sizing.get(symbol)
        if sz is None:
            continue

        current = current_positions.get(symbol)
        current_qty = int(current.position) if current else 0
        delta = sz.target_contracts - current_qty
        margin_used = abs(sz.target_contracts) * margin_map.get(symbol, 0)
        total_margin += margin_used

        print(
            f"{symbol:8s} {targets[symbol]:+10.6f} {sz.target_contracts:+7d} "
            f"{current_qty:+8d} {delta:+6d} "
            f"${sz.notional_per_contract:>13,.0f} ${margin_used:>7,.0f}"
        )

    print(f"\nMargin: ${total_margin:,.0f} / ${available_margin:,.0f} "
          f"({total_margin / available_margin * 100:.0f}% of cap)" if available_margin > 0 else "")


def main():
    parser = argparse.ArgumentParser(
        description="Futures executor — IBKR-based portfolio executor",
    )
    parser.add_argument("-v", "--verbose", action="store_true")

    sub = parser.add_subparsers(dest="command")

    # run-once
    p_run = sub.add_parser("run-once", help="Execute one rebalance cycle")
    p_run.add_argument("--dry-run", action="store_true",
                       help="Show what would happen without executing")
    p_run.set_defaults(func=cmd_run_once)

    # status
    p_status = sub.add_parser("status", help="Show account/position/contract status")
    p_status.set_defaults(func=cmd_status)

    # flatten
    p_flat = sub.add_parser("flatten", help="Close all positions")
    p_flat.add_argument("--confirm", action="store_true",
                        help="Actually execute (without this, just shows positions)")
    p_flat.set_defaults(func=cmd_flatten)

    # roll-status
    p_roll = sub.add_parser("roll-status", help="Show roll status for all instruments")
    p_roll.set_defaults(func=cmd_roll_status)

    # audit
    p_audit = sub.add_parser("audit", help="Show execution history")
    p_audit.add_argument("--days", type=int, default=30, help="Number of records")
    p_audit.set_defaults(func=cmd_audit)

    # slippage
    p_slip = sub.add_parser("slippage", help="Show per-fill slippage detail")
    p_slip.add_argument("-n", "--limit", type=int, default=100, help="Number of fills")
    p_slip.set_defaults(func=cmd_slippage)

    args = parser.parse_args()
    _setup_logging(args.verbose)

    if not args.command:
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main() or 0)
