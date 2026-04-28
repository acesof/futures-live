"""Daily snapshot writer — the futures-live side of the monitor contract.

Mirrors forex-live's ``monitoring/snapshot.py`` shape. Reads IBKR
account / positions / fills, local audit.db, and the persisted
``targets_<date>.json`` / ``close_prices_<date>.json`` from the
executor's audit dir; writes a canonical ``Snapshot`` JSON under
R-factory's ``artifacts/monitor/<set>/snapshots/``.

Broker-side math (executed here, not in R-factory):

    effective_fraction[inst] = |contracts| × multiplier × close_price / equity

    slippage_amount[fill] = sign × (fill_price − bar_close_price)
                            × multiplier × contracts         (USD cost)

Schema + paths live in
``algo_research_factory.src.monitor.snapshot_contract`` — both sides
import from there.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from algo_research_factory.src.monitor.snapshot_contract import (
    AccountSnap,
    FillSnap,
    PositionSnap,
    SCHEMA_VERSION,
    Snapshot,
    TransactionSnap,
    snapshot_path,
)

from futures_executor.config.loader import ExecutorConfig
from futures_executor.execution.broker import BrokerConnection

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _hash_file(path: Path) -> str:
    if not path.exists():
        return ""
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _executor_commit() -> str:
    """Best-effort ``git rev-parse HEAD`` in the futures-live repo."""
    try:
        repo_dir = Path(__file__).resolve().parents[3]
        out = subprocess.run(
            ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=5,
        )
        if out.returncode == 0:
            return out.stdout.strip()[:12]
    except Exception:
        pass
    return ""


def _bridge_version() -> str:
    """No bridge for futures — use ib_insync version as the closest analog."""
    try:
        import ib_insync
        return f"ib_insync-{getattr(ib_insync, '__version__', '')}"
    except Exception:
        return ""


def _account_id(broker: BrokerConnection) -> str:
    try:
        accounts = broker.ib.managedAccounts()
        return accounts[0] if accounts else ""
    except Exception:
        return ""


def _dataset_version(r_factory_data_dir: Path, instrument_set: str) -> str:
    manifest_path = r_factory_data_dir / "parquet" / instrument_set / "_manifest.json"
    if not manifest_path.exists():
        return ""
    try:
        with open(manifest_path) as f:
            return str(json.load(f).get("dataset_version", ""))
    except Exception:
        return ""


def _exec_to_portfolio_map(config: ExecutorConfig) -> dict[str, str]:
    """MES → ES, MNQ → NQ, … for mapping raw positions to monitor symbols."""
    out: dict[str, str] = {}
    for inst in config.instruments:
        out[inst.symbol] = inst.portfolio_symbol or inst.symbol
    return out


def _multiplier_map(config: ExecutorConfig) -> dict[str, float]:
    """Portfolio-symbol → contract multiplier (e.g. ES → 5.0 for MES)."""
    out: dict[str, float] = {}
    for inst in config.instruments:
        p_sym = inst.portfolio_symbol or inst.symbol
        out[p_sym] = float(inst.multiplier)
    return out


# ---------------------------------------------------------------------------
# Positions
# ---------------------------------------------------------------------------

def _net_positions(
    broker: BrokerConnection,
    close_prices: dict[str, float],
    equity_account_ccy: float,
    usd_to_account: float,
    exec_to_portfolio: dict[str, str],
    multiplier_map: dict[str, float],
) -> list[PositionSnap]:
    """Convert broker positions to PositionSnap rows keyed on portfolio symbol.

    Effective fraction is in ACCOUNT currency:
      notional_account_ccy = contracts × multiplier × close_usd × usd_to_account
      effective_fraction   = notional_account_ccy / equity_account_ccy
    """
    raw = broker.get_positions()
    out: list[PositionSnap] = []
    for p in raw:
        p_sym = exec_to_portfolio.get(p.symbol, p.symbol)
        side = "LONG" if p.position > 0 else "SHORT"
        contracts = abs(float(p.position))
        multiplier = float(p.multiplier or multiplier_map.get(p_sym, 1.0))
        mark_usd = float(close_prices.get(p_sym, 0.0))
        eff = 0.0
        if equity_account_ccy > 0 and mark_usd > 0:
            notional_account = contracts * multiplier * mark_usd * usd_to_account
            eff = notional_account / equity_account_ccy
        out.append(PositionSnap(
            label=p.local_symbol or p_sym,
            instrument=p_sym,           # MONITOR KEYS ON PORTFOLIO SYMBOL (stable across rolls)
            side=side,
            amount=contracts,
            open_price=float(p.avg_cost) / multiplier if multiplier else 0.0,
            unrealized_pnl_amount=0.0,  # IBKR per-position unrealized requires reqPnLSingle subscription
            effective_fraction=eff,
        ))
    return out


# ---------------------------------------------------------------------------
# Fills + transactions from audit.db
# ---------------------------------------------------------------------------

def _fills_and_transactions_from_audit(
    audit_db_path: Path,
    run_date: str,
    tracking_since_iso: str,
    close_prices: dict[str, float],
    usd_to_account: float,
    exec_to_portfolio: dict[str, str],
    multiplier_map: dict[str, float],
) -> tuple[list[FillSnap], list[TransactionSnap]]:
    """Read futures-live's audit.db::executions for fills + transactions.

    futures-live logs every execution with commission + slippage_ticks +
    fill_price + bar_close. We compute slippage_amount (USD) here.

    Since audit.db doesn't track per-trade realized PnL, transactions
    carry ``realized_pnl_amount=0.0``. Balance-jump check in the monitor
    will accumulate commission only — adequate for our v1 contract
    (monitor reset on rollover covers the one case where this matters).
    """
    if not Path(audit_db_path).exists():
        return [], []

    conn = sqlite3.connect(str(audit_db_path))
    conn.row_factory = sqlite3.Row
    try:
        today_rows = conn.execute(
            "SELECT * FROM executions WHERE run_date = ? ORDER BY id",
            (run_date,),
        ).fetchall()
        window_rows = conn.execute(
            "SELECT * FROM executions WHERE timestamp >= ? ORDER BY id",
            (tracking_since_iso,),
        ).fetchall()
    finally:
        conn.close()

    fills = [_row_to_fill(r, close_prices, usd_to_account, exec_to_portfolio, multiplier_map)
             for r in today_rows if _is_real_execution(r)]
    transactions = [_row_to_transaction(r, usd_to_account, exec_to_portfolio)
                    for r in window_rows if _is_real_execution(r)]
    return fills, transactions


def _is_real_execution(row: sqlite3.Row) -> bool:
    """Filter out skip/hold/no-op rows — only keep actual fills."""
    status = (row["status"] or "").strip().lower()
    action = (row["action"] or "").strip().upper()
    if action not in ("BUY", "SELL"):
        return False
    if row["fill_price"] is None:
        return False
    # "Filled" / "filled" variations from IBKR
    if "fill" not in status:
        return False
    return True


def _row_to_fill(
    row: sqlite3.Row,
    close_prices: dict[str, float],
    usd_to_account: float,
    exec_to_portfolio: dict[str, str],
    multiplier_map: dict[str, float],
) -> FillSnap:
    exec_sym = row["symbol"] or ""
    p_sym = exec_to_portfolio.get(exec_sym, exec_sym)
    side = (row["action"] or "").strip().upper()
    qty = abs(float(row["quantity"] or 0.0))
    fill_price = float(row["fill_price"]) if row["fill_price"] is not None else None
    bar_close = float(row["bar_close"]) if row["bar_close"] is not None else None
    multiplier = float(multiplier_map.get(p_sym, 1.0))

    slippage_amount: float | None = None
    slippage_bps: float | None = None
    if fill_price is not None and bar_close is not None and bar_close > 0:
        sign = 1.0 if side == "BUY" else -1.0
        # USD cost → account currency: sign × (fill − close) × multiplier × qty × usd_to_account
        slippage_amount = (
            sign * (fill_price - bar_close) * multiplier * qty * usd_to_account
        )
        slippage_bps = sign * (fill_price - bar_close) / bar_close * 10_000.0

    return FillSnap(
        fill_timestamp=row["timestamp"] or "",
        instrument=p_sym,
        side=side,
        lots_submitted=qty,
        lots_filled=qty,
        bar_close_price=bar_close,
        market_price=None,       # futures-live doesn't track a mid/market reference
        fill_price=fill_price,
        total_slippage_bps=slippage_bps,
        slippage_amount=slippage_amount,
    )


def _row_to_transaction(
    row: sqlite3.Row,
    usd_to_account: float,
    exec_to_portfolio: dict[str, str],
) -> TransactionSnap:
    exec_sym = row["symbol"] or ""
    p_sym = exec_to_portfolio.get(exec_sym, exec_sym)
    qty = abs(float(row["quantity"] or 0.0))
    fill_price = float(row["fill_price"] or 0.0)
    commission = float(row["commission"] or 0.0)
    # IBKR reports commission as a positive number (amount charged). Monitor
    # convention: negative = paid. Flip sign here so forex + futures agree.
    # IBKR commission is denominated in USD on this account; FX-convert to
    # account currency for consistency with slippage_amount in _row_to_fill
    # (the dashboard's COSTS section sums these in account currency, not USD).
    commission_signed = -abs(commission) * usd_to_account if commission else 0.0
    ts = row["timestamp"] or ""
    ts_ms = _iso_to_ms(ts)
    return TransactionSnap(
        label=f"{exec_sym}-{row['id']}",
        instrument=p_sym,
        side=(row["action"] or "").strip().upper(),
        amount=qty,
        open_price=fill_price,        # single-fill → open ≈ close
        close_price=fill_price,
        commission_amount=commission_signed,
        realized_pnl_amount=0.0,      # futures-live audit doesn't track per-trade realized PnL yet
        open_time_ms=ts_ms,
        close_time_ms=ts_ms,
    )


def _iso_to_ms(iso: str) -> int:
    if not iso:
        return 0
    try:
        # Accept both naive and tz-aware ISO strings.
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Targets + close prices (persisted by cmd_run_once)
# ---------------------------------------------------------------------------

def _load_targets(audit_db_path: Path, run_date: str) -> tuple[dict[str, float], bool]:
    path = audit_db_path.parent / f"targets_{run_date}.json"
    if not path.exists():
        logger.warning(f"targets snapshot not found: {path}")
        return {}, False
    with open(path) as f:
        data = json.load(f)
    return dict(data.get("targets", {})), bool(data.get("is_v2", False))


def _load_close_prices(audit_db_path: Path, run_date: str) -> dict[str, float]:
    path = audit_db_path.parent / f"close_prices_{run_date}.json"
    if not path.exists():
        logger.warning(f"close prices snapshot not found: {path}")
        return {}
    with open(path) as f:
        return {k: float(v) for k, v in json.load(f).items()}


def _parquet_close_fallback(
    r_factory_data_dir: Path,
    instrument_set: str,
    symbols: list[str],
) -> dict[str, float]:
    """Load the latest close per symbol from R-factory's canonical parquet.

    Used when ``close_prices_<date>.json`` is missing (manual snapshot runs,
    or cron order variations) — monitor_cycle.sh runs ingest-futures-ibkr
    immediately before snapshot, so the parquet has today's close.
    """
    out: dict[str, float] = {}
    pq_dir = Path(r_factory_data_dir) / "parquet" / instrument_set
    for sym in symbols:
        p = pq_dir / f"{sym}.parquet"
        if not p.exists():
            continue
        try:
            import pandas as pd  # lazy — only needed on fallback
            df = pd.read_parquet(p)
            if "Close" in df.columns and len(df):
                out[sym] = float(df["Close"].iloc[-1])
        except Exception as e:
            logger.warning(f"parquet-close fallback failed for {sym}: {e}")
    return out


def _eurusd_spot(broker: BrokerConnection) -> float:
    """Fetch EUR/USD spot from IBKR. Returns 0.0 on failure (caller handles)."""
    try:
        from ib_insync import Contract
        c = Contract()
        c.symbol, c.secType, c.exchange, c.currency = "EUR", "CASH", "IDEALPRO", "USD"
        qualified = broker.ib.qualifyContracts(c)
        if not qualified:
            return 0.0
        ticker = broker.ib.reqMktData(qualified[0], "", snapshot=True, regulatorySnapshot=False)
        broker.ib.sleep(2.5)
        price = None
        for attr in ("last", "close", "marketPrice"):
            v = getattr(ticker, attr, None)
            if v and v > 0:
                price = v
                break
        try:
            broker.ib.cancelMktData(qualified[0])
        except Exception:
            pass
        return float(price) if price else 0.0
    except Exception as e:
        logger.warning(f"EUR/USD fetch failed: {e}")
        return 0.0


def _usd_to_account_factor(account_currency: str, eurusd_spot: float) -> float:
    """Multiplier that converts a USD-denominated amount into the account's
    base currency. Returns 1.0 for USD-base or any unrecognised currency
    (logged).
    """
    ccy = (account_currency or "").upper()
    if ccy == "USD":
        return 1.0
    if ccy == "EUR":
        if eurusd_spot and eurusd_spot > 0:
            return 1.0 / eurusd_spot
        logger.warning("EUR account but no EUR/USD rate available; using 1.0 (wrong)")
        return 1.0
    logger.warning(f"Unsupported account currency {ccy!r}; using 1.0 USD→{ccy} factor")
    return 1.0


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_snapshot(
    config: ExecutorConfig,
    broker: BrokerConnection,
    instrument_set: str,
    tracking_since_iso: str,
    run_date: str | None = None,
    run_timestamp: str | None = None,
    strategies_yaml_path: Path | None = None,
) -> Snapshot:
    """Build the canonical daily Snapshot for futures-live.

    ``tracking_since_iso`` is the lower bound for transactions_since.
    The monitor filters down to the actual tracking_start on its side.
    """
    run_timestamp = run_timestamp or datetime.now(timezone.utc).isoformat()
    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    account_info = broker.get_account_info()
    account = AccountSnap(
        equity=float(account_info.equity),
        balance=float(account_info.equity),  # futures: equity ≈ balance (no separate cash concept exposed)
        used_margin=0.0,                      # BrokerConnection doesn't expose; fill later if needed
        currency=account_info.currency or "USD",
        leverage=float(config.execution.portfolio_leverage),
        account_id=_account_id(broker),
    )

    close_prices = _load_close_prices(Path(config.audit.db_path), run_date)

    exec_to_portfolio = _exec_to_portfolio_map(config)
    multiplier_map = _multiplier_map(config)

    # Fallback: if the persisted close_prices file is missing any
    # instrument (manual snapshot without prior run-once, or partial
    # write), read from R-factory's freshly-ingested parquet instead.
    portfolio_symbols = [exec_to_portfolio[i.symbol] for i in config.instruments]
    missing = [s for s in portfolio_symbols if s not in close_prices]
    if missing:
        fallback = _parquet_close_fallback(
            Path(config.monitor.r_factory_data_dir), instrument_set, missing,
        )
        if fallback:
            logger.info(f"close_prices parquet-fallback for: {sorted(fallback)}")
        close_prices.update(fallback)

    # FX conversion USD notional → account currency. EUR-base account
    # trading USD-denominated futures: fetch EUR/USD from IBKR once and
    # apply. For USD-base, factor is 1.0.
    if account.currency.upper() == "USD":
        eurusd_spot = 0.0
        usd_to_account = 1.0
    else:
        eurusd_spot = _eurusd_spot(broker)
        usd_to_account = _usd_to_account_factor(account.currency, eurusd_spot)
        logger.info(
            f"FX USD→{account.currency}: factor={usd_to_account:.6f} "
            f"(EURUSD={eurusd_spot:.5f})"
        )

    positions = _net_positions(
        broker, close_prices, account.equity, usd_to_account,
        exec_to_portfolio, multiplier_map,
    )

    fills, transactions = _fills_and_transactions_from_audit(
        Path(config.audit.db_path), run_date, tracking_since_iso,
        close_prices, usd_to_account, exec_to_portfolio, multiplier_map,
    )

    # is_v2 is a stable config property — decoupled from the optional
    # targets file (absent on pre-executor-run snapshots).
    targets, _ = _load_targets(Path(config.audit.db_path), run_date)
    is_v2 = bool(config.vol_target.instrument_level)

    if strategies_yaml_path is None:
        strategies_yaml_path = (
            Path(__file__).resolve().parents[1] / "config" / "strategies.yaml"
        )
    strategies_hash = _hash_file(strategies_yaml_path)
    weights_hash = strategies_hash

    dataset_version = _dataset_version(
        Path(config.monitor.r_factory_data_dir), instrument_set,
    )

    tracking_since_ms = _iso_to_ms(tracking_since_iso)

    # Vol-target block (sub-dict carried on the operational fingerprint).
    # Post-leverage-refactor vocabulary; R-factory's
    # `replay_params_from_snapshot` accepts both this and the pre-refactor
    # form (back-compat layer in capture.py:34) so cross-repo migration
    # order doesn't matter.
    vol_target_dict = {
        "enabled": bool(config.vol_target.enabled),
        "target_sleeve_vol": float(config.vol_target.target_sleeve_vol),
        "vol_window": int(config.vol_target.vol_window),
        "vol_floor": float(config.vol_target.vol_floor),
        "instrument_level": bool(config.vol_target.instrument_level),
    }

    return Snapshot(
        schema_version=SCHEMA_VERSION,
        instrument_set=instrument_set,
        broker_id=config.monitor.broker_id,
        run_timestamp=run_timestamp,
        run_date=run_date,
        bridge_version=_bridge_version(),
        executor_commit=_executor_commit(),
        strategies_yaml_hash=strategies_hash,
        weights_json_hash=weights_hash,
        dataset_version=dataset_version,
        account=account,
        positions=positions,
        targets=targets,
        is_v2=is_v2,
        transactions_since=transactions,
        fills_today=fills,
        tracking_since_ms=tracking_since_ms,
        portfolio_leverage=float(config.execution.portfolio_leverage),
        gross_exposure_cap=(
            float(config.execution.gross_exposure_cap)
            if config.execution.gross_exposure_cap is not None else None
        ),
        dynamic_lot_sizing=True,              # futures always size from equity + price
        min_delta_lots=float(config.execution.abs_threshold),  # in contracts
        vol_target=vol_target_dict,
    )


def write_snapshot(
    snapshot: Snapshot,
    r_factory_artifacts_dir: Path,
) -> Path:
    path = snapshot_path(
        r_factory_artifacts_dir, snapshot.instrument_set, snapshot.run_date,
    )
    snapshot.write(path)
    logger.info(f"Wrote snapshot: {path}")
    return path
