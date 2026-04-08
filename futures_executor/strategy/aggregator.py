"""Multi-strategy signal aggregation with vol-targeting.

V1: per-strategy vol scaling, weighted per-instrument position reconstruction.
    Output is sized_j — matching the R-factory portfolio pipeline exactly:
    sized_j = Σ(w_i × scale_i × pos_i[last, j]) / n_instruments

V2: raw signal aggregation with active-weight normalization,
    then instrument-level vol targeting. Output is sized_j —
    a fraction of capital already containing the risk budget B_j.

Both modes return sized positions (fraction of capital).
Imports core math from R-factory to maintain backtest-live parity.
"""

import logging
import sys
import types
from pathlib import Path

import numpy as np

from futures_executor.config.loader import (
    ExecutorConfig,
    StrategyEntry,
    VolTargetSettings,
)

logger = logging.getLogger(__name__)


def _import_strategy(entry: StrategyEntry):
    """Dynamically import a strategy module from file path."""
    path = Path(entry.module_path)
    if not path.exists():
        raise FileNotFoundError(f"Strategy file not found: {path}")

    module_name = f"_futures_strategy_{path.stem}"
    if module_name in sys.modules:
        del sys.modules[module_name]

    source = path.read_text()
    code = compile(source, str(path), "exec")
    module = types.ModuleType(module_name)
    module.__file__ = str(path)
    sys.modules[module_name] = module
    exec(code, module.__dict__)
    return module


# ---------------------------------------------------------------------------
# V1: per-strategy vol scaling, pipeline-matching position reconstruction
# ---------------------------------------------------------------------------

def _aggregate_v1(
    market_data,
    strategies: list[StrategyEntry],
    config: ExecutorConfig,
) -> dict[str, float]:
    """V1: replay strategies, vol-target scale, reconstruct per-instrument positions.

    Matches R-factory pipeline.py exactly:
      sized_pos[j] = Σ(w_i × vt_scale_i × pos_i[last, j]) / n_instruments

    Returns sized positions (fraction of capital per instrument).
    """
    from algo_research_factory.src.kernel.pnl import aggregate_returns, compute_returns
    from algo_research_factory.src.kernel.positions import signals_to_positions
    from algo_research_factory.src.portfolio.vol_target import vol_target_scale

    instrument_names = market_data.instrument_names
    n_instruments = len(instrument_names)
    vt = config.vol_target

    # Collect per-strategy: weight, last-bar scale, last-bar positions
    strategy_data = []  # list of (weight, scale, positions_last)

    for entry in strategies:
        if not entry.enabled:
            continue
        try:
            module = _import_strategy(entry)
            output = module.generate_signals(market_data, entry.params)
            positions = signals_to_positions(output.target_position)

            # Compute vol-target scale from aggregated 1D returns
            returns_2d = compute_returns(positions, market_data.close, cost_bps=0.0)
            returns_1d = aggregate_returns(returns_2d)

            if vt.enabled:
                scale_series = vol_target_scale(
                    returns_1d,
                    target_vol=vt.target_vol,
                    vol_window=vt.vol_window,
                    max_leverage=vt.max_leverage,
                )
                last_scale = float(scale_series[-1])
            else:
                last_scale = 1.0

            # Last-bar positions per instrument
            if positions.ndim == 2:
                pos_last = positions[-1]  # (n_instruments,)
            else:
                pos_last = np.array([positions[-1]])

            strategy_data.append((entry.weight, last_scale, pos_last))
            logger.debug(
                f"  {entry.name}: vt_scale={last_scale:.3f} "
                f"weight={entry.weight:.4f} pos_last={pos_last}"
            )
        except Exception as e:
            logger.error(f"Strategy {entry.name} failed: {e}")
            continue

    if not strategy_data:
        logger.warning("No strategies produced signals")
        return {name: 0.0 for name in instrument_names}

    # Reconstruct per-instrument sized positions
    # sized_pos[j] = Σ(w_i × scale_i × pos_i[j]) / n_instruments
    sized = np.zeros(n_instruments)
    for j in range(n_instruments):
        for weight, scale, pos_last in strategy_data:
            sized[j] += weight * scale * pos_last[j]
        sized[j] /= n_instruments

    targets = {}
    for i, name in enumerate(instrument_names):
        targets[name] = float(sized[i])

    logger.info(
        f"V1 targets ({len(strategy_data)} strategies): "
        + ", ".join(f"{k}={v:+.6f}" for k, v in targets.items())
    )
    return targets


# ---------------------------------------------------------------------------
# V2: instrument-level vol targeting
# ---------------------------------------------------------------------------

def _aggregate_v2(
    market_data,
    strategies: list[StrategyEntry],
    config: ExecutorConfig,
) -> dict[str, float]:
    """V2: aggregate raw signals → instrument-level vol target → sized positions.

    Output is sized_j: a fraction of capital that already includes
    the risk budget B_j = 1/n_instruments. The executor should NOT
    divide by n_instruments again when sizing contracts/lots.
    """
    from algo_research_factory.src.portfolio.vol_target import rolling_volatility

    instrument_names = market_data.instrument_names
    n_instruments = len(instrument_names)
    vt = config.vol_target
    epsilon = 1e-8

    # Step 1: Collect last-bar raw signals from each strategy
    strategy_signals = []  # list of (weight, signals_array)
    for entry in strategies:
        if not entry.enabled:
            continue
        try:
            module = _import_strategy(entry)
            output = module.generate_signals(market_data, entry.params)
            signals = np.clip(output.target_position[-1], -1.0, 1.0)
            strategy_signals.append((entry.weight, signals))
            logger.debug(f"  {entry.name}: weight={entry.weight:.4f} signals={signals}")
        except Exception as e:
            logger.error(f"Strategy {entry.name} failed: {e}")
            continue

    if not strategy_signals:
        logger.warning("No strategies produced signals")
        return {name: 0.0 for name in instrument_names}

    # Step 2: Aggregate per instrument with active-weight normalization
    # s_j = Σ(w_i × σ_{i,j}) / Σ(w_i for active_j)
    # where active = |σ_{i,j}| > epsilon
    agg_signals = np.zeros(n_instruments)
    for j in range(n_instruments):
        weighted_sum = 0.0
        active_weight_sum = 0.0
        for weight, signals in strategy_signals:
            sig_j = signals[j] if signals.ndim > 0 and len(signals) > j else signals
            if abs(sig_j) > epsilon:
                weighted_sum += weight * sig_j
                active_weight_sum += weight
        if active_weight_sum > epsilon:
            agg_signals[j] = np.clip(weighted_sum / active_weight_sum, -1.0, 1.0)

    # Step 3: Compute trailing vol for each instrument from close prices
    # and apply instrument-level vol targeting
    risk_budgets = np.full(n_instruments, 1.0 / n_instruments)
    sized = np.zeros(n_instruments)

    for j in range(n_instruments):
        # Close-to-close returns for this instrument
        inst_close = market_data.close[:, j]
        inst_returns = np.zeros(len(inst_close))
        for t in range(1, len(inst_close)):
            if inst_close[t - 1] > 0 and not np.isnan(inst_close[t - 1]):
                inst_returns[t] = inst_close[t] / inst_close[t - 1] - 1.0

        # Trailing vol (use t-1, no look-ahead)
        vol_series = rolling_volatility(inst_returns, window=vt.vol_window)

        if len(vol_series) >= 2 and not np.isnan(vol_series[-2]) and vol_series[-2] > 1e-10:
            vol_scale = min(vt.target_vol / vol_series[-2], vt.max_leverage)
        else:
            vol_scale = 1.0

        sized[j] = risk_budgets[j] * agg_signals[j] * vol_scale

        logger.info(
            f"  {instrument_names[j]}: agg_signal={agg_signals[j]:+.4f} "
            f"vol_scale={vol_scale:.3f} → sized={sized[j]:+.6f}"
        )

    targets = {}
    for i, name in enumerate(instrument_names):
        targets[name] = float(sized[i])

    logger.info(
        f"V2 aggregate targets ({len(strategy_signals)} strategies): "
        + ", ".join(f"{k}={v:+.6f}" for k, v in targets.items())
    )
    return targets


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _apply_gross_cap(
    targets: dict[str, float],
    cap: float,
) -> dict[str, float]:
    """Proportionally scale all positions if gross exposure exceeds cap."""
    gross = sum(abs(v) for v in targets.values())
    if gross > cap:
        scale = cap / gross
        logger.warning(
            f"Gross exposure {gross:.4f} exceeds cap {cap:.4f}, "
            f"scaling all positions by {scale:.4f}"
        )
        return {k: v * scale for k, v in targets.items()}
    return targets


def compute_aggregate_targets(
    market_data,
    strategies: list[StrategyEntry],
    config: ExecutorConfig,
) -> tuple[dict[str, float], bool]:
    """Compute per-instrument target positions.

    Returns (targets_dict, is_v2) where:
      - Both V1 and V2 targets are sized exposure fractions.
      - V1: reconstructed from per-strategy vol-scaled positions / n_instruments
      - V2: instrument-level vol targeting with risk budget B_j embedded
      - is_v2: True if V2 was used
    """
    is_v2 = config.vol_target.instrument_level

    if is_v2:
        targets = _aggregate_v2(market_data, strategies, config)
    else:
        targets = _aggregate_v1(market_data, strategies, config)

    # Apply gross exposure cap (proportional scale-down)
    cap = config.execution.gross_exposure_cap
    if cap is not None:
        targets = _apply_gross_cap(targets, cap)

    logger.info(
        f"{'V2' if is_v2 else 'V1'} targets: "
        + ", ".join(f"{k}={v:+.4f}" for k, v in targets.items())
    )
    return targets, is_v2
