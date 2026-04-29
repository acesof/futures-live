"""Tests for futures_executor.strategy.aggregator.compute_aggregate_targets.

Parallel to forex's aggregator tests but futures-specific differences:
  - takes ExecutorConfig (not split params)
  - V1 reconstructs from per-strategy vol-scaled positions / n_instruments
  - V2 uses active-weight signal aggregation + per-instrument vol-target
  - gross_exposure_cap applies to BOTH V1 and V2 (forex applies only to V2)

Phase 1 of strategy attribution will extend this module to also emit
per-strategy targets — these tests lock in current behavior.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from algo_research_factory.src.strategy.interface import MarketData
from futures_executor.config.loader import (
    ExecutionSettings,
    StrategyEntry,
    VolTargetSettings,
)
from futures_executor.strategy.aggregator import compute_aggregate_targets


_LONG_ONLY_STRATEGY = '''
import numpy as np
from algo_research_factory.src.strategy.interface import StrategyOutput

STRATEGY_META = {
    "name": "long_only", "family": "test", "parameters": [], "available_fields": ["close"],
}

def generate_signals(market_data, params):
    return StrategyOutput(target_position=np.ones_like(market_data.close))
'''

_SHORT_ONLY_STRATEGY = '''
import numpy as np
from algo_research_factory.src.strategy.interface import StrategyOutput

STRATEGY_META = {
    "name": "short_only", "family": "test", "parameters": [], "available_fields": ["close"],
}

def generate_signals(market_data, params):
    return StrategyOutput(target_position=-np.ones_like(market_data.close))
'''

_BROKEN_STRATEGY = '''
import numpy as np
from algo_research_factory.src.strategy.interface import StrategyOutput

STRATEGY_META = {
    "name": "broken", "family": "test", "parameters": [], "available_fields": ["close"],
}

def generate_signals(market_data, params):
    raise RuntimeError("intentional failure for test")
'''


def _write(tmp_path: Path, name: str, source: str) -> Path:
    p = tmp_path / f"{name}.py"
    p.write_text(source)
    return p


def _make_market_data(n_bars: int = 100, n_inst: int = 3) -> MarketData:
    rng = np.random.default_rng(seed=42)
    rets = rng.normal(0.0001, 0.005, size=(n_bars, n_inst))
    close = 100 * np.exp(np.cumsum(rets, axis=0))
    dates = np.array([np.datetime64("2024-01-01") + np.timedelta64(i, "D")
                      for i in range(n_bars)])
    return MarketData(
        dates=dates, open=close, high=close * 1.001, low=close * 0.999,
        close=close, volume=np.zeros((n_bars, n_inst)),
        instrument_names=["ES", "CL", "GC"][:n_inst],
    )


def _make_config(
    instrument_level: bool = True,
    enabled: bool = True,
    target_sleeve_vol: float = 0.10,
    vol_window: int = 20,
    vol_floor: float = 0.05,
    gross_exposure_cap: float | None = None,
) -> SimpleNamespace:
    """Minimal config matching the duck-typed surface the aggregator reads."""
    return SimpleNamespace(
        vol_target=VolTargetSettings(
            instrument_level=instrument_level,
            enabled=enabled,
            target_sleeve_vol=target_sleeve_vol,
            vol_window=vol_window,
            vol_floor=vol_floor,
        ),
        execution=ExecutionSettings(gross_exposure_cap=gross_exposure_cap),
    )


# ---------------------------------------------------------------------------
# V1 / V2 dispatch
# ---------------------------------------------------------------------------

def test_v1_path_when_instrument_level_false(tmp_path):
    md = _make_market_data()
    p = _write(tmp_path, "long_only", _LONG_ONLY_STRATEGY)
    strategies = [StrategyEntry(name="s1", module_path=str(p), weight=1.0)]
    cfg = _make_config(instrument_level=False, enabled=False)

    targets, is_v2 = compute_aggregate_targets(md, strategies, cfg)

    assert is_v2 is False
    assert set(targets.keys()) == set(md.instrument_names)
    assert all(v > 0 for v in targets.values())


def test_v2_path_when_instrument_level_true(tmp_path):
    md = _make_market_data()
    p = _write(tmp_path, "long_only", _LONG_ONLY_STRATEGY)
    strategies = [StrategyEntry(name="s1", module_path=str(p), weight=1.0)]
    cfg = _make_config(instrument_level=True)

    targets, is_v2 = compute_aggregate_targets(md, strategies, cfg)

    assert is_v2 is True
    assert all(v > 0 for v in targets.values())


# ---------------------------------------------------------------------------
# Fault tolerance
# ---------------------------------------------------------------------------

def test_broken_strategy_skipped_others_proceed(tmp_path, caplog):
    md = _make_market_data()
    good = _write(tmp_path, "good", _LONG_ONLY_STRATEGY)
    bad = _write(tmp_path, "bad", _BROKEN_STRATEGY)
    strategies = [
        StrategyEntry(name="good", module_path=str(good), weight=1.0),
        StrategyEntry(name="bad", module_path=str(bad), weight=1.0),
    ]
    cfg = _make_config(instrument_level=True)

    import logging
    with caplog.at_level(logging.ERROR):
        targets, _ = compute_aggregate_targets(md, strategies, cfg)

    assert all(v > 0 for v in targets.values())
    assert any("bad" in rec.message and "fail" in rec.message.lower()
               for rec in caplog.records)


def test_disabled_strategies_excluded(tmp_path):
    md = _make_market_data()
    p_long = _write(tmp_path, "long_only", _LONG_ONLY_STRATEGY)
    p_short = _write(tmp_path, "short_only", _SHORT_ONLY_STRATEGY)
    strategies = [
        StrategyEntry(name="long_only", module_path=str(p_long), weight=1.0, enabled=True),
        StrategyEntry(name="short_only", module_path=str(p_short), weight=1.0, enabled=False),
    ]
    cfg = _make_config(instrument_level=True)

    targets, _ = compute_aggregate_targets(md, strategies, cfg)
    assert all(v > 0 for v in targets.values())


def test_no_strategies_returns_zero_targets(tmp_path):
    md = _make_market_data()
    cfg = _make_config(instrument_level=True)

    targets, is_v2 = compute_aggregate_targets(md, strategies=[], config=cfg)

    assert set(targets.keys()) == set(md.instrument_names)
    assert all(v == 0.0 for v in targets.values())
    assert is_v2 is True


# ---------------------------------------------------------------------------
# Symmetry
# ---------------------------------------------------------------------------

def test_long_short_equal_weight_net_to_zero_v1(tmp_path):
    md = _make_market_data()
    p_long = _write(tmp_path, "long_only", _LONG_ONLY_STRATEGY)
    p_short = _write(tmp_path, "short_only", _SHORT_ONLY_STRATEGY)
    strategies = [
        StrategyEntry(name="long_only", module_path=str(p_long), weight=1.0),
        StrategyEntry(name="short_only", module_path=str(p_short), weight=1.0),
    ]
    cfg = _make_config(instrument_level=False, enabled=False)

    targets, _ = compute_aggregate_targets(md, strategies, cfg)
    for inst, v in targets.items():
        assert abs(v) < 1e-9, f"{inst}: expected ~0, got {v}"


def test_long_short_equal_weight_net_to_zero_v2(tmp_path):
    """V2 active-weight aggregation: opposing signals on every instrument
    cancel because (w_long*1 + w_short*-1)/(w_long+w_short) = 0."""
    md = _make_market_data()
    p_long = _write(tmp_path, "long_only", _LONG_ONLY_STRATEGY)
    p_short = _write(tmp_path, "short_only", _SHORT_ONLY_STRATEGY)
    strategies = [
        StrategyEntry(name="long_only", module_path=str(p_long), weight=1.0),
        StrategyEntry(name="short_only", module_path=str(p_short), weight=1.0),
    ]
    cfg = _make_config(instrument_level=True)

    targets, _ = compute_aggregate_targets(md, strategies, cfg)
    for inst, v in targets.items():
        assert abs(v) < 1e-9, f"{inst}: expected ~0, got {v}"


# ---------------------------------------------------------------------------
# Gross exposure cap — applies to BOTH V1 and V2 in futures
# ---------------------------------------------------------------------------

def test_gross_cap_engages_in_v2(tmp_path):
    md = _make_market_data()
    p = _write(tmp_path, "long_only", _LONG_ONLY_STRATEGY)
    strategies = [StrategyEntry(name="s1", module_path=str(p), weight=1.0)]
    cfg_no_cap = _make_config(
        instrument_level=True, target_sleeve_vol=2.0, vol_floor=0.5,
        gross_exposure_cap=None,
    )
    cfg_capped = _make_config(
        instrument_level=True, target_sleeve_vol=2.0, vol_floor=0.5,
        gross_exposure_cap=0.5,
    )

    uncapped, _ = compute_aggregate_targets(md, strategies, cfg_no_cap)
    capped, _ = compute_aggregate_targets(md, strategies, cfg_capped)

    assert sum(abs(v) for v in capped.values()) <= 0.5 + 1e-9
    assert sum(abs(v) for v in capped.values()) < sum(abs(v) for v in uncapped.values())


def test_gross_cap_engages_in_v1_too(tmp_path):
    """Futures aggregator applies gross_cap to V1 as well — different from
    forex which only applies it to V2. Lock that in."""
    md = _make_market_data()
    p = _write(tmp_path, "long_only", _LONG_ONLY_STRATEGY)
    strategies = [StrategyEntry(name="s1", module_path=str(p), weight=1.0)]
    cfg_no_cap = _make_config(
        instrument_level=False, enabled=True,
        target_sleeve_vol=2.0, vol_floor=0.5,
        gross_exposure_cap=None,
    )
    cfg_capped = _make_config(
        instrument_level=False, enabled=True,
        target_sleeve_vol=2.0, vol_floor=0.5,
        gross_exposure_cap=0.05,
    )

    uncapped, _ = compute_aggregate_targets(md, strategies, cfg_no_cap)
    capped, _ = compute_aggregate_targets(md, strategies, cfg_capped)

    # Cap actually engaged
    assert sum(abs(v) for v in capped.values()) <= 0.05 + 1e-9
    assert sum(abs(v) for v in capped.values()) < sum(abs(v) for v in uncapped.values())
