"""Configuration models and loaders for the futures executor."""

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel


class BrokerSettings(BaseModel):
    host: str = "127.0.0.1"
    port: int = 4001
    client_id: int = 1
    readonly: bool = False
    timeout: int = 30


class InstrumentSettings(BaseModel):
    symbol: str  # execution symbol (e.g. MES, MNQ — what we trade)
    exchange: str
    portfolio_symbol: str = (
        ""  # backtest symbol (e.g. ES, NQ — what strategies reference)
    )
    multiplier: float = 1.0  # dollar value per point per contract
    margin: float = 0.0  # initial margin per contract (USD)
    delivery_buffer_days: int = 0  # calendar days before expiry to skip front contract


class RollSettings(BaseModel):
    days_before_expiry: int = 7
    hard_deadline_days: int = 3
    min_next_volume: int = 1000
    merge_with_rebalance_window: int = 3


class ExecutionSettings(BaseModel):
    portfolio_leverage: float = 1.0
    abs_threshold: int = 1  # min contract delta to trade (noise filter)
    rel_threshold: float = 0.15  # min % change vs current position to trade
    gross_exposure_cap: float | None = None  # max Σ|sized_j|; None = no cap
    margin_cap: float = 0.8  # max fraction of equity usable for margin
    order_type: str = "market"


class VolTargetSettings(BaseModel):
    """Per-sleeve vol targeting configuration (matches R-factory's
    PortfolioConfig fields after the 2026-04 leverage-knob refactor).

    Pre-refactor vocabulary (``target_vol`` + ``max_leverage``) was renamed
    to align with forex-live's settings shape:
        OLD: min(target_vol / realized_vol, max_leverage) × portfolio_leverage
        NEW: target_sleeve_vol / max(realized_vol, vol_floor)

    Mapping (preserves NON-CAP-ENGAGED region bit-for-bit):
        target_sleeve_vol = old_target_vol × old_portfolio_leverage
        vol_floor         = target_sleeve_vol / old_max_leverage

    In the cap-engaged region the effective cap drops by old_portfolio_leverage
    (e.g. 15→10 for futures, 50→10 for forex). The cap rarely engages for
    typical futures realized vols (0.15-0.30); see settings.yaml comment.
    """
    enabled: bool = True
    target_sleeve_vol: float = 0.10  # was target_vol
    vol_window: int = 60
    vol_floor: float = 0.0           # was max_leverage; default 0 = no cap
    instrument_level: bool = True  # V2: vol-target per instrument (not per strategy)


class DataSettings(BaseModel):
    """Market-data source for cmd_run_once.

    Post-2026-05-05 (futures Option 2): cmd_run_once reads R-factory's
    canonical parquet via ``load_universe`` instead of fetching IBKR
    directly. Eliminates the live/replay window-mismatch where
    futures-live's local ContinuousSeries (built from a lookback window)
    produced different signals than R-factory monitor's replay
    (built from full parquet history). 2026-05-05 monitor.db surfaced
    the bug as CL/ES/GC target ≠ sim across all 3 instruments.
    """
    # Legacy — no longer consumed by cmd_run_once after 2026-05-05.
    # Kept so existing settings.yaml files don't pydantic-fail on
    # extra-field strict mode if/when we tighten validation.
    lookback_bars: int = 200
    continuous_dir: str = "data/continuous"
    bar_history_dir: str = "data/bars"
    # Subdir under ``{monitor.r_factory_data_dir}/parquet/``. Each
    # futures-live deployment is single-set; this names the set.
    parquet_set_name: str = "futures_mini"
    # Hard refusal threshold for parquet staleness — counted in
    # business days from parquet's last bar to today (UTC). cron
    # sequence is ``data ingest-futures-ibkr --synthesize-eod`` →
    # ``futures-executor run-once``, so on a clean cycle the parquet's
    # last bar IS today (0 BD old). 1 = tolerate the rare case where
    # ingest succeeded but run-once started right at midnight boundary.
    max_parquet_age_business_days: int = 1


class SafetySettings(BaseModel):
    max_position_contracts: int = 10
    max_total_contracts: int = 30
    max_daily_turnover: int = 20
    kill_switch_file: str = ".kill_switch"
    heartbeat_timeout: int = 300
    # Capital controls v1: daily-loss circuit breaker. Set strictly below
    # worst historical day on this instrument set so it fires only on
    # outcomes outside our entire backtest envelope. ≤0 disables.
    # See R-factory PLAN_CAPITAL_CONTROLS_V1.md for calibration rationale.
    # Note: futures_mini xray hasn't been generated yet — 5.0 is a
    # placeholder; recalibrate before futures real-money launch.
    daily_loss_circuit_pct: float = 5.0
    # Persisted today's start-of-day equity. Seeded on the first cycle
    # of each UTC date.
    reference_equity_file: str = "data/reference_equity.json"


class AuditSettings(BaseModel):
    db_path: str = "data/audit.db"


class SignalSettings(BaseModel):
    account: str = ""
    recipient: str = ""
    enabled: bool = False
    cli_path: str = "signal-cli"


class MonitorSettings(BaseModel):
    """Live-vs-sim monitor integration (mirrors forex-live's block).

    When ``enabled`` is true, ``fxf snapshot`` writes canonical snapshot
    JSON into ``r_factory_artifacts_dir/monitor/<set>/snapshots/``.
    R-factory's monitor reads parquet from
    ``r_factory_data_dir/parquet/<set>/`` for sim replay.
    """
    enabled: bool = False
    r_factory_artifacts_dir: str = "/Users/acess/projects/R-factory/artifacts"
    r_factory_data_dir: str = "/Users/acess/projects/R-factory/data"
    broker_id: str = "ibkr-futures"


class ExecutorConfig(BaseModel):
    broker: BrokerSettings = BrokerSettings()
    rfactory_path: str = ""
    instruments: list[InstrumentSettings] = []
    roll: RollSettings = RollSettings()
    execution: ExecutionSettings = ExecutionSettings()
    vol_target: VolTargetSettings = VolTargetSettings()
    data: DataSettings = DataSettings()
    safety: SafetySettings = SafetySettings()
    audit: AuditSettings = AuditSettings()
    signal: SignalSettings = SignalSettings()
    monitor: MonitorSettings = MonitorSettings()


class StrategyEntry(BaseModel):
    name: str
    module_path: str
    params: dict[str, Any] = {}
    weight: float = 1.0
    enabled: bool = True


class AggregationSettings(BaseModel):
    consensus_threshold: float = 0.3
    method: str = "weighted_average"


class StrategiesConfig(BaseModel):
    strategies: list[StrategyEntry] = []
    aggregation: AggregationSettings = AggregationSettings()


def load_settings(config_dir: Path) -> ExecutorConfig:
    """Load settings.yaml into ExecutorConfig."""
    path = Path(config_dir) / "settings.yaml"
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    return ExecutorConfig.model_validate(raw)


def load_strategies(config_dir: Path) -> StrategiesConfig:
    """Load strategies.yaml into StrategiesConfig."""
    path = Path(config_dir) / "strategies.yaml"
    if not path.exists():
        return StrategiesConfig()
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    return StrategiesConfig.model_validate(raw)
