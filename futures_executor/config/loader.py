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
    symbol: str           # execution symbol (e.g. MES, MNQ — what we trade)
    exchange: str
    portfolio_symbol: str = ""  # backtest symbol (e.g. ES, NQ — what strategies reference)
    multiplier: float = 1.0    # dollar value per point per contract
    margin: float = 0.0        # initial margin per contract (USD)


class RollSettings(BaseModel):
    days_before_expiry: int = 7
    hard_deadline_days: int = 3
    min_next_volume: int = 1000
    merge_with_rebalance_window: int = 3


class ExecutionSettings(BaseModel):
    portfolio_leverage: float = 1.0
    abs_threshold: int = 1          # min contract delta to trade (noise filter)
    rel_threshold: float = 0.15     # min % change vs current position to trade
    gross_exposure_cap: float | None = None  # max Σ|sized_j|; None = no cap
    margin_cap: float = 0.8        # max fraction of equity usable for margin
    order_type: str = "market"


class VolTargetSettings(BaseModel):
    enabled: bool = True
    target_vol: float = 0.10
    vol_window: int = 60
    max_leverage: float = 3.0
    instrument_level: bool = True  # V2: vol-target per instrument (not per strategy)


class DataSettings(BaseModel):
    lookback_bars: int = 200
    continuous_dir: str = "data/continuous"
    bar_history_dir: str = "data/bars"


class SafetySettings(BaseModel):
    max_position_contracts: int = 10
    max_total_contracts: int = 30
    max_daily_turnover: int = 20
    kill_switch_file: str = ".kill_switch"
    heartbeat_timeout: int = 300


class AuditSettings(BaseModel):
    db_path: str = "data/audit.db"


class SignalSettings(BaseModel):
    account: str = ""
    recipient: str = ""
    enabled: bool = False
    cli_path: str = "signal-cli"


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
