"""Persistent executor state helpers."""

import json
from pathlib import Path

STATE_FILE = Path("data/executor_state.json")


def load_executor_state() -> dict:
    """Load persistent executor state from disk."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def save_executor_state(state: dict) -> None:
    """Persist executor state to disk."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True))


def get_active_contracts(state: dict | None = None) -> dict[str, str]:
    """Return symbol -> active contract month overrides."""
    state = state or load_executor_state()
    return dict(state.get("active_contracts", {}))


def set_active_contract(state: dict, symbol: str, contract_month: str) -> dict:
    """Store the active contract month for one symbol."""
    active = dict(state.get("active_contracts", {}))
    active[symbol] = contract_month
    state["active_contracts"] = active
    return state
