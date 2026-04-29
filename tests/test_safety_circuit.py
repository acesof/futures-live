"""Tests for futures-live's daily-loss circuit breaker integration.

Pure decision logic is tested in R-factory's
`tests/test_safety_circuit_breaker.py`. These cover the futures-side
integration: reference_equity load/persist/seeding, UTC-date rollover,
kill-switch activation on trip.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from futures_executor.execution.safety import (
    _load_reference_equity,
    _persist_reference_equity,
    activate_kill_switch,
    check_daily_loss_circuit,
    check_kill_switch,
    deactivate_kill_switch,
)


def _make_config(tmp_path: Path, threshold_pct: float = 5.0) -> SimpleNamespace:
    return SimpleNamespace(
        safety=SimpleNamespace(
            kill_switch_file=str(tmp_path / ".kill_switch"),
            daily_loss_circuit_pct=threshold_pct,
            reference_equity_file=str(tmp_path / "data" / "reference_equity.json"),
        ),
    )


# ---------------------------------------------------------------------------
# Kill switch primitives (parity with forex side)
# ---------------------------------------------------------------------------

def test_kill_switch_lifecycle(tmp_path):
    config = _make_config(tmp_path)
    assert check_kill_switch(config) is False
    activate_kill_switch(config)
    assert check_kill_switch(config) is True
    deactivate_kill_switch(config)
    assert check_kill_switch(config) is False


# ---------------------------------------------------------------------------
# Reference equity load/persist
# ---------------------------------------------------------------------------

def test_load_reference_equity_returns_none_when_file_missing(tmp_path):
    assert _load_reference_equity(tmp_path / "no_such.json", "2026-04-29") is None


def test_persist_then_load_round_trip(tmp_path):
    path = tmp_path / "data" / "reference_equity.json"
    _persist_reference_equity(path, "2026-04-29", 1_053_527.79)
    assert _load_reference_equity(path, "2026-04-29") == pytest.approx(1_053_527.79)


def test_load_returns_none_on_stale_date(tmp_path):
    path = tmp_path / "ref.json"
    _persist_reference_equity(path, "2026-04-28", 1_000_000.0)
    assert _load_reference_equity(path, "2026-04-29") is None


def test_load_returns_none_on_corrupt_file(tmp_path):
    path = tmp_path / "ref.json"
    path.write_text("{bad json")
    assert _load_reference_equity(path, "2026-04-29") is None


# ---------------------------------------------------------------------------
# check_daily_loss_circuit integration
# ---------------------------------------------------------------------------

def test_first_call_seeds_reference_and_does_not_trip(tmp_path):
    config = _make_config(tmp_path)

    with patch("futures_executor.execution.safety.datetime") as mock_dt:
        mock_dt.now.return_value.strftime.return_value = "2026-04-29"
        d = check_daily_loss_circuit(config, current_equity=1_053_527.79)

    assert d.should_trip is False
    ref_path = Path(config.safety.reference_equity_file)
    data = json.loads(ref_path.read_text())
    assert data["date"] == "2026-04-29"
    assert data["equity"] == pytest.approx(1_053_527.79)
    assert not Path(config.safety.kill_switch_file).exists()


def test_subsequent_call_below_threshold_does_not_trip(tmp_path):
    config = _make_config(tmp_path)
    with patch("futures_executor.execution.safety.datetime") as mock_dt:
        mock_dt.now.return_value.strftime.return_value = "2026-04-29"
        check_daily_loss_circuit(config, current_equity=1_053_527.79)
        d = check_daily_loss_circuit(config, current_equity=1_043_000.0)  # ~-1%

    assert d.should_trip is False
    assert d.daily_loss_pct < 1.5  # close to -1%
    assert not Path(config.safety.kill_switch_file).exists()


def test_subsequent_call_above_threshold_trips_and_activates_kill_switch(tmp_path):
    config = _make_config(tmp_path, threshold_pct=5.0)

    with patch("futures_executor.execution.safety.datetime") as mock_dt:
        mock_dt.now.return_value.strftime.return_value = "2026-04-29"
        check_daily_loss_circuit(config, current_equity=1_053_527.79)
        d = check_daily_loss_circuit(config, current_equity=1_000_000.0)  # ~-5.08%

    assert d.should_trip is True
    assert Path(config.safety.kill_switch_file).exists()


def test_new_utc_day_reseeds_anchor(tmp_path):
    config = _make_config(tmp_path)
    ref_path = Path(config.safety.reference_equity_file)

    with patch("futures_executor.execution.safety.datetime") as mock_dt:
        mock_dt.now.return_value.strftime.return_value = "2026-04-29"
        check_daily_loss_circuit(config, current_equity=1_053_527.79)
        mock_dt.now.return_value.strftime.return_value = "2026-04-30"
        d = check_daily_loss_circuit(config, current_equity=900_000.0)

    assert d.should_trip is False  # seeding day
    data = json.loads(ref_path.read_text())
    assert data["date"] == "2026-04-30"
    assert data["equity"] == pytest.approx(900_000.0)


def test_zero_threshold_disables_circuit(tmp_path):
    config = _make_config(tmp_path, threshold_pct=0.0)
    with patch("futures_executor.execution.safety.datetime") as mock_dt:
        mock_dt.now.return_value.strftime.return_value = "2026-04-29"
        check_daily_loss_circuit(config, current_equity=1_000_000.0)
        d = check_daily_loss_circuit(config, current_equity=500_000.0)  # -50%

    assert d.should_trip is False
    assert "disabled" in d.reason
    assert not Path(config.safety.kill_switch_file).exists()
