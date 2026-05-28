"""Tests for the #228 tradability-gate helper (`_compute_tradable_now`).

Real IBKR `tradingHours` strings captured by the #228 probe on 2026-05-27
(commit 881603f, futures_20260527_205500.log) are baked in below so the
gate logic is validated against actual broker data, not synthetic shapes.
"""
from datetime import datetime, timezone

from ib_insync import ContractDetails

from futures_executor.data.contract_resolver import _compute_tradable_now


# --- Real probe data (2026-05-27 23:55 EEST capture) ---------------------
# MCL / MGC trade on US/Eastern, Globex session 1800 prev-day → 1700 today.
# MES trades on US/Central, Globex session 1700 prev-day → 1600 today.
# Both daily halts span 1700-1800 ET (= 1600-1700 CT).
MCL_HOURS = (
    "20260526:1800-20260527:1700;20260527:1800-20260528:1700;"
    "20260528:1800-20260529:1700;20260530:CLOSED;"
    "20260531:1800-20260601:1700;20260601:1800-20260602:1700"
)
MGC_HOURS = MCL_HOURS  # COMEX same shape as NYMEX in the probe data
MES_HOURS = (
    "20260526:1700-20260527:1600;20260527:1700-20260528:1600;"
    "20260528:1700-20260529:1600;20260530:CLOSED;"
    "20260531:1700-20260601:1600;20260601:1700-20260602:1600"
)
MCL_TZ = MGC_TZ = "US/Eastern"
MES_TZ = "US/Central"


def _details(hours: str, tz: str) -> ContractDetails:
    """Minimal ContractDetails with just the fields the gate reads."""
    return ContractDetails(tradingHours=hours, timeZoneId=tz)


# --- The cases that matter ----------------------------------------------

def test_normal_fire_time_all_tradable():
    """20:55 UTC weekday = 16:55 ET = 15:55 CT — the actual cron fire.
    Inside every instrument's session → all three must say tradable."""
    fire = datetime(2026, 5, 28, 20, 55, tzinfo=timezone.utc)
    assert _compute_tradable_now(_details(MCL_HOURS, MCL_TZ), fire) is True
    assert _compute_tradable_now(_details(MES_HOURS, MES_TZ), fire) is True
    assert _compute_tradable_now(_details(MGC_HOURS, MGC_TZ), fire) is True


def test_daily_halt_all_closed():
    """21:30 UTC = 17:30 ET = 16:30 CT — squarely inside the 17:00-18:00 ET
    Globex daily halt. None of the three is in any session → all False."""
    halt = datetime(2026, 5, 28, 21, 30, tzinfo=timezone.utc)
    assert _compute_tradable_now(_details(MCL_HOURS, MCL_TZ), halt) is False
    assert _compute_tradable_now(_details(MES_HOURS, MES_TZ), halt) is False
    assert _compute_tradable_now(_details(MGC_HOURS, MGC_TZ), halt) is False


def test_saturday_all_closed():
    """05-30 noon UTC = Saturday. ``20260530:CLOSED`` is dropped by the
    parser and no other date's session covers Sat noon → not tradable.
    This is the *Memorial-Day-class* signal the gate exists to honor."""
    sat = datetime(2026, 5, 30, 12, 0, tzinfo=timezone.utc)
    assert _compute_tradable_now(_details(MCL_HOURS, MCL_TZ), sat) is False
    assert _compute_tradable_now(_details(MES_HOURS, MES_TZ), sat) is False
    assert _compute_tradable_now(_details(MGC_HOURS, MGC_TZ), sat) is False


def test_empty_hours_fails_open():
    """No tradingHours data = uncertainty. Gate MUST fail OPEN
    (return True) — its design rule is "miss a trade, never place a
    wrong trade." A transient empty would otherwise silently kill a
    real trading day's rebalance."""
    fire = datetime(2026, 5, 28, 20, 55, tzinfo=timezone.utc)
    assert _compute_tradable_now(_details("", "US/Eastern"), fire) is True


def test_session_edge_minute_before_close():
    """20:59 UTC = 16:59 ET — exactly one minute before MCL/MGC's 17:00 ET
    close. Should still be tradable (inclusive at the session end)."""
    near_close = datetime(2026, 5, 28, 20, 59, tzinfo=timezone.utc)
    assert _compute_tradable_now(_details(MCL_HOURS, MCL_TZ), near_close) is True
    assert _compute_tradable_now(_details(MGC_HOURS, MGC_TZ), near_close) is True
