"""Contract resolution: front/next month, active overrides, contract details."""

import logging
from dataclasses import dataclass
from datetime import datetime, date, timezone

from ib_insync import IB, Contract, ContractDetails

from futures_executor.config.loader import InstrumentSettings, RollSettings

logger = logging.getLogger(__name__)


@dataclass
class ResolvedContract:
    """A fully resolved futures contract with all details from IBKR."""

    symbol: str
    con_id: int
    exchange: str
    currency: str
    expiry: date  # lastTradeDateOrContractMonth as date
    expiry_str: str  # raw YYYYMMDD or YYYYMM
    multiplier: float
    local_symbol: str  # e.g. "ESH6", "NQM5"
    min_tick: float
    contract: Contract  # qualified IB contract object


@dataclass
class ContractPair:
    """Front and next contract for an instrument, plus roll info."""

    symbol: str
    front: ResolvedContract
    next: ResolvedContract | None
    days_to_expiry: int  # trading days to front expiry
    roll_due: bool  # True if within roll window
    hard_deadline: bool  # True if within hard deadline
    # [#228] False = venue closed at fire time → skip this run's rebalance
    # (matches the sim's no-bar-on-holiday behavior). Default True so any
    # legacy construction site that omits it falls through to current behavior.
    tradable_now: bool = True


def _compute_tradable_now(
    details: ContractDetails,
    now_utc: datetime | None = None,
) -> bool:
    """Is ``details``'s contract in an open trading session right now?

    Uses IBKR ``tradingHours`` (full Globex), not ``liquidHours`` (RTH).
    ib_insync's ``tradingSessions()`` parses tradingHours into a list of
    timezone-aware ``(start, end)`` segments, dropping ``CLOSED`` ranges.

    Fail-OPEN on uncertainty (returns True if hours are empty or parsing
    fails) so a missing/transient data condition does NOT cause us to
    wrongly skip a real trading day. The gate's failure mode is "miss a
    trade," never "place a wrong trade" — by design (#228).

    ``now_utc`` lets tests inject a fixed instant; defaults to wall clock.
    """
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    if not details.tradingHours:
        return True  # no data → fail-open
    try:
        sessions = details.tradingSessions()
    except Exception as e:
        sym = getattr(getattr(details, "contract", None), "localSymbol", "?")
        logger.warning(
            f"tradability parse failed for {sym}: {e}; "
            f"defaulting to tradable_now=True"
        )
        return True
    if not sessions:
        # Populated tradingHours but every segment was CLOSED → genuinely closed.
        return False
    tz = sessions[0].start.tzinfo
    now_local = now_utc.astimezone(tz)
    return any(s.start <= now_local <= s.end for s in sessions)


class ContractResolver:
    """Resolves front/next contracts and roll status from IBKR."""

    def __init__(self, ib: IB, roll_config: RollSettings):
        self.ib = ib
        self.roll_config = roll_config

    def resolve(
        self,
        instrument: InstrumentSettings,
        active_contract_month: str | None = None,
    ) -> ContractPair:
        """Resolve front and next contracts for an instrument.

        Queries IBKR for all available expiries, picks front (nearest
        tradeable) and next (second nearest).
        """
        template = Contract()
        template.symbol = instrument.symbol
        template.secType = "FUT"
        template.exchange = instrument.exchange
        template.currency = "USD"

        details_list = self.ib.reqContractDetails(template)
        if not details_list:
            raise ValueError(
                f"No contract details for {instrument.symbol} on {instrument.exchange}"
            )

        # Parse and sort by expiry
        today = date.today()
        candidates: list[tuple[date, ContractDetails]] = []

        for d in details_list:
            expiry_str = d.contract.lastTradeDateOrContractMonth
            try:
                if len(expiry_str) == 8:
                    exp = datetime.strptime(expiry_str, "%Y%m%d").date()
                else:
                    exp = datetime.strptime(expiry_str, "%Y%m").date()
            except ValueError:
                continue

            if exp > today:
                candidates.append((exp, d))

        candidates.sort(key=lambda x: x[0])

        if not candidates:
            raise ValueError(
                f"No future expiries found for {instrument.symbol} "
                f"(all {len(details_list)} contracts expired)"
            )

        selected_idx = 0
        if active_contract_month:
            for idx, (_exp, details) in enumerate(candidates):
                if details.contract.lastTradeDateOrContractMonth == active_contract_month:
                    selected_idx = idx
                    logger.info(
                        f"{instrument.symbol}: honoring active contract override "
                        f"{active_contract_month}"
                    )
                    break

        # Front = nearest, unless active override selects a later valid month
        front_exp, front_details = candidates[selected_idx]
        front = self._to_resolved(front_details, front_exp)

        # [#228 tradability probe — TEMP] Verify IBKR populates trading-hours
        # fields for the planned "skip-when-venue-closed" gate. Raw strings
        # only (no parse → cannot throw). Read in tomorrow's log; remove once
        # confirmed. tradingHours = full Globex session (what the gate needs);
        # liquidHours = RTH only (would wrongly skip the 16:55 ET fire).
        logger.info(
            f"[tradability-probe #228] {instrument.symbol} "
            f"{front_details.contract.localSymbol}: "
            f"tz={front_details.timeZoneId!r} "
            f"tradingHours={front_details.tradingHours!r} "
            f"liquidHours={front_details.liquidHours!r}"
        )

        # Next = following month after the selected front (if available)
        next_contract = None
        next_exp = None
        if len(candidates) > selected_idx + 1:
            next_exp, next_details = candidates[selected_idx + 1]
            next_contract = self._to_resolved(next_details, next_exp)

        # Delivery buffer: if front is inside IBKR's delivery window,
        # skip to next contract to avoid order rejections (error 201).
        cal_days_front = (front_exp - today).days
        if (
            instrument.delivery_buffer_days > 0
            and cal_days_front <= instrument.delivery_buffer_days
            and next_contract is not None
        ):
            logger.info(
                f"{instrument.symbol}: front {front.local_symbol} inside "
                f"delivery buffer ({cal_days_front}d <= "
                f"{instrument.delivery_buffer_days}d), "
                f"advancing to {next_contract.local_symbol}"
            )
            front = next_contract
            front_exp = next_exp
            next_contract = None
            if len(candidates) > selected_idx + 2:
                third_exp, third_details = candidates[selected_idx + 2]
                next_contract = self._to_resolved(third_details, third_exp)

        # Qualify the front contract
        qualified = self.ib.qualifyContracts(front.contract)
        if qualified:
            front.contract = qualified[0]
            front.con_id = qualified[0].conId

        if next_contract:
            qualified = self.ib.qualifyContracts(next_contract.contract)
            if qualified:
                next_contract.contract = qualified[0]
                next_contract.con_id = qualified[0].conId

        # Compute days to expiry (approximate trading days = calendar * 5/7)
        cal_days = (front_exp - today).days
        trading_days = max(0, int(cal_days * 5 / 7))

        roll_due = trading_days <= self.roll_config.days_before_expiry
        hard_deadline = trading_days <= self.roll_config.hard_deadline_days

        # [#228] Tradability gate: is the venue open NOW for this contract?
        # Drives the per-instrument skip in order_manager.execute_rebalance.
        # Fail-OPEN — see _compute_tradable_now docstring.
        tradable_now = _compute_tradable_now(front_details)

        logger.info(
            f"{instrument.symbol}: front={front.local_symbol} "
            f"(exp={front.expiry}, {trading_days}d), "
            f"next={next_contract.local_symbol if next_contract else 'N/A'}"
            f"{' [ROLL DUE]' if roll_due else ''}"
            f"{' [HARD DEADLINE]' if hard_deadline else ''}"
            f"{' [NOT TRADABLE]' if not tradable_now else ''}"
        )

        return ContractPair(
            symbol=instrument.symbol,
            front=front,
            next=next_contract,
            days_to_expiry=trading_days,
            roll_due=roll_due,
            hard_deadline=hard_deadline,
            tradable_now=tradable_now,
        )

    def resolve_all(
        self,
        instruments: list[InstrumentSettings],
        active_contracts: dict[str, str] | None = None,
    ) -> dict[str, ContractPair]:
        """Resolve contracts for all instruments."""
        result = {}
        for inst in instruments:
            try:
                result[inst.symbol] = self.resolve(
                    inst,
                    active_contract_month=(active_contracts or {}).get(inst.symbol),
                )
                self.ib.sleep(0.5)  # pace IBKR requests
            except Exception as e:
                logger.error(f"Failed to resolve {inst.symbol}: {e}")
        return result

    def check_next_volume(self, pair: ContractPair) -> bool:
        """Check if next contract has sufficient volume for roll."""
        if pair.next is None:
            return False

        # Fetch 1 day of bars to check volume
        bars = self.ib.reqHistoricalData(
            pair.next.contract,
            endDateTime="",
            durationStr="1 D",
            barSizeSetting="1 day",
            whatToShow="TRADES",
            useRTH=False,
            formatDate=1,
        )
        if not bars:
            return False

        volume = bars[-1].volume
        ok = volume >= self.roll_config.min_next_volume
        logger.info(
            f"{pair.symbol}: next contract volume={volume} "
            f"(threshold={self.roll_config.min_next_volume}) "
            f"{'OK' if ok else 'INSUFFICIENT'}"
        )
        return ok

    def _to_resolved(self, details: ContractDetails, expiry: date) -> ResolvedContract:
        c = details.contract
        return ResolvedContract(
            symbol=c.symbol,
            con_id=c.conId,
            exchange=c.exchange,
            currency=c.currency,
            expiry=expiry,
            expiry_str=c.lastTradeDateOrContractMonth,
            multiplier=float(c.multiplier) if c.multiplier else 1.0,
            local_symbol=c.localSymbol or f"{c.symbol}{c.lastTradeDateOrContractMonth}",
            min_tick=details.minTick,
            contract=c,
        )
