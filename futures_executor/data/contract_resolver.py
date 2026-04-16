"""Contract resolution: front/next month, roll dates, contract details."""

import logging
from dataclasses import dataclass
from datetime import datetime, date, timedelta

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


class ContractResolver:
    """Resolves front/next contracts and roll status from IBKR."""

    def __init__(self, ib: IB, roll_config: RollSettings):
        self.ib = ib
        self.roll_config = roll_config

    def resolve(self, instrument: InstrumentSettings) -> ContractPair:
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

        # Front = nearest
        front_exp, front_details = candidates[0]
        front = self._to_resolved(front_details, front_exp)

        # Next = second nearest (if available)
        next_contract = None
        if len(candidates) > 1:
            next_exp, next_details = candidates[1]
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
            if len(candidates) > 2:
                third_exp, third_details = candidates[2]
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

        logger.info(
            f"{instrument.symbol}: front={front.local_symbol} "
            f"(exp={front.expiry}, {trading_days}d), "
            f"next={next_contract.local_symbol if next_contract else 'N/A'}"
            f"{' [ROLL DUE]' if roll_due else ''}"
            f"{' [HARD DEADLINE]' if hard_deadline else ''}"
        )

        return ContractPair(
            symbol=instrument.symbol,
            front=front,
            next=next_contract,
            days_to_expiry=trading_days,
            roll_due=roll_due,
            hard_deadline=hard_deadline,
        )

    def resolve_all(
        self, instruments: list[InstrumentSettings]
    ) -> dict[str, ContractPair]:
        """Resolve contracts for all instruments."""
        result = {}
        for inst in instruments:
            try:
                result[inst.symbol] = self.resolve(inst)
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
