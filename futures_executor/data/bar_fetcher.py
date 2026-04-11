"""Fetch historical bars from IBKR and persist to parquet."""

import logging
from pathlib import Path

import pandas as pd
from ib_insync import IB, Contract, util

logger = logging.getLogger(__name__)


class BarFetcher:
    """Downloads and caches daily bars from IBKR."""

    def __init__(self, ib: IB, bar_history_dir: Path):
        self.ib = ib
        self.bar_dir = Path(bar_history_dir)
        self.bar_dir.mkdir(parents=True, exist_ok=True)

    def fetch_bars(
        self,
        contract: Contract,
        duration: str = "1 Y",
        bar_size: str = "1 day",
        what_to_show: str = "TRADES",
    ) -> pd.DataFrame:
        """Fetch historical bars for a specific contract.

        Returns DataFrame with columns: date, open, high, low, close, volume.
        """
        bars = self.ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow=what_to_show,
            useRTH=False,
            formatDate=1,
        )

        if not bars:
            logger.warning(
                f"No bars returned for {contract.symbol} "
                f"({contract.lastTradeDateOrContractMonth})"
            )
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

        df = util.df(bars)
        df = df.rename(columns={
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        })
        df["date"] = pd.to_datetime(df["date"])
        df = df[["date", "open", "high", "low", "close", "volume"]].copy()
        df = df.sort_values("date").reset_index(drop=True)

        logger.info(
            f"Fetched {len(df)} bars for {contract.symbol} "
            f"({contract.lastTradeDateOrContractMonth}): "
            f"{df['date'].iloc[0].date()} to {df['date'].iloc[-1].date()}"
        )
        return df

    def fetch_and_cache(
        self,
        contract: Contract,
        symbol: str,
        duration: str = "1 Y",
    ) -> pd.DataFrame:
        """Fetch bars and save to parquet. Returns the DataFrame."""
        df = self.fetch_bars(contract, duration)
        if df.empty:
            return df

        month = contract.lastTradeDateOrContractMonth or "unknown"
        path = self.bar_dir / symbol / f"{month}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Cached {len(df)} bars to {path}")
        return df

    def fetch_todays_bar(
        self,
        contract: Contract,
    ) -> pd.DataFrame:
        """Build today's daily bar from 5-min intraday bars.

        Aggregates: O=first open, H=max high, L=min low, C=last close, V=sum volume.
        Returns single-row DataFrame matching daily bar format.
        """
        bars = self.ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr="1 D",
            barSizeSetting="5 mins",
            whatToShow="TRADES",
            useRTH=False,
            formatDate=2,
        )

        if not bars:
            logger.warning(
                f"No intraday bars for {contract.symbol} "
                f"({contract.lastTradeDateOrContractMonth})"
            )
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

        df = util.df(bars)
        today_bar = pd.DataFrame([{
            "date": pd.Timestamp.now().normalize(),
            "open": df["open"].iloc[0],
            "high": df["high"].max(),
            "low": df["low"].min(),
            "close": df["close"].iloc[-1],
            "volume": df["volume"].sum(),
        }])

        logger.info(
            f"Built today's bar for {contract.symbol} from {len(df)} "
            f"5-min bars: O={today_bar['open'].iloc[0]:.2f} "
            f"H={today_bar['high'].iloc[0]:.2f} "
            f"L={today_bar['low'].iloc[0]:.2f} "
            f"C={today_bar['close'].iloc[0]:.2f}"
        )
        return today_bar

    def load_cached(self, symbol: str, month: str) -> pd.DataFrame | None:
        """Load cached bars from parquet if available."""
        path = self.bar_dir / symbol / f"{month}.parquet"
        if path.exists():
            return pd.read_parquet(path)
        return None
