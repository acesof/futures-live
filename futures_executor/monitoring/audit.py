"""SQLite audit trail for all execution events."""

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS executions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    run_date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    event_type TEXT NOT NULL,       -- 'adjustment', 'roll', 'close', 'open', 'flatten'
    action TEXT,                    -- 'BUY', 'SELL', 'HOLD'
    quantity INTEGER,
    target_contracts INTEGER,
    current_contracts INTEGER,
    target_signal REAL,
    fill_price REAL,
    bar_close REAL,                -- last bar close for slippage calc
    slippage_ticks REAL,
    commission REAL,
    status TEXT,                   -- 'Filled', 'FAILED', etc.
    error TEXT,
    details TEXT                   -- JSON blob for extra info
);

CREATE TABLE IF NOT EXISTS run_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    run_date TEXT NOT NULL,
    equity REAL,
    n_instruments INTEGER,
    n_orders INTEGER,
    n_rolls INTEGER,
    n_errors INTEGER,
    total_commission REAL,
    last_rebalance_date TEXT,
    details TEXT
);

CREATE TABLE IF NOT EXISTS roll_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    run_date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    from_month TEXT NOT NULL,
    to_month TEXT NOT NULL,
    quantity INTEGER,
    gap REAL,
    cumulative_adjustment REAL,
    fill_price REAL,
    commission REAL,
    status TEXT
);
"""


class AuditLog:
    """SQLite-backed audit trail for execution events."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path))
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def log_execution(
        self,
        run_date: str,
        symbol: str,
        event_type: str,
        action: str | None = None,
        quantity: int | None = None,
        target_contracts: int | None = None,
        current_contracts: int | None = None,
        target_signal: float | None = None,
        fill_price: float | None = None,
        bar_close: float | None = None,
        commission: float | None = None,
        status: str | None = None,
        error: str | None = None,
        details: dict | None = None,
    ) -> None:
        """Log a single execution event."""
        slippage = None
        if fill_price is not None and bar_close is not None and bar_close > 0:
            # Signed: positive = worse than expected (paid more on BUY, got less on SELL)
            sign = 1.0 if action == "BUY" else -1.0
            slippage = sign * (fill_price - bar_close)

        self._conn.execute(
            """INSERT INTO executions
            (timestamp, run_date, symbol, event_type, action, quantity,
             target_contracts, current_contracts, target_signal,
             fill_price, bar_close, slippage_ticks, commission,
             status, error, details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.utcnow().isoformat(),
                run_date, symbol, event_type, action, quantity,
                target_contracts, current_contracts, target_signal,
                fill_price, bar_close, slippage, commission,
                status, error,
                json.dumps(details) if details else None,
            ),
        )
        self._conn.commit()

    def log_run(
        self,
        run_date: str,
        equity: float,
        n_instruments: int,
        n_orders: int,
        n_rolls: int,
        n_errors: int,
        total_commission: float,
        last_rebalance_date: str | None = None,
        details: dict | None = None,
    ) -> None:
        """Log a complete run summary."""
        self._conn.execute(
            """INSERT INTO run_log
            (timestamp, run_date, equity, n_instruments, n_orders,
             n_rolls, n_errors, total_commission, last_rebalance_date, details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.utcnow().isoformat(),
                run_date, equity, n_instruments, n_orders,
                n_rolls, n_errors, total_commission,
                last_rebalance_date,
                json.dumps(details) if details else None,
            ),
        )
        self._conn.commit()

    def log_roll(
        self,
        run_date: str,
        symbol: str,
        from_month: str,
        to_month: str,
        quantity: int,
        gap: float = 0.0,
        cumulative_adjustment: float = 0.0,
        fill_price: float | None = None,
        commission: float | None = None,
        status: str | None = None,
    ) -> None:
        """Log a contract roll event."""
        self._conn.execute(
            """INSERT INTO roll_log
            (timestamp, run_date, symbol, from_month, to_month,
             quantity, gap, cumulative_adjustment, fill_price,
             commission, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.utcnow().isoformat(),
                run_date, symbol, from_month, to_month,
                quantity, gap, cumulative_adjustment,
                fill_price, commission, status,
            ),
        )
        self._conn.commit()

    def get_recent_executions(self, n: int = 50) -> list[dict]:
        """Fetch recent execution records."""
        cursor = self._conn.execute(
            "SELECT * FROM executions ORDER BY id DESC LIMIT ?", (n,)
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def get_run_history(self, n: int = 30) -> list[dict]:
        """Fetch recent run summaries."""
        cursor = self._conn.execute(
            "SELECT * FROM run_log ORDER BY id DESC LIMIT ?", (n,)
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def get_slippage_stats(self, n_days: int = 30) -> dict:
        """Compute slippage statistics over recent executions."""
        cursor = self._conn.execute(
            """SELECT symbol,
                      COUNT(*) as n_fills,
                      AVG(slippage_ticks) as avg_slippage,
                      MAX(ABS(slippage_ticks)) as max_slippage,
                      SUM(commission) as total_commission
               FROM executions
               WHERE slippage_ticks IS NOT NULL
                 AND status != 'FAILED'
               GROUP BY symbol
               ORDER BY symbol""",
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def close(self):
        self._conn.close()
