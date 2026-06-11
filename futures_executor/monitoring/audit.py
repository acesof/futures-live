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
    realized_pnl REAL,             -- account-currency realized P&L on closing legs (NULL for opens / pre-migration rows)
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
        self._migrate()
        self._conn.commit()

    def _migrate(self) -> None:
        """Idempotent column-add migrations for older audit.db files.

        ALTER TABLE ADD COLUMN is the SQLite-friendly path. Each addition is
        guarded by a PRAGMA table_info check so re-running on a fresh DB
        (where the column already exists from SCHEMA) is a no-op.
        """
        existing = {
            row[1] for row in self._conn.execute(
                "PRAGMA table_info(executions)"
            ).fetchall()
        }
        if "realized_pnl" not in existing:
            # Pre-existing rows get NULL → snapshot reader treats NULL as 0.0
            # for back-compat with the pre-migration "realized always zero" world.
            self._conn.execute("ALTER TABLE executions ADD COLUMN realized_pnl REAL")
            logger.info("audit.db: added executions.realized_pnl column")
        if "perm_id" not in existing:
            # #228 A2 #4 late-fill audit reconciler: IBKR-wide unique order id,
            # captured at order placement and used to correlate later
            # `reqExecutions` fills back to the originating audit row. Nullable
            # so pre-A2 #4 rows continue to load unchanged.
            self._conn.execute("ALTER TABLE executions ADD COLUMN perm_id INTEGER")
            logger.info("audit.db: added executions.perm_id column")

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
        realized_pnl: float | None = None,
        status: str | None = None,
        error: str | None = None,
        details: dict | None = None,
        perm_id: int | None = None,
    ) -> None:
        """Log a single execution event.

        ``perm_id`` (#228 A2 #4): IBKR-wide unique order identifier, captured
        from ``trade.order.permId`` at placement. Lets the late-fill reconciler
        correlate later ``reqExecutions`` results back to this audit row.
        Pass ``None`` for events with no order behind them (e.g.,
        ``contract_advance``, ``migration_blocked``).
        """
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
             realized_pnl, status, error, details, perm_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.utcnow().isoformat(),
                run_date, symbol, event_type, action, quantity,
                target_contracts, current_contracts, target_signal,
                fill_price, bar_close, slippage, commission,
                realized_pnl,
                status, error,
                json.dumps(details) if details else None,
                perm_id,
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

    def get_last_run_timestamp(self) -> str | None:
        """Return the ISO timestamp of the most recently logged run, or None.

        Used by the #228 A2 #4 late-fill reconciler to bound the
        ``reqExecutions`` query window. Returns None on a fresh install where
        no run has been logged yet — caller skips reconciliation in that case.
        """
        row = self._conn.execute(
            "SELECT timestamp FROM run_log ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return row[0] if row else None

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

    def reconcile_off_session(
        self,
        working_perm_ids: set[int],
        positions_by_month: dict[str, dict[str, float]],
        today: str,
        last_run_date: str | None,
    ) -> dict:
        """Resolve audit rows stuck in an active status from a PREVIOUS run.

        Closes the cross-session half of #228 A2 #4: ``reqExecutions``
        cannot see fills from a prior trading day, so an order left
        working at disconnect that filled off-cycle leaves its audit row
        at 'Submitted' forever. This runs at cycle start, BEFORE any new
        orders, and resolves those rows from broker truth:

        - perm_id still in the working-order set → order genuinely still
          working; leave the row (the open-order guard handles trading).
        - otherwise the order terminated off-session. For roll /
          migration_roll rows from the IMMEDIATELY PRECEDING run whose
          details carry from_month/to_month, infer the outcome from
          current positions-by-contract-month (no orders have been
          placed yet this cycle, so positions still reflect that order):
          position on to_month and none on from_month → FilledOffSession;
          the reverse → CancelledOffSession. Anything older or without
          month info → TerminalUnknown (honest: outcome not verifiable).

        Month inference is restricted to ``last_run_date`` rows because
        intervening cycles' trades invalidate position inference for
        older rows.

        Returns counts: ``{filled, cancelled, unknown, still_working}``.
        """
        active = ("Submitted", "PreSubmitted", "PendingSubmit", "ApiPending")
        rows = self._conn.execute(
            f"""SELECT id, run_date, symbol, event_type, perm_id, details
            FROM executions
            WHERE status IN ({','.join('?' * len(active))})
            AND run_date < ?""",
            (*active, today),
        ).fetchall()

        counts = {"filled": 0, "cancelled": 0, "unknown": 0, "still_working": 0}
        stamp = f"off-session reconcile {today}"
        for rid, run_date, symbol, event_type, perm_id, details_json in rows:
            if perm_id and perm_id in working_perm_ids:
                counts["still_working"] += 1
                continue

            new_status = "TerminalUnknown"
            note = (
                f"{stamp}: not in open orders → terminated off-session; "
                "outcome not verifiable from positions"
            )
            try:
                details = json.loads(details_json) if details_json else {}
            except (ValueError, TypeError):
                details = {}
            from_m = details.get("from_month")
            to_m = details.get("to_month")
            if (
                event_type in ("roll", "migration_roll")
                and from_m and to_m
                and run_date == last_run_date
            ):
                by_month = positions_by_month.get(symbol, {})
                on_to = by_month.get(to_m, 0.0)
                on_from = by_month.get(from_m, 0.0)
                if on_to and not on_from:
                    new_status = "FilledOffSession"
                    note = (
                        f"{stamp}: position now on {to_m}, none on "
                        f"{from_m} → filled off-session"
                    )
                    counts["filled"] += 1
                elif on_from and not on_to:
                    new_status = "CancelledOffSession"
                    note = (
                        f"{stamp}: position still on {from_m}, none on "
                        f"{to_m} → cancelled/expired off-session"
                    )
                    counts["cancelled"] += 1
                else:
                    counts["unknown"] += 1
            else:
                counts["unknown"] += 1

            self._conn.execute(
                """UPDATE executions
                SET status = ?,
                    error = COALESCE(error || '; ', '') || ?
                WHERE id = ?""",
                (new_status, note, rid),
            )
        self._conn.commit()
        return counts

    def reconcile_late_fills(
        self,
        executions: list[dict],
        run_date: str,
    ) -> dict:
        """Reconcile late fills (#228 A2 #4) into the audit trail.

        ``executions`` is the result of ``ib.reqExecutions`` translated to a
        plain-dict shape; each entry must have keys:
          - ``perm_id`` (int): IBKR-wide order id.
          - ``exec_id`` (str): IBKR-wide unique fill id.
          - ``symbol`` (str): portfolio-symbol (NOT contract local-symbol).
          - ``action`` (str): 'BUY' or 'SELL'.
          - ``quantity`` (int): signed share count from this execution.
          - ``fill_price`` (float).
          - ``commission`` (float).
          - ``realized_pnl`` (float).
          - ``time_iso`` (str): execution timestamp ISO-8601.

        Algorithm:
          For each execution:
          1. SELECT existing row by ``perm_id``.
          2. If a row exists with ``status IN ('Submitted','PreSubmitted',
             'PendingSubmit')``: UPDATE that row in-place to ``status='Filled'``
             with the fill data. Counts as ``updated``.
          3. If a row exists with ``status='Filled'`` (already correctly
             recorded same-cycle): skip silently. Counts as ``skipped``.
          4. If no row exists: INSERT a new row with
             ``event_type='late_fill_orphan'``. Covers manual GUI orders +
             BAG-combo executions whose parent had no log_execution call.
             Counts as ``inserted_orphan``.

        Returns ``{'updated': N, 'inserted_orphan': M, 'skipped': K}``.

        Idempotency: re-running on the same ``executions`` list after a
        successful pass produces all-``skipped`` (matched rows are now
        ``Filled``; orphans are now present so a fresh lookup finds them).
        """
        result = {"updated": 0, "inserted_orphan": 0, "skipped": 0}
        ELIGIBLE_FOR_UPDATE = ("Submitted", "PreSubmitted", "PendingSubmit")

        for ex in executions:
            perm_id = ex.get("perm_id")
            if perm_id is None:
                # Without a perm_id we can't correlate; treat as orphan with
                # exec_id as the disambiguator. Rare: ib_insync trades always
                # set permId after submission acks.
                self._insert_late_fill_orphan(ex, run_date)
                result["inserted_orphan"] += 1
                continue

            row = self._conn.execute(
                "SELECT id, status FROM executions WHERE perm_id=? "
                "ORDER BY id DESC LIMIT 1",
                (perm_id,),
            ).fetchone()

            if row is None:
                self._insert_late_fill_orphan(ex, run_date)
                result["inserted_orphan"] += 1
                continue

            row_id, row_status = row
            if row_status in ELIGIBLE_FOR_UPDATE:
                slippage = None  # bar_close isn't known here; leave NULL
                self._conn.execute(
                    """UPDATE executions
                    SET status='Filled',
                        fill_price=?, commission=?, realized_pnl=?,
                        slippage_ticks=COALESCE(slippage_ticks, ?),
                        details=json_set(COALESCE(details, '{}'),
                                         '$.exec_id', ?,
                                         '$.reconciled_at', ?,
                                         '$.reconciled_in_run', ?)
                    WHERE id=?""",
                    (
                        ex.get("fill_price"),
                        ex.get("commission"),
                        ex.get("realized_pnl"),
                        slippage,
                        ex.get("exec_id"),
                        datetime.utcnow().isoformat(),
                        run_date,
                        row_id,
                    ),
                )
                result["updated"] += 1
            else:
                # Already Filled, or FAILED, or some terminal state we don't
                # touch from here. Same-cycle fills land here on second pass.
                result["skipped"] += 1

        self._conn.commit()
        if result["updated"] or result["inserted_orphan"]:
            logger.info(
                f"reconcile_late_fills: updated={result['updated']}, "
                f"inserted_orphan={result['inserted_orphan']}, "
                f"skipped={result['skipped']}"
            )
        return result

    def _insert_late_fill_orphan(self, ex: dict, run_date: str) -> None:
        """Insert an orphan late-fill row (no matching originating audit row)."""
        details = {
            "exec_id": ex.get("exec_id"),
            "time_iso": ex.get("time_iso"),
            "reconciled_at": datetime.utcnow().isoformat(),
            "reconciled_in_run": run_date,
        }
        self._conn.execute(
            """INSERT INTO executions
            (timestamp, run_date, symbol, event_type, action, quantity,
             target_contracts, current_contracts, target_signal,
             fill_price, bar_close, slippage_ticks, commission,
             realized_pnl, status, error, details, perm_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.utcnow().isoformat(),
                run_date,
                ex.get("symbol", ""),
                "late_fill_orphan",
                ex.get("action"),
                ex.get("quantity"),
                None, None, None,
                ex.get("fill_price"),
                None, None,
                ex.get("commission"),
                ex.get("realized_pnl"),
                "Filled",
                None,
                json.dumps(details),
                ex.get("perm_id"),
            ),
        )

    def get_slippage_report(self, n: int = 100) -> list[dict]:
        """Per-fill slippage detail (matches FXE format)."""
        cursor = self._conn.execute(
            """SELECT timestamp, symbol, action, bar_close, fill_price,
                      slippage_ticks, commission
               FROM executions
               WHERE status != 'FAILED'
                 AND fill_price IS NOT NULL
                 AND action IN ('BUY', 'SELL')
               ORDER BY id DESC LIMIT ?""",
            (n,),
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def close(self):
        self._conn.close()
