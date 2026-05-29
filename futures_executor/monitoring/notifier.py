"""Signal messenger notifications for execution events."""

import logging
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone

from futures_executor.config.loader import SignalSettings

logger = logging.getLogger(__name__)

# Alert emoji prefixes — visible on mobile lock-screen previews and
# distinguishable without color support in signal-cli.
EMOJI_SUCCESS = "✅"
EMOJI_INFO = "ℹ️"
EMOJI_WARNING = "⚠️"
EMOJI_ERROR = "🔴"
EMOJI_CRITICAL = "🚨"
EMOJI_ROLL = "🔄"


class SignalNotifier:
    """Send notifications via signal-cli."""

    def __init__(self, settings: SignalSettings):
        self.settings = settings

    @property
    def enabled(self) -> bool:
        return (
            self.settings.enabled
            and bool(self.settings.account)
            and bool(self.settings.recipient)
        )

    def send(self, message: str) -> bool:
        """Send a message via Signal. Returns True on success."""
        if not self.enabled:
            return False

        try:
            result = subprocess.run(
                [
                    self.settings.cli_path,
                    "-a",
                    self.settings.account,
                    "send",
                    "-m",
                    message,
                    self.settings.recipient,
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode != 0:
                logger.warning(f"signal-cli failed: {result.stderr.strip()}")
                return False
            return True
        except FileNotFoundError:
            logger.warning(f"signal-cli not found at {self.settings.cli_path}")
            return False
        except subprocess.TimeoutExpired:
            logger.warning("signal-cli timed out")
            return False
        except Exception as e:
            logger.warning(f"Signal notification failed: {e}")
            return False

    def build_run_summary(
        self,
        run_date: str,
        equity: float,
        targets: dict[str, float],
        records: list[dict],
        n_orders: int,
        n_rolls: int,
        n_errors: int,
        total_commission: float,
        positions: dict[str, int],
        account_currency: str = "EUR",
    ) -> str:
        """Build FXE-style rich summary for Signal/logging."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        # Severity: errors > rolls > orders > flat.
        if n_errors > 0:
            emoji = EMOJI_ERROR
        elif n_rolls > 0:
            emoji = EMOJI_ROLL
        elif n_orders > 0:
            emoji = EMOJI_SUCCESS
        else:
            emoji = EMOJI_INFO
        lines = [f"{emoji} Futures Executor — {now}", ""]

        # Target signals
        lines.append("Targets:")
        for sym, signal in sorted(targets.items()):
            if abs(signal) < 1e-8:
                label = "FLAT"
            elif signal > 0:
                label = f"+{signal:.6f}"
            else:
                label = f"{signal:.6f}"
            lines.append(f"  {sym}: {label}")
        lines.append("")

        # Per-order detail
        if records:
            lines.append(
                f"Orders executed: {n_orders} | Rolls: {n_rolls} | Errors: {n_errors}"
            )
            for rec in records:
                status = rec.get("status", "?")
                event_type = rec.get("type", "?")
                symbol = rec.get("symbol", "?")

                if event_type == "roll":
                    from_m = rec.get("from_month", "?")
                    to_m = rec.get("to_month", "?")
                    qty = rec.get("quantity", "?")
                    lines.append(
                        f"  ROLL {symbol}: {from_m} -> {to_m}, qty={qty} -> {status}"
                    )
                elif event_type == "migration_roll":
                    from_m = rec.get("from_month", "?")
                    to_m = rec.get("to_month", "?")
                    qty = rec.get("quantity", "?")
                    lines.append(
                        f"  MIGRATION-ROLL {symbol}: {from_m} -> {to_m}, "
                        f"qty={qty} -> {status} (buffer-triggered)"
                    )
                elif event_type == "migration_blocked":
                    error = rec.get("error", "blocked")
                    lines.append(
                        f"  MIGRATION-BLOCKED {symbol}: {error} -> {status}"
                    )
                elif event_type == "contract_advance":
                    from_m = rec.get("from_month", "?")
                    to_m = rec.get("to_month", "?")
                    lines.append(
                        f"  CONTRACT-ADVANCE {symbol}: {from_m} -> {to_m} "
                        "(delivery buffer; no migration needed)"
                    )
                elif event_type in ("reconcile_error", "reconcile_failed"):
                    error = rec.get("error", "unknown")
                    lines.append(f"  {event_type.upper()}: {error} -> {status}")
                else:
                    action = rec.get("action", "?")
                    qty = rec.get("quantity", "?")
                    fill = rec.get("fill_price")
                    bar_close = rec.get("bar_close")
                    fill_str = f"@ {fill:.2f}" if fill else "@ ?"
                    slip_str = ""
                    if fill and bar_close and bar_close > 0:
                        sign = 1.0 if action == "BUY" else -1.0
                        slip = sign * (fill - bar_close)
                        slip_str = f" (slip={slip:+.4f})"
                    lines.append(
                        f"  {action} {qty} {symbol} {fill_str} -> {status}{slip_str}"
                    )
        else:
            lines.append("No orders executed")
        lines.append("")

        # Account
        # Equity is in account currency (EUR for our EUR-base IBKR account).
        # Commission is in contract currency (USD for CME/NYMEX/COMEX futures,
        # which is all we trade); IBKR reports it raw and we don't convert.
        lines.append(f"Equity: {equity:,.0f} {account_currency}")
        lines.append(f"Commission: ${total_commission:.2f} USD")
        lines.append("")

        # Final positions
        lines.append("Positions:")
        for sym, qty in sorted(positions.items()):
            lines.append(f"  {sym}: {qty:+d}")

        return "\n".join(lines)

    def notify_roll(
        self,
        symbol: str,
        from_month: str,
        to_month: str,
        quantity: int,
        status: str,
        kind: str = "ROLL",
    ) -> bool:
        """Send roll notification. ``kind`` lets callers distinguish a
        scheduled ROLL from a buffer-triggered MIGRATION-ROLL in the
        Signal subject without forking the method."""
        msg = (
            f"{EMOJI_ROLL} {kind}: {symbol}\n"
            f"{from_month} -> {to_month}\n"
            f"Qty: {quantity}, Status: {status}"
        )
        return self.send(msg)

    def notify_contract_advance(
        self,
        symbol: str,
        from_month: str,
        to_month: str,
        reason: str = "delivery_buffer",
    ) -> bool:
        """Signal that resolve() advanced the front contract pointer
        (e.g., delivery_buffer fired). Distinct from notify_roll — no
        position was migrated; only the system's idea of "the front"
        moved. Pair with notify_roll if a stranded position also got
        migrated this run."""
        msg = (
            f"{EMOJI_ROLL} CONTRACT-ADVANCE: {symbol}\n"
            f"{from_month} -> {to_month}\n"
            f"Reason: {reason} (no position migration this run)"
        )
        return self.send(msg)

    def notify_error(self, symbol: str, error: str) -> bool:
        """Send error alert."""
        msg = f"{EMOJI_ERROR} ERROR: {symbol}\n{error}"
        return self.send(msg)

    def notify_kill_switch(self) -> bool:
        """Send kill switch activation alert."""
        return self.send(f"{EMOJI_CRITICAL} KILL SWITCH ACTIVATED — executor halted")
