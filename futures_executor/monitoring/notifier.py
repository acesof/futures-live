"""Signal messenger notifications for execution events."""

import logging
import subprocess
from dataclasses import dataclass

from futures_executor.config.loader import SignalSettings

logger = logging.getLogger(__name__)


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
                    "-a", self.settings.account,
                    "send",
                    "-m", message,
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
    ) -> str:
        """Build FXE-style rich summary for Signal/logging."""
        lines = [f"Futures Executor — {run_date}", ""]

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
            lines.append(f"Orders executed: {n_orders} | Rolls: {n_rolls} | Errors: {n_errors}")
            for rec in records:
                status = rec.get("status", "?")
                event_type = rec.get("type", "?")
                symbol = rec.get("symbol", "?")

                if event_type == "roll":
                    from_m = rec.get("from_month", "?")
                    to_m = rec.get("to_month", "?")
                    qty = rec.get("quantity", "?")
                    lines.append(f"  ROLL {symbol}: {from_m} -> {to_m}, qty={qty} -> {status}")
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
                    lines.append(f"  {action} {qty} {symbol} {fill_str} -> {status}{slip_str}")
        else:
            lines.append("No orders executed")
        lines.append("")

        # Account
        lines.append(f"Equity: ${equity:,.0f}")
        lines.append(f"Commission: ${total_commission:.2f}")
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
    ) -> bool:
        """Send roll notification."""
        msg = (
            f"ROLL: {symbol}\n"
            f"{from_month} -> {to_month}\n"
            f"Qty: {quantity}, Status: {status}"
        )
        return self.send(msg)

    def notify_error(self, symbol: str, error: str) -> bool:
        """Send error alert."""
        msg = f"ERROR: {symbol}\n{error}"
        return self.send(msg)

    def notify_kill_switch(self) -> bool:
        """Send kill switch activation alert."""
        return self.send("KILL SWITCH ACTIVATED — executor halted")
