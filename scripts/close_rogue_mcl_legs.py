#!/usr/bin/env python3
"""One-off operator script: close the rogue MCL legs.

Background: 2026-05-08 futures-executor's run-once placed a SELL 7 MCL
order that was supposed to close yesterday's MCLN6 long position (+7,
opened by Thu 2026-05-07's calendar-spread roll). Instead the executor's
contract picker selected MCLM6 (still nominal front-month at the time;
expiry May 18 was 10 days out, beyond the 7-day-pre-expiry roll
threshold). The SELL opened a MCLM6 short rather than closing the
MCLN6 long — leaving the broker with TWO open MCL positions in
opposite directions:

    MCLN6 LONG  +7  @ 93.31    (June expiry; correct, intended)
    MCLM6 SHORT -7  @ 94.67    (May expiry; ROGUE, opened by mistake)

Net portfolio MCL exposure is 0, but operationally we hold two
positions. The MCLM6 short carries delivery risk (May 18 expiry) and
both consume margin.

This script closes BOTH positions with explicit-contract market orders:

    BUY  7 MCLM6 → closes the rogue short
    SELL 7 MCLN6 → closes the legitimate long

After execution: zero MCL position across both contracts.

Safeguards:
  - Defaults to --dry-run (prints plan, places no orders)
  - --execute required to actually trade
  - Verifies live position state matches expected (refuses if either
    contract holds a different qty/side)
  - Fresh clientId (105) to avoid collision with cron jobs (21, 101)

Usage:

    # Dry run (verify state, print plan):
    python scripts/close_rogue_mcl_legs.py

    # Real execution (markets must be open: Sun 18:00 ET → Fri 17:00 ET
    # for CME WTI futures):
    python scripts/close_rogue_mcl_legs.py --execute

Once executed: no need to keep this script around. Move on to the
executor-side fix that prevents this in the first place (close orders
must target the contract that holds the position, not the front-month).
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from futures_executor.config.loader import load_settings
from futures_executor.execution.broker import BrokerConnection
from ib_insync import Contract


_EXPECTED = {
    # local_symbol → (expected_position_signed, action_to_close)
    "MCLN6": (+7, "SELL"),  # close long
    "MCLM6": (-7, "BUY"),   # close short
}


def _verify_state(broker: BrokerConnection) -> dict[str, "BrokerPosition"]:  # noqa: F821
    """Verify both legs are present at exactly the expected qty/side.
    Raises RuntimeError on any mismatch — refuses to trade against
    unexpected state."""
    positions = broker.get_positions()
    by_local = {p.local_symbol: p for p in positions}
    for local, (expected_pos, _) in _EXPECTED.items():
        p = by_local.get(local)
        if p is None:
            raise RuntimeError(
                f"Refusing to close: expected {local} position {expected_pos:+d} "
                f"but no position found. Live state may have changed since "
                f"this script was authored. Re-verify before running."
            )
        if int(p.position) != expected_pos:
            raise RuntimeError(
                f"Refusing to close: {local} expected {expected_pos:+d}, "
                f"broker has {p.position:+.0f}. Live state diverged — "
                f"re-verify before running."
            )
    return by_local


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute", action="store_true",
        help="Actually place orders (default: dry-run, print plan only)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logger = logging.getLogger("close_rogue_mcl")

    config = load_settings(Path("/Users/acess/projects/futures-live/futures_executor/config"))
    config.broker.client_id = 105            # fresh, distinct from cron's 21/101
    config.broker.readonly = not args.execute  # readonly in dry-run

    broker = BrokerConnection(config.broker)
    broker.connect()
    try:
        logger.info("Verifying live broker state matches expected …")
        positions = _verify_state(broker)
        for local, (exp_pos, action) in _EXPECTED.items():
            p = positions[local]
            print(
                f"  {local}  pos={p.position:+.0f}  avg_cost={p.avg_cost:.4f}  "
                f"unrl={p.unrealized_pnl:+.2f}  →  will {action} {abs(int(exp_pos))}"
            )

        info = broker.get_account_info()
        print(f"\nAccount equity (pre-close): {info.equity:,.2f} {info.currency}")

        if not args.execute:
            print("\nDRY-RUN — no orders placed. Re-run with --execute to trade.")
            return 0

        print("\nPlacing orders …")
        for local, (exp_pos, action) in _EXPECTED.items():
            qty = abs(int(exp_pos))
            # Build the contract from the existing position's con_id —
            # most robust against contract-spec drift (ib_insync's
            # qualifyContracts inside place_market_order fills in the
            # remaining fields from IBKR's authoritative metadata).
            p = positions[local]
            contract = Contract(conId=p.con_id, exchange=p.exchange or "NYMEX")
            trade = broker.place_market_order(contract, action, qty)
            fill = broker.get_fill_info(trade, timeout=30)
            print(
                f"  {action} {qty} {local}: avg={fill.avg_fill_price:.4f}  "
                f"qty_filled={fill.quantity}  comm=${fill.commission:.2f}  "
                f"realized=${fill.realized_pnl:.2f}  status={trade.orderStatus.status}"
            )

        print("\nVerifying positions are now flat in MCL …")
        post = {p.local_symbol: p.position for p in broker.get_positions()}
        for local in _EXPECTED:
            qty = post.get(local, 0)
            if qty != 0:
                logger.error(f"  {local} still has position {qty:+.0f} — INVESTIGATE")
            else:
                print(f"  {local}: flat ✓")

        info = broker.get_account_info()
        print(f"\nAccount equity (post-close): {info.equity:,.2f} {info.currency}")

    finally:
        broker.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(main())
