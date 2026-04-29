#!/usr/bin/env python3
"""Read-only IBKR account-values probe.

Connects to IB Gateway and dumps every accountSummary tag, accountValues
entry, position, and per-position PnL accessor. Prints reconciliation
candidates against `NetLiquidation` so we can identify the right
source-of-truth field for the futures monitor's day-over-day balance check.

Usage:
    python scripts/probe_ibkr_account.py [--client-id N]

Default clientId 1098 — the diag-probe slot, separate from the executor's
101 and any onboarding probes (1099). Safe to run alongside the live
executor — readonly=True prevents any submitOrder side effects.

Prerequisites:
    IB Gateway running and logged in (the same session the executor uses).

Output format: human-readable text, sectioned by data source. Captured to
stdout for easy `tee` into a log file.
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

# Reuse the executor's settings module so host/port match without duplication.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from futures_executor.config.loader import load_settings  # noqa: E402

from ib_insync import IB  # noqa: E402


# ---------------------------------------------------------------------------
# Reconciliation hints
# ---------------------------------------------------------------------------

# Tags we particularly care about for the futures balance-check problem.
# Listed by the question they help answer. Print these prominently even if
# they're already in the dump.
PRIORITY_TAGS = {
    # "What's today's P&L flowing into balance?" — primary candidate for #2.
    "RealizedPnL", "UnrealizedPnL",
    # Daily variants (some IBKR setups expose; uncertain on demo).
    "DayPnL", "DailyRealizedPnL", "DailyUnrealizedPnL",
    # Balance/equity reconciliation.
    "NetLiquidation", "TotalCashValue", "CashBalance",
    # Account state.
    "AccountReady", "Currency",
    # Margin (sanity check).
    "InitMarginReq", "MaintMarginReq", "ExcessLiquidity",
}


def _print_section(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def _dump_account_summary(ib: IB) -> None:
    _print_section("accountSummary() — all tags")
    summary = ib.accountSummary()
    # Group by tag for compact output (multi-currency tags appear once per ccy).
    by_tag: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    for item in summary:
        by_tag[item.tag].append((item.account, item.currency, item.value))
    for tag in sorted(by_tag):
        rows = by_tag[tag]
        marker = " ←" if tag in PRIORITY_TAGS else ""
        if len(rows) == 1:
            acct, ccy, val = rows[0]
            print(f"  {tag:<32}  {val:>20}  {ccy}{marker}")
        else:
            print(f"  {tag}{marker}")
            for acct, ccy, val in rows:
                print(f"      {val:>20}  {ccy}")


def _dump_account_values(ib: IB) -> None:
    """accountValues() is more granular than accountSummary — separate currencies,
    additional tags. Useful for finding day-of-MTM values that aren't in the
    summary feed."""
    _print_section("accountValues() — per-currency, full feed")
    values = ib.accountValues()
    by_tag: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for v in values:
        by_tag[v.tag].append((v.currency, v.value))
    for tag in sorted(by_tag):
        rows = by_tag[tag]
        marker = " ←" if tag in PRIORITY_TAGS else ""
        if len(rows) == 1:
            ccy, val = rows[0]
            print(f"  {tag:<32}  {val:>20}  {ccy}{marker}")
        else:
            print(f"  {tag}{marker}")
            for ccy, val in rows:
                print(f"      {val:>20}  {ccy}")


def _dump_positions(ib: IB) -> None:
    _print_section("positions() — open contracts")
    positions = ib.positions()
    if not positions:
        print("  (none)")
        return
    for p in positions:
        c = p.contract
        sym = getattr(c, "localSymbol", None) or c.symbol
        print(
            f"  {sym:<10} {c.secType:<5} pos={p.position:>8.0f}  "
            f"avgCost={p.avgCost:>12.4f}  account={p.account}"
        )


def _dump_pnl(ib: IB, account: str) -> None:
    """Subscribe to PnL feed for the account, wait briefly, dump.

    `reqPnL` returns a streaming PnL object. We poll once after a short sleep
    rather than subscribing long-term. If IBKR exposes `dailyPnL` here, this
    is exactly the field we need for #2.
    """
    _print_section(f"reqPnL(account={account!r}) — daily aggregate")
    pnl = ib.reqPnL(account, modelCode="")
    ib.sleep(2.0)
    print(f"  dailyPnL     = {pnl.dailyPnL}")
    print(f"  unrealizedPnL= {pnl.unrealizedPnL}")
    print(f"  realizedPnL  = {pnl.realizedPnL}")
    ib.cancelPnL(account, modelCode="")


def _dump_per_position_pnl(ib: IB, account: str) -> None:
    """Per-position PnL via reqPnLSingle. Slow (one round-trip per position) but
    gives the granular view: daily + cumulative per contract.
    """
    _print_section(f"reqPnLSingle(account={account!r}, conId=…) — per-position daily")
    positions = ib.positions()
    if not positions:
        print("  (no positions)")
        return
    subs = []
    for p in positions:
        subs.append(ib.reqPnLSingle(account, "", p.contract.conId))
    ib.sleep(2.5)
    for p, sub in zip(positions, subs):
        sym = getattr(p.contract, "localSymbol", None) or p.contract.symbol
        print(
            f"  {sym:<10} dailyPnL={sub.dailyPnL!s:>12}  "
            f"unrealizedPnL={sub.unrealizedPnL!s:>12}  "
            f"realizedPnL={sub.realizedPnL!s:>12}  "
            f"value={sub.value!s:>14}  pos={sub.position:>4.0f}"
        )
        ib.cancelPnLSingle(account, "", p.contract.conId)


def _print_reconciliation_hints(ib: IB) -> None:
    """Quick arithmetic check: NetLiquidation vs TotalCashValue + UnrealizedPnL.

    For futures, NetLiquidation = TotalCashValue + UnrealizedPnL (broker
    quotes equity inclusive of MTM on still-open positions). If this
    relationship holds, then for the monitor's balance check:
        Δ(TotalCashValue) day-over-day = realized + commission + cash flows
        Δ(NetLiquidation) day-over-day = realized + commission + ΔMTM
    The right column to reconcile transactions against is TotalCashValue,
    NOT NetLiquidation. This is the design hypothesis #2 will test.
    """
    _print_section("Reconciliation arithmetic")
    tags = {v.tag: float(v.value) for v in ib.accountValues()
            if v.currency in ("EUR", "BASE") and v.value not in ("", None)
            and _is_numeric(v.value)}
    nl = tags.get("NetLiquidation")
    tcv = tags.get("TotalCashValue")
    upnl = tags.get("UnrealizedPnL")
    rpnl = tags.get("RealizedPnL")
    print(f"  NetLiquidation        = {nl}")
    print(f"  TotalCashValue        = {tcv}")
    print(f"  UnrealizedPnL         = {upnl}")
    print(f"  RealizedPnL           = {rpnl}")
    if nl is not None and tcv is not None and upnl is not None:
        diff = nl - (tcv + upnl)
        print(f"  NetLiq − (TCV + UPnL) = {diff:+.4f}  (expect ≈ 0 if hypothesis holds)")


def _is_numeric(s: str) -> bool:
    try:
        float(s)
        return True
    except (TypeError, ValueError):
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client-id", type=int, default=1098,
                        help="IBKR clientId (default 1098 — diag-probe slot)")
    args = parser.parse_args()

    settings = load_settings()
    host = settings.broker.host
    port = settings.broker.port
    print(f"Connecting to IB Gateway at {host}:{port} (clientId={args.client_id}, readonly)")

    ib = IB()
    ib.connect(host, port, clientId=args.client_id, readonly=True, timeout=15)
    try:
        accounts = ib.managedAccounts()
        print(f"Managed accounts: {accounts}")
        account = accounts[0] if accounts else ""

        _dump_account_summary(ib)
        _dump_account_values(ib)
        _dump_positions(ib)
        _print_reconciliation_hints(ib)
        if account:
            _dump_pnl(ib, account)
            _dump_per_position_pnl(ib, account)
    finally:
        ib.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(main())
