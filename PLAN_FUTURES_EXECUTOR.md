# Plan: Futures Live Executor (IBKR)

## Goal

Live execution system for the `futures_major` instrument set (NQ, CL, ES, GC, ZN)
via Interactive Brokers Gateway. Mirrors the forex-live executor architecture but
with futures-specific contract management, roll logic, and IBKR-native connectivity.

## Design Principles

- **Backtest-live parity:** signals computed on the same continuous series used in
  R-factory backtests. Vol-targeting, signal clipping, and aggregation match exactly.
- **No black boxes:** build our own continuous series from individual FUT contracts.
  Never use IBKR's CONTFUT for signals or data.
- **Atomic rolls:** use IBKR calendar spread (BAG) orders for contract rolls.
  Zero leg risk, minimal slippage.
- **Contract specs from broker:** config only specifies root symbols + exchange.
  Multiplier, tick size, expiry dates fetched live via `reqContractDetails()`.
- **Reuse R-factory code:** strategy loading, vol-targeting, signal aggregation
  import from R-factory (editable install). No drift, no duplication.

---

## Architecture

```
┌─────────────────────────────────────────────┐
│ cron (daily, CME session close)             │
│   futures-live run-once                     │
├─────────────────────────────────────────────┤
│ 1. Connect to IB Gateway (ib_insync)        │
│ 2. Resolve contracts (front + next month)   │
│ 3. Fetch bars, update continuous series     │
│ 4. Check if roll needed                     │
│ 5. Load strategies, compute signals         │
│ 6. Vol-target + aggregate → target pos      │
│ 7. Compute position diff (contracts)        │
│ 8. Execute: rolls (spread) + rebalance      │
│ 9. Audit log + Signal notification          │
└─────────────────────────────────────────────┘
```

### Three-Layer Design

**Data Layer** — contract resolution, bar history, continuous series construction:
- Fetch daily bars from IBKR for front + next contract per instrument
- Maintain local bar history (parquet per contract)
- Build back-adjusted continuous series on each run
- Roll calendar: track current front contract, next contract, roll date

**Signal Layer** — strategy execution on continuous series:
- Load strategies + params from `strategies.yaml` (exported from R-factory)
- Run `generate_signals()` on continuous series (as MarketData)
- Vol-target using R-factory's `rolling_volatility()`
- Clip signals to [-1, 1], weight, aggregate → per-instrument target position

**Execution Layer** — position management on actual contracts:
- Query current positions via `ib.positions()`
- Compute diff: target contracts - current contracts
- Roll expiring contracts via calendar spread (BAG) orders
- Adjust positions via market orders on front contract
- Audit all fills with slippage tracking

---

## Contract Management

### Front Contract Resolution

On each run:
1. Call `ib.reqContractDetails()` for each root symbol
2. Filter to tradeable expiries (today < lastTradeDate)
3. Front = nearest expiry with volume above threshold
4. Next = second nearest expiry

Store resolved contracts in local state (JSON) so we know when front changes.

### Roll Logic

**Primary trigger:** 7 trading days before front contract expiry
**Sanity check:** next contract daily volume > 1000 contracts
**Hard deadline:** 3 trading days before expiry (roll regardless of volume)

**Roll + rebalance coordination:**
- Portfolio rebalances every 21 days (from R-factory config)
- If roll falls within ±3 days of a scheduled rebalance → merge into one pass
  (roll to new contract AND apply new weights simultaneously)
- If roll is forced outside rebalance window → roll standalone, keep weights unchanged

### Roll Execution

Use IBKR calendar spread (BAG contract):
```python
spread = Contract()
spread.symbol = 'ES'
spread.secType = 'BAG'
spread.exchange = 'CME'
spread.currency = 'USD'
spread.comboLegs = [
    ComboLeg(conId=front_conId, ratio=1, action='SELL', exchange='CME'),
    ComboLeg(conId=next_conId, ratio=1, action='BUY', exchange='CME'),
]
order = MarketOrder('BUY', n_contracts)
ib.placeOrder(spread, order)
```

Both legs execute atomically at the spread price.

### Continuous Series Construction

**Method:** back-adjustment (subtract roll gap from all historical bars)

On each roll:
1. Record `gap = next_close[roll_day] - front_close[roll_day]`
2. Subtract `gap` from all historical bars in the series
3. Switch front pointer to next contract
4. Persist adjustment history (for auditing)

Between rolls: just append today's front contract bar to the series.

**Storage:** local parquet per instrument (`data/continuous/{symbol}.parquet`)
with columns: date, open, high, low, close, volume, raw_close, contract_month

---

## Lot Sizing (Contract Sizing)

**Formula:** `n_contracts = floor(target × equity × portfolio_leverage / (n_instruments × contract_value))`

Where:
- `target` = aggregated signal for this instrument (after vol-targeting)
- `equity` = account equity (from `ib.accountSummary()`)
- `portfolio_leverage` = config multiplier (default 1.0)
- `n_instruments` = 5
- `contract_value` = last_price × multiplier (e.g., ES at 5800 × $50 = $290,000)

Multiplier fetched from `reqContractDetails()`, not hardcoded.

**Min contract delta:** skip trades where |delta| < 1 contract (can't trade fractional).

---

## Configuration

### settings.yaml

```yaml
broker:
  host: 127.0.0.1
  port: 4001          # IB Gateway
  client_id: 1
  readonly: false
  timeout: 30

rfactory_path: /Users/acess/projects/R-factory

instruments:
  - symbol: NQ
    exchange: CME
  - symbol: CL
    exchange: NYMEX
  - symbol: ES
    exchange: CME
  - symbol: GC
    exchange: COMEX
  - symbol: ZN
    exchange: CBOT

roll:
  days_before_expiry: 7
  hard_deadline_days: 3
  min_next_volume: 1000
  merge_with_rebalance_window: 3   # merge if roll within ±3 days of rebalance

execution:
  portfolio_leverage: 1.0
  min_delta_contracts: 1
  order_type: market               # market or limit

vol_target:
  enabled: true
  target_vol: 0.10
  vol_window: 60
  max_leverage: 3.0

data:
  lookback_bars: 200
  continuous_dir: data/continuous
  bar_history_dir: data/bars

safety:
  max_position_contracts: 10       # per instrument
  max_total_contracts: 30          # across all instruments
  max_daily_turnover: 20           # contracts
  kill_switch_file: .kill_switch
  heartbeat_timeout: 300           # seconds since last IB heartbeat

audit:
  db_path: data/audit.db

signal:
  account: "+37069693289"
  recipient: "+37069693181"
  enabled: true
  cli_path: /opt/homebrew/bin/signal-cli
```

### strategies.yaml

Same format as forex-live — exported from R-factory via:
```bash
python -m algo_research_factory.cli portfolio export-live --instrument-set futures_major
```

---

## Module Structure

```
futures-live/
  futures_executor/
    __init__.py
    cli.py                     # run-once, status, flatten, roll-status
    config/
      __init__.py
      loader.py                # Pydantic models, load settings/strategies
      settings.yaml
      strategies.yaml          # from R-factory export
    data/
      __init__.py
      contract_resolver.py     # front/next contract resolution from IBKR
      bar_fetcher.py           # reqHistoricalData wrapper
      continuous_series.py     # back-adjustment, roll tracking, parquet I/O
      continuous/              # persistent continuous series (parquet)
      bars/                    # raw per-contract bar history (parquet)
    execution/
      __init__.py
      broker.py                # IB Gateway connection, order placement, positions
      order_manager.py         # position diff, contract sizing, roll execution
    strategy/
      __init__.py
      aggregator.py            # signal aggregation (imports from R-factory)
    monitoring/
      __init__.py
      notifier.py              # Signal notifications (reuse from forex-live)
      audit.py                 # SQLite audit trail with slippage tracking
  pyproject.toml               # with R-factory as editable dependency
```

---

## Implementation Phases

### Phase 1 — Broker Connectivity + Contract Resolution
**Files:** `config/loader.py`, `execution/broker.py`, `data/contract_resolver.py`

- Pydantic config models (settings.yaml schema)
- `BrokerConnection` class: connect/disconnect to IB Gateway, account queries,
  position queries, order placement, spread order support
- `ContractResolver` class: resolve front/next contracts per symbol,
  fetch multiplier and contract details, roll date computation
- CLI `status` command: show connection, account equity, positions, contract info

### Phase 2 — Data Layer + Continuous Series
**Files:** `data/bar_fetcher.py`, `data/continuous_series.py`

- `BarFetcher`: download daily bars from IBKR for specific FUT contracts,
  store to local parquet (per contract month)
- `ContinuousSeries`: build and maintain back-adjusted series per instrument,
  handle roll gaps, persist to parquet, load as MarketData for strategies
- Roll gap tracking and adjustment history

### Phase 3 — Signal Aggregation + Vol-Targeting
**Files:** `strategy/aggregator.py`

- Port aggregator from forex-live (or import shared code from R-factory)
- Load strategies.yaml, run generate_signals on continuous series
- Vol-target, clip, weight, aggregate → per-instrument target positions
- Verify parity with R-factory backtest output

### Phase 4 — Execution + Roll Logic
**Files:** `execution/order_manager.py`, CLI `run-once`

- `compute_position_diff()`: target contracts - current contracts
- Contract sizing: equity-based, using live contract values from IBKR
- Roll detection: check days to expiry, volume on next contract
- Roll execution: calendar spread (BAG) orders
- Roll + rebalance merge logic
- Regular position adjustment: market orders on front contract
- Close-first-then-open sequencing for reversals

### Phase 5 — Safety, Audit, Notifications
**Files:** `monitoring/audit.py`, `monitoring/notifier.py`, CLI `flatten`, `audit`

- Kill switch file check
- Position limits enforcement (per-instrument + total)
- Daily turnover tracking
- SQLite audit log: order details, fill prices, slippage vs bar close
- Signal notifications: daily summary, roll alerts, error alerts
- `flatten` command: emergency close all positions
- `audit` command: show recent fills and slippage

### Phase 6 — Cron + Production Hardening
- Cron schedule: daily after CME close (e.g., 17:15 CT / 00:15 UTC)
- IB Gateway auto-restart detection (heartbeat check)
- Graceful handling of IB disconnects mid-execution
- Weekend/holiday detection (CME calendar)
- Dry-run mode (whatIf orders) for testing

---

## Dependencies

- `ib_insync` — IBKR API wrapper
- `numpy`, `pandas` — data handling
- `pyyaml`, `pydantic` — config
- `algo_research_factory` — editable install for strategy code, vol-targeting, etc.

---

## Differences from Forex Executor

| Aspect | Forex | Futures |
|---|---|---|
| Broker | Dukascopy (JForex REST bridge) | IBKR (ib_insync, direct Python) |
| Connectivity | HTTP REST to localhost:8090 | TCP to IB Gateway localhost:4001 |
| Bridge | Custom Kotlin/Spring Boot | None needed |
| Instruments | Spot FX (CASH) | Futures (FUT) |
| Position units | Lots (100K base) | Contracts |
| Contract mgmt | None (spot, no expiry) | Roll logic, continuous series |
| Data source | Bridge REST | ib.reqHistoricalData() |
| Lot sizing | equity / (n × lot_value_eur) | equity / (n × price × multiplier) |
| Roll | N/A | Calendar spread (BAG) orders |
| Min trade size | 0.001 lots | 1 contract |

## What's Shared (from R-factory)

- `generate_signals()` — strategy interface
- `rolling_volatility()` — vol-targeting
- `signals_to_positions()` — signal clipping
- `compute_returns()`, `aggregate_returns()` — for vol-scale computation
- `MarketData`, `StrategyOutput` — data contracts

---

## Edge Cases

1. **IB Gateway disconnects mid-run:** catch `ConnectionError`, log, send alert,
   do NOT leave partial positions. If disconnect happens between close and open
   of a reversal, the close already went through → we have reduced exposure,
   which is safe. Next run will pick up and correct.

2. **Contract not found:** if `reqContractDetails()` returns empty for a symbol
   (e.g., exchange holiday), skip that instrument for this run. Log warning.

3. **Roll day falls on holiday:** roll logic checks trading days, not calendar days.
   Use IBKR's trading hours from contract details to determine trading days.

4. **Zero signal:** if aggregated target = 0 for an instrument but we hold a
   position → close it. Same as forex executor behavior.

5. **Fractional contracts:** floor to nearest integer. If target is 0.4 contracts,
   that rounds to 0 → flat. This is inherent to futures — can't trade fractions.
   For small accounts, consider micro contracts (MES, MNQ, MGC, MYM) instead.

6. **First run (no history):** download full lookback (200 bars) of front contract,
   build initial continuous series with no adjustments. First signals may use
   shorter history but that's fine — same as backtest warmup.

7. **Multiple rolls in one day:** shouldn't happen with daily frequency.
   If somehow triggered, execute only the first roll and flag for review.
