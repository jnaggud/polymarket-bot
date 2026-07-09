# Polymarket Bot Scaffold

This project builds the architecture from the X thread as a local, inspectable bot:

- target-wallet discovery from `poly_data`-style CSV exports
- market scanning via the official `polymarket` CLI
- thesis generation through OpenAI
- three-agent consensus
- Kelly sizing
- exit monitoring
- local monitoring dashboard
- paper-trading by default

## Important

As of April 22, 2026, Polymarket's own docs list the United States (`US`) as blocked for order placement. This repo therefore defaults to `paper` mode and refuses live trading when the geoblock endpoint reports the current IP is restricted.

Relevant docs:

- Polymarket API overview: https://docs.polymarket.com/api-reference
- Geographic restrictions: https://docs.polymarket.com/api-reference/geoblock
- CLOB auth model: https://docs.polymarket.com/developers/proxy-wallet
- Official CLI: https://github.com/Polymarket/polymarket-cli
- `poly_data`: https://github.com/warproxxx/poly_data

## What This Bot Actually Does

The X thread mixes real repos with pseudocode and leaves out important details. This scaffold keeps the usable parts:

1. `discover-targets`
Reads a CSV and ranks wallets. If the CSV contains `profit`, `pnl`, `total_pnl`, or `win_rate`, those are used. Otherwise it falls back to activity-based ranking so you still get a target list.

2. `refresh-target-activity`
Pulls recent trades for the target wallets through the official CLI and normalizes them into a local cache.

3. `scan`
Lists active markets, fetches midpoint and book depth, filters by liquidity and time-to-resolution, and writes a queue.

4. `brain`
Calls OpenAI with a structured JSON prompt and asks for probability, confidence, thesis, catalysts, and crowd-error framing.

5. `trade`
Runs three votes:
- convergence: thesis probability vs current midpoint
- whale copy: target-wallet activity on the same token/market
- microstructure: order-book imbalance and spread

6. `monitor-exits`
Checks target-hit, order-flow spike proxy, and stale-thesis exits.

## Setup

1. Install the official CLI.

```bash
brew tap Polymarket/polymarket-cli https://github.com/Polymarket/polymarket-cli
brew install polymarket
```

2. Copy the env file.

```bash
cp .env.example .env
```

3. Run one cycle in paper mode.

```bash
python3 main.py cycle
```

## Commands

```bash
python3 main.py discover-targets --leaderboard
python3 main.py discover-targets --csv /path/to/trades.csv
python3 main.py refresh-target-activity
python3 main.py scan
python3 main.py brain
python3 main.py trade
python3 main.py monitor-exits
python3 main.py cycle
python3 main.py daemon --interval 300
python3 main.py serve-dashboard --host 127.0.0.1 --port 8080
```

## 24/7 Local Run

Terminal 1:

```bash
python3 main.py daemon --interval 300
```

Terminal 2:

```bash
python3 main.py serve-dashboard
```

Then open `http://127.0.0.1:8080`.

Or use the helper scripts:

```bash
./scripts/start_bot_bg.sh
./scripts/start_dashboard_bg.sh
./scripts/status_local.sh
./scripts/stop_local.sh
```

If a background process exits immediately, the start script now prints the last log lines instead of leaving a misleading PID file behind.

## Latency Bot Cockpit

The paper-first latency engine is separate from the original scanner/trader loop. It tracks short-horizon crypto markets, runs the temporal inventory maker and late-resolution paper models, and keeps every live maker action behind dry-run, reconciliation, heartbeat, and PnL gates.

Run the engine and cockpit directly:

```bash
python3 main.py daemon-latency-bot-engine --interval 10
python3 main.py serve-latency-bot-dashboard --host 127.0.0.1 --port 8090
```

Then open `http://127.0.0.1:8090`. The cockpit also exposes its compact refresh payload at `http://127.0.0.1:8090/api/state`.

The temporal maker, guarded live-maker, and late-resolution settings are documented in `.env.example`. Live maker trading defaults to disabled and `dry_run`; do not add credentials or change the live confirmation gates in a committed file.

Run the repository test suite with:

```bash
python3 -m unittest discover -s tests
```

## `poly_data` Notes

The public `poly_data` README documents `processed/trades.csv` fields like:

- `timestamp`
- `market_id`
- `maker`
- `taker`
- `price`
- `usd_amount`
- `token_amount`

It does not document a built-in `profit` column. If your local export does include realized PnL fields, this bot will use them. If it does not, wallet ranking falls back to activity/notional so you can still build a watchlist instead of pretending the PnL is available when it isn't.

## Startup Script

See [scripts/run_cycle.sh](/Users/jeffersonduggan/dev/polymarket/scripts/run_cycle.sh) for a simple cron or `systemd` entrypoint.
