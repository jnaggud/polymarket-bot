# Quant Model Requirements And Research Plan

Last updated: 2026-06-13

## Purpose

Build a practical short-horizon quant model for BTC/ETH/SOL Polymarket Up/Down markets. The model should improve the existing CEX Latency Paper Bot by replacing the current weak `market_blend` fair-value estimate with a calibrated probability engine that can decide whether a Polymarket YES or NO price is mispriced relative to crypto exchange movement, book state, and time to expiry.

The goal is not to force trades. The goal is to identify when the market price is wrong enough that expected value survives spread, slippage, latency, stale data, and drawdown risk.

## Current Bot Diagnosis

The current CEX Latency Paper Bot is directional, not true arbitrage. It buys one side, then exits by take-profit, stop-loss, edge close, or force-exit. Recent diagnostics showed it was losing because the internal edge score was not predictive enough:

- Stop-loss exits dominated realized losses.
- Cheap-tail entries, especially sub-$0.25 contracts, were a major loss source.
- High quoted edge did not imply real predictive edge, which means the current fair-value model is overconfident.
- BTC/ETH and YES/NO slices were all negative in the latest sample, so this is a model-quality problem, not just a single asset problem.

The model must therefore be judged by out-of-sample realized PnL, calibration, and drawdown, not by internal edge alone.

## Model Contract

The model should output one row per market per cycle:

```json
{
  "model_version": "cex_v1",
  "timestamp_utc": "2026-06-13T00:00:00Z",
  "market_id": "2470000",
  "asset": "btc",
  "tenor_minutes": 5,
  "seconds_left": 240,
  "prob_up": 0.542,
  "prob_down": 0.458,
  "confidence": 0.68,
  "fair_yes": 0.542,
  "fair_no": 0.458,
  "do_not_trade": false,
  "reason": "edge_persistent"
}
```

Required properties:

- `prob_up + prob_down` must be approximately `1.0`.
- Probabilities must be calibrated, not just ranked.
- Output must be timestamped so stale predictions can be rejected.
- The model must support a no-trade state when features are stale, volatility regime is bad, or the model confidence is low.
- The output must be serializable to SQLite and usable from `latency_bot/strategy/signals.py`.

## Feature Requirements

Minimum feature set:

- Polymarket market metadata: asset, tenor, expiry, strike/reference price if available, seconds left, question/slug.
- Polymarket order book: YES bid/ask, NO bid/ask, bid/ask spread, visible depth, VWAP at target notional, book timestamp, book hash if available.
- Polymarket microstructure: recent YES/NO price changes, best-bid/best-ask changes, last trade price/side/size, liquidity gaps, depth imbalance.
- CEX price state: Binance mid/bookTicker for BTC/ETH/SOL, spread, book age, short-window returns, realized volatility, acceleration, and volatility regime.
- Cross-market state: distance of CEX price from the market start/reference level, movement since market open, movement over last 1s/5s/15s/30s/60s/180s, and momentum reversal flags.
- Execution constraints: estimated fill price, min order size, tick size, book staleness, latency budget, expected slippage, cooldowns, and capital usage.
- Optional external predictor: a user-provided BTC model signal with prediction horizon, probability, confidence, and timestamp.

Feature windows to build first:

- `1s`, `5s`, `15s`, `30s`, `60s`, `180s`, and since-market-open returns.
- Rolling realized volatility over `30s`, `60s`, and `180s`.
- Polymarket price changes over the last `1`, `2`, `3`, and `5` engine cycles.
- Book age and CEX tick age at decision time.

## Label Requirements

Primary label:

- `market_winner`: whether UP/YES or DOWN/NO resolved true.

Secondary labels:

- `entry_side_won`: whether the selected side won.
- `max_favorable_mark_after_entry`.
- `max_adverse_mark_after_entry`.
- `exit_price_if_take_profit_stop_loss_force_exit`.
- `realized_pnl_after_execution_costs`.

The model should not train on labels that use future data unavailable at entry time. All joins must be replay-safe.

## Evaluation Requirements

Use walk-forward evaluation. Do not random-shuffle 5-minute market rows because neighboring cycles from the same market leak future information.

Minimum metrics:

- Brier score and log loss by asset, side, tenor, seconds-left bucket, and price band.
- Calibration table: predicted probability bucket vs actual win rate.
- Realized PnL by asset/side, price band, edge band, seconds-left band, and exit reason.
- Max drawdown, worst 10-trade stretch, stop-loss contribution, average win, average loss, and capital utilization.
- Edge persistence: how many cycles a signal stayed positive before entry.
- Stale-data rejection rate.

Backtest acceptance criteria before live use:

- Positive out-of-sample PnL after realistic execution pricing.
- Positive PnL in at least two independent time windows, not only one lucky cluster.
- Stop-loss PnL cannot dominate total PnL.
- Calibration must improve over current market midpoint and current `market_blend` baseline.
- No single market, asset, side, or hour should explain most of the profit.
- Dashboard must show model version, calibration, PnL breakdowns, and live/paper divergence.

## Baselines To Beat

The model must beat these baselines:

- No-trade baseline.
- Polymarket midpoint as fair value.
- Current `market_blend` fair-value model.
- Simple CEX momentum model.
- Simple CEX mean-reversion model.
- Volatility-only model.
- Market-price-following model that buys the side with improving Polymarket book pressure.

## Model Families To Explore

First pass:

- Logistic regression with engineered microstructure features.
- Gradient-boosted trees for non-linear interactions.
- Isotonic or Platt calibration on top of raw model scores.
- Online/rolling retraining by day or regime.

Second pass:

- Ensemble of momentum, mean-reversion, book-pressure, and volatility-regime models.
- Meta-model that decides when the base models are reliable enough to trade.
- Separate models per asset/tenor/side if pooled calibration is unstable.

Avoid initially:

- Deep learning models until the dataset is clean and the simpler models have been exhausted.
- Reinforcement learning for entry/exit until supervised probabilities and execution replay are reliable.
- Any model that cannot explain feature contribution enough to debug failures.

## Execution Gates

The model can only recommend a trade if all hard gates pass:

- Prediction age is inside the configured stale-data limit.
- Polymarket book age is inside the configured stale-data limit.
- CEX tick age is inside the configured stale-data limit.
- Entry price is not in known bad tail bands unless historical validation proves otherwise.
- Target notional can be filled at VWAP with enough visible depth.
- Signal survives at least two cycles or has a specific fast-entry exception.
- Seconds-left is inside the validated window.
- The asset/side bucket is currently healthy by recent realized PnL.
- Daily loss and drawdown limits are not breached.

## External BTC Model Integration

If the user's separate BTC predictor is available, integrate it as an optional feature provider, not as an automatic trade trigger.

Required adapter fields:

```json
{
  "timestamp_utc": "2026-06-13T00:00:00Z",
  "asset": "btc",
  "horizon_seconds": 300,
  "prob_up": 0.56,
  "confidence": 0.72,
  "model_version": "external_btc_v1",
  "source": "local_file_or_http"
}
```

Integration rules:

- Reject if timestamp is stale.
- Reject if horizon does not overlap the Polymarket market expiry window.
- Treat it as one feature unless it proves calibrated on Polymarket labels.
- Compare external-only, internal-only, and blended performance.
- Never live trade the external signal until it passes replay and paper validation.

## Data Sources In This Repo

Existing useful tables and panes:

- `book_snapshots`: Polymarket book state.
- `binance_ticks`: CEX price references.
- `complete_set_arb_signals`: complete-set tick history.
- `cex_latency_paper_signals`: directional signal history.
- `cex_latency_paper_positions`: paper entries and exits.
- `cex_latency_paper_events`: open/close events and realized PnL.

Needed additions:

- A feature-builder script that materializes replay-safe rows.
- A label-builder script that joins markets to final resolved winners.
- A model registry table with version, training window, feature set hash, and calibration summary.
- Dashboard panels for calibration and model-vs-baseline PnL.

## Research Sources And What To Extract

| Source | What To Extract | Usefulness |
| --- | --- | --- |
| Polymarket CLOB trading docs | Order signing, auth, order lifecycle, CLOB constraints, primary server region, non-custodial settlement | Defines real execution limits and latency assumptions |
| Polymarket orderbook docs | Full books, prices, spreads, midpoints, fill-price estimation, batch requests, WebSocket market events | Defines correct book features and how to move from polling to real-time data |
| Qlib | ML quant workflow: data processing, model training, backtesting, online serving, supervised learning, regime adaptation | Good architecture template for feature/model/backtest separation |
| VectorBT | Large-scale vectorized parameter sweeps, walk-forward testing, label generation, performance diagnostics | Useful for fast strategy sweeps once feature rows exist |
| Hummingbot | HFT-style crypto bot architecture, exchange connectors, market making/arbitrage patterns | Useful for execution architecture and real-time connector ideas |
| Freqtrade/FreqAI | Dry-run-first workflow, backtesting, ML-assisted optimization, adaptive prediction modeling | Useful for practical bot operations and model validation discipline |
| Prediction-market arbitrage research | Complete-set and dependent-outcome arbitrage framing, why summed outcome probabilities should converge toward 1 | Useful for separating true arb from directional prediction |

The X post `https://x.com/l1vsun/status/2065496274959307069?s=42` was not directly accessible from this environment, and exact-status search did not return indexed contents. If we can get the post text or a repo list from the user, add each repository to the source table and score it against this model contract.

## Ranked Repo Inventory From The Quant-Firm List

The public hedge-fund repositories are mostly infrastructure. They are still useful because our failure mode is not "missing a magic alpha"; it is weak data alignment, weak labeling, weak calibration, and unrealistic execution assumptions.

Highest priority:

- `twosigma/flint`: Use the idea of temporal joins with tolerance. Our feature dataset needs nearest-valid joins across Polymarket books, Binance ticks, market metadata, and resolved outcomes without future leakage.
- `man-group/ArcticDB`: Use as the storage target if SQLite becomes too slow for multi-day tick replay. Do not migrate immediately; first prove the model can make money.
- `man-group/dtale`: Use for fast inspection of feature rows, bad trades, calibration buckets, and PnL by slice.
- `vectorbt`: Use the vectorized parameter-sweep pattern for replaying thresholds, stop/take-profit rules, and model gates over historical rows.
- `hummingbot`: Use the connector/execution architecture ideas, especially separation between market data, strategy logic, order lifecycle, and risk controls.

Medium priority:

- `man-group/notebooker` and `man-group/PyBloqs`: Useful later for scheduled daily model reports.
- `deshaw/versioned-hdf5`: Useful if feature datasets become large and need versioned snapshots.
- `optiver/timestamp9`: Useful if Python timestamp precision becomes a bottleneck in latency analysis.
- `freqtrade` and `FreqAI`: Useful for dry-run discipline, backtest reporting, and ML-prediction lifecycle patterns.

Low priority for this bot:

- Jane Street `core`, `async`, and `hardcaml`: Important infrastructure, but OCaml/FPGA tooling does not directly help the current Python Polymarket paper model.
- HRT `corral` and `slang-server`: Useful for low-latency C++/FPGA shops, not the current prototype.
- D.E. Shaw `pyflyby`, `pjrmi`, and notebook utilities: Developer-productivity tools, not model edge.
- `WorldQuant_alpha101_code`: Good for learning alpha formula style, but equity cross-sectional alphas do not directly transfer to 5-minute binary crypto markets.

Immediate extraction for our POC:

- Adopt Flint-style "as-of join with max age" as a hard design requirement.
- Adopt VectorBT-style parameter sweeps for thresholds.
- Adopt Hummingbot-style separation of feed, strategy, execution, and risk accounting.
- Adopt Freqtrade-style "paper first, then live only after evidence" workflow.

## Implementation Plan

1. Build `scripts/build_cex_feature_dataset.py`.
2. Build `scripts/label_polymarket_outcomes.py`.
3. Add a replay-safe training dataset table or Parquet output.
4. Train baseline logistic regression and gradient-boosted tree models.
5. Add probability calibration.
6. Add a new model mode in `latency_bot/strategy/signals.py`, for example `cex_calibrated_v1`.
7. Run it paper-only beside the existing CEX Latency Paper Bot.
8. Add dashboard panes for calibration, model comparison, and model PnL by bucket.
9. Only consider live trading after multiple-day paper validation.

## Proof-Of-Concept Paper Model

The first POC model mode is `quant_poc`. It is intentionally simple:

- Shrink the CEX-derived fair probability toward `0.50` when the raw signal is weak.
- Blend in Polymarket midpoint so the model does not blindly fight the market.
- Penalize wide spreads, shallow depth, and bad time-to-expiry windows.
- Use stricter paper gates by default: higher minimum edge and no cheap-tail entries below `$0.25`.

This is not the final model. It is a safer paper-trading scaffold that should create cleaner feature/label/PnL data for the real calibrated model.

## Near-Term Parameter Changes To Test

These are hypotheses, not live settings:

- Raise the minimum entry price from `0.15` to at least `0.25` until cheap-tail entries prove profitable.
- Require edge persistence for at least two cycles.
- Split diagnostics by `asset + side + tenor` and disable buckets with negative recent PnL.
- Test lower take-profit/stop-loss asymmetry instead of symmetric `35% / 35%`.
- Use VWAP slippage at target notional for both entry and exit.
- Reject entries where Polymarket book age or Binance tick age is near the max limit.

## Open Questions

- Where is the user's BTC prediction project located?
- Does it output probability, direction, price target, or classification?
- What is its prediction horizon?
- Is it trained on CEX prices only, or does it include Polymarket market-state features?
- Can it run locally with low latency?
- Does it have an out-of-sample calibration report?

## Decision Standard

A model is not ready because it has a clever thesis. It is ready when replay-safe historical tests, paper trading, calibration, and drawdown diagnostics agree that it has durable edge after realistic execution costs.
