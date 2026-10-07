# Engineering Case Study

## Problem

Prediction-market prototypes often report attractive simulated PnL while assuming fills at the displayed top of
book. That assumption ignores latency, available depth, queue priority, partial execution, fees, slippage, adverse
selection, and the operational risk of multi-leg trades.

The project needed a research workflow that could explore strategies without promoting optimistic model output as
live-ready performance.

## Approach

The system was split into four evidence-producing layers:

1. **Discovery and modeling** produce candidates and explicit probability or constraint signals.
2. **Execution simulation** applies venue-shaped assumptions to each candidate.
3. **Event accounting** records orders, fills, positions, lifecycle events, rebates, and reconciliation state.
4. **Strategy Truth** reports only the PnL supported by each strategy's execution tier and applies repeatable
   promotion gates.

The dashboard consumes this evidence rather than calculating an independent version of performance.

## Notable design decisions

### Separate epochs after execution changes

When promoted directional strategies moved from top-of-book fills to a VWAP/latency/partial-fill model, historical
results were archived rather than reused. The realistic epoch began with zero observations.

### Model queue-aware maker behavior

Passive orders track queue-ahead assumptions, quote TTLs, fill probability, inventory skew, adverse selection, and
forced exits. Maker rebates count only when they are observed, not when they are merely expected.

### Treat live connectivity as a constrained subsystem

Live-capable code is behind multiple independent gates: disabled defaults, dry-run mode, confirmation values,
geographic eligibility, reconciliation, heartbeat, daily loss, exposure, cooldown, stale-book checks, and validated
paper performance.

### Make failure visible

The operations cockpit surfaces lifecycle events, blocked decisions, active versus archived strategies, execution
assumptions, and validation reasons. A no-trade result remains visible instead of being silently discarded.

### Make operational data bounded and reviewable

High-frequency observations can outgrow a single-machine research ledger. The maintenance workflow inventories the
database without scanning every row, previews retention through indexed timestamps, archives selected raw records to
compressed JSON Lines, and deletes only bounded batches after an explicit confirmation. Trading and accounting
evidence is outside the default retention boundary.

### Turn strategy promotion into a repeatable report

The validation report combines expanding-window out-of-sample folds, market-implied and no-trade baselines,
fee/latency/depth/partial-fill stress bands, queue-probability calibration, and reconciliation against independently
captured fills. A strategy remains paper-only when any evidence gate lacks enough observations or fails its threshold.

### Provide a reviewer-safe walkthrough

The demo generator creates an isolated SQLite ledger, market caches, status payload, and validation input with no
credentials and no live orders. A reviewer can launch the full dashboard locally and reproduce the report from one
documented command sequence.

## Verification

- 152 deterministic tests cover research accounting, strategy selection, market lifecycles, execution models, live
  preflight, validation gates, retention safety, demo generation, and dashboard rendering.
- CI runs lint, command-line smoke checks, and the test suite on Python 3.10–3.13.
- Tests treat deprecation and resource warnings as errors.
- The committed environment template contains placeholders only; state, logs, databases, and secrets are ignored.

## Outcome

The result is an inspectable, paper-first research platform that demonstrates Python systems design, event-driven
accounting, SQLite data modeling, trading-risk controls, API integration, test engineering, and operational UI work.
It is intentionally not presented as evidence of live profitability. The deterministic showcase dataset validates
the reporting path, not a trading edge; live-pilot eligibility remains false whenever calibration, reconciliation,
sample-size, or risk gates fail.
