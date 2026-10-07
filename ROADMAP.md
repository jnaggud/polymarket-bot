# Roadmap

This project is deliberately paper-first. Roadmap items are ordered by the confidence they add to the research
process, not by how quickly they enable live order placement.

## Validation and risk

- Move every portfolio cap to current mark-to-market equity rather than compounded realized bankroll.
- Extend forced de-risk rules and hard-stop coverage across every strategy family.
- Add event-family concentration caps for correlated politics, crypto-launch, and valuation ladders.
- Add walk-forward validation reports with fee, latency, depth, and partial-fill sensitivity bands.
- Reconcile paper fills against independently captured order-book snapshots.

## Execution research

- Calibrate the queue-ahead model with observed maker-fill data.
- Model multi-leg execution atomically where the venue supports it and pessimistically where it does not.
- Add deterministic replay fixtures for market-data outages, crossed books, stale clocks, and partial fills.
- Compare promoted directional variants against simple market-implied and no-trade baselines.

## Operations

- Export structured metrics for long-running engine health and strategy drift.
- Add database retention and migration tooling.
- Package a credential-free demonstration dataset for reproducible dashboard walkthroughs.

None of these items should weaken the current live-trading defaults: disabled, dry-run, geoblock-aware, and gated by
minimum sample size, market-day coverage, drawdown, concentration, and lower-confidence-bound requirements.
