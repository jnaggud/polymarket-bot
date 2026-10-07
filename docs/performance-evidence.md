# Performance Evidence Policy

This project separates software capability from trading-performance claims. A green test suite proves that the
implemented behaviors are reproducible; it does not prove that a strategy has durable economic edge.

## Evidence tiers

| Evidence | What it can support | What it cannot support |
| --- | --- | --- |
| Deterministic demo | Dashboard, accounting, report, and gate behavior | Historical or future profitability |
| Optimistic simulation | Research comparison under declared assumptions | Executable fills or deployable returns |
| Shadow observation | Signal frequency and market coverage | Fill quality or realized PnL |
| Executable paper | After-cost results under explicit fill assumptions | Cash returns or production reliability |
| Reconciled live cash | Observed wallet and venue outcomes for the measured period | Future returns or generalization |

## Public claim rules

- Demo PnL must be labeled synthetic or deterministic.
- Paper PnL must name the execution model, fees, latency, depth, and partial-fill assumptions.
- Historical results must remain associated with the execution epoch that produced them.
- A live-readiness statement requires every configured validation, calibration, reconciliation, concentration, and
  risk gate to pass.
- No result is described as guaranteed, risk-free, or predictive of future performance.

The credential-free sample dataset intentionally exercises both passing and failing report sections. Its current
queue-calibration and independent-fill-reconciliation gates fail, so the generated report correctly declares the
sample ineligible for a live pilot.
