from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Iterable


def _number(record: dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        value = float(record.get(key, default) or 0.0)
    except (TypeError, ValueError):
        return default
    return value if math.isfinite(value) else default


def _max_drawdown(pnls: Iterable[float]) -> float:
    equity = 0.0
    peak = 0.0
    drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return round(drawdown, 6)


def _performance(records: list[dict[str, Any]], pnl_key: str) -> dict[str, Any]:
    pnls = [_number(record, pnl_key) for record in records]
    wins = sum(1 for pnl in pnls if pnl > 0)
    return {
        "trades": len(records),
        "net_pnl_usdc": round(sum(pnls), 6),
        "mean_pnl_usdc": round(sum(pnls) / len(pnls), 6) if pnls else 0.0,
        "win_rate": round(wins / len(pnls), 6) if pnls else 0.0,
        "max_drawdown_usdc": _max_drawdown(pnls),
    }


def walk_forward_report(
    records: list[dict[str, Any]],
    *,
    minimum_train_trades: int = 6,
    test_trades: int = 3,
) -> dict[str, Any]:
    if minimum_train_trades < 1 or test_trades < 1:
        raise ValueError("walk-forward window sizes must be positive")
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(str(record.get("strategy_id") or "unknown"), []).append(record)
    strategy_reports: list[dict[str, Any]] = []
    for strategy_id, strategy_records in sorted(grouped.items()):
        ordered = sorted(strategy_records, key=lambda item: str(item.get("closed_at") or ""))
        folds: list[dict[str, Any]] = []
        train_end = minimum_train_trades
        while train_end + test_trades <= len(ordered):
            train = ordered[:train_end]
            test = ordered[train_end : train_end + test_trades]
            strategy = _performance(test, "gross_pnl_usdc")
            baseline = _performance(test, "baseline_pnl_usdc")
            folds.append(
                {
                    "train_trades": len(train),
                    "test_start": str(test[0].get("closed_at") or ""),
                    "test_end": str(test[-1].get("closed_at") or ""),
                    "strategy": strategy,
                    "market_implied_baseline": baseline,
                    "excess_pnl_usdc": round(strategy["net_pnl_usdc"] - baseline["net_pnl_usdc"], 6),
                    "profitable": strategy["net_pnl_usdc"] > 0,
                    "beats_baseline": strategy["net_pnl_usdc"] > baseline["net_pnl_usdc"],
                }
            )
            train_end += test_trades
        strategy_reports.append(
            {
                "strategy_id": strategy_id,
                "available_trades": len(ordered),
                "folds": folds,
                "profitable_folds": sum(1 for fold in folds if fold["profitable"]),
                "baseline_beating_folds": sum(1 for fold in folds if fold["beats_baseline"]),
                "passes": bool(folds) and all(fold["profitable"] and fold["beats_baseline"] for fold in folds),
            }
        )
    return {
        "minimum_train_trades": minimum_train_trades,
        "test_trades": test_trades,
        "strategies": strategy_reports,
        "passes": bool(strategy_reports) and all(report["passes"] for report in strategy_reports),
    }


SENSITIVITY_SCENARIOS = (
    {"name": "favorable", "fee_bps": 0.0, "latency_ms": 250.0, "depth_haircut": 0.80, "partial_fill_fraction": 0.90},
    {"name": "base", "fee_bps": 5.0, "latency_ms": 750.0, "depth_haircut": 0.50, "partial_fill_fraction": 0.50},
    {"name": "stressed", "fee_bps": 15.0, "latency_ms": 1500.0, "depth_haircut": 0.25, "partial_fill_fraction": 0.25},
)


def sensitivity_report(records: list[dict[str, Any]]) -> dict[str, Any]:
    scenarios: list[dict[str, Any]] = []
    for scenario in SENSITIVITY_SCENARIOS:
        adjusted: list[float] = []
        total_fill_ratio = 0.0
        for record in records:
            requested = max(_number(record, "requested_notional_usdc", _number(record, "notional_usdc")), 0.000001)
            available = max(_number(record, "available_depth_usdc", requested), 0.0)
            depth_ratio = min(1.0, (available * float(scenario["depth_haircut"])) / requested)
            fill_ratio = min(depth_ratio, float(scenario["partial_fill_fraction"]))
            total_fill_ratio += fill_ratio
            notional = max(_number(record, "notional_usdc", requested), 0.0)
            scenario_fee = notional * float(scenario["fee_bps"]) / 10_000.0
            observed_latency = _number(record, "observed_latency_ms")
            extra_latency_seconds = max(float(scenario["latency_ms"]) - observed_latency, 0.0) / 1000.0
            latency_penalty = notional * 0.0025 * extra_latency_seconds
            adjusted.append(_number(record, "gross_pnl_usdc") * fill_ratio - scenario_fee - latency_penalty)
        scenarios.append(
            {
                **scenario,
                "trades": len(records),
                "mean_fill_ratio": round(total_fill_ratio / len(records), 6) if records else 0.0,
                "net_pnl_usdc": round(sum(adjusted), 6),
                "mean_pnl_usdc": round(sum(adjusted) / len(adjusted), 6) if adjusted else 0.0,
                "max_drawdown_usdc": _max_drawdown(adjusted),
                "profitable": sum(adjusted) > 0,
            }
        )
    base = next(item for item in scenarios if item["name"] == "base")
    stressed = next(item for item in scenarios if item["name"] == "stressed")
    return {"scenarios": scenarios, "base_profitable": base["profitable"], "stressed_profitable": stressed["profitable"]}


def baseline_report(records: list[dict[str, Any]]) -> dict[str, Any]:
    strategy = _performance(records, "gross_pnl_usdc")
    baseline = _performance(records, "baseline_pnl_usdc")
    no_trade = {"trades": 0, "net_pnl_usdc": 0.0, "mean_pnl_usdc": 0.0, "win_rate": 0.0, "max_drawdown_usdc": 0.0}
    return {
        "strategy": strategy,
        "market_implied_baseline": baseline,
        "no_trade_baseline": no_trade,
        "excess_vs_market_implied_usdc": round(strategy["net_pnl_usdc"] - baseline["net_pnl_usdc"], 6),
        "beats_market_implied": strategy["net_pnl_usdc"] > baseline["net_pnl_usdc"],
        "beats_no_trade": strategy["net_pnl_usdc"] > 0,
    }


def queue_calibration_report(records: list[dict[str, Any]]) -> dict[str, Any]:
    observations: list[tuple[float, int]] = []
    for record in records:
        if "predicted_fill_probability" not in record or "observed_fill" not in record:
            continue
        predicted = min(max(_number(record, "predicted_fill_probability"), 0.0), 1.0)
        observed = 1 if _number(record, "observed_fill") >= 0.5 else 0
        observations.append((predicted, observed))
    bins: list[dict[str, Any]] = []
    for lower in (0.0, 0.2, 0.4, 0.6, 0.8):
        upper = lower + 0.2
        bucket = [
            (predicted, observed)
            for predicted, observed in observations
            if (lower <= predicted <= upper if upper == 1.0 else lower <= predicted < upper)
        ]
        if not bucket:
            continue
        bins.append(
            {
                "range": f"{lower:.1f}-{upper:.1f}",
                "count": len(bucket),
                "mean_predicted": round(sum(item[0] for item in bucket) / len(bucket), 6),
                "observed_fill_rate": round(sum(item[1] for item in bucket) / len(bucket), 6),
            }
        )
    brier = sum((predicted - observed) ** 2 for predicted, observed in observations) / len(observations) if observations else 0.0
    calibration_error = (
        sum(abs(item["mean_predicted"] - item["observed_fill_rate"]) * item["count"] for item in bins) / len(observations)
        if observations
        else 0.0
    )
    return {
        "observations": len(observations),
        "brier_score": round(brier, 6),
        "expected_calibration_error": round(calibration_error, 6),
        "bins": bins,
        "passes": len(observations) >= 20 and brier <= 0.25 and calibration_error <= 0.15,
    }


def fill_reconciliation_report(records: list[dict[str, Any]]) -> dict[str, Any]:
    matched = [
        record
        for record in records
        if record.get("independent_fill_price") is not None and _number(record, "independent_fill_size") > 0
    ]
    price_errors = [abs(_number(record, "paper_fill_price") - _number(record, "independent_fill_price")) for record in matched]
    size_ratios = [
        min(_number(record, "independent_fill_size") / max(_number(record, "paper_fill_size"), 0.000001), 1.0)
        for record in matched
    ]
    paper_fills = sum(1 for record in records if _number(record, "paper_fill_size") > 0)
    return {
        "paper_fills": paper_fills,
        "independently_matched_fills": len(matched),
        "match_rate": round(len(matched) / paper_fills, 6) if paper_fills else 0.0,
        "price_mae": round(sum(price_errors) / len(price_errors), 6) if price_errors else None,
        "mean_size_coverage": round(sum(size_ratios) / len(size_ratios), 6) if size_ratios else 0.0,
        "passes": len(matched) >= 20 and bool(price_errors) and (sum(price_errors) / len(price_errors)) <= 0.01 and (sum(size_ratios) / len(size_ratios)) >= 0.80,
    }


def build_validation_report(records: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(records, key=lambda item: (str(item.get("strategy_id") or ""), str(item.get("closed_at") or "")))
    walk_forward = walk_forward_report(ordered)
    sensitivity = sensitivity_report(ordered)
    baselines = baseline_report(ordered)
    queue = queue_calibration_report(ordered)
    reconciliation = fill_reconciliation_report(ordered)
    gates = {
        "walk_forward": walk_forward["passes"],
        "base_cost_sensitivity": sensitivity["base_profitable"],
        "baseline_comparison": baselines["beats_market_implied"] and baselines["beats_no_trade"],
        "queue_calibration": queue["passes"],
        "independent_fill_reconciliation": reconciliation["passes"],
    }
    return {
        "record_count": len(ordered),
        "walk_forward": walk_forward,
        "sensitivity": sensitivity,
        "baselines": baselines,
        "queue_calibration": queue,
        "fill_reconciliation": reconciliation,
        "gates": gates,
        "live_pilot_eligible": bool(gates) and all(gates.values()),
        "disclaimer": "Research validation only; passing these gates does not guarantee future profitability.",
    }


def load_validation_records(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload.get("records") if isinstance(payload, dict) else payload
    if not isinstance(records, list) or not all(isinstance(item, dict) for item in records):
        raise ValueError("validation input must be a JSON list or an object containing a records list")
    return records


def write_validation_report(input_path: Path, output_path: Path | None = None) -> dict[str, Any]:
    report = build_validation_report(load_validation_records(input_path))
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report
