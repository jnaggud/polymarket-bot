from __future__ import annotations

import math
import sqlite3
from datetime import datetime, timedelta, timezone
from statistics import stdev
from typing import Any, Iterable

from .config import LatencyBotSettings
from .storage import connect_latency_bot_db


TIMEFRAME_DAYS: dict[str, int | None] = {
    "all": None,
    "30d": 30,
    "7d": 7,
    "24h": 1,
}


def _cutoff(days: int | None) -> str | None:
    if days is None:
        return None
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat().replace("+00:00", "Z")


def _drawdown(values: Iterable[float]) -> float:
    running = 0.0
    peak = 0.0
    worst = 0.0
    for value in values:
        running += float(value)
        peak = max(peak, running)
        worst = min(worst, running - peak)
    return round(worst, 6)


def _timeframe_stats(rows: list[dict[str, Any]], cutoff: str | None) -> dict[str, Any]:
    scoped = [row for row in rows if cutoff is None or str(row.get("ts") or "") >= cutoff]
    pnls = [float(row.get("pnl") or 0.0) for row in scoped]
    positive_total = sum(max(value, 0.0) for value in pnls)
    market_pnl: dict[str, float] = {}
    for row, pnl in zip(scoped, pnls):
        market_id = str(row.get("market_id") or "unknown")
        market_pnl[market_id] = market_pnl.get(market_id, 0.0) + pnl
    market_days = {str(row.get("ts") or "")[:10] for row in scoped if str(row.get("ts") or "")}
    lower_95 = 0.0
    if len(pnls) >= 2:
        average = sum(pnls) / len(pnls)
        lower_95 = average - 1.96 * stdev(pnls) / math.sqrt(len(pnls))
    elif pnls:
        lower_95 = pnls[0]
    largest_positive_market = max((max(value, 0.0) for value in market_pnl.values()), default=0.0)
    return {
        "closed": len(scoped),
        "net_pnl_usdc": round(sum(pnls), 6),
        "win_rate": round(sum(value > 0.0 for value in pnls) / len(pnls), 6) if pnls else 0.0,
        "average_trade_pnl_usdc": round(sum(pnls) / len(pnls), 6) if pnls else 0.0,
        "lower_95_trade_ev_usdc": round(lower_95, 6),
        "max_drawdown_usdc": _drawdown(pnls),
        "market_days": len(market_days),
        "unique_markets": len(market_pnl),
        "largest_market_profit_share": round(largest_positive_market / positive_total, 6) if positive_total > 0.0 else 0.0,
    }


def _validation(settings: LatencyBotSettings, tier: str, stats: dict[str, Any], bankroll: float) -> dict[str, Any]:
    reasons: list[str] = []
    if tier not in {"executable_paper", "live_cash"}:
        reasons.append("execution tier is not eligible for live promotion")
    if int(stats.get("closed") or 0) < int(settings.validation_min_closed_trades):
        reasons.append(f"needs {settings.validation_min_closed_trades}+ resolved trades")
    if int(stats.get("market_days") or 0) < int(settings.validation_min_market_days):
        reasons.append(f"needs {settings.validation_min_market_days}+ market days")
    if float(stats.get("net_pnl_usdc") or 0.0) <= 0.0:
        reasons.append("after-cost PnL is not positive")
    drawdown_limit = max(float(bankroll), 0.0) * max(float(settings.validation_max_drawdown_fraction), 0.0)
    if abs(min(float(stats.get("max_drawdown_usdc") or 0.0), 0.0)) > drawdown_limit:
        reasons.append(f"drawdown exceeds {100.0 * settings.validation_max_drawdown_fraction:.1f}% of bankroll")
    if float(stats.get("largest_market_profit_share") or 0.0) > float(settings.validation_max_single_market_profit_share):
        reasons.append(f"one market contributes more than {100.0 * settings.validation_max_single_market_profit_share:.1f}% of positive PnL")
    if float(stats.get("lower_95_trade_ev_usdc") or 0.0) <= 0.0:
        reasons.append("lower 95% trade-EV bound is not positive")
    return {
        "status": "passed" if not reasons else "blocked",
        "passed": not reasons,
        "reasons": reasons,
    }


def _query_rows(conn: sqlite3.Connection, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _event_rows(
    conn: sqlite3.Connection,
    kind: str,
    mode: str = "",
    execution_model: str = "",
) -> tuple[list[dict[str, Any]], int]:
    if kind == "position":
        rows = _query_rows(
            conn,
            """
            SELECT e.ts, e.pnl, p.market_id
            FROM position_events e
            JOIN positions p ON p.position_id = e.position_id
            WHERE e.event_type = 'close' AND p.mode = ?
              AND COALESCE(p.execution_model, 'legacy') = ?
            ORDER BY e.ts, e.id
            """,
            (mode, execution_model or "legacy"),
        )
        open_count = int(conn.execute("SELECT COUNT(*) FROM positions WHERE status = 'open' AND mode = ? AND COALESCE(execution_model, 'legacy') = ?", (mode, execution_model or "legacy")).fetchone()[0])
        return rows, open_count
    if kind == "position_legacy_promoted":
        rows = _query_rows(
            conn,
            """
            SELECT e.ts, e.pnl, p.market_id
            FROM position_events e
            JOIN positions p ON p.position_id = e.position_id
            WHERE e.event_type = 'close'
              AND p.mode LIKE 'promoted_variant:%'
              AND COALESCE(p.execution_model, 'legacy') = 'legacy'
            ORDER BY e.ts, e.id
            """,
        )
        open_count = int(conn.execute("SELECT COUNT(*) FROM positions WHERE status = 'open' AND mode LIKE 'promoted_variant:%' AND COALESCE(execution_model, 'legacy') = 'legacy'").fetchone()[0])
        return rows, open_count
    if kind == "position_promoted_v1":
        modes = [item for item in mode.split(",") if item]
        if not modes:
            return [], 0
        placeholders = ", ".join("?" for _ in modes)
        rows = _query_rows(
            conn,
            f"""
            SELECT e.ts, e.pnl, p.market_id
            FROM position_events e
            JOIN positions p ON p.position_id = e.position_id
            WHERE e.event_type = 'close'
              AND p.mode IN ({placeholders})
              AND COALESCE(p.execution_model, 'legacy') = 'vwap_latency_partial_fill_v1'
            ORDER BY e.ts, e.id
            """,
            tuple(modes),
        )
        open_count = int(
            conn.execute(
                f"SELECT COUNT(*) FROM positions WHERE status = 'open' AND mode IN ({placeholders}) AND COALESCE(execution_model, 'legacy') = 'vwap_latency_partial_fill_v1'",
                tuple(modes),
            ).fetchone()[0]
        )
        return rows, open_count
    if kind == "cex":
        rows = _query_rows(
            conn,
            """
            SELECT e.ts, e.pnl, p.market_id
            FROM cex_latency_paper_events e
            JOIN cex_latency_paper_positions p ON p.position_id = e.position_id
            WHERE e.event_type = 'close' AND p.mode = ?
            ORDER BY e.ts, e.id
            """,
            (mode,),
        )
        open_count = int(conn.execute("SELECT COUNT(*) FROM cex_latency_paper_positions WHERE status = 'open' AND mode = ?", (mode,)).fetchone()[0])
        return rows, open_count
    if kind == "complete_set":
        rows = _query_rows(
            conn,
            """
            SELECT e.ts, e.pnl, p.market_id
            FROM complete_set_arb_events e
            JOIN complete_set_arb_positions p ON p.position_id = e.position_id
            WHERE e.event_type = 'close'
            ORDER BY e.ts, e.id
            """,
        )
        open_count = int(conn.execute("SELECT COUNT(*) FROM complete_set_arb_positions WHERE status = 'open'").fetchone()[0])
        return rows, open_count
    if kind == "late":
        rows = _query_rows(
            conn,
            """
            SELECT e.ts, e.pnl, p.market_id
            FROM late_resolution_capture_events e
            JOIN late_resolution_capture_positions p ON p.position_id = e.position_id
            WHERE e.event_type = 'close'
            ORDER BY e.ts, e.id
            """,
        )
        open_count = int(conn.execute("SELECT COUNT(*) FROM late_resolution_capture_positions WHERE status = 'open'").fetchone()[0])
        return rows, open_count
    if kind == "temporal":
        epoch_row = conn.execute(
            "SELECT started_at FROM strategy_epochs WHERE strategy_id = 'temporal_inventory_maker'"
        ).fetchone()
        epoch_start = str(epoch_row["started_at"] or "") if epoch_row is not None else ""
        rows = _query_rows(
            conn,
            """
            SELECT e.ts, COALESCE(e.pnl_usdc, 0.0) AS pnl, e.market_id
            FROM temporal_inventory_events e
            JOIN temporal_inventory_markets m ON m.market_id = e.market_id
            WHERE e.event_type IN ('SELL', 'EXPIRE', 'RESOLVE')
              AND e.ts >= ?
              AND COALESCE(m.execution_model, 'legacy') = 'queue_aware_passive_v1'
            ORDER BY e.ts, e.id
            """,
            (epoch_start,),
        )
        open_count = int(conn.execute("SELECT COUNT(*) FROM temporal_inventory_markets WHERE state != 'CLOSED' AND COALESCE(execution_model, 'legacy') = 'queue_aware_passive_v1'").fetchone()[0])
        return rows, open_count
    if kind == "temporal_legacy":
        rows = _query_rows(
            conn,
            """
            SELECT e.ts, COALESCE(e.pnl_usdc, 0.0) AS pnl, e.market_id
            FROM temporal_inventory_events e
            JOIN temporal_inventory_markets m ON m.market_id = e.market_id
            WHERE e.event_type IN ('SELL', 'EXPIRE', 'RESOLVE')
              AND COALESCE(m.execution_model, 'legacy') = 'legacy'
            ORDER BY e.ts, e.id
            """,
        )
        return rows, 0
    return [], 0


def build_strategy_truth_rows(settings: LatencyBotSettings) -> list[dict[str, Any]]:
    definitions: list[dict[str, Any]] = []
    definitions.append(
        {
            "strategy_id": "promoted:legacy_family",
            "label": "Promoted variants — legacy fill model",
            "kind": "position_legacy_promoted",
            "mode": "",
            "execution_model": "legacy",
            "execution_tier": "optimistic_simulation",
            "enabled": False,
            "bankroll": settings.bankroll_usdc,
            "assumptions": "historical top-of-book paper fills; preserved but never promotion eligible",
        }
    )
    definitions.append(
        {
            "strategy_id": "promoted:directional_basket:v1",
            "label": "Promoted ETH/BTC 5m directional basket",
            "kind": "position_promoted_v1",
            "mode": ",".join(f"promoted_variant:{variant_id}" for variant_id in settings.promoted_variant_ids),
            "execution_model": "vwap_latency_partial_fill_v1",
            "execution_tier": "executable_paper",
            "enabled": bool(settings.promoted_variant_ids),
            "member_count": len(settings.promoted_variant_ids),
            "bankroll": settings.bankroll_usdc,
            "assumptions": "Aggregate of the promoted variants using VWAP book walk, latency/slippage/fees, and partial-fill haircuts",
        }
    )
    for variant_id in settings.promoted_variant_ids:
        definitions.append(
            {
                "strategy_id": f"promoted:{variant_id}:v1",
                "label": f"{variant_id} — realistic v1",
                "kind": "position",
                "mode": f"promoted_variant:{variant_id}",
                "execution_model": "vwap_latency_partial_fill_v1",
                "execution_tier": "executable_paper",
                "enabled": True,
                "parent_strategy_id": "promoted:directional_basket:v1",
                "bankroll": settings.bankroll_usdc,
                "assumptions": "VWAP book walk, latency/slippage/fees, partial-fill haircut",
            }
        )
    definitions.extend(
        [
            {"strategy_id": "temporal_inventory_maker", "label": "Temporal inventory maker — queue-aware v1", "kind": "temporal", "mode": "", "execution_tier": "executable_paper", "enabled": settings.temporal_inventory_maker_paper_enabled, "bankroll": settings.temporal_inventory_maker_paper_capital_usdc, "assumptions": "passive post-only queue model with inventory skew; actual rebates only"},
            {"strategy_id": "temporal_inventory_maker:legacy", "label": "Temporal inventory maker — legacy fills", "kind": "temporal_legacy", "mode": "", "execution_tier": "optimistic_simulation", "enabled": False, "bankroll": settings.temporal_inventory_maker_paper_capital_usdc, "assumptions": "pre-queue-aware historical paper epoch; never promotion eligible"},
            {"strategy_id": "complete_set_arb", "label": "Complete-set arb", "kind": "complete_set", "mode": "", "execution_tier": "optimistic_simulation", "enabled": settings.complete_set_arb_enabled, "bankroll": settings.bankroll_usdc, "assumptions": "non-atomic research simulation; excluded from promotion"},
            {"strategy_id": "late_resolution_capture", "label": "Late-resolution capture", "kind": "late", "mode": "", "execution_tier": "executable_paper", "enabled": settings.late_resolution_capture_paper_enabled, "bankroll": settings.late_resolution_capture_paper_capital_usdc, "assumptions": "paper bid exit with fees/slippage"},
            {"strategy_id": "cex_latency", "label": "CEX latency", "kind": "cex", "mode": "cex_latency_paper", "execution_tier": "executable_paper", "enabled": settings.cex_latency_paper_enabled, "bankroll": settings.cex_latency_paper_capital_usdc, "assumptions": "paper VWAP execution"},
            {"strategy_id": "btc_fair_value", "label": "BTC fair value", "kind": "cex", "mode": "btc_fair_value_paper", "execution_tier": "executable_paper", "enabled": settings.btc_fair_value_paper_enabled, "bankroll": settings.btc_fair_value_paper_capital_usdc, "assumptions": "paper VWAP execution"},
            {"strategy_id": "wallet_teacher", "label": "Wallet teacher", "kind": "cex", "mode": "wallet_teacher_sniper", "execution_tier": "shadow", "enabled": settings.wallet_teacher_sniper_enabled, "bankroll": settings.wallet_teacher_sniper_capital_usdc, "assumptions": "research-only copied fill signal"},
        ]
    )
    output: list[dict[str, Any]] = []
    with connect_latency_bot_db(settings) as conn:
        rebate_by_strategy = {
            str(row["strategy_id"]): float(row["rebate"] or 0.0)
            for row in conn.execute(
                "SELECT strategy_id, SUM(rebate_usdc) AS rebate FROM maker_rebate_events GROUP BY strategy_id"
            ).fetchall()
        }
        for definition in definitions:
            rows, open_count = _event_rows(
                conn,
                str(definition["kind"]),
                str(definition.get("mode") or ""),
                str(definition.get("execution_model") or ""),
            )
            timeframes = {name: _timeframe_stats(rows, _cutoff(days)) for name, days in TIMEFRAME_DAYS.items()}
            all_stats = timeframes["all"]
            tier = str(definition["execution_tier"])
            net_pnl = float(all_stats["net_pnl_usdc"])
            row = {
                **definition,
                "open": open_count,
                "data_status": "loaded" if rows or open_count else "no_observations",
                "last_event_ts": str(rows[-1].get("ts") or "") if rows else "",
                "timeframes": timeframes,
                "model_pnl_usdc": net_pnl if tier in {"shadow", "optimistic_simulation"} else None,
                "executable_paper_pnl_usdc": net_pnl if tier == "executable_paper" else None,
                "rebate_pnl_usdc": round(rebate_by_strategy.get(str(definition["strategy_id"]), 0.0), 6),
                "wallet_pnl_usdc": None,
                "wallet_reconciled": False,
            }
            row["validation"] = _validation(settings, tier, all_stats, float(definition["bankroll"]))
            output.append(row)
    output.sort(
        key=lambda item: (
            bool(item.get("validation", {}).get("passed")),
            float(item.get("timeframes", {}).get("all", {}).get("net_pnl_usdc") or 0.0),
        ),
        reverse=True,
    )
    return output


def strategy_validation_gate(settings: LatencyBotSettings, strategy_id: str) -> dict[str, Any]:
    for row in build_strategy_truth_rows(settings):
        if str(row.get("strategy_id") or "") == strategy_id:
            return dict(row.get("validation") or {})
    return {"status": "blocked", "passed": False, "reasons": ["strategy is missing from the truth registry"]}
