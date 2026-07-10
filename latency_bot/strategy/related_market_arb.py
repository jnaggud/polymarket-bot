from __future__ import annotations

from typing import Any

from ..config import LatencyBotSettings


def build_related_market_constraint_graph(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
) -> dict[str, Any]:
    """Build only explicit logical groups; similarity alone never creates an arb."""
    markets = [item for item in markets_payload.get("items", []) if isinstance(item, dict)]
    cache_by_id = {
        str(item.get("market_id") or ""): item
        for item in polymarket_cache.get("items", [])
        if isinstance(item, dict)
    }
    groups: dict[str, list[dict[str, Any]]] = {}
    informational_edges: list[dict[str, Any]] = []
    for market in markets:
        group_id = str(market.get("constraint_group_id") or market.get("event_id") or "").strip()
        if group_id:
            groups.setdefault(group_id, []).append(market)
    by_asset: dict[str, list[dict[str, Any]]] = {}
    for market in markets:
        by_asset.setdefault(str(market.get("asset") or "unknown"), []).append(market)
    for asset, items in by_asset.items():
        ordered = sorted(items, key=lambda item: (str(item.get("expiry_ts") or ""), int(item.get("tenor_minutes") or 0)))
        for left, right in zip(ordered, ordered[1:]):
            informational_edges.append(
                {
                    "source": str(left.get("market_id") or ""),
                    "target": str(right.get("market_id") or ""),
                    "relation": "same_asset_temporal",
                    "asset": asset,
                    "tradable_constraint": False,
                }
            )
    opportunities: list[dict[str, Any]] = []
    logical_edges: list[dict[str, Any]] = []
    for group_id, items in groups.items():
        if len(items) < 2:
            continue
        exhaustive = all(bool(item.get("constraint_exhaustive")) for item in items)
        exclusive = all(bool(item.get("constraint_mutually_exclusive")) for item in items)
        for left, right in zip(items, items[1:]):
            logical_edges.append(
                {
                    "source": str(left.get("market_id") or ""),
                    "target": str(right.get("market_id") or ""),
                    "relation": "mutually_exclusive_exhaustive" if exhaustive and exclusive else "explicit_group_unverified",
                    "group_id": group_id,
                    "tradable_constraint": exhaustive and exclusive,
                }
            )
        if not settings.related_market_arb_enabled or not exhaustive or not exclusive:
            continue
        legs: list[dict[str, Any]] = []
        total_cost = 0.0
        max_notional = max(float(settings.related_market_arb_max_notional_usdc), 0.0)
        for market in items:
            market_id = str(market.get("market_id") or "")
            cache = cache_by_id.get(market_id) or {}
            ask = float(cache.get("ask_vwap") or cache.get("best_ask") or 0.0)
            fillable = float(cache.get("ask_fillable_usdc") or 0.0)
            if ask <= 0.0 or fillable <= 0.0:
                legs = []
                break
            total_cost += ask
            max_notional = min(max_notional, fillable * 0.50)
            legs.append({"market_id": market_id, "outcome": str(market.get("outcome_name") or market.get("question") or ""), "ask": ask})
        edge = 1.0 - total_cost
        if legs and edge >= float(settings.related_market_arb_min_edge_per_share) and max_notional >= 1.0:
            opportunities.append(
                {
                    "group_id": group_id,
                    "legs": legs,
                    "total_cost": round(total_cost, 6),
                    "edge_per_share": round(edge, 6),
                    "max_notional_usdc": round(max_notional, 6),
                    "execution_tier": "research_candidate",
                    "requires_atomic_or_preowned_execution": True,
                }
            )
    return {
        "enabled": bool(settings.related_market_arb_enabled),
        "nodes": [
            {
                "market_id": str(item.get("market_id") or ""),
                "question": str(item.get("question") or ""),
                "asset": str(item.get("asset") or ""),
                "group_id": str(item.get("constraint_group_id") or item.get("event_id") or ""),
            }
            for item in markets
        ],
        "logical_edges": logical_edges,
        "informational_edges": informational_edges,
        "opportunities": opportunities,
        "warning": "Only explicit exhaustive and mutually-exclusive metadata can create a tradable candidate.",
    }
