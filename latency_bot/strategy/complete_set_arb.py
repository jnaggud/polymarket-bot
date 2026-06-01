from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ..config import LatencyBotSettings


def _seconds_remaining(item: dict[str, Any]) -> float:
    raw = str(item.get("expiry_ts") or "")
    if not raw:
        return 0.0
    text = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        expiry = datetime.fromisoformat(text)
    except ValueError:
        return 0.0
    if expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=timezone.utc)
    return max((expiry.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds(), 0.0)


def build_complete_set_arb_signals(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
) -> list[dict[str, Any]]:
    markets = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    market_by_id = {str(item.get("market_id") or ""): item for item in markets if isinstance(item, dict)}
    cache_items = polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else []
    results: list[dict[str, Any]] = []
    for cache in cache_items:
        if not isinstance(cache, dict):
            continue
        market_id = str(cache.get("market_id") or "")
        market = market_by_id.get(market_id)
        if market is None:
            continue
        yes_ask = float(cache.get("best_ask") or 0.0)
        no_ask = float(cache.get("no_best_ask") or 0.0)
        if no_ask <= 0.0:
            no_ask = max(1.0 - float(cache.get("best_bid") or 0.0), 0.0)
        yes_depth = float(cache.get("asks_depth_usdc") or 0.0)
        no_depth = float(cache.get("no_asks_depth_usdc") or 0.0)
        if no_depth <= 0.0:
            no_depth = float(cache.get("bids_depth_usdc") or 0.0)
        book_age_ms = float(cache.get("book_age_ms") or 0.0)
        seconds_left = _seconds_remaining(market)
        total_cost = yes_ask + no_ask
        gross_edge = 1.0 - total_cost
        net_edge = gross_edge - settings.complete_set_arb_slippage_per_share
        executable_depth_usdc = min(yes_depth, no_depth)
        signal = {
            "market_id": market_id,
            "asset": str(market.get("asset") or cache.get("asset") or "").lower(),
            "tenor_minutes": int(market.get("tenor_minutes") or 0),
            "yes_ask": round(yes_ask, 6),
            "no_ask": round(no_ask, 6),
            "total_cost": round(total_cost, 6),
            "gross_edge": round(gross_edge, 6),
            "net_edge": round(net_edge, 6),
            "yes_depth_usdc": round(yes_depth, 6),
            "no_depth_usdc": round(no_depth, 6),
            "executable_depth_usdc": round(executable_depth_usdc, 6),
            "book_age_ms": round(book_age_ms, 3),
            "seconds_left": round(seconds_left, 3),
            "eligible": False,
            "reason": "complete set cost too high",
        }
        if not settings.complete_set_arb_enabled:
            signal["reason"] = "complete set arb disabled"
        elif yes_ask <= 0.0 or no_ask <= 0.0:
            signal["reason"] = "missing yes/no ask"
        elif book_age_ms > settings.complete_set_arb_max_book_age_ms:
            signal["reason"] = "book snapshot too stale"
        elif executable_depth_usdc < settings.complete_set_arb_min_depth_usdc:
            signal["reason"] = "insufficient paired depth"
        elif seconds_left < settings.complete_set_arb_min_seconds_left:
            signal["reason"] = "too close to expiry"
        elif settings.complete_set_arb_max_seconds_left > 0 and seconds_left > settings.complete_set_arb_max_seconds_left:
            signal["reason"] = "too far from expiry"
        elif net_edge < settings.complete_set_arb_min_profit_per_share:
            signal["reason"] = "complete set net edge below threshold"
        else:
            signal.update({"eligible": True, "reason": "complete set net edge clears threshold"})
        results.append(signal)
    results.sort(key=lambda item: (bool(item.get("eligible")), float(item.get("net_edge") or 0.0)), reverse=True)
    return results
