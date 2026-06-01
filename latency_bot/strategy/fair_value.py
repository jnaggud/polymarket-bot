from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from bot.core import _extract_intraday_question_window

from ..config import LatencyBotSettings


def _iso_to_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def realized_volatility(prices: list[float], *, floor: float) -> float:
    clean = [float(price) for price in prices if float(price) > 0.0]
    if len(clean) < 2:
        return floor
    returns: list[float] = []
    for previous, current in zip(clean, clean[1:]):
        if previous <= 0.0 or current <= 0.0:
            continue
        returns.append(math.log(current / previous))
    if len(returns) < 2:
        return floor
    mean = sum(returns) / len(returns)
    variance = sum((value - mean) ** 2 for value in returns) / max(len(returns) - 1, 1)
    return max(math.sqrt(max(variance, 0.0)), floor)


def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def compute_updown_fair_yes(
    *,
    reference_price: float,
    current_price: float,
    return_volatility: float,
    seconds_remaining: float,
    floor: float,
) -> float:
    if reference_price <= 0.0 or current_price <= 0.0:
        return 0.5
    if seconds_remaining <= 0.0:
        return 1.0 if current_price > reference_price else 0.0
    sigma = max(return_volatility, floor) * math.sqrt(max(seconds_remaining, 1.0))
    if sigma <= 0.0:
        return 1.0 if current_price > reference_price else 0.0
    z = math.log(reference_price / current_price) / sigma
    return min(max(1.0 - _normal_cdf(z), 0.0), 1.0)


def _sample_mid_near(samples: list[dict[str, Any]], target: datetime, *, max_age_seconds: int) -> float | None:
    best_mid: float | None = None
    best_age: float | None = None
    for sample in samples:
        if not isinstance(sample, dict):
            continue
        seen_at = _iso_to_dt(str(sample.get("seen_at") or ""))
        mid = float(sample.get("mid") or 0.0)
        if seen_at is None or mid <= 0.0:
            continue
        age = abs((seen_at - target).total_seconds())
        if age <= max_age_seconds and (best_age is None or age < best_age):
            best_age = age
            best_mid = mid
    return best_mid


def build_fair_values(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
    binance_cache: dict[str, Any],
) -> list[dict[str, Any]]:
    markets = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    market_by_id = {str(item.get("market_id") or ""): item for item in markets if isinstance(item, dict)}
    cache_items = polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else []
    binance_items = binance_cache.get("items", []) if isinstance(binance_cache.get("items"), list) else []
    binance_by_asset = {str(item.get("asset") or "").lower(): item for item in binance_items if isinstance(item, dict)}
    now = datetime.now(timezone.utc)
    results: list[dict[str, Any]] = []
    for entry in cache_items:
        if not isinstance(entry, dict):
            continue
        market_id = str(entry.get("market_id") or "")
        market = market_by_id.get(market_id)
        if market is None:
            continue
        asset = str(entry.get("asset") or market.get("asset") or "").lower()
        binance = binance_by_asset.get(asset)
        if binance is None:
            continue
        samples = binance.get("samples", []) if isinstance(binance.get("samples"), list) else []
        prices = [float(sample.get("mid") or 0.0) for sample in samples if isinstance(sample, dict)]
        if len(prices) < settings.min_binance_observations:
            continue
        current_price = float(binance.get("mid") or 0.0)
        reference_price = float(prices[0] or 0.0)
        question = str(market.get("question") or "")
        window = _extract_intraday_question_window(question)
        if window is None:
            continue
        start_ts, end_ts = window
        seconds_remaining = max((end_ts.astimezone(timezone.utc) - now).total_seconds(), 0.0)
        vol = realized_volatility(prices, floor=settings.min_volatility_floor)
        if now < start_ts.astimezone(timezone.utc):
            reference_price = current_price
            fair_yes = 0.5
        else:
            start_reference = _sample_mid_near(
                samples,
                start_ts.astimezone(timezone.utc),
                max_age_seconds=settings.max_reference_sample_age_seconds,
            )
            if start_reference is None:
                continue
            reference_price = start_reference
            fair_yes = compute_updown_fair_yes(
                reference_price=reference_price,
                current_price=current_price,
                return_volatility=vol,
                seconds_remaining=seconds_remaining,
                floor=settings.min_volatility_floor,
            )
        results.append(
            {
                "market_id": market_id,
                "asset": asset,
                "fair_yes": round(fair_yes, 6),
                "fair_no": round(1.0 - fair_yes, 6),
                "reference_price": reference_price,
                "current_price": current_price,
                "volatility": round(vol, 8),
                "time_to_expiry_sec": round(seconds_remaining, 3),
                "binance_seen_at": str(binance.get("seen_at") or ""),
            }
        )
    return results
