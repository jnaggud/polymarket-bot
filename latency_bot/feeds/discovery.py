from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from bot.config import Settings as MainSettings
from bot.core import (
    _extract_intraday_question_window,
    _extract_market_id,
    _extract_market_token_ids,
    _extract_question,
    _extract_short_market_hours_to_resolution,
    _extract_slug,
    _gamma_tag_updown_market_payload,
    _short_crypto_updown_asset,
)

from ..config import LatencyBotSettings


def _iso_or_now(value: datetime | None) -> str:
    current = value or datetime.now(timezone.utc)
    return current.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_latency_discovery_payload(
    payload: dict[str, Any],
    settings: LatencyBotSettings,
    *,
    seen_at: str | None = None,
) -> list[dict[str, Any]]:
    seen_ts = seen_at or _iso_or_now(None)
    items: list[dict[str, Any]] = []
    markets = payload.get("markets", []) if isinstance(payload, dict) else []
    if not isinstance(markets, list):
        return items
    for market in markets:
        if not isinstance(market, dict):
            continue
        question = _extract_question(market)
        slug = _extract_slug(market)
        asset = _short_crypto_updown_asset(question, slug)
        if asset is None or asset not in settings.assets:
            continue
        token_ids = _extract_market_token_ids(market)
        if len(token_ids) < 2:
            continue
        window = _extract_intraday_question_window(question)
        if window is None:
            continue
        start_ts, end_ts = window
        tenor_minutes = int(round((end_ts - start_ts).total_seconds() / 60.0))
        if tenor_minutes not in settings.tenors_minutes:
            continue
        hours_to_expiry = _extract_short_market_hours_to_resolution(market)
        if hours_to_expiry <= 0:
            continue
        market_id = _extract_market_id(market) or slug or question
        if not market_id:
            continue
        items.append(
            {
                "market_id": market_id,
                "question": question,
                "slug": slug,
                "asset": asset,
                "tenor_minutes": tenor_minutes,
                "hours_to_expiry": round(hours_to_expiry, 6),
                "expiry_ts": _iso_or_now(end_ts),
                "yes_token_id": str(token_ids[0]),
                "no_token_id": str(token_ids[1]),
                "created_at": seen_ts,
                "first_seen_at": seen_ts,
                "status": "tracked",
            }
        )
    items.sort(key=lambda item: (float(item.get("hours_to_expiry", 0.0)), str(item.get("asset", "")), str(item.get("question", ""))))
    return items


def discover_latency_markets(settings: LatencyBotSettings) -> dict[str, Any]:
    main_settings = MainSettings.from_env()
    main_settings.intraday_registry_watchlist_max_minutes_to_resolution = settings.discovery_lookahead_minutes
    payload = _gamma_tag_updown_market_payload(settings.discovery_limit, main_settings)
    generated_at = _iso_or_now(None)
    items = _normalize_latency_discovery_payload(payload, settings, seen_at=generated_at)
    return {
        "generated_at": generated_at,
        "count": len(items),
        "items": items,
        "assets": list(settings.assets),
        "tenors_minutes": list(settings.tenors_minutes),
        "source": {
            "name": "gamma_events_by_tag",
            "tag_candidate_count": int(payload.get("tag_candidate_count", 0) or 0),
            "fetched_count": int(payload.get("fetched_count", 0) or 0),
            "tag_candidates": list(payload.get("tag_candidates", []))[:8] if isinstance(payload.get("tag_candidates"), list) else [],
        },
    }
