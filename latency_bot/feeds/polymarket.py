from __future__ import annotations

import json
import shutil
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bot.core import PolymarketCLI, _best_price, _midpoint_from_book, _sum_book_depth

from ..config import LatencyBotSettings


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text())
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _fetch_clob_book(endpoint: str, token_id: str) -> dict[str, Any]:
    query = urllib.parse.urlencode({"token_id": token_id})
    request = urllib.request.Request(
        f"{endpoint}?{query}",
        headers={"User-Agent": "polymarket-latency-bot/0.1", "Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload if isinstance(payload, dict) else {}


def _book_metrics(book: dict[str, Any]) -> dict[str, float]:
    bids = book.get("bids", []) if isinstance(book.get("bids"), list) else []
    asks = book.get("asks", []) if isinstance(book.get("asks"), list) else []
    best_bid = float(_best_price(bids, side="bid"))
    best_ask = float(_best_price(asks, default=1.0, side="ask"))
    bids_depth = float(_sum_book_depth(bids))
    asks_depth = float(_sum_book_depth(asks))
    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "midpoint": float(_midpoint_from_book(book) or 0.0),
        "bids_depth_usdc": bids_depth,
        "asks_depth_usdc": asks_depth,
        "min_depth_usdc": min(bids_depth, asks_depth),
        "spread": max(best_ask - best_bid, 0.0),
    }


def _normalize_iso(raw: str | None) -> datetime | None:
    if not raw:
        return None
    text = str(raw).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _fallback_from_shared_tape(settings: LatencyBotSettings, tracked: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    source = _read_json(settings.bootstrap_intraday_book_tape_path)
    entries = source.get("entries", []) if isinstance(source.get("entries"), list) else []
    by_market: dict[str, dict[str, Any]] = {}
    tracked_ids = {str(item.get("market_id") or "") for item in tracked}
    tracked_questions = {str(item.get("question") or "") for item in tracked}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        market_id = str(entry.get("market_id") or "")
        question = str(entry.get("question") or "")
        if market_id not in tracked_ids and question not in tracked_questions:
            continue
        by_market[market_id or question] = dict(entry)
    return by_market


def refresh_polymarket_cache(settings: LatencyBotSettings, markets_payload: dict[str, Any], *, cli: PolymarketCLI | None = None) -> dict[str, Any]:
    tracked = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    max_hours = max(settings.book_fetch_horizon_minutes, 1) / 60.0
    tracked = [
        item
        for item in tracked
        if isinstance(item, dict) and float(item.get("hours_to_expiry") or 0.0) <= max_hours
    ]
    updated_at = _now_iso()
    items: list[dict[str, Any]] = []
    errors: list[str] = []
    cli = cli or PolymarketCLI("polymarket", timeout_seconds=settings.polymarket_cli_timeout_seconds)
    fallback = _fallback_from_shared_tape(settings, tracked)
    now = datetime.now(timezone.utc)
    cli_available = shutil.which("polymarket") is not None
    if not cli_available:
        errors.append("polymarket_cli_missing")
    source_counts = {"clob_rest_book": 0, "polymarket_cli_book": 0, "shared_intraday_book_tape_fallback": 0}
    for market in tracked:
        if not isinstance(market, dict):
            continue
        market_id = str(market.get("market_id") or "")
        question = str(market.get("question") or "")
        token_id = str(market.get("yes_token_id") or "")
        no_token_id = str(market.get("no_token_id") or "")
        if not token_id:
            continue
        try:
            try:
                book = _fetch_clob_book(settings.polymarket_clob_book_endpoint, token_id)
                item_source = "clob_rest_book"
            except Exception as clob_exc:
                if not cli_available:
                    raise RuntimeError(f"clob_rest_failed:{clob_exc}")
                book = cli.book(token_id)
                item_source = "polymarket_cli_book"
            yes_metrics = _book_metrics(book)
            no_metrics: dict[str, float] = {}
            no_item_source = ""
            if no_token_id:
                try:
                    no_book = _fetch_clob_book(settings.polymarket_clob_book_endpoint, no_token_id)
                    no_metrics = _book_metrics(no_book)
                    no_item_source = "clob_rest_book"
                except Exception as no_clob_exc:
                    if not cli_available:
                        raise RuntimeError(f"no_clob_rest_failed:{no_clob_exc}")
                    no_book = cli.book(no_token_id)
                    no_metrics = _book_metrics(no_book)
                    no_item_source = "polymarket_cli_book"
            if not no_metrics:
                no_metrics = {
                    "best_bid": max(1.0 - yes_metrics["best_ask"], 0.0),
                    "best_ask": max(1.0 - yes_metrics["best_bid"], 0.0),
                    "midpoint": max(1.0 - yes_metrics["midpoint"], 0.0),
                    "bids_depth_usdc": yes_metrics["asks_depth_usdc"],
                    "asks_depth_usdc": yes_metrics["bids_depth_usdc"],
                    "min_depth_usdc": yes_metrics["min_depth_usdc"],
                    "spread": yes_metrics["spread"],
                }
                no_item_source = "derived_from_yes_book"
            source_counts[item_source] += 1
            items.append(
                {
                    "market_id": market_id,
                    "question": question,
                    "asset": str(market.get("asset") or ""),
                    "token_id": token_id,
                    "yes_token_id": token_id,
                    "no_token_id": no_token_id,
                    "hours_to_expiry": float(market.get("hours_to_expiry") or 0.0),
                    "midpoint": yes_metrics["midpoint"],
                    "best_bid": yes_metrics["best_bid"],
                    "best_ask": yes_metrics["best_ask"],
                    "spread": yes_metrics["spread"],
                    "bids_depth_usdc": yes_metrics["bids_depth_usdc"],
                    "asks_depth_usdc": yes_metrics["asks_depth_usdc"],
                    "min_depth_usdc": yes_metrics["min_depth_usdc"],
                    "no_midpoint": no_metrics["midpoint"],
                    "no_best_bid": no_metrics["best_bid"],
                    "no_best_ask": no_metrics["best_ask"],
                    "no_spread": no_metrics["spread"],
                    "no_bids_depth_usdc": no_metrics["bids_depth_usdc"],
                    "no_asks_depth_usdc": no_metrics["asks_depth_usdc"],
                    "no_min_depth_usdc": no_metrics["min_depth_usdc"],
                    "complete_set_cost": yes_metrics["best_ask"] + no_metrics["best_ask"],
                    "complete_set_edge": 1.0 - yes_metrics["best_ask"] - no_metrics["best_ask"],
                    "seen_at": updated_at,
                    "book_age_ms": 0.0,
                    "source": item_source,
                    "no_source": no_item_source,
                }
            )
        except Exception as exc:
            if cli_available:
                errors.append(f"{market_id}:{exc}")
            fallback_entry = fallback.get(market_id) or fallback.get(question)
            if isinstance(fallback_entry, dict):
                seen_at = _normalize_iso(str(fallback_entry.get("seen_at") or ""))
                age_ms = max((now - seen_at).total_seconds() * 1000.0, 0.0) if seen_at is not None else 0.0
                source_counts["shared_intraday_book_tape_fallback"] += 1
                items.append(
                    {
                        "market_id": market_id,
                        "question": question,
                        "asset": str(market.get("asset") or ""),
                        "token_id": token_id,
                        "hours_to_expiry": float(fallback_entry.get("hours_to_resolution") or market.get("hours_to_expiry") or 0.0),
                        "midpoint": float(fallback_entry.get("midpoint") or 0.0),
                        "best_bid": float(fallback_entry.get("best_bid") or 0.0),
                        "best_ask": float(fallback_entry.get("best_ask") or 0.0),
                        "spread": float(fallback_entry.get("spread") or 0.0),
                        "bids_depth_usdc": float(fallback_entry.get("bids_depth_usdc") or 0.0),
                        "asks_depth_usdc": float(fallback_entry.get("asks_depth_usdc") or 0.0),
                        "min_depth_usdc": float(fallback_entry.get("min_depth_usdc") or 0.0),
                        "no_best_bid": max(1.0 - float(fallback_entry.get("best_ask") or 0.0), 0.0),
                        "no_best_ask": max(1.0 - float(fallback_entry.get("best_bid") or 0.0), 0.0),
                        "no_bids_depth_usdc": float(fallback_entry.get("asks_depth_usdc") or 0.0),
                        "no_asks_depth_usdc": float(fallback_entry.get("bids_depth_usdc") or 0.0),
                        "no_min_depth_usdc": float(fallback_entry.get("min_depth_usdc") or 0.0),
                        "complete_set_cost": float(fallback_entry.get("best_ask") or 0.0) + max(1.0 - float(fallback_entry.get("best_bid") or 0.0), 0.0),
                        "complete_set_edge": 1.0 - float(fallback_entry.get("best_ask") or 0.0) - max(1.0 - float(fallback_entry.get("best_bid") or 0.0), 0.0),
                        "seen_at": str(fallback_entry.get("seen_at") or ""),
                        "book_age_ms": round(age_ms, 2),
                        "source": "shared_intraday_book_tape_fallback",
                        "no_source": "derived_from_yes_book",
                    }
                )
    items.sort(key=lambda item: (float(item.get("hours_to_expiry", 0.0)), str(item.get("asset", "")), str(item.get("question", ""))))
    source_name = max(source_counts.items(), key=lambda item: item[1])[0] if any(source_counts.values()) else "shared_intraday_book_tape_fallback"
    payload = {
        "updated_at": updated_at,
        "count": len(items),
        "source": source_name,
        "items": items,
        "errors": errors,
    }
    _write_json(settings.polymarket_cache_path, payload)
    return payload
