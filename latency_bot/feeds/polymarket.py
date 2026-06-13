from __future__ import annotations

import json
import shutil
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def _level_price_size(level: Any) -> tuple[float, float]:
    if not isinstance(level, dict):
        return 0.0, 0.0
    try:
        price = float(level.get("price") or 0.0)
        size = float(level.get("size") or 0.0)
    except (TypeError, ValueError):
        return 0.0, 0.0
    return price, size


def _vwap_for_notional(levels: list[Any], *, side: str, target_notional_usdc: float) -> dict[str, float]:
    parsed = []
    for level in levels:
        price, size = _level_price_size(level)
        if price > 0.0 and size > 0.0:
            parsed.append((price, size))
    parsed.sort(key=lambda item: item[0], reverse=(side == "bid"))
    available_usdc = sum(price * size for price, size in parsed)
    if target_notional_usdc <= 0.0 or not parsed:
        return {
            "vwap": parsed[0][0] if parsed else 0.0,
            "fillable_usdc": available_usdc,
            "fillable_shares": sum(size for _, size in parsed),
        }
    remaining = target_notional_usdc
    shares = 0.0
    notional = 0.0
    for price, size in parsed:
        level_notional = price * size
        take_notional = min(level_notional, remaining)
        if take_notional <= 0.0:
            continue
        take_shares = take_notional / price
        shares += take_shares
        notional += take_notional
        remaining -= take_notional
        if remaining <= 1e-9:
            break
    return {
        "vwap": (notional / shares) if shares > 0.0 else 0.0,
        "fillable_usdc": available_usdc,
        "fillable_shares": shares,
    }


def _book_metrics(book: dict[str, Any], *, target_notional_usdc: float = 0.0) -> dict[str, float]:
    bids = book.get("bids", []) if isinstance(book.get("bids"), list) else []
    asks = book.get("asks", []) if isinstance(book.get("asks"), list) else []
    best_bid = float(_best_price(bids, side="bid"))
    best_ask = float(_best_price(asks, default=1.0, side="ask"))
    bids_depth = float(_sum_book_depth(bids))
    asks_depth = float(_sum_book_depth(asks))
    bid_fill = _vwap_for_notional(bids, side="bid", target_notional_usdc=target_notional_usdc)
    ask_fill = _vwap_for_notional(asks, side="ask", target_notional_usdc=target_notional_usdc)
    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "bid_vwap": float(bid_fill["vwap"] or best_bid),
        "ask_vwap": float(ask_fill["vwap"] or best_ask),
        "bid_fillable_usdc": float(bid_fill["fillable_usdc"]),
        "ask_fillable_usdc": float(ask_fill["fillable_usdc"]),
        "bid_fillable_shares": float(bid_fill["fillable_shares"]),
        "ask_fillable_shares": float(ask_fill["fillable_shares"]),
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
    target_notional = max(
        float(getattr(settings, "cex_latency_paper_notional_usdc", 0.0) or 0.0),
        float(getattr(settings, "paper_position_notional_usdc", 0.0) or 0.0),
        float(getattr(settings, "live_complete_set_arb_pilot_notional_usdc", 0.0) or 0.0),
    )
    def build_item(market: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None, str | None]:
        if not isinstance(market, dict):
            return None, None, None
        market_id = str(market.get("market_id") or "")
        question = str(market.get("question") or "")
        token_id = str(market.get("yes_token_id") or "")
        no_token_id = str(market.get("no_token_id") or "")
        if not token_id:
            return None, None, None
        try:
            try:
                book = _fetch_clob_book(settings.polymarket_clob_book_endpoint, token_id)
                item_source = "clob_rest_book"
            except Exception as clob_exc:
                if not cli_available:
                    raise RuntimeError(f"clob_rest_failed:{clob_exc}")
                book = cli.book(token_id)
                item_source = "polymarket_cli_book"
            yes_metrics = _book_metrics(book, target_notional_usdc=target_notional)
            no_metrics: dict[str, float] = {}
            no_item_source = ""
            if no_token_id:
                try:
                    no_book = _fetch_clob_book(settings.polymarket_clob_book_endpoint, no_token_id)
                    no_metrics = _book_metrics(no_book, target_notional_usdc=target_notional)
                    no_item_source = "clob_rest_book"
                except Exception as no_clob_exc:
                    if not cli_available:
                        raise RuntimeError(f"no_clob_rest_failed:{no_clob_exc}")
                    no_book = cli.book(no_token_id)
                    no_metrics = _book_metrics(no_book, target_notional_usdc=target_notional)
                    no_item_source = "polymarket_cli_book"
            if not no_metrics:
                no_metrics = {
                    "best_bid": max(1.0 - yes_metrics["best_ask"], 0.0),
                    "best_ask": max(1.0 - yes_metrics["best_bid"], 0.0),
                    "bid_vwap": max(1.0 - yes_metrics["ask_vwap"], 0.0),
                    "ask_vwap": max(1.0 - yes_metrics["bid_vwap"], 0.0),
                    "bid_fillable_usdc": yes_metrics["ask_fillable_usdc"],
                    "ask_fillable_usdc": yes_metrics["bid_fillable_usdc"],
                    "bid_fillable_shares": yes_metrics["ask_fillable_shares"],
                    "ask_fillable_shares": yes_metrics["bid_fillable_shares"],
                    "midpoint": max(1.0 - yes_metrics["midpoint"], 0.0),
                    "bids_depth_usdc": yes_metrics["asks_depth_usdc"],
                    "asks_depth_usdc": yes_metrics["bids_depth_usdc"],
                    "min_depth_usdc": yes_metrics["min_depth_usdc"],
                    "spread": yes_metrics["spread"],
                }
                no_item_source = "derived_from_yes_book"
            return (
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
                    "bid_vwap": yes_metrics["bid_vwap"],
                    "ask_vwap": yes_metrics["ask_vwap"],
                    "bid_fillable_usdc": yes_metrics["bid_fillable_usdc"],
                    "ask_fillable_usdc": yes_metrics["ask_fillable_usdc"],
                    "bid_fillable_shares": yes_metrics["bid_fillable_shares"],
                    "ask_fillable_shares": yes_metrics["ask_fillable_shares"],
                    "spread": yes_metrics["spread"],
                    "bids_depth_usdc": yes_metrics["bids_depth_usdc"],
                    "asks_depth_usdc": yes_metrics["asks_depth_usdc"],
                    "min_depth_usdc": yes_metrics["min_depth_usdc"],
                    "no_midpoint": no_metrics["midpoint"],
                    "no_best_bid": no_metrics["best_bid"],
                    "no_best_ask": no_metrics["best_ask"],
                    "no_bid_vwap": no_metrics["bid_vwap"],
                    "no_ask_vwap": no_metrics["ask_vwap"],
                    "no_bid_fillable_usdc": no_metrics["bid_fillable_usdc"],
                    "no_ask_fillable_usdc": no_metrics["ask_fillable_usdc"],
                    "no_bid_fillable_shares": no_metrics["bid_fillable_shares"],
                    "no_ask_fillable_shares": no_metrics["ask_fillable_shares"],
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
                },
                item_source,
                None,
            )
        except Exception as exc:
            error = f"{market_id}:{exc}" if cli_available else None
            if cli_available:
                pass
            fallback_entry = fallback.get(market_id) or fallback.get(question)
            if isinstance(fallback_entry, dict):
                seen_at = _normalize_iso(str(fallback_entry.get("seen_at") or ""))
                age_ms = max((now - seen_at).total_seconds() * 1000.0, 0.0) if seen_at is not None else 0.0
                return (
                    {
                        "market_id": market_id,
                        "question": question,
                        "asset": str(market.get("asset") or ""),
                        "token_id": token_id,
                        "hours_to_expiry": float(fallback_entry.get("hours_to_resolution") or market.get("hours_to_expiry") or 0.0),
                        "midpoint": float(fallback_entry.get("midpoint") or 0.0),
                        "best_bid": float(fallback_entry.get("best_bid") or 0.0),
                        "best_ask": float(fallback_entry.get("best_ask") or 0.0),
                        "bid_vwap": float(fallback_entry.get("bid_vwap") or fallback_entry.get("best_bid") or 0.0),
                        "ask_vwap": float(fallback_entry.get("ask_vwap") or fallback_entry.get("best_ask") or 0.0),
                        "bid_fillable_usdc": float(fallback_entry.get("bid_fillable_usdc") or fallback_entry.get("bids_depth_usdc") or 0.0),
                        "ask_fillable_usdc": float(fallback_entry.get("ask_fillable_usdc") or fallback_entry.get("asks_depth_usdc") or 0.0),
                        "bid_fillable_shares": 0.0,
                        "ask_fillable_shares": 0.0,
                        "spread": float(fallback_entry.get("spread") or 0.0),
                        "bids_depth_usdc": float(fallback_entry.get("bids_depth_usdc") or 0.0),
                        "asks_depth_usdc": float(fallback_entry.get("asks_depth_usdc") or 0.0),
                        "min_depth_usdc": float(fallback_entry.get("min_depth_usdc") or 0.0),
                        "no_best_bid": max(1.0 - float(fallback_entry.get("best_ask") or 0.0), 0.0),
                        "no_best_ask": max(1.0 - float(fallback_entry.get("best_bid") or 0.0), 0.0),
                        "no_bid_vwap": max(1.0 - float(fallback_entry.get("ask_vwap") or fallback_entry.get("best_ask") or 0.0), 0.0),
                        "no_ask_vwap": max(1.0 - float(fallback_entry.get("bid_vwap") or fallback_entry.get("best_bid") or 0.0), 0.0),
                        "no_bid_fillable_usdc": float(fallback_entry.get("ask_fillable_usdc") or fallback_entry.get("asks_depth_usdc") or 0.0),
                        "no_ask_fillable_usdc": float(fallback_entry.get("bid_fillable_usdc") or fallback_entry.get("bids_depth_usdc") or 0.0),
                        "no_bid_fillable_shares": 0.0,
                        "no_ask_fillable_shares": 0.0,
                        "no_bids_depth_usdc": float(fallback_entry.get("asks_depth_usdc") or 0.0),
                        "no_asks_depth_usdc": float(fallback_entry.get("bids_depth_usdc") or 0.0),
                        "no_min_depth_usdc": float(fallback_entry.get("min_depth_usdc") or 0.0),
                        "complete_set_cost": float(fallback_entry.get("best_ask") or 0.0) + max(1.0 - float(fallback_entry.get("best_bid") or 0.0), 0.0),
                        "complete_set_edge": 1.0 - float(fallback_entry.get("best_ask") or 0.0) - max(1.0 - float(fallback_entry.get("best_bid") or 0.0), 0.0),
                        "seen_at": str(fallback_entry.get("seen_at") or ""),
                        "book_age_ms": round(age_ms, 2),
                        "source": "shared_intraday_book_tape_fallback",
                        "no_source": "derived_from_yes_book",
                    },
                    "shared_intraday_book_tape_fallback",
                    error,
                )
            return None, None, error

    max_workers = min(max(len(tracked), 1), 16)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(build_item, market) for market in tracked if isinstance(market, dict)]
        for future in as_completed(futures):
            item, item_source, error = future.result()
            if error:
                errors.append(error)
            if item_source:
                source_counts[item_source] += 1
            if item:
                items.append(item)
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
