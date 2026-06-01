from __future__ import annotations

import base64
import json
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
except Exception:  # noqa: BLE001
    hashes = None
    serialization = None
    padding = None

from ..config import LatencyBotSettings


_ASSET_MARKERS = {
    "btc": ("btc", "bitcoin"),
    "eth": ("eth", "ethereum"),
    "sol": ("sol", "solana"),
}
_ASSET_SERIES = {
    "btc": "KXBTC15M",
    "eth": "KXETH15M",
    "sol": "KXSOL15M",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_ts(raw: Any) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _path_and_query(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    return parsed.path + (f"?{parsed.query}" if parsed.query else "")


def _kalshi_headers(settings: LatencyBotSettings, method: str, url: str) -> dict[str, str]:
    headers = {"Accept": "application/json", "User-Agent": "polymarket-latency-bot-kalshi-arb-probe/0.1"}
    if not settings.kalshi_api_key_id:
        return headers
    private_key_bytes = b""
    if settings.kalshi_private_key_pem:
        private_key_bytes = settings.kalshi_private_key_pem.replace("\\n", "\n").encode("utf-8")
    elif settings.kalshi_private_key_path:
        private_key_bytes = Path(settings.kalshi_private_key_path).read_bytes()
    if not private_key_bytes:
        return headers
    if serialization is None or padding is None or hashes is None:
        raise RuntimeError("cryptography package is required for Kalshi signed requests")
    private_key = serialization.load_pem_private_key(private_key_bytes, password=None)
    timestamp = str(int(time.time() * 1000))
    message = f"{timestamp}{method.upper()}{_path_and_query(url)}".encode("utf-8")
    signature = private_key.sign(
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    headers.update(
        {
            "KALSHI-ACCESS-KEY": settings.kalshi_api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
        }
    )
    return headers


def _get_json(settings: LatencyBotSettings, path: str, query: dict[str, Any] | None = None) -> dict[str, Any]:
    base = settings.kalshi_arb_api_base.rstrip("/")
    query_text = urllib.parse.urlencode(query or {}, doseq=True)
    url = f"{base}{path}" + (f"?{query_text}" if query_text else "")
    request = urllib.request.Request(url, headers=_kalshi_headers(settings, "GET", url))
    with urllib.request.urlopen(request, timeout=5) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload if isinstance(payload, dict) else {}


def _asset_for_market(market: dict[str, Any]) -> str:
    text = " ".join(
        str(market.get(key) or "")
        for key in ("ticker", "event_ticker", "title", "subtitle", "yes_sub_title", "no_sub_title", "rules_primary")
    ).lower()
    for asset, markers in _ASSET_MARKERS.items():
        if any(marker in text for marker in markers):
            return asset
    return ""


def _looks_like_short_crypto_market(market: dict[str, Any], allowed_assets: set[str]) -> bool:
    asset = _asset_for_market(market)
    if asset not in allowed_assets:
        return False
    text = " ".join(
        str(market.get(key) or "")
        for key in ("ticker", "event_ticker", "title", "subtitle", "yes_sub_title", "no_sub_title", "rules_primary")
    ).lower()
    short_horizon = any(token in text for token in ("15m", "15-min", "15 min", "15-minute", "fifteen"))
    direction = any(token in text for token in ("up", "down", "above", "higher", "lower"))
    series_match = str(market.get("event_ticker") or market.get("ticker") or "").upper().startswith(
        tuple(_ASSET_SERIES.values())
    )
    return short_horizon and (direction or series_match)


def discover_kalshi_arb_markets(settings: LatencyBotSettings) -> list[dict[str, Any]]:
    markets: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ticker in settings.kalshi_arb_tickers:
        text = str(ticker or "").strip().upper()
        if text and text not in seen:
            seen.add(text)
            markets.append({"ticker": text, "asset": "", "title": text, "status": "manual"})
    if not settings.kalshi_arb_auto_discover:
        return markets

    allowed_assets = {str(item).strip().lower() for item in settings.kalshi_arb_assets if str(item).strip()}
    for asset in sorted(allowed_assets):
        series_ticker = _ASSET_SERIES.get(asset)
        if not series_ticker:
            continue
        try:
            payload = _get_json(settings, "/markets", {"status": "open", "limit": 100, "series_ticker": series_ticker})
        except Exception:
            continue
        page = payload.get("markets", []) if isinstance(payload.get("markets"), list) else []
        for market in page:
            if not isinstance(market, dict):
                continue
            ticker = str(market.get("ticker") or "").strip().upper()
            if not ticker or ticker in seen:
                continue
            seen.add(ticker)
            item = dict(market)
            item["asset"] = asset
            markets.append(item)
    if any(str(market.get("asset") or "") in allowed_assets for market in markets):
        return markets

    cursor = ""
    fetched = 0
    while fetched < max(int(settings.kalshi_arb_market_limit), 1):
        page_limit = min(max(int(settings.kalshi_arb_market_limit) - fetched, 1), 1000)
        query: dict[str, Any] = {"status": "open", "limit": page_limit}
        if cursor:
            query["cursor"] = cursor
        payload = _get_json(settings, "/markets", query)
        page = payload.get("markets", []) if isinstance(payload.get("markets"), list) else []
        fetched += len(page)
        for market in page:
            if not isinstance(market, dict):
                continue
            ticker = str(market.get("ticker") or "").strip().upper()
            if not ticker or ticker in seen:
                continue
            if not _looks_like_short_crypto_market(market, allowed_assets):
                continue
            seen.add(ticker)
            item = dict(market)
            item["asset"] = _asset_for_market(market)
            markets.append(item)
        cursor = str(payload.get("cursor") or "").strip()
        if not cursor or not page:
            break
    return markets


def _best_bid(levels: list[Any]) -> tuple[float, float]:
    best_price = 0.0
    best_qty = 0.0
    for level in levels:
        if isinstance(level, dict):
            price = _number(level.get("price") or level.get("yes_price") or level.get("no_price"))
            qty = _number(level.get("quantity") or level.get("count") or level.get("size"))
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price = _number(level[0])
            qty = _number(level[1])
        else:
            continue
        if price > 1.0:
            price = price / 100.0
        if price > best_price and qty > 0.0:
            best_price = price
            best_qty = qty
    return best_price, best_qty


def fetch_kalshi_arb_ticks(settings: LatencyBotSettings, *, ts: str | None = None) -> list[dict[str, Any]]:
    observed_at = ts or _now_iso()
    ticks: list[dict[str, Any]] = []
    markets: list[dict[str, Any]] = []
    discovery_error = ""
    try:
        markets = discover_kalshi_arb_markets(settings)
    except Exception as exc:  # noqa: BLE001
        discovery_error = str(exc)

    if discovery_error:
        ticks.append(
            {
                "ts": observed_at,
                "ticker": "",
                "asset": "",
                "title": "",
                "status": "",
                "yes_bid": 0.0,
                "no_bid": 0.0,
                "yes_bid_qty": 0.0,
                "no_bid_qty": 0.0,
                "yes_ask": 0.0,
                "no_ask": 0.0,
                "total_cost": 0.0,
                "gross_edge": 0.0,
                "executable_depth_usdc": 0.0,
                "seconds_left": 0.0,
                "close_time": "",
                "source": "kalshi_rest_discovery",
                "error": discovery_error,
            }
        )
        return ticks

    now = datetime.now(timezone.utc)
    for market in markets:
        ticker = str(market.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        error = ""
        payload: dict[str, Any] = {}
        try:
            payload = _get_json(settings, f"/markets/{urllib.parse.quote(ticker)}/orderbook", {"depth": 10})
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
        orderbook = {}
        if isinstance(payload.get("orderbook_fp"), dict):
            orderbook = payload["orderbook_fp"]
        elif isinstance(payload.get("orderbook"), dict):
            orderbook = payload["orderbook"]
        yes_levels = orderbook.get("yes_dollars") or orderbook.get("yes") or []
        no_levels = orderbook.get("no_dollars") or orderbook.get("no") or []
        yes_bid, yes_qty = _best_bid(yes_levels if isinstance(yes_levels, list) else [])
        no_bid, no_qty = _best_bid(no_levels if isinstance(no_levels, list) else [])
        yes_ask = max(1.0 - no_bid, 0.0) if no_bid > 0.0 else 0.0
        no_ask = max(1.0 - yes_bid, 0.0) if yes_bid > 0.0 else 0.0
        total_cost = yes_ask + no_ask if yes_ask > 0.0 and no_ask > 0.0 else 0.0
        gross_edge = 1.0 - total_cost if total_cost > 0.0 else 0.0
        executable_shares = min(yes_qty, no_qty)
        executable_depth = executable_shares * total_cost if total_cost > 0.0 else 0.0
        close_time = str(market.get("close_time") or market.get("expiration_time") or market.get("latest_expiration_time") or "")
        close_dt = _parse_ts(close_time)
        seconds_left = max((close_dt - now).total_seconds(), 0.0) if close_dt else 0.0
        ticks.append(
            {
                "ts": observed_at,
                "ticker": ticker,
                "asset": str(market.get("asset") or _asset_for_market(market)),
                "title": str(market.get("title") or market.get("subtitle") or ticker),
                "status": str(market.get("status") or ""),
                "yes_bid": yes_bid,
                "no_bid": no_bid,
                "yes_bid_qty": yes_qty,
                "no_bid_qty": no_qty,
                "yes_ask": yes_ask,
                "no_ask": no_ask,
                "total_cost": total_cost,
                "gross_edge": gross_edge,
                "executable_depth_usdc": executable_depth,
                "seconds_left": seconds_left,
                "close_time": close_time,
                "source": "kalshi_rest_orderbook",
                "error": error,
            }
        )
    return ticks
