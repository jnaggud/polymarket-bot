from __future__ import annotations

import base64
import json
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

try:
    from cryptography.hazmat.primitives.asymmetric import ed25519
except Exception:  # noqa: BLE001
    ed25519 = None

from ..config import LatencyBotSettings


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _number(value: Any, default: float = 0.0) -> float:
    if isinstance(value, dict):
        value = value.get("value", default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _levels(payload: dict[str, Any], key: str) -> list[dict[str, float]]:
    raw_levels = payload.get(key, []) if isinstance(payload.get(key), list) else []
    levels: list[dict[str, float]] = []
    for raw in raw_levels:
        if not isinstance(raw, dict):
            continue
        price = _number(raw.get("px"))
        qty = _number(raw.get("qty"))
        if price <= 0.0 or qty <= 0.0:
            continue
        levels.append({"price": price, "qty": qty})
    return levels


def _fetch_orderbook(settings: LatencyBotSettings, symbol: str) -> dict[str, Any]:
    base = settings.polymarket_us_arb_orderbook_endpoint.rstrip("/")
    path = f"/v1/orderbook/{urllib.parse.quote(symbol)}"
    query = urllib.parse.urlencode({"depth": 10})
    url = f"{base}/{urllib.parse.quote(symbol)}?{query}"
    headers = {"User-Agent": "polymarket-latency-bot-us-arb-probe/0.1", "Accept": "application/json"}
    if settings.polymarket_us_key_id and settings.polymarket_us_secret_key:
        if ed25519 is None:
            raise RuntimeError("cryptography package is required for Polymarket US signed requests")
        timestamp = str(int(time.time() * 1000))
        private_key = ed25519.Ed25519PrivateKey.from_private_bytes(
            base64.b64decode(settings.polymarket_us_secret_key)[:32]
        )
        signature = base64.b64encode(private_key.sign(f"{timestamp}GET{path}".encode("utf-8"))).decode("utf-8")
        headers.update(
            {
                "X-PM-Access-Key": settings.polymarket_us_key_id,
                "X-PM-Timestamp": timestamp,
                "X-PM-Signature": signature,
            }
        )
    if settings.polymarket_us_arb_api_key:
        headers["Authorization"] = f"Bearer {settings.polymarket_us_arb_api_key}"
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=5) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload if isinstance(payload, dict) else {}


def fetch_polymarket_us_arb_ticks(settings: LatencyBotSettings, *, ts: str | None = None) -> list[dict[str, Any]]:
    observed_at = ts or _now_iso()
    ticks: list[dict[str, Any]] = []
    for symbol in settings.polymarket_us_arb_symbols:
        symbol_text = str(symbol or "").strip()
        if not symbol_text:
            continue
        error = ""
        payload: dict[str, Any] = {}
        try:
            payload = _fetch_orderbook(settings, symbol_text)
        except Exception as exc:  # noqa: BLE001
            error = str(exc)

        bids = sorted(_levels(payload, "bids"), key=lambda item: item["price"], reverse=True)
        offers = sorted(_levels(payload, "offers"), key=lambda item: item["price"])
        best_bid = bids[0]["price"] if bids else 0.0
        best_ask = offers[0]["price"] if offers else 0.0
        bid_qty = bids[0]["qty"] if bids else 0.0
        ask_qty = offers[0]["qty"] if offers else 0.0
        executable_shares = min(bid_qty, ask_qty)
        executable_depth_usdc = executable_shares * best_ask if best_ask > 0.0 else 0.0
        gross_edge = best_bid - best_ask if best_bid > 0.0 and best_ask > 0.0 else 0.0
        state = str(payload.get("state") or "").strip()
        transact_time = str(payload.get("transactTime") or "")

        ticks.append(
            {
                "ts": observed_at,
                "symbol": symbol_text,
                "state": state,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "bid_qty": bid_qty,
                "ask_qty": ask_qty,
                "gross_edge": gross_edge,
                "executable_depth_usdc": executable_depth_usdc,
                "transact_time": transact_time,
                "source": "polymarket_us_rest_orderbook",
                "error": error,
            }
        )
    return ticks
