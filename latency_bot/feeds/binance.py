from __future__ import annotations

import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..config import LatencyBotSettings


_BINANCE_SYMBOLS = {
    "btc": "BTCUSDT",
    "eth": "ETHUSDT",
    "sol": "SOLUSDT",
}

_FALLBACK_ENDPOINTS = (
    "https://api1.binance.com/api/v3/ticker/bookTicker",
    "https://api.binance.us/api/v3/ticker/bookTicker",
)


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


def _fetch_book_ticker(endpoint: str, symbol: str) -> dict[str, Any]:
    query = urllib.parse.urlencode({"symbol": symbol})
    request = urllib.request.Request(
        f"{endpoint}?{query}",
        headers={"User-Agent": "polymarket-latency-bot/0.1", "Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload if isinstance(payload, dict) else {}


def _candidate_endpoints(settings: LatencyBotSettings) -> tuple[str, ...]:
    ordered = [settings.binance_rest_endpoint, *_FALLBACK_ENDPOINTS]
    deduped: list[str] = []
    for endpoint in ordered:
        clean = str(endpoint or "").strip()
        if clean and clean not in deduped:
            deduped.append(clean)
    return tuple(deduped)


def refresh_binance_cache(settings: LatencyBotSettings) -> dict[str, Any]:
    existing = _read_json(settings.binance_cache_path)
    existing_items = existing.get("items", []) if isinstance(existing.get("items"), list) else []
    existing_by_asset = {str(item.get("asset") or "").lower(): item for item in existing_items if isinstance(item, dict)}
    updated_at = _now_iso()
    items: list[dict[str, Any]] = []
    errors: list[str] = []
    used_endpoint = ""
    endpoints = _candidate_endpoints(settings)
    for asset in settings.assets:
        symbol = _BINANCE_SYMBOLS.get(asset)
        if not symbol:
            continue
        previous = existing_by_asset.get(asset, {})
        previous_samples = previous.get("samples", []) if isinstance(previous.get("samples"), list) else []
        try:
            asset_errors: list[str] = []
            payload: dict[str, Any] = {}
            endpoint_used = ""
            for endpoint in endpoints:
                try:
                    payload = _fetch_book_ticker(endpoint, symbol)
                    endpoint_used = endpoint
                    break
                except Exception as exc:
                    asset_errors.append(f"{endpoint}:{exc}")
            if not endpoint_used:
                raise RuntimeError("; ".join(asset_errors))
            used_endpoint = endpoint_used or used_endpoint
            bid = float(payload.get("bidPrice") or 0.0)
            ask = float(payload.get("askPrice") or 0.0)
            mid = (bid + ask) / 2.0 if bid > 0.0 and ask > 0.0 else max(bid, ask, 0.0)
            sample = {"seen_at": updated_at, "mid": mid, "bid": bid, "ask": ask}
            samples = [*previous_samples, sample][-max(settings.binance_sample_history, 1) :]
            items.append(
                {
                    "asset": asset,
                    "symbol": symbol,
                    "mid": mid,
                    "bid": bid,
                    "ask": ask,
                    "last": mid,
                    "source_latency_ms": 0.0,
                    "seen_at": updated_at,
                    "source_endpoint": endpoint_used,
                    "samples": samples,
                }
            )
        except Exception as exc:
            errors.append(f"{asset}:{exc}")
            if previous:
                stale = dict(previous)
                stale["stale"] = True
                items.append(stale)
    payload = {
        "updated_at": updated_at,
        "count": len(items),
        "source": "binance_rest_bookticker",
        "source_endpoint": used_endpoint,
        "items": items,
        "errors": errors,
    }
    _write_json(settings.binance_cache_path, payload)
    return payload
