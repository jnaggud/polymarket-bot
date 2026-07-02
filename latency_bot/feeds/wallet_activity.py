from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import Any

from ..config import LatencyBotSettings


def fetch_wallet_teacher_trades(settings: LatencyBotSettings) -> dict[str, Any]:
    """Fetch recent public trades for the wallet-teacher paper strategy."""
    wallet = str(settings.wallet_teacher_sniper_wallet or "").strip()
    if not wallet or not settings.wallet_teacher_sniper_enabled:
        return {"items": [], "errors": []}
    limit = max(int(settings.wallet_teacher_sniper_fetch_limit or 0), 1)
    query = urllib.parse.urlencode({"user": wallet, "limit": min(limit, 500), "offset": 0})
    request = urllib.request.Request(
        f"https://data-api.polymarket.com/trades?{query}",
        headers={"User-Agent": "polymarket-latency-bot/0.1", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        return {"items": [], "errors": [f"wallet trade fetch error: {exc}"]}
    if isinstance(payload, list):
        return {"items": [item for item in payload if isinstance(item, dict)], "errors": []}
    if isinstance(payload, dict):
        items = payload.get("trades") or payload.get("items") or payload.get("data") or []
        return {"items": [item for item in items if isinstance(item, dict)] if isinstance(items, list) else [], "errors": []}
    return {"items": [], "errors": ["wallet trade fetch returned unexpected payload"]}
