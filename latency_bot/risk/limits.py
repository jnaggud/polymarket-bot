from __future__ import annotations

from typing import Any

from ..config import LatencyBotSettings
from ..storage import load_open_positions


def risk_snapshot(settings: LatencyBotSettings) -> dict[str, Any]:
    positions = load_open_positions(settings)
    total_notional = 0.0
    btc_notional = 0.0
    eth_notional = 0.0
    for position in positions:
        notional = float(position.get("entry_price") or 0.0) * float(position.get("size") or 0.0)
        total_notional += notional
        asset = str(position.get("asset") or "").lower()
        if asset == "btc":
            btc_notional += notional
        elif asset == "eth":
            eth_notional += notional
    bankroll = max(settings.bankroll_usdc, 1.0)
    return {
        "open_positions_count": len(positions),
        "total_open_notional_usdc": round(total_notional, 2),
        "btc_open_notional_usdc": round(btc_notional, 2),
        "eth_open_notional_usdc": round(eth_notional, 2),
        "total_open_fraction": round(total_notional / bankroll, 6),
        "btc_open_fraction": round(btc_notional / bankroll, 6),
        "eth_open_fraction": round(eth_notional / bankroll, 6),
    }


def can_open_new_position(settings: LatencyBotSettings, *, asset: str, side: str = "") -> tuple[bool, str]:
    snapshot = risk_snapshot(settings)
    positions = load_open_positions(settings)
    if int(snapshot["open_positions_count"]) >= settings.max_simultaneous_positions:
        return False, "max_simultaneous_positions"
    if float(snapshot["total_open_fraction"]) >= settings.max_total_open_notional_fraction:
        return False, "max_total_open_notional_fraction"
    if asset == "btc" and float(snapshot["btc_open_fraction"]) >= settings.max_btc_open_notional_fraction:
        return False, "max_btc_open_notional_fraction"
    if asset == "eth" and float(snapshot["eth_open_fraction"]) >= settings.max_eth_open_notional_fraction:
        return False, "max_eth_open_notional_fraction"
    if side:
        same_direction_count = sum(
            1
            for position in positions
            if str(position.get("asset") or "").lower() == asset.lower()
            and str(position.get("side") or "").upper() == side.upper()
        )
        if same_direction_count >= settings.max_same_direction_positions_per_asset:
            return False, "max_same_direction_positions_per_asset"
    return True, ""
