from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def as_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def side_contract_price(side: str, yes_midpoint: float) -> float:
    midpoint = min(0.99, max(0.01, as_float(yes_midpoint, 0.5)))
    return midpoint if str(side).upper() == "BUY" else 1.0 - midpoint


def effective_position_shares(position: dict[str, Any]) -> float:
    side = str(position.get("side", "BUY")).upper()
    notional = max(as_float(position.get("notional_usdc"), 0.0), 0.0)
    entry_midpoint = as_float(position.get("entry_price"), 0.5)
    entry_contract_price = side_contract_price(side, entry_midpoint)
    if entry_contract_price <= 0:
        return 0.0
    return round(notional / entry_contract_price, 6)


def position_mark(position: dict[str, Any], current_yes_midpoint: float) -> tuple[float, float, float]:
    side = str(position.get("side", "BUY")).upper()
    entry_midpoint = as_float(position.get("entry_price"), 0.5)
    shares = effective_position_shares(position)
    entry_contract_price = side_contract_price(side, entry_midpoint)
    current_contract_price = side_contract_price(side, current_yes_midpoint)
    pnl = round((current_contract_price - entry_contract_price) * shares, 2)
    mark_value = round(current_contract_price * shares, 2)
    return shares, mark_value, pnl


def dedupe_closed_trades(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen_position_ids: set[str] = set()
    for trade in trades:
        if trade.get("type") != "CLOSE":
            continue
        position_id = str(trade.get("position_id", ""))
        if position_id and position_id in seen_position_ids:
            continue
        if position_id:
            seen_position_ids.add(position_id)
        deduped.append(trade)
    return deduped


def _normalize_ts(raw: str | None) -> datetime:
    text = (raw or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


def rebuild_closed_trades(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    opens = sorted(
        [trade for trade in trades if trade.get("type") == "OPEN"],
        key=lambda trade: _normalize_ts(str(trade.get("opened_at"))),
    )
    scales = sorted(
        [trade for trade in trades if trade.get("type") == "SCALE"],
        key=lambda trade: _normalize_ts(str(trade.get("scaled_at"))),
    )
    closes = sorted(
        dedupe_closed_trades(trades),
        key=lambda trade: _normalize_ts(str(trade.get("closed_at"))),
    )

    ledger: dict[str, dict[str, Any]] = {}
    for trade in opens:
        position_id = str(trade.get("position_id", ""))
        if not position_id:
            continue
        shares = effective_position_shares(trade)
        ledger[position_id] = {
            "position_id": position_id,
            "market_id": trade.get("market_id"),
            "token_id": trade.get("token_id"),
            "question": trade.get("question"),
            "category": trade.get("category", "unknown"),
            "side": str(trade.get("side", "BUY")).upper(),
            "notional_usdc": max(as_float(trade.get("notional_usdc"), 0.0), 0.0),
            "shares": shares,
            "entry_price": as_float(trade.get("entry_price"), 0.5),
            "opened_at": trade.get("opened_at"),
            "mode": trade.get("mode"),
        }

    for trade in scales:
        position_id = str(trade.get("position_id", ""))
        if not position_id or position_id not in ledger:
            continue
        position = ledger[position_id]
        add_notional = max(as_float(trade.get("add_notional_usdc"), 0.0), 0.0)
        add_midpoint = as_float(trade.get("entry_price"), position["entry_price"])
        add_shares = 0.0
        if add_notional > 0:
            add_shares = round(add_notional / side_contract_price(position["side"], add_midpoint), 6)
        total_shares = position["shares"] + add_shares
        if total_shares <= 0:
            continue
        position["entry_price"] = round(((position["entry_price"] * position["shares"]) + (add_midpoint * add_shares)) / total_shares, 6)
        position["shares"] = round(total_shares, 6)
        position["notional_usdc"] = round(position["notional_usdc"] + add_notional, 2)
        if trade.get("category"):
            position["category"] = trade.get("category")

    rebuilt: list[dict[str, Any]] = []
    for trade in closes:
        position_id = str(trade.get("position_id", ""))
        position = ledger.get(position_id, {})
        if not position:
            rebuilt.append(dict(trade))
            continue
        side = str(position.get("side", trade.get("side", "BUY"))).upper()
        entry_midpoint = as_float(position.get("entry_price", trade.get("entry_price")), 0.5)
        shares = as_float(position.get("shares"), 0.0)
        if shares <= 0:
            fallback = {
                "side": side,
                "notional_usdc": as_float(trade.get("notional_usdc"), 0.0),
                "entry_price": entry_midpoint,
            }
            shares = effective_position_shares(fallback)
        if shares <= 0:
            rebuilt.append(dict(trade))
            continue
        exit_midpoint = as_float(trade.get("exit_price"), entry_midpoint)
        pnl = round((side_contract_price(side, exit_midpoint) - side_contract_price(side, entry_midpoint)) * shares, 2)
        rebuilt.append(
            {
                **trade,
                "market_id": position.get("market_id", trade.get("market_id")),
                "token_id": position.get("token_id", trade.get("token_id")),
                "question": position.get("question", trade.get("question")),
                "category": position.get("category", trade.get("category", "unknown")),
                "side": side,
                "shares": shares,
                "notional_usdc": round(as_float(position.get("notional_usdc"), as_float(trade.get("notional_usdc"), 0.0)), 2),
                "entry_price": round(entry_midpoint, 6),
                "pnl_usdc": pnl,
                "opened_at": position.get("opened_at"),
            }
        )
    return rebuilt


def realized_pnl_from_trades(trades: list[dict[str, Any]]) -> float:
    return round(sum(as_float(trade.get("pnl_usdc"), 0.0) for trade in rebuild_closed_trades(trades)), 2)
