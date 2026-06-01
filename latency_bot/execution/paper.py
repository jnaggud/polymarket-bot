from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ..config import LatencyBotSettings
from ..risk.limits import can_open_new_position
from ..storage import (
    cancel_order,
    close_position,
    close_complete_set_arb_position,
    close_shadow_position,
    close_shadow_variant_position,
    connect_latency_bot_db,
    create_order,
    create_complete_set_arb_position,
    create_position,
    create_shadow_position,
    create_shadow_variant_position,
    fill_open_order_as_position,
    load_latest_polymarket_books,
    load_open_orders,
    load_open_positions,
    load_recent_position_closes,
    load_shadow_open_positions,
    load_shadow_variant_open_positions,
)


PROMOTED_VARIANT_MODE_PREFIX = "promoted_variant:"


def _promoted_variant_id(position: dict[str, Any]) -> str:
    mode = str(position.get("mode") or "")
    if mode.startswith(PROMOTED_VARIANT_MODE_PREFIX):
        return mode[len(PROMOTED_VARIANT_MODE_PREFIX) :]
    return ""


def _seconds_remaining(item: dict[str, Any]) -> float:
    raw = str(item.get("expiry_ts") or "")
    if not raw:
        return 0.0
    text = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        expiry = datetime.fromisoformat(text)
    except ValueError:
        return 0.0
    if expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=timezone.utc)
    return max((expiry.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds(), 0.0)


def _current_exit_price(position: dict[str, Any], cache: dict[str, Any]) -> float:
    best_bid = float(cache.get("best_bid") or 0.0)
    best_ask = float(cache.get("best_ask") or 0.0)
    if str(position.get("side") or "") == "YES":
        return best_bid
    return max(1.0 - best_ask, 0.0)


def _pnl(position: dict[str, Any], exit_price: float) -> float:
    size = float(position.get("size") or 0.0)
    entry = float(position.get("entry_price") or 0.0)
    return round((exit_price - entry) * size, 6)


def _stop_loss_hit(settings: LatencyBotSettings, position: dict[str, Any], exit_price: float) -> bool:
    entry = float(position.get("entry_price") or 0.0)
    if entry <= 0.0:
        return False
    return exit_price <= (entry * (1.0 - settings.stop_loss_fraction))


def _take_profit_hit(settings: LatencyBotSettings, position: dict[str, Any], exit_price: float) -> bool:
    entry = float(position.get("entry_price") or 0.0)
    if entry <= 0.0:
        return False
    return exit_price >= (entry * (1.0 + settings.take_profit_fraction))


def _ts_to_dt(raw: str) -> datetime:
    text = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _complete_set_recently_traded(settings: LatencyBotSettings, *, market_id: str, ts: str) -> bool:
    if settings.complete_set_arb_same_market_cooldown_seconds <= 0:
        return False
    cutoff = _ts_to_dt(ts).timestamp() - float(settings.complete_set_arb_same_market_cooldown_seconds)
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT entry_ts
            FROM complete_set_arb_positions
            WHERE market_id = ?
            ORDER BY entry_ts DESC
            LIMIT 5
            """,
            (market_id,),
        ).fetchall()
    for row in rows:
        try:
            if _ts_to_dt(str(row["entry_ts"] or "")).timestamp() >= cutoff:
                return True
        except Exception:
            continue
    return False


def _quote_life_seconds(settings: LatencyBotSettings, *, tenor_minutes: int) -> int:
    if tenor_minutes <= 5:
        return settings.maker_quote_life_5m_seconds
    return settings.maker_quote_life_15m_seconds


def _maker_fill_price(cache: dict[str, Any], *, side: str) -> float:
    if side == "YES":
        return float(cache.get("best_ask") or 0.0)
    return max(1.0 - float(cache.get("best_bid") or 0.0), 0.0)


def _signal_side(signal: dict[str, Any] | None) -> str:
    if not isinstance(signal, dict):
        return ""
    signal_type = str(signal.get("signal_type") or "")
    if "YES" in signal_type:
        return "YES"
    if "NO" in signal_type:
        return "NO"
    return ""


def _recent_trade_block_reason(
    settings: LatencyBotSettings,
    *,
    ts: str,
    market_id: str,
    recent_closes: list[dict[str, Any]],
) -> str:
    now = _ts_to_dt(ts)
    if settings.same_market_cooldown_seconds > 0:
        for close in recent_closes:
            if str(close.get("market_id") or "") != market_id:
                continue
            close_ts = _ts_to_dt(str(close.get("ts") or ts))
            age_seconds = (now - close_ts).total_seconds()
            if 0 <= age_seconds < settings.same_market_cooldown_seconds:
                return "same market cooldown"
            break

    stop_streak = 0
    latest_close_ts: datetime | None = None
    for close in recent_closes:
        if latest_close_ts is None:
            latest_close_ts = _ts_to_dt(str(close.get("ts") or ts))
        if str(close.get("reason") or "") != "STOP_LOSS":
            break
        stop_streak += 1
    if (
        settings.stop_loss_streak_pause_count > 0
        and stop_streak >= settings.stop_loss_streak_pause_count
        and latest_close_ts is not None
        and (now - latest_close_ts).total_seconds() < settings.stop_loss_pause_seconds
    ):
        return f"stop loss pause: streak {stop_streak}"

    window = max(settings.recent_loss_window_trades, 1)
    recent_window = recent_closes[:window]
    if len(recent_window) >= window:
        net_pnl = round(sum(float(close.get("pnl") or 0.0) for close in recent_window), 6)
        latest_window_ts = _ts_to_dt(str(recent_window[0].get("ts") or ts))
        if (
            net_pnl <= settings.recent_loss_pause_threshold_usdc
            and (now - latest_window_ts).total_seconds() < settings.stop_loss_pause_seconds
        ):
            return f"recent loss pause: last {window} net {net_pnl:.2f}"

    return ""


def _eligible_rate_block_reason(settings: LatencyBotSettings, signal: dict[str, Any], signals: list[dict[str, Any]]) -> str:
    asset = str(signal.get("asset") or "").lower()
    tenor = int(signal.get("tenor_minutes") or 0)
    if asset != "btc" or tenor != 5 or settings.max_btc_5m_eligible_rate <= 0:
        return ""
    slice_signals = [
        item
        for item in signals
        if str(item.get("asset") or "").lower() == "btc" and int(item.get("tenor_minutes") or 0) == 5
    ]
    if not slice_signals:
        return ""
    eligible = sum(1 for item in slice_signals if bool(item.get("eligible")))
    rate = eligible / len(slice_signals)
    if rate > settings.max_btc_5m_eligible_rate:
        return f"eligible rate guard: BTC 5m {rate:.1%}"
    return ""


def _position_blocked_by_settings(settings: LatencyBotSettings, position: dict[str, Any], market: dict[str, Any] | None) -> bool:
    asset = str(position.get("asset") or "").lower()
    side = str(position.get("side") or "")
    mode = str(position.get("mode") or "")
    tenor_minutes = int((market or {}).get("tenor_minutes") or 0)
    if side == "YES" and mode == "taker":
        if asset == "btc" and not settings.allow_btc_yes_taker:
            return True
        if asset == "eth" and not settings.allow_eth_yes_taker:
            return True
        if tenor_minutes > 5 and not settings.allow_15m_taker:
            return True
    if side == "NO" and mode == "taker" and not settings.allow_taker_no:
        return True
    if asset == "eth" and side == "NO" and mode == "taker" and not settings.allow_eth_no_taker:
        return True
    if "JOIN" in mode and not settings.allow_maker_join:
        return True
    if "IMPROVE" in mode and not settings.allow_maker_improve:
        return True
    return False


def _order_blocked_by_settings(settings: LatencyBotSettings, order: dict[str, Any]) -> bool:
    mode = str(order.get("mode") or "")
    if "JOIN" in mode and not settings.allow_maker_join:
        return True
    if "IMPROVE" in mode and not settings.allow_maker_improve:
        return True
    return False


def run_paper_execution_cycle(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
    signals: list[dict[str, Any]],
    ts: str,
) -> dict[str, Any]:
    markets = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    market_by_id = {str(item.get("market_id") or ""): item for item in markets if isinstance(item, dict)}
    cache_items = polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else []
    cache_by_id = {str(item.get("market_id") or ""): item for item in cache_items if isinstance(item, dict)}
    open_positions = load_open_positions(settings)
    open_orders = load_open_orders(settings)
    latest_books = load_latest_polymarket_books(
        settings,
        [str(item.get("market_id") or "") for item in open_positions] + [str(item.get("market_id") or "") for item in open_orders],
    )
    opened: list[dict[str, Any]] = []
    closed: list[dict[str, Any]] = []
    cancelled: list[dict[str, Any]] = []
    entry_blocks: list[dict[str, Any]] = []
    maker_opened: list[dict[str, Any]] = []
    open_by_market = {str(item.get("market_id") or ""): item for item in open_positions}
    open_order_by_market = {str(item.get("market_id") or ""): item for item in open_orders}
    recent_closes = load_recent_position_closes(
        settings,
        limit=max(settings.recent_loss_window_trades, settings.stop_loss_streak_pause_count, 20),
    )

    for position in open_positions:
        market_id = str(position.get("market_id") or "")
        market = market_by_id.get(market_id)
        cache = cache_by_id.get(market_id) or latest_books.get(market_id)
        if cache is None:
            continue
        if _position_blocked_by_settings(settings, position, market):
            exit_price = _current_exit_price(position, cache)
            pnl = _pnl(position, exit_price)
            close_position(settings, position_id=str(position.get("position_id") or ""), ts=ts, exit_price=exit_price, pnl=pnl, reason="STRATEGY_DISABLED")
            closed.append({"market_id": market_id, "reason": "STRATEGY_DISABLED", "pnl": pnl, "exit_price": exit_price})
            continue
        if market is None:
            exit_price = _current_exit_price(position, cache)
            pnl = _pnl(position, exit_price)
            close_position(settings, position_id=str(position.get("position_id") or ""), ts=ts, exit_price=exit_price, pnl=pnl, reason="MARKET_ROLLED_OFF")
            closed.append({"market_id": market_id, "reason": "MARKET_ROLLED_OFF", "pnl": pnl, "exit_price": exit_price})
            continue
        seconds_left = _seconds_remaining(market)
        force_exit_seconds = settings.force_exit_seconds_5m if int(market.get("tenor_minutes") or 0) <= 5 else settings.force_exit_seconds_15m
        matching_signal = next((signal for signal in signals if str(signal.get("market_id") or "") == market_id), None)
        if matching_signal is None:
            continue
        should_close = False
        reason = ""
        held_seconds = (_ts_to_dt(ts) - _ts_to_dt(str(position.get("entry_ts") or ts))).total_seconds()
        signal_side = _signal_side(matching_signal)
        opposite_signal = bool(matching_signal.get("eligible")) and signal_side and signal_side != str(position.get("side") or "")
        exit_price = _current_exit_price(position, cache)
        if not should_close and _stop_loss_hit(settings, position, exit_price):
            should_close = True
            reason = "STOP_LOSS"
        elif not should_close and _take_profit_hit(settings, position, exit_price):
            should_close = True
            reason = "TAKE_PROFIT"
        if not should_close and seconds_left <= force_exit_seconds:
            should_close = True
            reason = "TIME_EXIT"
        elif not should_close and held_seconds >= settings.min_hold_seconds_before_edge_close and opposite_signal:
            should_close = True
            reason = "EDGE_CLOSED"
        if should_close:
            pnl = _pnl(position, exit_price)
            close_position(settings, position_id=str(position.get("position_id") or ""), ts=ts, exit_price=exit_price, pnl=pnl, reason=reason)
            closed.append({"market_id": market_id, "reason": reason, "pnl": pnl, "exit_price": exit_price})

    for order in open_orders:
        market_id = str(order.get("market_id") or "")
        if _order_blocked_by_settings(settings, order):
            cancel_order(settings, order_id=str(order.get("order_id") or ""), reason="STRATEGY_DISABLED")
            cancelled.append({"market_id": market_id, "reason": "STRATEGY_DISABLED"})
            continue
        market = market_by_id.get(market_id)
        cache = cache_by_id.get(market_id) or latest_books.get(market_id)
        if market is None or cache is None:
            cancel_order(settings, order_id=str(order.get("order_id") or ""), reason="MARKET_UNAVAILABLE")
            cancelled.append({"market_id": market_id, "reason": "MARKET_UNAVAILABLE"})
            continue
        if market_id in open_by_market:
            cancel_order(settings, order_id=str(order.get("order_id") or ""), reason="POSITION_ALREADY_OPEN")
            cancelled.append({"market_id": market_id, "reason": "POSITION_ALREADY_OPEN"})
            continue
        matching_signal = next((signal for signal in signals if str(signal.get("market_id") or "") == market_id), None)
        if matching_signal is None:
            cancel_order(settings, order_id=str(order.get("order_id") or ""), reason="SIGNAL_MISSING")
            cancelled.append({"market_id": market_id, "reason": "SIGNAL_MISSING"})
            continue
        order_side = str(order.get("side") or "")
        quote_seconds = (_ts_to_dt(ts) - _ts_to_dt(str(order.get("ts_created") or ts))).total_seconds()
        force_exit_seconds = settings.force_exit_seconds_5m if int(market.get("tenor_minutes") or 0) <= 5 else settings.force_exit_seconds_15m
        if _seconds_remaining(market) <= force_exit_seconds:
            cancel_order(settings, order_id=str(order.get("order_id") or ""), reason="TOO_CLOSE_TO_EXPIRY")
            cancelled.append({"market_id": market_id, "reason": "TOO_CLOSE_TO_EXPIRY"})
            continue
        fill_cross = _maker_fill_price(cache, side=order_side)
        if fill_cross > 0.0 and float(order.get("price") or 0.0) >= fill_cross:
            filled = fill_open_order_as_position(
                settings,
                order_id=str(order.get("order_id") or ""),
                ts=ts,
                asset=str(market.get("asset") or ""),
            )
            if filled is not None:
                opened.append(filled)
            continue
        current_signal_side = "YES" if "YES" in str(matching_signal.get("signal_type") or "") else "NO" if "NO" in str(matching_signal.get("signal_type") or "") else ""
        if str(matching_signal.get("mode") or "") != "maker" or current_signal_side != order_side:
            cancel_order(settings, order_id=str(order.get("order_id") or ""), reason="EDGE_DECAY")
            cancelled.append({"market_id": market_id, "reason": "EDGE_DECAY"})
            continue
        if quote_seconds >= _quote_life_seconds(settings, tenor_minutes=int(market.get("tenor_minutes") or 0)):
            next_reprices = int(order.get("reprices") or 0) + 1
            cancel_order(settings, order_id=str(order.get("order_id") or ""), reason="REPRICE")
            cancelled.append({"market_id": market_id, "reason": "REPRICE"})
            if next_reprices <= settings.max_reprices and float(matching_signal.get("order_price") or 0.0) > 0.0:
                maker_opened.append(
                    create_order(
                        settings,
                        ts=ts,
                        market_id=market_id,
                        side=order_side,
                        price=float(matching_signal.get("order_price") or 0.0),
                        size=float(order.get("size") or 0.0),
                        mode=str(matching_signal.get("signal_type") or "maker"),
                        reprices=next_reprices,
                    )
                )

    for signal in signals:
        if not bool(signal.get("eligible")):
            continue
        market_id = str(signal.get("market_id") or "")
        if market_id in open_by_market or market_id in open_order_by_market:
            continue
        market = market_by_id.get(market_id)
        if market is None:
            continue
        signal_type = str(signal.get("signal_type") or "")
        side = "YES" if "YES" in signal_type else "NO"
        seconds_left = _seconds_remaining(market)
        force_exit_seconds = settings.force_exit_seconds_5m if int(market.get("tenor_minutes") or 0) <= 5 else settings.force_exit_seconds_15m
        if seconds_left <= force_exit_seconds:
            continue
        block_reason = _recent_trade_block_reason(settings, ts=ts, market_id=market_id, recent_closes=recent_closes)
        if not block_reason:
            block_reason = _eligible_rate_block_reason(settings, signal, signals)
        if block_reason:
            entry_blocks.append({"market_id": market_id, "reason": block_reason})
            continue
        allowed, reason = can_open_new_position(settings, asset=str(signal.get("asset") or ""), side=side)
        if not allowed:
            entry_blocks.append({"market_id": market_id, "reason": reason})
            continue
        if str(signal.get("mode") or "") == "taker":
            entry_price = float(signal.get("yes_ask") or 0.0) if side == "YES" else float(signal.get("no_ask") or 0.0)
            if entry_price <= 0.0 or entry_price < settings.min_trade_price or entry_price > settings.max_trade_price:
                continue
            size = settings.paper_position_notional_usdc / entry_price
            position = create_position(
                settings,
                ts=ts,
                market_id=market_id,
                asset=str(signal.get("asset") or ""),
                side=side,
                entry_price=entry_price,
                size=size,
                mode="taker",
                signal=signal,
            )
            opened.append(position)
            continue
        if str(signal.get("mode") or "") == "maker":
            order_price = float(signal.get("order_price") or 0.0)
            if order_price <= 0.0 or order_price < settings.min_trade_price or order_price > settings.max_trade_price:
                continue
            size = settings.paper_position_notional_usdc / order_price
            maker_opened.append(
                create_order(
                    settings,
                    ts=ts,
                    market_id=market_id,
                    side=side,
                    price=order_price,
                    size=size,
                    mode=signal_type,
                )
            )

    return {
        "opened_positions_count": len(opened),
        "closed_positions_count": len(closed),
        "opened": opened,
        "closed": closed,
        "opened_orders_count": len(maker_opened),
        "opened_orders": maker_opened,
        "cancelled_orders_count": len(cancelled),
        "cancelled_orders": cancelled,
        "entry_blocks_count": len(entry_blocks),
        "entry_blocks": entry_blocks,
        "open_positions_count": len(load_open_positions(settings)),
        "open_orders_count": len(load_open_orders(settings)),
    }


def run_shadow_btc_no_paper_cycle(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
    signals: list[dict[str, Any]],
    ts: str,
) -> dict[str, Any]:
    if not settings.shadow_allow_btc_no_taker:
        return {
            "opened_positions_count": 0,
            "closed_positions_count": 0,
            "opened": [],
            "closed": [],
            "open_positions_count": 0,
        }
    markets = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    market_by_id = {str(item.get("market_id") or ""): item for item in markets if isinstance(item, dict)}
    cache_items = polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else []
    cache_by_id = {str(item.get("market_id") or ""): item for item in cache_items if isinstance(item, dict)}
    open_positions = load_shadow_open_positions(settings)
    latest_books = load_latest_polymarket_books(settings, [str(item.get("market_id") or "") for item in open_positions])
    opened: list[dict[str, Any]] = []
    closed: list[dict[str, Any]] = []
    open_by_market = {str(item.get("market_id") or ""): item for item in open_positions}

    for position in open_positions:
        market_id = str(position.get("market_id") or "")
        market = market_by_id.get(market_id)
        cache = cache_by_id.get(market_id) or latest_books.get(market_id)
        if cache is None:
            continue
        exit_price = _current_exit_price(position, cache)
        if market is None:
            pnl = _pnl(position, exit_price)
            close_shadow_position(settings, position_id=str(position.get("position_id") or ""), ts=ts, exit_price=exit_price, pnl=pnl, reason="MARKET_ROLLED_OFF")
            closed.append({"market_id": market_id, "reason": "MARKET_ROLLED_OFF", "pnl": pnl, "exit_price": exit_price})
            continue
        matching_signal = next((signal for signal in signals if str(signal.get("market_id") or "") == market_id), None)
        seconds_left = _seconds_remaining(market)
        held_seconds = (_ts_to_dt(ts) - _ts_to_dt(str(position.get("entry_ts") or ts))).total_seconds()
        force_exit_seconds = settings.force_exit_seconds_5m if int(position.get("entry_tenor_minutes") or 0) <= 5 else settings.force_exit_seconds_15m
        should_close = False
        reason = ""
        if _stop_loss_hit(settings, position, exit_price):
            should_close = True
            reason = "STOP_LOSS"
        elif _take_profit_hit(settings, position, exit_price):
            should_close = True
            reason = "TAKE_PROFIT"
        elif seconds_left <= force_exit_seconds:
            should_close = True
            reason = "TIME_EXIT"
        elif held_seconds >= settings.min_hold_seconds_before_edge_close and not bool((matching_signal or {}).get("eligible")):
            should_close = True
            reason = "EDGE_CLOSED"
        if should_close:
            pnl = _pnl(position, exit_price)
            close_shadow_position(settings, position_id=str(position.get("position_id") or ""), ts=ts, exit_price=exit_price, pnl=pnl, reason=reason)
            closed.append({"market_id": market_id, "reason": reason, "pnl": pnl, "exit_price": exit_price})

    shadow_open_positions = load_shadow_open_positions(settings)
    shadow_open_by_market = {str(item.get("market_id") or ""): item for item in shadow_open_positions}
    same_direction_count = sum(1 for item in shadow_open_positions if str(item.get("asset") or "").lower() == "btc" and str(item.get("side") or "").upper() == "NO")
    for signal in signals:
        if not bool(signal.get("eligible")):
            continue
        market_id = str(signal.get("market_id") or "")
        if market_id in shadow_open_by_market:
            continue
        market = market_by_id.get(market_id)
        if market is None:
            continue
        if _seconds_remaining(market) <= settings.force_exit_seconds_5m:
            continue
        if len(shadow_open_positions) >= settings.max_simultaneous_positions:
            continue
        if same_direction_count >= settings.max_same_direction_positions_per_asset:
            continue
        entry_price = float(signal.get("no_ask") or 0.0)
        if entry_price <= 0.0 or entry_price < settings.min_trade_price or entry_price > settings.max_trade_price:
            continue
        size = settings.paper_position_notional_usdc / entry_price
        position = create_shadow_position(
            settings,
            ts=ts,
            market_id=market_id,
            asset="btc",
            side="NO",
            entry_price=entry_price,
            size=size,
            mode="shadow_taker",
            signal=signal,
        )
        opened.append(position)
        shadow_open_positions.append(position)
        same_direction_count += 1
    return {
        "opened_positions_count": len(opened),
        "closed_positions_count": len(closed),
        "opened": opened,
        "closed": closed,
        "open_positions_count": len(load_shadow_open_positions(settings)),
    }


def run_shadow_btc_yes_variant_paper_cycle(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
    signals: list[dict[str, Any]],
    ts: str,
) -> dict[str, Any]:
    markets = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    market_by_id = {str(item.get("market_id") or ""): item for item in markets if isinstance(item, dict)}
    cache_items = polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else []
    cache_by_id = {str(item.get("market_id") or ""): item for item in cache_items if isinstance(item, dict)}
    open_positions = load_shadow_variant_open_positions(settings)
    latest_books = load_latest_polymarket_books(settings, [str(item.get("market_id") or "") for item in open_positions])
    signals_by_variant: dict[str, list[dict[str, Any]]] = {}
    for signal in signals:
        variant_id = str(signal.get("variant_id") or "")
        if variant_id:
            signals_by_variant.setdefault(variant_id, []).append(signal)
    opened: list[dict[str, Any]] = []
    closed: list[dict[str, Any]] = []

    for position in open_positions:
        variant_id = str(position.get("variant_id") or "")
        market_id = str(position.get("market_id") or "")
        market = market_by_id.get(market_id)
        cache = cache_by_id.get(market_id) or latest_books.get(market_id)
        if cache is None:
            continue
        exit_price = _current_exit_price(position, cache)
        if market is None:
            pnl = _pnl(position, exit_price)
            close_shadow_variant_position(settings, position_id=str(position.get("position_id") or ""), ts=ts, exit_price=exit_price, pnl=pnl, reason="MARKET_ROLLED_OFF")
            closed.append({"variant_id": variant_id, "market_id": market_id, "reason": "MARKET_ROLLED_OFF", "pnl": pnl, "exit_price": exit_price})
            continue
        matching_signal = next((signal for signal in signals_by_variant.get(variant_id, []) if str(signal.get("market_id") or "") == market_id), None)
        seconds_left = _seconds_remaining(market)
        held_seconds = (_ts_to_dt(ts) - _ts_to_dt(str(position.get("entry_ts") or ts))).total_seconds()
        entry_tenor = int(position.get("entry_tenor_minutes") or market.get("tenor_minutes") or 5)
        force_exit_seconds = settings.force_exit_seconds_5m if entry_tenor <= 5 else settings.force_exit_seconds_15m
        should_close = False
        reason = ""
        if _stop_loss_hit(settings, position, exit_price):
            should_close = True
            reason = "STOP_LOSS"
        elif _take_profit_hit(settings, position, exit_price):
            should_close = True
            reason = "TAKE_PROFIT"
        elif seconds_left <= force_exit_seconds:
            should_close = True
            reason = "TIME_EXIT"
        elif held_seconds >= settings.min_hold_seconds_before_edge_close and not bool((matching_signal or {}).get("eligible")):
            should_close = True
            reason = "EDGE_CLOSED"
        if should_close:
            pnl = _pnl(position, exit_price)
            close_shadow_variant_position(settings, position_id=str(position.get("position_id") or ""), ts=ts, exit_price=exit_price, pnl=pnl, reason=reason)
            closed.append({"variant_id": variant_id, "market_id": market_id, "reason": reason, "pnl": pnl, "exit_price": exit_price})

    open_positions = load_shadow_variant_open_positions(settings)
    open_by_variant_market = {
        (str(item.get("variant_id") or ""), str(item.get("market_id") or "")): item for item in open_positions
    }
    open_count_by_variant: dict[str, int] = {}
    for item in open_positions:
        variant_id = str(item.get("variant_id") or "")
        open_count_by_variant[variant_id] = open_count_by_variant.get(variant_id, 0) + 1

    for variant_id, variant_signals in signals_by_variant.items():
        for signal in variant_signals:
            if not bool(signal.get("eligible")):
                continue
            market_id = str(signal.get("market_id") or "")
            if (variant_id, market_id) in open_by_variant_market:
                continue
            market = market_by_id.get(market_id)
            if market is None:
                continue
            side = str(signal.get("side") or _signal_side(signal)).upper()
            asset = str(signal.get("asset") or market.get("asset") or "btc").lower()
            tenor = int(signal.get("tenor_minutes") or market.get("tenor_minutes") or 5)
            force_exit_seconds = settings.force_exit_seconds_5m if tenor <= 5 else settings.force_exit_seconds_15m
            if _seconds_remaining(market) <= force_exit_seconds:
                continue
            if open_count_by_variant.get(variant_id, 0) >= settings.max_same_direction_positions_per_asset:
                continue
            entry_price = float(signal.get("order_price") or (signal.get("yes_ask") if side == "YES" else signal.get("no_ask")) or 0.0)
            if entry_price <= 0.0 or entry_price < settings.min_trade_price or entry_price > settings.max_trade_price:
                continue
            size = settings.paper_position_notional_usdc / entry_price
            position = create_shadow_variant_position(
                settings,
                ts=ts,
                variant_id=variant_id,
                market_id=market_id,
                asset=asset,
                side=side,
                entry_price=entry_price,
                size=size,
                mode=f"shadow_{side.lower()}_taker",
                signal=signal,
            )
            opened.append(position)
            open_by_variant_market[(variant_id, market_id)] = position
            open_count_by_variant[variant_id] = open_count_by_variant.get(variant_id, 0) + 1
    return {
        "opened_positions_count": len(opened),
        "closed_positions_count": len(closed),
        "opened": opened,
        "closed": closed,
        "open_positions_count": len(load_shadow_variant_open_positions(settings)),
    }


def run_promoted_variant_paper_cycle(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
    signals: list[dict[str, Any]],
    ts: str,
) -> dict[str, Any]:
    promoted_ids = tuple(dict.fromkeys(str(item).strip().lower() for item in settings.promoted_variant_ids if str(item).strip()))
    if not promoted_ids:
        return {
            "enabled_variant_ids": [],
            "opened_positions_count": 0,
            "closed_positions_count": 0,
            "opened": [],
            "closed": [],
            "entry_blocks_count": 0,
            "entry_blocks": [],
            "open_positions_count": 0,
        }

    markets = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    market_by_id = {str(item.get("market_id") or ""): item for item in markets if isinstance(item, dict)}
    cache_items = polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else []
    cache_by_id = {str(item.get("market_id") or ""): item for item in cache_items if isinstance(item, dict)}
    signals_by_variant: dict[str, list[dict[str, Any]]] = {}
    for signal in signals:
        variant_id = str(signal.get("variant_id") or "").lower()
        if variant_id in promoted_ids:
            signals_by_variant.setdefault(variant_id, []).append(signal)

    opened: list[dict[str, Any]] = []
    closed: list[dict[str, Any]] = []
    entry_blocks: list[dict[str, Any]] = []

    open_positions = load_open_positions(settings)
    promoted_open_positions = [position for position in open_positions if _promoted_variant_id(position)]
    latest_books = load_latest_polymarket_books(settings, [str(item.get("market_id") or "") for item in promoted_open_positions])

    for position in promoted_open_positions:
        variant_id = _promoted_variant_id(position)
        market_id = str(position.get("market_id") or "")
        market = market_by_id.get(market_id)
        cache = cache_by_id.get(market_id) or latest_books.get(market_id)
        if cache is None:
            continue
        exit_price = _current_exit_price(position, cache)
        if market is None:
            pnl = _pnl(position, exit_price)
            close_position(settings, position_id=str(position.get("position_id") or ""), ts=ts, exit_price=exit_price, pnl=pnl, reason="MARKET_ROLLED_OFF")
            closed.append({"variant_id": variant_id, "market_id": market_id, "reason": "MARKET_ROLLED_OFF", "pnl": pnl, "exit_price": exit_price})
            continue
        matching_signal = next((signal for signal in signals_by_variant.get(variant_id, []) if str(signal.get("market_id") or "") == market_id), None)
        seconds_left = _seconds_remaining(market)
        held_seconds = (_ts_to_dt(ts) - _ts_to_dt(str(position.get("entry_ts") or ts))).total_seconds()
        entry_tenor = int(position.get("entry_tenor_minutes") or market.get("tenor_minutes") or 5)
        force_exit_seconds = settings.force_exit_seconds_5m if entry_tenor <= 5 else settings.force_exit_seconds_15m
        should_close = False
        reason = ""
        if _stop_loss_hit(settings, position, exit_price):
            should_close = True
            reason = "STOP_LOSS"
        elif _take_profit_hit(settings, position, exit_price):
            should_close = True
            reason = "TAKE_PROFIT"
        elif seconds_left <= force_exit_seconds:
            should_close = True
            reason = "TIME_EXIT"
        elif held_seconds >= settings.min_hold_seconds_before_edge_close and not bool((matching_signal or {}).get("eligible")):
            should_close = True
            reason = "EDGE_CLOSED"
        if should_close:
            pnl = _pnl(position, exit_price)
            close_position(settings, position_id=str(position.get("position_id") or ""), ts=ts, exit_price=exit_price, pnl=pnl, reason=reason)
            closed.append({"variant_id": variant_id, "market_id": market_id, "reason": reason, "pnl": pnl, "exit_price": exit_price})

    open_positions = load_open_positions(settings)
    open_orders = load_open_orders(settings)
    open_market_ids = {str(item.get("market_id") or "") for item in open_positions}
    open_market_ids.update(str(item.get("market_id") or "") for item in open_orders)
    promoted_open_count = sum(1 for position in open_positions if _promoted_variant_id(position))
    recent_closes = load_recent_position_closes(
        settings,
        limit=max(settings.recent_loss_window_trades, settings.stop_loss_streak_pause_count, 20),
    )

    variant_rank = {variant_id: idx for idx, variant_id in enumerate(promoted_ids)}
    candidates = [
        signal
        for variant_signals in signals_by_variant.values()
        for signal in variant_signals
        if bool(signal.get("eligible"))
    ]
    candidates.sort(
        key=lambda signal: (
            -float(signal.get("edge") or 0.0),
            -float(signal.get("fair_yes") or signal.get("fair_no") or 0.0),
            variant_rank.get(str(signal.get("variant_id") or "").lower(), 9999),
        )
    )

    for signal in candidates:
        if promoted_open_count >= max(settings.promoted_variant_max_open_positions, 0):
            break
        variant_id = str(signal.get("variant_id") or "").lower()
        market_id = str(signal.get("market_id") or "")
        if market_id in open_market_ids:
            continue
        market = market_by_id.get(market_id)
        if market is None:
            continue
        side = str(signal.get("side") or _signal_side(signal)).upper()
        if side not in {"YES", "NO"}:
            continue
        asset = str(signal.get("asset") or market.get("asset") or "").lower()
        tenor = int(signal.get("tenor_minutes") or market.get("tenor_minutes") or 5)
        force_exit_seconds = settings.force_exit_seconds_5m if tenor <= 5 else settings.force_exit_seconds_15m
        if _seconds_remaining(market) <= force_exit_seconds:
            continue
        block_reason = _recent_trade_block_reason(settings, ts=ts, market_id=market_id, recent_closes=recent_closes)
        if block_reason:
            entry_blocks.append({"variant_id": variant_id, "market_id": market_id, "reason": block_reason})
            continue
        allowed, reason = can_open_new_position(settings, asset=asset, side=side)
        if not allowed:
            entry_blocks.append({"variant_id": variant_id, "market_id": market_id, "reason": reason})
            continue
        entry_price = float(signal.get("order_price") or (signal.get("yes_ask") if side == "YES" else signal.get("no_ask")) or 0.0)
        if entry_price <= 0.0 or entry_price < settings.min_trade_price or entry_price > settings.max_trade_price:
            continue
        size = settings.paper_position_notional_usdc / entry_price
        position = create_position(
            settings,
            ts=ts,
            market_id=market_id,
            asset=asset,
            side=side,
            entry_price=entry_price,
            size=size,
            mode=f"{PROMOTED_VARIANT_MODE_PREFIX}{variant_id}",
            signal=signal,
        )
        opened.append(position)
        open_market_ids.add(market_id)
        promoted_open_count += 1

    return {
        "enabled_variant_ids": list(promoted_ids),
        "opened_positions_count": len(opened),
        "closed_positions_count": len(closed),
        "opened": opened,
        "closed": closed,
        "entry_blocks_count": len(entry_blocks),
        "entry_blocks": entry_blocks,
        "open_positions_count": sum(1 for position in load_open_positions(settings) if _promoted_variant_id(position)),
    }


def run_complete_set_arb_paper_cycle(
    settings: LatencyBotSettings,
    *,
    signals: list[dict[str, Any]],
    ts: str,
) -> dict[str, Any]:
    if not settings.complete_set_arb_enabled:
        return {
            "execution_policy": settings.complete_set_arb_execution_policy,
            "simulated_atomicity": "paired_fok_batch",
            "opened_positions_count": 0,
            "closed_positions_count": 0,
            "opened": [],
            "closed": [],
            "entry_blocks_count": 0,
            "entry_blocks": [],
        }
    opened: list[dict[str, Any]] = []
    closed: list[dict[str, Any]] = []
    entry_blocks: list[dict[str, Any]] = []
    candidates = [signal for signal in signals if bool(signal.get("eligible"))]
    candidates.sort(key=lambda signal: float(signal.get("net_edge") or 0.0), reverse=True)
    for signal in candidates:
        if len(opened) >= max(settings.complete_set_arb_max_sets_per_cycle, 0):
            break
        market_id = str(signal.get("market_id") or "")
        if not market_id:
            continue
        if _complete_set_recently_traded(settings, market_id=market_id, ts=ts):
            entry_blocks.append({"market_id": market_id, "reason": "same market complete-set cooldown"})
            continue
        yes_entry = float(signal.get("yes_ask") or 0.0)
        no_entry = float(signal.get("no_ask") or 0.0)
        total_cost = yes_entry + no_entry
        if total_cost <= 0.0:
            continue
        executable_depth_usdc = float(signal.get("executable_depth_usdc") or 0.0)
        notional = min(settings.complete_set_arb_notional_usdc, executable_depth_usdc)
        if notional <= 0.0:
            continue
        size = notional / total_cost
        yes_required_usdc = yes_entry * size
        no_required_usdc = no_entry * size
        yes_depth_usdc = float(signal.get("yes_depth_usdc") or executable_depth_usdc)
        no_depth_usdc = float(signal.get("no_depth_usdc") or executable_depth_usdc)
        # Paper model for a real paired FOK attempt: both marketable legs must be able
        # to fill in full, otherwise neither side should be counted as executed.
        if yes_required_usdc > yes_depth_usdc or no_required_usdc > no_depth_usdc:
            entry_blocks.append(
                {
                    "market_id": market_id,
                    "reason": "paired FOK leg depth check failed",
                    "yes_required_usdc": round(yes_required_usdc, 6),
                    "no_required_usdc": round(no_required_usdc, 6),
                    "yes_depth_usdc": round(yes_depth_usdc, 6),
                    "no_depth_usdc": round(no_depth_usdc, 6),
                }
            )
            continue
        net_edge = float(signal.get("net_edge") or 0.0)
        gross_edge = float(signal.get("gross_edge") or 0.0)
        position = create_complete_set_arb_position(
            settings,
            ts=ts,
            market_id=market_id,
            asset=str(signal.get("asset") or ""),
            tenor_minutes=int(signal.get("tenor_minutes") or 0),
            yes_entry_price=yes_entry,
            no_entry_price=no_entry,
            size=size,
            notional_usdc=notional,
            gross_edge=gross_edge,
            net_edge=net_edge,
        )
        opened.append(position)
        pnl = round(net_edge * size, 6)
        close_complete_set_arb_position(
            settings,
            position_id=str(position.get("position_id") or ""),
            ts=ts,
            payout_price=1.0,
            pnl=pnl,
            reason="COMPLETE_SET_LOCKED",
        )
        closed.append({"market_id": market_id, "pnl": pnl, "net_edge": net_edge, "size": size, "reason": "COMPLETE_SET_LOCKED"})
    return {
        "execution_policy": settings.complete_set_arb_execution_policy,
        "simulated_atomicity": "paired_fok_batch",
        "opened_positions_count": len(opened),
        "closed_positions_count": len(closed),
        "opened": opened,
        "closed": closed,
        "entry_blocks_count": len(entry_blocks),
        "entry_blocks": entry_blocks,
    }
