from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from ..config import LatencyBotSettings
from ..risk.limits import can_open_new_position
from ..storage import (
    cancel_order,
    close_cex_latency_paper_position,
    close_late_resolution_capture_position,
    close_temporal_inventory_quote,
    close_position,
    close_complete_set_arb_position,
    close_shadow_position,
    close_shadow_variant_position,
    connect_latency_bot_db,
    create_cex_latency_paper_position,
    create_late_resolution_capture_position,
    create_order,
    create_complete_set_arb_position,
    create_position,
    create_shadow_position,
    create_shadow_variant_position,
    create_temporal_inventory_quote,
    fill_open_order_as_position,
    load_latest_polymarket_books,
    load_cex_latency_paper_open_positions,
    load_late_resolution_capture_open_positions,
    load_open_orders,
    load_open_positions,
    load_recent_position_closes,
    load_shadow_open_positions,
    load_shadow_variant_open_positions,
    load_temporal_inventory_open_markets,
    load_temporal_inventory_open_quotes,
    record_temporal_inventory_event,
    upsert_temporal_inventory_market,
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


def _cex_latency_recently_traded(
    settings: LatencyBotSettings,
    *,
    market_id: str,
    ts: str,
    mode: str = "cex_latency_paper",
    cooldown_seconds: float | None = None,
) -> bool:
    cooldown = settings.cex_latency_paper_same_market_cooldown_seconds if cooldown_seconds is None else cooldown_seconds
    if cooldown <= 0:
        return False
    cutoff = _ts_to_dt(ts).timestamp() - float(cooldown)
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT entry_ts
            FROM cex_latency_paper_positions
            WHERE market_id = ?
              AND mode = ?
            ORDER BY entry_ts DESC
            LIMIT 5
            """,
            (market_id, mode),
        ).fetchall()
    for row in rows:
        try:
            if _ts_to_dt(str(row["entry_ts"] or "")).timestamp() >= cutoff:
                return True
        except Exception:
            continue
    return False


def _cex_latency_exit_price(position: dict[str, Any], cache: dict[str, Any]) -> float:
    side = str(position.get("side") or "").upper()
    best_bid = float(cache.get("best_bid") or 0.0)
    best_ask = float(cache.get("best_ask") or 0.0)
    if side == "YES":
        return float(cache.get("bid_vwap") or best_bid)
    no_bid = cache.get("no_best_bid")
    no_bid_vwap = cache.get("no_bid_vwap")
    if no_bid_vwap is not None:
        return float(no_bid_vwap or 0.0)
    if no_bid is not None:
        return float(no_bid or 0.0)
    return max(1.0 - best_ask, 0.0)


def _cex_latency_pnl(position: dict[str, Any], exit_price: float) -> float:
    size = float(position.get("size") or 0.0)
    entry = float(position.get("entry_price") or 0.0)
    return round((exit_price - entry) * size, 6)


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


def _run_directional_paper_cycle(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
    signals: list[dict[str, Any]],
    ts: str,
    mode: str,
    enabled: bool,
    capital_usdc: float,
    notional_usdc: float,
    max_open_positions: int,
    min_trade_price: float,
    max_trade_price: float,
    stop_loss_fraction: float,
    take_profit_fraction: float,
    exit_edge_floor: float,
    force_exit_seconds: int,
    same_market_cooldown_seconds: int,
    price_band_reason: str,
) -> dict[str, Any]:
    if not enabled:
        return {
            "opened_positions_count": 0,
            "closed_positions_count": 0,
            "opened": [],
            "closed": [],
            "entry_blocks_count": 0,
            "entry_blocks": [],
            "open_positions_count": 0,
            "open_capital_usdc": 0.0,
        }
    markets = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    market_by_id = {str(item.get("market_id") or ""): item for item in markets if isinstance(item, dict)}
    cache_items = polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else []
    cache_by_id = {str(item.get("market_id") or ""): item for item in cache_items if isinstance(item, dict)}
    open_positions = load_cex_latency_paper_open_positions(settings, mode=mode)
    latest_books = load_latest_polymarket_books(settings, [str(item.get("market_id") or "") for item in open_positions])
    signal_by_market = {str(item.get("market_id") or ""): item for item in signals if isinstance(item, dict)}
    opened: list[dict[str, Any]] = []
    closed: list[dict[str, Any]] = []
    entry_blocks: list[dict[str, Any]] = []

    for position in open_positions:
        market_id = str(position.get("market_id") or "")
        market = market_by_id.get(market_id)
        cache = cache_by_id.get(market_id) or latest_books.get(market_id)
        if cache is None:
            continue
        matching_signal = signal_by_market.get(market_id)
        exit_price = _cex_latency_exit_price(position, cache)
        position_side = str(position.get("side") or "").upper()
        signal_side = str((matching_signal or {}).get("side") or "").upper()
        held_seconds = (_ts_to_dt(ts) - _ts_to_dt(str(position.get("entry_ts") or ts))).total_seconds()
        entry_price = float(position.get("entry_price") or 0.0)
        current_edge = float((matching_signal or {}).get("edge") or 0.0) if signal_side == position_side else 0.0
        should_close = False
        reason = ""
        if market is None:
            should_close = True
            reason = "MARKET_ROLLED_OFF"
        elif exit_price <= 0.0:
            should_close = False
        elif entry_price > 0.0 and exit_price <= entry_price * (1.0 - stop_loss_fraction):
            should_close = True
            reason = "STOP_LOSS"
        elif entry_price > 0.0 and exit_price >= entry_price * (1.0 + take_profit_fraction):
            should_close = True
            reason = "TAKE_PROFIT"
        elif _seconds_remaining(market) <= force_exit_seconds:
            should_close = True
            reason = "TIME_EXIT"
        elif (
            held_seconds >= settings.min_hold_seconds_before_edge_close
            and (
                matching_signal is None
                or not bool(matching_signal.get("eligible"))
                or signal_side != position_side
                or current_edge < exit_edge_floor
            )
        ):
            should_close = True
            reason = "EDGE_CLOSED"
        if should_close:
            pnl = _cex_latency_pnl(position, exit_price)
            close_cex_latency_paper_position(
                settings,
                position_id=str(position.get("position_id") or ""),
                ts=ts,
                exit_price=exit_price,
                pnl=pnl,
                reason=reason,
                edge=current_edge,
            )
            closed.append({"market_id": market_id, "reason": reason, "pnl": pnl, "exit_price": exit_price})

    open_positions = load_cex_latency_paper_open_positions(settings, mode=mode)
    open_by_market = {str(item.get("market_id") or ""): item for item in open_positions}
    open_capital = round(sum(float(item.get("notional_usdc") or 0.0) for item in open_positions), 6)
    available_capital = max(float(capital_usdc) - open_capital, 0.0)
    eligible_signals = sorted(
        [item for item in signals if bool(item.get("eligible"))],
        key=lambda item: float(item.get("edge") or 0.0),
        reverse=True,
    )
    for signal in eligible_signals:
        if len(open_positions) >= max_open_positions:
            break
        market_id = str(signal.get("market_id") or "")
        if not market_id or market_id in open_by_market:
            continue
        market = market_by_id.get(market_id)
        if market is None:
            entry_blocks.append({"market_id": market_id, "reason": "market unavailable"})
            continue
        if _seconds_remaining(market) <= force_exit_seconds:
            entry_blocks.append({"market_id": market_id, "reason": "too close to expiry"})
            continue
        if _cex_latency_recently_traded(
            settings,
            market_id=market_id,
            ts=ts,
            mode=mode,
            cooldown_seconds=float(same_market_cooldown_seconds),
        ):
            entry_blocks.append({"market_id": market_id, "reason": "same market cooldown"})
            continue
        side = str(signal.get("side") or "").upper()
        if side not in {"YES", "NO"}:
            side = "YES" if "YES" in str(signal.get("signal_type") or "") else "NO"
        entry_price = float(signal.get("order_price") or 0.0)
        if entry_price <= 0.0:
            entry_price = float(signal.get("yes_ask") or 0.0) if side == "YES" else float(signal.get("no_ask") or 0.0)
        if entry_price <= 0.0 or entry_price < min_trade_price or entry_price > max_trade_price:
            entry_blocks.append({"market_id": market_id, "reason": price_band_reason})
            continue
        notional = min(float(notional_usdc), available_capital)
        if notional < 5.0:
            entry_blocks.append({"market_id": market_id, "reason": "insufficient paper capital"})
            break
        size = notional / entry_price
        position = create_cex_latency_paper_position(
            settings,
            ts=ts,
            market_id=market_id,
            asset=str(signal.get("asset") or ""),
            side=side,
            entry_price=entry_price,
            size=size,
            signal=signal,
            mode=mode,
        )
        opened.append(position)
        open_positions.append(position)
        open_by_market[market_id] = position
        open_capital = round(open_capital + notional, 6)
        available_capital = max(float(capital_usdc) - open_capital, 0.0)

    final_open_positions = load_cex_latency_paper_open_positions(settings, mode=mode)
    return {
        "opened_positions_count": len(opened),
        "closed_positions_count": len(closed),
        "opened": opened,
        "closed": closed,
        "entry_blocks_count": len(entry_blocks),
        "entry_blocks": entry_blocks,
        "open_positions_count": len(final_open_positions),
        "open_capital_usdc": round(sum(float(item.get("notional_usdc") or 0.0) for item in final_open_positions), 6),
    }


def run_cex_latency_paper_cycle(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
    signals: list[dict[str, Any]],
    ts: str,
) -> dict[str, Any]:
    return _run_directional_paper_cycle(
        settings,
        markets_payload=markets_payload,
        polymarket_cache=polymarket_cache,
        signals=signals,
        ts=ts,
        mode="cex_latency_paper",
        enabled=bool(settings.cex_latency_paper_enabled),
        capital_usdc=float(settings.cex_latency_paper_capital_usdc),
        notional_usdc=float(settings.cex_latency_paper_notional_usdc),
        max_open_positions=int(settings.cex_latency_paper_max_open_positions),
        min_trade_price=float(settings.cex_latency_paper_min_trade_price),
        max_trade_price=float(settings.cex_latency_paper_max_trade_price),
        stop_loss_fraction=float(settings.cex_latency_paper_stop_loss_fraction),
        take_profit_fraction=float(settings.cex_latency_paper_take_profit_fraction),
        exit_edge_floor=float(settings.cex_latency_paper_exit_edge_floor),
        force_exit_seconds=int(settings.cex_latency_paper_force_exit_seconds),
        same_market_cooldown_seconds=int(settings.cex_latency_paper_same_market_cooldown_seconds),
        price_band_reason="price outside cex-latency band",
    )


def run_btc_fair_value_paper_cycle(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
    signals: list[dict[str, Any]],
    ts: str,
) -> dict[str, Any]:
    return _run_directional_paper_cycle(
        settings,
        markets_payload=markets_payload,
        polymarket_cache=polymarket_cache,
        signals=signals,
        ts=ts,
        mode="btc_fair_value_paper",
        enabled=bool(settings.btc_fair_value_paper_enabled),
        capital_usdc=float(settings.btc_fair_value_paper_capital_usdc),
        notional_usdc=float(settings.btc_fair_value_paper_notional_usdc),
        max_open_positions=int(settings.btc_fair_value_paper_max_open_positions),
        min_trade_price=float(settings.btc_fair_value_paper_min_trade_price),
        max_trade_price=float(settings.btc_fair_value_paper_max_trade_price),
        stop_loss_fraction=float(settings.btc_fair_value_paper_stop_loss_fraction),
        take_profit_fraction=float(settings.btc_fair_value_paper_take_profit_fraction),
        exit_edge_floor=float(settings.btc_fair_value_paper_exit_edge_floor),
        force_exit_seconds=int(settings.btc_fair_value_paper_force_exit_seconds),
        same_market_cooldown_seconds=int(settings.btc_fair_value_paper_same_market_cooldown_seconds),
        price_band_reason="price outside btc fair-value band",
    )


def run_wallet_teacher_sniper_paper_cycle(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
    signals: list[dict[str, Any]],
    ts: str,
) -> dict[str, Any]:
    return _run_directional_paper_cycle(
        settings,
        markets_payload=markets_payload,
        polymarket_cache=polymarket_cache,
        signals=signals,
        ts=ts,
        mode="wallet_teacher_sniper",
        enabled=bool(settings.wallet_teacher_sniper_enabled),
        capital_usdc=float(settings.wallet_teacher_sniper_capital_usdc),
        notional_usdc=float(settings.wallet_teacher_sniper_notional_usdc),
        max_open_positions=int(settings.wallet_teacher_sniper_max_open_positions),
        min_trade_price=float(settings.wallet_teacher_sniper_min_trade_price),
        max_trade_price=float(settings.wallet_teacher_sniper_max_trade_price),
        stop_loss_fraction=float(settings.wallet_teacher_sniper_stop_loss_fraction),
        take_profit_fraction=float(settings.wallet_teacher_sniper_take_profit_fraction),
        exit_edge_floor=float(settings.wallet_teacher_sniper_exit_edge_floor),
        force_exit_seconds=int(settings.wallet_teacher_sniper_force_exit_seconds),
        same_market_cooldown_seconds=int(settings.wallet_teacher_sniper_same_market_cooldown_seconds),
        price_band_reason="price outside wallet-teacher band",
    )


def _temporal_empty_market_row(signal: dict[str, Any], market: dict[str, Any] | None, ts: str) -> dict[str, Any]:
    return {
        "market_id": str(signal.get("market_id") or (market or {}).get("market_id") or ""),
        "asset": str(signal.get("asset") or (market or {}).get("asset") or ""),
        "tenor_minutes": int(signal.get("tenor_minutes") or (market or {}).get("tenor_minutes") or 0),
        "first_seen_ts": ts,
        "updated_ts": ts,
        "state": "FLAT",
        "yes_shares": 0.0,
        "no_shares": 0.0,
        "yes_cost_usdc": 0.0,
        "no_cost_usdc": 0.0,
        "realized_pnl_usdc": 0.0,
        "expired_inventory_cost_usdc": 0.0,
        "locked_pair_shares": 0.0,
        "locked_pair_cost": 0.0,
        "locked_pair_pnl_usdc": 0.0,
        "last_signal_side": str(signal.get("side") or ""),
        "last_signal_edge": float(signal.get("edge") or 0.0),
        "last_quote_id": "",
    }


def _temporal_pair_metrics(row: dict[str, Any]) -> tuple[float, float, float]:
    yes_shares = float(row.get("yes_shares") or 0.0)
    no_shares = float(row.get("no_shares") or 0.0)
    yes_cost = float(row.get("yes_cost_usdc") or 0.0)
    no_cost = float(row.get("no_cost_usdc") or 0.0)
    paired = min(yes_shares, no_shares)
    if paired <= 0.0:
        return 0.0, 0.0, 0.0
    avg_yes = yes_cost / yes_shares if yes_shares > 0.0 else 0.0
    avg_no = no_cost / no_shares if no_shares > 0.0 else 0.0
    pair_cost = avg_yes + avg_no
    pair_pnl = paired * (1.0 - pair_cost)
    return round(paired, 8), round(pair_cost, 8), round(pair_pnl, 8)


def _temporal_state_from_inventory(row: dict[str, Any], *, max_pair_cost: float) -> str:
    yes_shares = float(row.get("yes_shares") or 0.0)
    no_shares = float(row.get("no_shares") or 0.0)
    if yes_shares <= 0.0 and no_shares <= 0.0:
        return "FLAT"
    paired, pair_cost, _pair_pnl = _temporal_pair_metrics(row)
    if paired > 0.0 and abs(yes_shares - no_shares) < 1e-8 and pair_cost <= max_pair_cost:
        return "LOCKED_PAIR"
    if paired > 0.0:
        return "HEDGING"
    return "SEEDED"


def _temporal_persist_market(settings: LatencyBotSettings, row: dict[str, Any], *, ts: str) -> dict[str, Any]:
    return upsert_temporal_inventory_market(
        settings,
        ts=ts,
        market_id=str(row.get("market_id") or ""),
        asset=str(row.get("asset") or ""),
        tenor_minutes=int(row.get("tenor_minutes") or 0),
        state=str(row.get("state") or "FLAT"),
        yes_shares=float(row.get("yes_shares") or 0.0),
        no_shares=float(row.get("no_shares") or 0.0),
        yes_cost_usdc=float(row.get("yes_cost_usdc") or 0.0),
        no_cost_usdc=float(row.get("no_cost_usdc") or 0.0),
        realized_pnl_usdc=float(row.get("realized_pnl_usdc") or 0.0),
        expired_inventory_cost_usdc=float(row.get("expired_inventory_cost_usdc") or 0.0),
        locked_pair_shares=float(row.get("locked_pair_shares") or 0.0),
        locked_pair_cost=float(row.get("locked_pair_cost") or 0.0),
        locked_pair_pnl_usdc=float(row.get("locked_pair_pnl_usdc") or 0.0),
        last_signal_side=str(row.get("last_signal_side") or ""),
        last_signal_edge=float(row.get("last_signal_edge") or 0.0),
        last_quote_id=str(row.get("last_quote_id") or ""),
    )


def _temporal_book_side(cache: dict[str, Any], side: str) -> tuple[float, float]:
    side = side.upper()
    if side == "YES":
        return float(cache.get("best_bid") or 0.0), float(cache.get("best_ask") or 0.0)
    no_bid = float(cache.get("no_best_bid") or 0.0)
    no_ask = float(cache.get("no_best_ask") or 0.0)
    if no_bid <= 0.0:
        best_ask = float(cache.get("best_ask") or 0.0)
        no_bid = max(1.0 - best_ask, 0.0) if best_ask > 0.0 else 0.0
    if no_ask <= 0.0:
        best_bid = float(cache.get("best_bid") or 0.0)
        no_ask = max(1.0 - best_bid, 0.0) if best_bid > 0.0 else 0.0
    return no_bid, no_ask


def _temporal_post_only_quote_price(cache: dict[str, Any], side: str, ceiling: float | None = None) -> float:
    best_bid, best_ask = _temporal_book_side(cache, side)
    if best_bid <= 0.0:
        return 0.0
    tick = 0.01
    quote_price = best_bid + tick
    if best_ask > tick:
        quote_price = min(quote_price, best_ask - tick)
    if ceiling is not None:
        quote_price = min(quote_price, float(ceiling))
    return round(max(quote_price, 0.0), 4)


def _temporal_quote_fill(quote: dict[str, Any], cache: dict[str, Any]) -> tuple[float, float, float]:
    side = str(quote.get("side") or "").upper()
    quote_price = float(quote.get("price") or 0.0)
    quote_size = float(quote.get("size") or 0.0)
    best_bid, best_ask = _temporal_book_side(cache, side)
    if quote_price <= 0.0 or quote_size <= 0.0 or best_ask <= 0.0 or best_ask > quote_price:
        return 0.0, 0.0, 0.0
    moved_through = best_ask < quote_price - 0.005
    fill_fraction = 1.0 if moved_through else 0.50
    fill_size = round(quote_size * fill_fraction, 8)
    adverse_loss = max(quote_price - best_bid, 0.0) * fill_size if moved_through else 0.0
    return quote_price, fill_size, round(adverse_loss, 8)


def _temporal_quote_ttl_seconds(settings: LatencyBotSettings, quote: dict[str, Any]) -> int:
    explicit = int(float(quote.get("ttl_seconds") or 0))
    if explicit > 0:
        return explicit
    style = str(quote.get("quote_style") or "").upper()
    reason = str(quote.get("reason") or "").lower()
    if style == "HEDGE_LOCK" or "hedge" in reason:
        return max(int(settings.temporal_inventory_maker_paper_hedge_ttl_seconds), 1)
    if style in {"MID_AGGRESSIVE", "NEAR_TOUCH"}:
        return max(int(settings.temporal_inventory_maker_paper_high_edge_ttl_seconds), 1)
    return max(int(settings.temporal_inventory_maker_paper_quote_ttl_seconds), 1)


def _temporal_ttl_for_style(settings: LatencyBotSettings, style: str) -> int:
    style = style.upper()
    if style == "HEDGE_LOCK":
        return max(int(settings.temporal_inventory_maker_paper_hedge_ttl_seconds), 1)
    if style in {"MID_AGGRESSIVE", "NEAR_TOUCH"}:
        return max(int(settings.temporal_inventory_maker_paper_high_edge_ttl_seconds), 1)
    return max(int(settings.temporal_inventory_maker_paper_quote_ttl_seconds), 1)


def _temporal_quote_expected_value(
    *,
    quote_price: float,
    notional: float,
    edge: float,
    fill_probability: float,
    spread: float,
) -> float:
    if quote_price <= 0.0 or notional <= 0.0 or edge <= 0.0 or fill_probability <= 0.0:
        return 0.0
    shares = notional / quote_price
    adverse_penalty = min(max(spread, 0.0) * 0.10, 0.025) * shares * fill_probability
    return round(edge * shares * fill_probability - adverse_penalty, 6)


def _temporal_estimate_fill_probability(
    *,
    best_bid: float,
    best_ask: float,
    quote_price: float,
    edge: float,
) -> float:
    if best_bid <= 0.0 or best_ask <= 0.0 or quote_price <= 0.0 or best_ask <= best_bid:
        return 0.0
    spread = max(best_ask - best_bid, 0.01)
    proximity = 1.0 - min(max((best_ask - quote_price) / spread, 0.0), 1.0)
    return round(min(max((0.02 + 0.70 * proximity) * (1.0 + min(max(edge, 0.0), 0.20)), 0.0), 0.85), 6)


def _temporal_daily_realized_pnl(settings: LatencyBotSettings) -> float:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat().replace("+00:00", "Z")
    with connect_latency_bot_db(settings) as conn:
        row = conn.execute(
            """
            SELECT SUM(COALESCE(pnl_usdc, 0.0)) AS pnl
            FROM temporal_inventory_events
            WHERE ts >= ?
              AND event_type IN ('SELL', 'EXPIRE', 'RESOLVE')
            """,
            (cutoff,),
        ).fetchone()
    return float(row["pnl"] or 0.0) if row else 0.0


def _temporal_total_exposure(rows: list[dict[str, Any]], quotes: list[dict[str, Any]]) -> float:
    inventory = sum(
        float(row.get("yes_cost_usdc") or 0.0) + float(row.get("no_cost_usdc") or 0.0)
        for row in rows
        if str(row.get("state") or "") != "CLOSED"
    )
    reserved = sum(float(quote.get("notional_usdc") or 0.0) for quote in quotes if str(quote.get("status") or "") == "open")
    return round(inventory + reserved, 8)


def _temporal_market_exposure(row: dict[str, Any], quotes: list[dict[str, Any]]) -> float:
    market_id = str(row.get("market_id") or "")
    inventory = float(row.get("yes_cost_usdc") or 0.0) + float(row.get("no_cost_usdc") or 0.0)
    reserved = sum(
        float(quote.get("notional_usdc") or 0.0)
        for quote in quotes
        if str(quote.get("market_id") or "") == market_id and str(quote.get("status") or "") == "open"
    )
    return round(inventory + reserved, 8)


def _temporal_sell_side(
    settings: LatencyBotSettings,
    row: dict[str, Any],
    *,
    ts: str,
    cache: dict[str, Any],
    side: str,
    size: float,
    reason: str,
    event_type: str = "SELL",
) -> dict[str, Any]:
    side = side.upper()
    shares_key = "yes_shares" if side == "YES" else "no_shares"
    cost_key = "yes_cost_usdc" if side == "YES" else "no_cost_usdc"
    shares = min(float(row.get(shares_key) or 0.0), max(float(size), 0.0))
    if shares <= 0.0:
        return row
    side_shares = float(row.get(shares_key) or 0.0)
    side_cost = float(row.get(cost_key) or 0.0)
    avg_cost = side_cost / side_shares if side_shares > 0.0 else 0.0
    bid, _ask = _temporal_book_side(cache, side)
    if bid <= 0.0:
        expired_cost = shares * avg_cost
        row["expired_inventory_cost_usdc"] = round(float(row.get("expired_inventory_cost_usdc") or 0.0) + expired_cost, 8)
        pnl = -expired_cost
        sale_notional = 0.0
        actual_event_type = "EXPIRE" if event_type == "SELL" else event_type
    else:
        pnl = (bid - avg_cost) * shares
        sale_notional = bid * shares
        actual_event_type = event_type
    row[shares_key] = round(side_shares - shares, 8)
    row[cost_key] = round(max(side_cost - avg_cost * shares, 0.0), 8)
    row["realized_pnl_usdc"] = round(float(row.get("realized_pnl_usdc") or 0.0) + pnl, 8)
    paired, pair_cost, pair_pnl = _temporal_pair_metrics(row)
    row["locked_pair_shares"] = paired
    row["locked_pair_cost"] = pair_cost
    row["locked_pair_pnl_usdc"] = pair_pnl
    row["state"] = _temporal_state_from_inventory(
        row,
        max_pair_cost=float(settings.temporal_inventory_maker_paper_max_pair_cost),
    )
    record_temporal_inventory_event(
        settings,
        ts=ts,
        market_id=str(row.get("market_id") or ""),
        event_type=actual_event_type,
        state=str(row.get("state") or ""),
        side=side,
        price=bid,
        size=shares,
        notional_usdc=sale_notional,
        pnl_usdc=pnl,
        pair_cost=pair_cost,
        reason=reason,
    )
    return row


def run_temporal_inventory_maker_paper_cycle(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
    signals: list[dict[str, Any]],
    ts: str,
) -> dict[str, Any]:
    if not settings.temporal_inventory_maker_paper_enabled:
        return {
            "opened_quotes_count": 0,
            "filled_quotes_count": 0,
            "cancelled_quotes_count": 0,
            "events_count": 0,
            "entry_blocks_count": 0,
            "entry_blocks": [],
            "open_markets_count": 0,
            "open_quotes_count": 0,
        }
    markets = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    market_by_id = {str(item.get("market_id") or ""): item for item in markets if isinstance(item, dict)}
    cache_items = polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else []
    cache_by_id = {str(item.get("market_id") or ""): item for item in cache_items if isinstance(item, dict)}
    signals_by_market = {str(item.get("market_id") or ""): item for item in signals if isinstance(item, dict)}
    max_pair_cost = float(settings.temporal_inventory_maker_paper_max_pair_cost)
    force_exit_seconds = max(int(settings.temporal_inventory_maker_paper_force_exit_seconds), 0)
    base_order_usdc = max(float(settings.temporal_inventory_maker_paper_base_order_usdc), 0.0)
    min_edge = max(float(settings.temporal_inventory_maker_paper_min_net_edge), 0.0)
    max_market_exposure = max(float(settings.temporal_inventory_maker_paper_max_market_exposure_usdc), 0.0)
    max_total_exposure = max(float(settings.temporal_inventory_maker_paper_max_total_exposure_usdc), 0.0)
    daily_loss_limit = max(float(settings.temporal_inventory_maker_paper_daily_loss_limit_usdc), 0.0)
    unpaired_timeout_seconds = max(int(settings.temporal_inventory_maker_paper_unpaired_timeout_seconds), 0)

    active_rows = load_temporal_inventory_open_markets(settings)
    active_by_market = {str(row.get("market_id") or ""): row for row in active_rows}
    open_quotes = load_temporal_inventory_open_quotes(settings)
    opened_quotes: list[dict[str, Any]] = []
    filled_quotes: list[dict[str, Any]] = []
    cancelled_quotes: list[dict[str, Any]] = []
    markets_filled_this_cycle: set[str] = set()
    entry_blocks: list[dict[str, Any]] = []
    events_count = 0

    for quote in open_quotes:
        market_id = str(quote.get("market_id") or "")
        market = market_by_id.get(market_id)
        cache = cache_by_id.get(market_id)
        signal = signals_by_market.get(market_id)
        quote_age = (_ts_to_dt(ts) - _ts_to_dt(str(quote.get("ts_created") or ts))).total_seconds()
        ttl_seconds = _temporal_quote_ttl_seconds(settings, quote)
        cancel_reason = ""
        if quote_age > ttl_seconds:
            cancel_reason = "quote ttl expired"
        elif market is None or cache is None:
            cancel_reason = "market data unavailable"
        elif _seconds_remaining(market) <= force_exit_seconds:
            cancel_reason = "market near expiry"
        if cancel_reason:
            close_temporal_inventory_quote(settings, quote_id=str(quote.get("quote_id") or ""), ts=ts, status="cancelled", cancel_reason=cancel_reason)
            record_temporal_inventory_event(
                settings,
                ts=ts,
                market_id=market_id,
                event_type="CANCEL",
                state=str((active_by_market.get(market_id) or {}).get("state") or "FLAT"),
                side=str(quote.get("side") or ""),
                price=float(quote.get("price") or 0.0),
                size=float(quote.get("size") or 0.0),
                notional_usdc=float(quote.get("notional_usdc") or 0.0),
                reason=cancel_reason,
            )
            events_count += 1
            cancelled_quotes.append(quote)
            continue

        fill_price, fill_size, adverse_loss = _temporal_quote_fill(quote, cache or {})
        if fill_size <= 0.0:
            if signal is None or not bool(signal.get("eligible")) or float(signal.get("edge") or 0.0) < min_edge:
                close_temporal_inventory_quote(settings, quote_id=str(quote.get("quote_id") or ""), ts=ts, status="cancelled", cancel_reason="signal decayed")
                record_temporal_inventory_event(
                    settings,
                    ts=ts,
                    market_id=market_id,
                    event_type="CANCEL",
                    state=str((active_by_market.get(market_id) or {}).get("state") or "FLAT"),
                    side=str(quote.get("side") or ""),
                    price=float(quote.get("price") or 0.0),
                    size=float(quote.get("size") or 0.0),
                    notional_usdc=float(quote.get("notional_usdc") or 0.0),
                    reason="signal decayed",
                )
                events_count += 1
                cancelled_quotes.append(quote)
            continue
        side = str(quote.get("side") or "").upper()
        row = active_by_market.get(market_id) or _temporal_empty_market_row(signal or {"market_id": market_id}, market, ts)
        previous_yes = float(row.get("yes_shares") or 0.0)
        previous_no = float(row.get("no_shares") or 0.0)
        previous_locked_pnl = float(row.get("locked_pair_pnl_usdc") or 0.0)
        notional = fill_price * fill_size
        if side == "YES":
            row["yes_shares"] = round(previous_yes + fill_size, 8)
            row["yes_cost_usdc"] = round(float(row.get("yes_cost_usdc") or 0.0) + notional, 8)
        else:
            row["no_shares"] = round(previous_no + fill_size, 8)
            row["no_cost_usdc"] = round(float(row.get("no_cost_usdc") or 0.0) + notional, 8)
        paired, pair_cost, pair_pnl = _temporal_pair_metrics(row)
        row["locked_pair_shares"] = paired
        row["locked_pair_cost"] = pair_cost
        row["locked_pair_pnl_usdc"] = pair_pnl
        row["last_signal_side"] = str((signal or {}).get("side") or side)
        row["last_signal_edge"] = float((signal or {}).get("edge") or quote.get("edge") or 0.0)
        row["state"] = _temporal_state_from_inventory(row, max_pair_cost=max_pair_cost)
        row = _temporal_persist_market(settings, row, ts=ts)
        active_by_market[market_id] = row
        close_temporal_inventory_quote(
            settings,
            quote_id=str(quote.get("quote_id") or ""),
            ts=ts,
            status="filled",
            fill_price=fill_price,
            fill_size=fill_size,
            adverse_selection_loss_usdc=adverse_loss,
        )
        fill_context = "HEDGE" if (side == "YES" and previous_no > 0.0) or (side == "NO" and previous_yes > 0.0) else "SEED"
        record_temporal_inventory_event(
            settings,
            ts=ts,
            market_id=market_id,
            event_type="MAKER_FILL",
            state=str(row.get("state") or ""),
            side=side,
            price=fill_price,
            size=fill_size,
            notional_usdc=notional,
            pair_cost=pair_cost,
            reason="conservative queue fill",
            metadata={
                "quote_id": str(quote.get("quote_id") or ""),
                "quote_style": str(quote.get("quote_style") or "PASSIVE"),
                "fill_probability": float(quote.get("fill_probability") or 0.0),
                "expected_value_usdc": float(quote.get("expected_value_usdc") or 0.0),
                "adverse_selection_loss_usdc": adverse_loss,
            },
        )
        record_temporal_inventory_event(
            settings,
            ts=ts,
            market_id=market_id,
            event_type=fill_context,
            state=str(row.get("state") or ""),
            side=side,
            price=fill_price,
            size=fill_size,
            notional_usdc=notional,
            pair_cost=pair_cost,
            reason="owned inventory updated after maker fill",
        )
        events_count += 2
        if str(row.get("state") or "") == "LOCKED_PAIR" and pair_pnl > previous_locked_pnl:
            record_temporal_inventory_event(
                settings,
                ts=ts,
                market_id=market_id,
                event_type="LOCKED_PAIR",
                state="LOCKED_PAIR",
                side="",
                size=paired,
                notional_usdc=float(row.get("yes_cost_usdc") or 0.0) + float(row.get("no_cost_usdc") or 0.0),
                pnl_usdc=round(pair_pnl - previous_locked_pnl, 8),
                pair_cost=pair_cost,
                reason="actual owned YES/NO shares match below max pair cost",
            )
            events_count += 1
        filled_quotes.append(quote)
        markets_filled_this_cycle.add(market_id)

    active_rows = load_temporal_inventory_open_markets(settings)
    active_by_market = {str(row.get("market_id") or ""): row for row in active_rows}
    for market_id, row in list(active_by_market.items()):
        market = market_by_id.get(market_id)
        cache = cache_by_id.get(market_id)
        if market is None or cache is None:
            continue
        seconds_left = _seconds_remaining(market)
        yes_shares = float(row.get("yes_shares") or 0.0)
        no_shares = float(row.get("no_shares") or 0.0)
        signal = signals_by_market.get(market_id)
        if seconds_left <= force_exit_seconds and (yes_shares > 0.0 or no_shares > 0.0):
            paired, pair_cost, pair_pnl = _temporal_pair_metrics(row)
            if paired > 0.0:
                avg_yes = float(row.get("yes_cost_usdc") or 0.0) / yes_shares if yes_shares > 0.0 else 0.0
                avg_no = float(row.get("no_cost_usdc") or 0.0) / no_shares if no_shares > 0.0 else 0.0
                paired_cost = paired * (avg_yes + avg_no)
                row["yes_shares"] = round(yes_shares - paired, 8)
                row["no_shares"] = round(no_shares - paired, 8)
                row["yes_cost_usdc"] = round(max(float(row.get("yes_cost_usdc") or 0.0) - paired * avg_yes, 0.0), 8)
                row["no_cost_usdc"] = round(max(float(row.get("no_cost_usdc") or 0.0) - paired * avg_no, 0.0), 8)
                row["realized_pnl_usdc"] = round(float(row.get("realized_pnl_usdc") or 0.0) + pair_pnl, 8)
                record_temporal_inventory_event(
                    settings,
                    ts=ts,
                    market_id=market_id,
                    event_type="RESOLVE",
                    state="CLOSED" if float(row.get("yes_shares") or 0.0) <= 0.0 and float(row.get("no_shares") or 0.0) <= 0.0 else "HEDGING",
                    side="",
                    size=paired,
                    notional_usdc=paired_cost,
                    pnl_usdc=pair_pnl,
                    pair_cost=pair_cost,
                    reason="locked pair resolved at guaranteed payout",
                )
                events_count += 1
            if float(row.get("yes_shares") or 0.0) > 0.0:
                row = _temporal_sell_side(settings, row, ts=ts, cache=cache, side="YES", size=float(row.get("yes_shares") or 0.0), reason="force exit near expiry")
                events_count += 1
            if float(row.get("no_shares") or 0.0) > 0.0:
                row = _temporal_sell_side(settings, row, ts=ts, cache=cache, side="NO", size=float(row.get("no_shares") or 0.0), reason="force exit near expiry")
                events_count += 1
            row["state"] = "CLOSED"
            row["locked_pair_shares"] = 0.0
            row["locked_pair_cost"] = 0.0
            row["locked_pair_pnl_usdc"] = 0.0
            row = _temporal_persist_market(settings, row, ts=ts)
            active_by_market[market_id] = row
            continue

        current_side = "YES" if yes_shares > no_shares else "NO" if no_shares > yes_shares else ""
        unpaired_size = abs(yes_shares - no_shares)
        if current_side and unpaired_size > 1e-8 and unpaired_timeout_seconds > 0:
            try:
                inventory_age = (_ts_to_dt(ts) - _ts_to_dt(str(row.get("first_seen_ts") or ts))).total_seconds()
            except Exception:
                inventory_age = float(unpaired_timeout_seconds)
            signal_side = str((signal or {}).get("side") or "").upper()
            signal_edge = float((signal or {}).get("edge") or 0.0)
            same_side_still_strong = (
                signal is not None
                and bool(signal.get("eligible"))
                and signal_side == current_side
                and signal_edge >= (2.0 * min_edge)
            )
            if inventory_age >= unpaired_timeout_seconds and not same_side_still_strong:
                row = _temporal_sell_side(
                    settings,
                    row,
                    ts=ts,
                    cache=cache,
                    side=current_side,
                    size=unpaired_size,
                    reason="unpaired inventory timeout",
                )
                events_count += 1
                row = _temporal_persist_market(settings, row, ts=ts)
                active_by_market[market_id] = row
                continue

        if signal is None or not bool(signal.get("eligible")):
            continue
        signal_side = str(signal.get("side") or "").upper()
        if current_side and signal_side and signal_side != current_side and float(signal.get("edge") or 0.0) >= (2.0 * min_edge):
            record_temporal_inventory_event(
                settings,
                ts=ts,
                market_id=market_id,
                event_type="ROTATE",
                state="ROTATING",
                side=signal_side,
                reason="model flipped strongly against unpaired inventory",
                metadata={"from_side": current_side, "new_edge": float(signal.get("edge") or 0.0)},
            )
            events_count += 1
            row["state"] = "ROTATING"
            row = _temporal_sell_side(
                settings,
                row,
                ts=ts,
                cache=cache,
                side=current_side,
                size=max(yes_shares, no_shares),
                reason="rotation sold old unpaired side",
            )
            events_count += 1
            row = _temporal_persist_market(settings, row, ts=ts)
            active_by_market[market_id] = row

    open_quotes = load_temporal_inventory_open_quotes(settings)
    active_rows = load_temporal_inventory_open_markets(settings)
    active_by_market = {str(row.get("market_id") or ""): row for row in active_rows}
    daily_pnl = _temporal_daily_realized_pnl(settings)
    allow_new_quotes = daily_loss_limit <= 0.0 or daily_pnl > -daily_loss_limit
    for market_id, row in list(active_by_market.items()):
        if not allow_new_quotes:
            break
        if market_id in markets_filled_this_cycle:
            continue
        if any(str(quote.get("market_id") or "") == market_id for quote in open_quotes):
            continue
        market = market_by_id.get(market_id)
        cache = cache_by_id.get(market_id)
        if market is None or cache is None or _seconds_remaining(market) <= force_exit_seconds:
            continue
        yes_shares = float(row.get("yes_shares") or 0.0)
        no_shares = float(row.get("no_shares") or 0.0)
        if abs(yes_shares - no_shares) <= 1e-8:
            continue
        if yes_shares > no_shares:
            side = "NO"
            owned_shares = yes_shares
            owned_cost = float(row.get("yes_cost_usdc") or 0.0)
            hedge_share_cap = yes_shares - no_shares
        else:
            side = "YES"
            owned_shares = no_shares
            owned_cost = float(row.get("no_cost_usdc") or 0.0)
            hedge_share_cap = no_shares - yes_shares
        avg_owned = owned_cost / owned_shares if owned_shares > 0.0 else 0.0
        quote_price = _temporal_post_only_quote_price(cache, side)
        if quote_price <= 0.0 or avg_owned + quote_price > max_pair_cost:
            continue
        side_bid, side_ask = _temporal_book_side(cache, side)
        edge = max(1.0 - (avg_owned + quote_price), 0.0)
        fill_probability = _temporal_estimate_fill_probability(
            best_bid=side_bid,
            best_ask=side_ask,
            quote_price=quote_price,
            edge=edge,
        )
        if fill_probability < max(float(settings.temporal_inventory_maker_paper_min_fill_probability) * 0.5, 0.005):
            continue
        market_exposure = _temporal_market_exposure(row, open_quotes)
        total_exposure = _temporal_total_exposure(active_rows, open_quotes)
        available_market = max(max_market_exposure - market_exposure, 0.0) if max_market_exposure > 0.0 else base_order_usdc
        available_total = max(max_total_exposure - total_exposure, 0.0) if max_total_exposure > 0.0 else base_order_usdc
        notional = min(base_order_usdc, available_market, available_total, hedge_share_cap * quote_price)
        if notional < 5.0:
            continue
        expected_value = _temporal_quote_expected_value(
            quote_price=quote_price,
            notional=notional,
            edge=edge,
            fill_probability=fill_probability,
            spread=max(side_ask - side_bid, 0.0),
        )
        quote = create_temporal_inventory_quote(
            settings,
            ts=ts,
            market_id=market_id,
            side=side,
            price=quote_price,
            size=notional / quote_price,
            edge=edge,
            reason="hedge quote to lock owned inventory",
            quote_style="HEDGE_LOCK",
            fill_probability=fill_probability,
            expected_value_usdc=expected_value,
            ttl_seconds=_temporal_ttl_for_style(settings, "HEDGE_LOCK"),
        )
        opened_quotes.append(quote)
        open_quotes.append(quote)
        row["last_quote_id"] = str(quote.get("quote_id") or "")
        row = _temporal_persist_market(settings, row, ts=ts)
        active_by_market[market_id] = row
        record_temporal_inventory_event(
            settings,
            ts=ts,
            market_id=market_id,
            event_type="MAKER_QUOTE",
            state=str(row.get("state") or "SEEDED"),
            side=side,
            price=quote_price,
            size=notional / quote_price,
            notional_usdc=notional,
            reason="hedge quote to lock owned inventory",
            metadata={
                "quote_style": "HEDGE_LOCK",
                "fill_probability": fill_probability,
                "expected_value_usdc": expected_value,
                "locked_pair_edge": edge,
            },
        )
        events_count += 1

    eligible_signals = sorted(
        [item for item in signals if bool(item.get("eligible"))],
        key=lambda item: float(item.get("edge") or 0.0),
        reverse=True,
    )
    for signal in eligible_signals:
        market_id = str(signal.get("market_id") or "")
        if market_id in markets_filled_this_cycle:
            continue
        market = market_by_id.get(market_id)
        cache = cache_by_id.get(market_id)
        if market is None or cache is None:
            entry_blocks.append({"market_id": market_id, "reason": "market data unavailable"})
            continue
        if not allow_new_quotes:
            entry_blocks.append({"market_id": market_id, "reason": "daily loss limit hit"})
            break
        if _seconds_remaining(market) <= force_exit_seconds:
            entry_blocks.append({"market_id": market_id, "reason": "too close to expiry"})
            continue
        if any(str(quote.get("market_id") or "") == market_id for quote in open_quotes):
            continue
        row = active_by_market.get(market_id) or _temporal_empty_market_row(signal, market, ts)
        state = str(row.get("state") or "FLAT")
        yes_shares = float(row.get("yes_shares") or 0.0)
        no_shares = float(row.get("no_shares") or 0.0)
        if state == "LOCKED_PAIR" and abs(yes_shares - no_shares) < 1e-8:
            continue
        side = str(signal.get("side") or "").upper()
        quote_price = float(signal.get("order_price") or 0.0)
        reason = "seed maker quote"
        quote_style = str(signal.get("quote_style") or "PASSIVE").upper()
        hedge_share_cap: float | None = None
        hedge_edge: float | None = None
        if yes_shares > no_shares:
            hedge_price = _temporal_post_only_quote_price(cache, "NO")
            avg_yes = float(row.get("yes_cost_usdc") or 0.0) / yes_shares if yes_shares > 0.0 else 0.0
            if hedge_price > 0.0 and avg_yes + hedge_price <= max_pair_cost:
                side = "NO"
                quote_price = hedge_price
                reason = "hedge quote to lock owned YES inventory"
                hedge_share_cap = yes_shares - no_shares
                hedge_edge = max(1.0 - (avg_yes + hedge_price), 0.0)
                quote_style = "HEDGE_LOCK"
            elif side == "YES":
                quote_price = max(quote_price - min_edge, 0.0)
                reason = "inventory-adjusted same-side quote"
                quote_style = "INVENTORY_SAME_SIDE"
        elif no_shares > yes_shares:
            hedge_price = _temporal_post_only_quote_price(cache, "YES")
            avg_no = float(row.get("no_cost_usdc") or 0.0) / no_shares if no_shares > 0.0 else 0.0
            if hedge_price > 0.0 and avg_no + hedge_price <= max_pair_cost:
                side = "YES"
                quote_price = hedge_price
                reason = "hedge quote to lock owned NO inventory"
                hedge_share_cap = no_shares - yes_shares
                hedge_edge = max(1.0 - (avg_no + hedge_price), 0.0)
                quote_style = "HEDGE_LOCK"
            elif side == "NO":
                quote_price = max(quote_price - min_edge, 0.0)
                reason = "inventory-adjusted same-side quote"
                quote_style = "INVENTORY_SAME_SIDE"
        side_bid, side_ask = _temporal_book_side(cache, side)
        if side_ask > 0.01:
            quote_price = min(quote_price, side_ask - 0.01)
        if quote_style not in {"MID_AGGRESSIVE", "NEAR_TOUCH"}:
            quote_price = min(quote_price, side_bid + 0.01 if side_bid > 0.0 else quote_price)
        quote_price = round(max(quote_price, 0.0), 4)
        if quote_price <= 0.0:
            entry_blocks.append({"market_id": market_id, "reason": "invalid inventory-adjusted quote"})
            continue
        market_exposure = _temporal_market_exposure(row, open_quotes)
        total_exposure = _temporal_total_exposure(active_rows, open_quotes)
        available_market = max(max_market_exposure - market_exposure, 0.0) if max_market_exposure > 0.0 else base_order_usdc
        available_total = max(max_total_exposure - total_exposure, 0.0) if max_total_exposure > 0.0 else base_order_usdc
        notional = min(base_order_usdc, available_market, available_total)
        if hedge_share_cap is not None:
            notional = min(notional, max(hedge_share_cap, 0.0) * quote_price)
        if notional < 5.0:
            entry_blocks.append({"market_id": market_id, "reason": "exposure cap hit"})
            continue
        size = notional / quote_price
        quote_edge = float(signal.get("edge") or 0.0)
        if hedge_edge is not None:
            quote_edge = hedge_edge
        elif quote_price < float(signal.get("order_price") or quote_price):
            quote_edge += float(signal.get("order_price") or quote_price) - quote_price
        fill_probability = _temporal_estimate_fill_probability(
            best_bid=side_bid,
            best_ask=side_ask,
            quote_price=quote_price,
            edge=quote_edge,
        )
        expected_value = _temporal_quote_expected_value(
            quote_price=quote_price,
            notional=notional,
            edge=quote_edge,
            fill_probability=fill_probability,
            spread=max(side_ask - side_bid, 0.0),
        )
        if quote_style != "HEDGE_LOCK" and fill_probability < float(settings.temporal_inventory_maker_paper_min_fill_probability):
            entry_blocks.append({"market_id": market_id, "reason": "estimated maker fill probability below threshold"})
            continue
        if quote_style != "HEDGE_LOCK" and expected_value < float(settings.temporal_inventory_maker_paper_min_expected_value_usdc):
            entry_blocks.append({"market_id": market_id, "reason": "expected maker quote value below threshold"})
            continue
        quote = create_temporal_inventory_quote(
            settings,
            ts=ts,
            market_id=market_id,
            side=side,
            price=quote_price,
            size=size,
            edge=quote_edge,
            reason=reason,
            quote_style=quote_style,
            fill_probability=fill_probability,
            expected_value_usdc=expected_value,
            ttl_seconds=_temporal_ttl_for_style(settings, quote_style),
        )
        opened_quotes.append(quote)
        open_quotes.append(quote)
        row["state"] = state if state != "FLAT" else "FLAT"
        row["last_signal_side"] = str(signal.get("side") or "")
        row["last_signal_edge"] = float(signal.get("edge") or 0.0)
        row["last_quote_id"] = str(quote.get("quote_id") or "")
        row = _temporal_persist_market(settings, row, ts=ts)
        active_by_market[market_id] = row
        if row not in active_rows:
            active_rows.append(row)
        record_temporal_inventory_event(
            settings,
            ts=ts,
            market_id=market_id,
            event_type="MAKER_QUOTE",
            state=str(row.get("state") or "FLAT"),
            side=side,
            price=quote_price,
            size=size,
            notional_usdc=notional,
            reason=reason,
            metadata={
                "signal_side": str(signal.get("side") or ""),
                "edge": quote_edge,
                "quote_style": quote_style,
                "fill_probability": fill_probability,
                "expected_value_usdc": expected_value,
            },
        )
        events_count += 1

    final_open_markets = load_temporal_inventory_open_markets(settings)
    final_open_quotes = load_temporal_inventory_open_quotes(settings)
    return {
        "opened_quotes_count": len(opened_quotes),
        "filled_quotes_count": len(filled_quotes),
        "cancelled_quotes_count": len(cancelled_quotes),
        "events_count": events_count,
        "entry_blocks_count": len(entry_blocks),
        "entry_blocks": entry_blocks,
        "opened_quotes": opened_quotes,
        "filled_quotes": filled_quotes,
        "cancelled_quotes": cancelled_quotes,
        "open_markets_count": len(final_open_markets),
        "open_quotes_count": len(final_open_quotes),
        "open_exposure_usdc": round(_temporal_total_exposure(final_open_markets, final_open_quotes), 6),
    }


def _late_resolution_exit_price(position: dict[str, Any], cache: dict[str, Any], signal: dict[str, Any] | None) -> tuple[float, str]:
    side = str(position.get("side") or "").upper()
    entry_price = float(position.get("entry_price") or 0.0)
    confidence = float((signal or {}).get("official_confidence") or position.get("entry_official_confidence") or 0.0)
    matching_side = str((signal or {}).get("side") or "").upper() == side
    if matching_side and confidence >= 0.995:
        return 1.0, "high-confidence settlement proxy"
    best_bid = float(cache.get("best_bid") or 0.0)
    best_ask = float(cache.get("best_ask") or 0.0)
    no_bid = float(cache.get("no_best_bid") or max(1.0 - best_ask, 0.0))
    bid = best_bid if side == "YES" else no_bid
    if bid > 0.0:
        return bid, "bid-side forced exit"
    return max(min(entry_price * 0.25, 1.0), 0.0), "no bid; conservative residual mark"


def _late_resolution_daily_realized_pnl(settings: LatencyBotSettings) -> float:
    cutoff_ts = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat().replace("+00:00", "Z")
    with connect_latency_bot_db(settings) as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(pnl), 0.0) AS pnl
            FROM late_resolution_capture_events
            WHERE event_type = 'close'
              AND ts >= ?
            """,
            (cutoff_ts,),
        ).fetchone()
    return float(row["pnl"] or 0.0) if row is not None else 0.0


def run_late_resolution_capture_paper_cycle(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
    signals: list[dict[str, Any]],
    ts: str,
) -> dict[str, Any]:
    if not settings.late_resolution_capture_paper_enabled:
        return {
            "opened_positions_count": 0,
            "closed_positions_count": 0,
            "entry_blocks_count": 0,
            "entry_blocks": [],
            "open_positions_count": 0,
            "open_capital_usdc": 0.0,
        }
    markets = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    market_by_id = {str(item.get("market_id") or ""): item for item in markets if isinstance(item, dict)}
    cache_items = polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else []
    cache_by_id = {str(item.get("market_id") or ""): item for item in cache_items if isinstance(item, dict)}
    signals_by_market = {str(item.get("market_id") or ""): item for item in signals if isinstance(item, dict)}
    opened: list[dict[str, Any]] = []
    closed: list[dict[str, Any]] = []
    entry_blocks: list[dict[str, Any]] = []

    open_positions = load_late_resolution_capture_open_positions(settings)
    for position in open_positions:
        market_id = str(position.get("market_id") or "")
        market = market_by_id.get(market_id)
        cache = cache_by_id.get(market_id)
        signal = signals_by_market.get(market_id)
        close_reason = ""
        if market is None or cache is None:
            close_reason = "market data unavailable"
        elif _seconds_remaining(market) <= max(int(settings.late_resolution_capture_paper_min_seconds_left), 0):
            close_reason = "late-resolution expiry window reached"
        elif signal is not None and str(signal.get("side") or "").upper() != str(position.get("side") or "").upper() and float(signal.get("official_confidence") or 0.0) >= float(settings.late_resolution_capture_paper_min_official_confidence):
            close_reason = "official proxy flipped against held side"
        if not close_reason:
            continue
        mark, mark_reason = _late_resolution_exit_price(position, cache or {}, signal)
        pnl = round((mark - float(position.get("entry_price") or 0.0)) * float(position.get("size") or 0.0), 6)
        close_late_resolution_capture_position(
            settings,
            position_id=str(position.get("position_id") or ""),
            ts=ts,
            mark=mark,
            pnl=pnl,
            reason=f"{close_reason}; {mark_reason}",
        )
        closed.append({"market_id": market_id, "side": str(position.get("side") or ""), "mark": mark, "pnl": pnl, "reason": close_reason})

    open_positions = load_late_resolution_capture_open_positions(settings)
    open_by_market = {str(item.get("market_id") or ""): item for item in open_positions}
    total_open = sum(float(item.get("notional_usdc") or 0.0) for item in open_positions)
    open_by_market_notional: dict[str, float] = {}
    for item in open_positions:
        market_id = str(item.get("market_id") or "")
        open_by_market_notional[market_id] = open_by_market_notional.get(market_id, 0.0) + float(item.get("notional_usdc") or 0.0)

    daily_pnl = _late_resolution_daily_realized_pnl(settings)
    daily_loss_limit = max(float(settings.late_resolution_capture_paper_daily_loss_limit_usdc), 0.0)
    allow_new = daily_loss_limit <= 0.0 or daily_pnl > -daily_loss_limit
    eligible_signals = sorted(
        [item for item in signals if isinstance(item, dict) and bool(item.get("eligible"))],
        key=lambda item: float(item.get("edge") or 0.0),
        reverse=True,
    )
    for signal in eligible_signals:
        market_id = str(signal.get("market_id") or "")
        if market_id in open_by_market:
            continue
        if not allow_new:
            entry_blocks.append({"market_id": market_id, "reason": "daily loss limit hit"})
            break
        market = market_by_id.get(market_id)
        cache = cache_by_id.get(market_id)
        if market is None or cache is None:
            entry_blocks.append({"market_id": market_id, "reason": "market data unavailable"})
            continue
        if float(cache.get("book_age_ms") or 0.0) > float(settings.late_resolution_capture_paper_max_book_age_ms):
            entry_blocks.append({"market_id": market_id, "reason": "book snapshot too stale"})
            continue
        seconds_left = _seconds_remaining(market)
        if seconds_left < float(settings.late_resolution_capture_paper_min_seconds_left):
            entry_blocks.append({"market_id": market_id, "reason": "too close to expiry"})
            continue
        if seconds_left > float(settings.late_resolution_capture_paper_max_seconds_left):
            entry_blocks.append({"market_id": market_id, "reason": "too far from expiry"})
            continue
        side = str(signal.get("side") or "").upper()
        yes_bid = float(cache.get("best_bid") or 0.0)
        yes_ask = float(cache.get("best_ask") or 0.0)
        no_bid = float(cache.get("no_best_bid") or max(1.0 - yes_ask, 0.0))
        selected_bid = yes_bid if side == "YES" else no_bid
        min_depth = (
            float(cache.get("ask_fillable_usdc") or cache.get("asks_depth_usdc") or 0.0)
            if side == "YES"
            else float(cache.get("no_ask_fillable_usdc") or cache.get("no_asks_depth_usdc") or 0.0)
        )
        if min_depth < float(settings.late_resolution_capture_paper_min_depth_usdc):
            entry_blocks.append({"market_id": market_id, "reason": "entry depth below threshold"})
            continue
        if selected_bid < float(settings.late_resolution_capture_paper_min_exit_bid):
            entry_blocks.append({"market_id": market_id, "reason": "exit bid below threshold"})
            continue
        price = float(signal.get("order_price") or 0.0)
        if price <= 0.0:
            entry_blocks.append({"market_id": market_id, "reason": "missing entry price"})
            continue
        market_open = open_by_market_notional.get(market_id, 0.0)
        max_market = max(float(settings.late_resolution_capture_paper_max_market_exposure_usdc), 0.0)
        max_total = max(float(settings.late_resolution_capture_paper_max_total_exposure_usdc), 0.0)
        target = max(float(settings.late_resolution_capture_paper_notional_usdc), 0.0)
        available_market = max(max_market - market_open, 0.0) if max_market > 0.0 else target
        available_total = max(max_total - total_open, 0.0) if max_total > 0.0 else target
        notional = min(target, available_market, available_total)
        if notional < 1.0:
            entry_blocks.append({"market_id": market_id, "reason": "exposure cap hit"})
            continue
        size = notional / price
        position = create_late_resolution_capture_position(
            settings,
            ts=ts,
            market_id=market_id,
            asset=str(signal.get("asset") or market.get("asset") or ""),
            side=side,
            entry_price=price,
            size=size,
            signal=signal,
        )
        opened.append(position)
        open_by_market[market_id] = position
        open_by_market_notional[market_id] = open_by_market_notional.get(market_id, 0.0) + notional
        total_open += notional

    final_open = load_late_resolution_capture_open_positions(settings)
    return {
        "opened_positions_count": len(opened),
        "closed_positions_count": len(closed),
        "entry_blocks_count": len(entry_blocks),
        "entry_blocks": entry_blocks,
        "opened": opened,
        "closed": closed,
        "open_positions_count": len(final_open),
        "open_capital_usdc": round(sum(float(item.get("notional_usdc") or 0.0) for item in final_open), 6),
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
