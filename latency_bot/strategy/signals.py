from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from ..config import LatencyBotSettings


def _threshold_for_tenor(settings: LatencyBotSettings, tenor_minutes: int) -> tuple[float, float]:
    if tenor_minutes <= 5:
        return settings.taker_min_edge_5m, settings.maker_min_edge_5m
    return settings.taker_min_edge_15m, settings.maker_min_edge_15m


def _price_in_trade_band(settings: LatencyBotSettings, price: float) -> bool:
    return settings.min_trade_price <= price <= settings.max_trade_price


def _allow_taker_no(settings: LatencyBotSettings, *, asset: str) -> bool:
    if not settings.allow_taker_no:
        return False
    if asset == "eth" and not settings.allow_eth_no_taker:
        return False
    return True


def _min_fair_yes_for_tenor(settings: LatencyBotSettings, tenor_minutes: int) -> float:
    if tenor_minutes <= 5:
        return settings.taker_min_fair_yes_5m
    return settings.taker_min_fair_yes_15m


def _min_taker_entry_seconds_left(settings: LatencyBotSettings, tenor_minutes: int) -> int:
    if tenor_minutes <= 5:
        return settings.min_taker_entry_seconds_left_5m
    return settings.min_taker_entry_seconds_left_15m


def _allow_taker_yes(settings: LatencyBotSettings, *, asset: str, tenor_minutes: int) -> bool:
    if tenor_minutes > 5 and not settings.allow_15m_taker:
        return False
    if asset == "btc":
        return settings.allow_btc_yes_taker
    if asset == "eth":
        return settings.allow_eth_yes_taker
    return False


def _skip(signal: dict[str, Any], *, reason: str, blocked_reason: str, edge: float = 0.0) -> None:
    signal.update(
        {
            "reason": reason,
            "blocked_reason": blocked_reason,
            "edge": round(max(float(edge), 0.0), 6),
        }
    )


def _clamp_probability(value: float) -> float:
    return min(max(float(value), 0.001), 0.999)


def _logit(value: float) -> float:
    value = _clamp_probability(value)
    return math.log(value / (1.0 - value))


def _sigmoid(value: float) -> float:
    if value >= 0:
        z = math.exp(-value)
        return 1.0 / (1.0 + z)
    z = math.exp(value)
    return z / (1.0 + z)


def _shadow_model_fair_yes(
    model: str,
    *,
    fair_yes: float,
    market_mid: float,
    seconds_left: float,
    min_depth_usdc: float,
    spread: float,
) -> float:
    model = model.strip().lower()
    fair_yes = _clamp_probability(fair_yes)
    market_mid = _clamp_probability(market_mid if market_mid > 0.0 else 0.5)
    if model == "calibrated_digital":
        # A conservative digital-option variant that shrinks extreme probabilities toward 50%.
        return _clamp_probability(0.5 + 0.85 * (fair_yes - 0.5))
    if model == "logit_proxy":
        depth_adj = 0.04 if min_depth_usdc >= 5000.0 else -0.02 if min_depth_usdc < 1500.0 else 0.0
        time_adj = 0.04 if 180.0 <= seconds_left <= 420.0 else -0.03 if seconds_left > 900.0 else 0.0
        spread_adj = -0.05 if spread >= 0.04 else 0.02 if spread <= 0.02 else 0.0
        return _clamp_probability(_sigmoid(-0.03 + 1.05 * _logit(fair_yes) + 0.25 * _logit(market_mid) + depth_adj + time_adj + spread_adj))
    if model == "gbt_proxy":
        adjustment = 0.0
        edge_to_market = fair_yes - market_mid
        if edge_to_market >= 0.08:
            adjustment += 0.035
        elif edge_to_market >= 0.04:
            adjustment += 0.015
        elif edge_to_market < 0.0:
            adjustment -= 0.025
        if 180.0 <= seconds_left <= 420.0:
            adjustment += 0.015
        elif seconds_left > 900.0:
            adjustment -= 0.020
        if min_depth_usdc >= 7500.0:
            adjustment += 0.010
        elif min_depth_usdc < 1500.0:
            adjustment -= 0.025
        if spread >= 0.04:
            adjustment -= 0.030
        return _clamp_probability(fair_yes + adjustment)
    if model == "market_blend":
        return _clamp_probability(0.70 * fair_yes + 0.30 * market_mid)
    if model == "quant_poc":
        raw_distance = fair_yes - 0.5
        if abs(raw_distance) < 0.015:
            return market_mid
        elif abs(raw_distance) < 0.04:
            fair_component = 0.5 + 0.55 * raw_distance
        else:
            fair_component = 0.5 + 0.75 * raw_distance

        # Keep the POC from blindly fighting the prediction-market book when
        # the source fair value is weak or market quality is poor.
        model_fair = 0.62 * fair_component + 0.38 * market_mid
        penalty = 0.0
        if spread >= 0.06:
            penalty += 0.035
        elif spread >= 0.04:
            penalty += 0.020
        if min_depth_usdc < 1500.0:
            penalty += 0.030
        elif min_depth_usdc < 3000.0:
            penalty += 0.012
        if seconds_left < 90.0 or seconds_left > 900.0:
            penalty += 0.035
        elif seconds_left < 150.0 or seconds_left > 600.0:
            penalty += 0.015
        if model_fair >= 0.5:
            model_fair -= penalty
        else:
            model_fair += penalty
        return _clamp_probability(model_fair)
    return fair_yes


def build_signals(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
    fair_values: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    markets = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    market_by_id = {str(item.get("market_id") or ""): item for item in markets if isinstance(item, dict)}
    cache_items = polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else []
    cache_by_id = {str(item.get("market_id") or ""): item for item in cache_items if isinstance(item, dict)}
    results: list[dict[str, Any]] = []
    for fair in fair_values:
        if not isinstance(fair, dict):
            continue
        market_id = str(fair.get("market_id") or "")
        market = market_by_id.get(market_id)
        cache = cache_by_id.get(market_id)
        if market is None or cache is None:
            continue
        asset = str(fair.get("asset") or "").lower()
        tenor = int(market.get("tenor_minutes") or 0)
        taker_threshold, maker_threshold = _threshold_for_tenor(settings, tenor)
        best_bid = float(cache.get("best_bid") or 0.0)
        best_ask = float(cache.get("best_ask") or 0.0)
        min_depth_usdc = float(cache.get("min_depth_usdc") or 0.0)
        book_age_ms = float(cache.get("book_age_ms") or 0.0)
        fair_yes = float(fair.get("fair_yes") or 0.5)
        fair_no = float(fair.get("fair_no") or 0.5)
        reference_price = float(fair.get("reference_price") or 0.0)
        volatility = float(fair.get("volatility") or 0.0)
        seconds_left = float(fair.get("time_to_expiry_sec") or 0.0)
        force_exit_seconds = settings.force_exit_seconds_5m if tenor <= 5 else settings.force_exit_seconds_15m
        yes_edge = fair_yes - (best_ask + settings.taker_fee_per_share + settings.taker_slippage_per_share)
        yes_join_edge = fair_yes - best_bid
        yes_improve_price = min(best_bid + 0.01, max(best_ask - 0.01, 0.0)) if best_bid > 0.0 and best_ask > best_bid else best_bid
        yes_improve_edge = fair_yes - yes_improve_price if yes_improve_price > 0.0 else 0.0
        no_ask = max(1.0 - best_bid, 0.0)
        no_bid = max(1.0 - best_ask, 0.0)
        no_edge = fair_no - (no_ask + settings.taker_fee_per_share + settings.taker_slippage_per_share)
        no_join_edge = fair_no - no_bid
        no_improve_price = min(no_bid + 0.01, max(no_ask - 0.01, 0.0)) if no_bid > 0.0 and no_ask > no_bid else no_bid
        no_improve_edge = fair_no - no_improve_price if no_improve_price > 0.0 else 0.0
        signal = {
            "market_id": market_id,
            "asset": asset,
            "tenor_minutes": tenor,
            "fair_yes": fair_yes,
            "fair_no": fair_no,
            "yes_ask": best_ask,
            "yes_bid": best_bid,
            "no_ask": no_ask,
            "no_bid": no_bid,
            "min_depth_usdc": min_depth_usdc,
            "book_age_ms": book_age_ms,
            "seconds_left": seconds_left,
            "reference_price": reference_price,
            "volatility": volatility,
            "signal_type": "SKIP",
            "mode": "none",
            "edge": 0.0,
            "eligible": False,
            "reason": "edge below thresholds",
            "blocked_reason": "",
            "order_price": 0.0,
        }
        if seconds_left <= force_exit_seconds:
            signal.update({"reason": "too close to expiry", "blocked_reason": "time_to_expiry"})
        elif seconds_left < _min_taker_entry_seconds_left(settings, tenor):
            signal.update({"reason": "entry too close to expiry", "blocked_reason": "entry_time_to_expiry"})
        elif min_depth_usdc < settings.min_book_depth_usdc:
            signal.update({"reason": "insufficient visible depth", "blocked_reason": "min_book_depth"})
        elif book_age_ms > settings.max_book_age_ms:
            signal.update({"reason": "book snapshot too stale", "blocked_reason": "book_age"})
        elif (
            yes_edge >= taker_threshold
            and fair_yes >= _min_fair_yes_for_tenor(settings, tenor)
            and _allow_taker_yes(settings, asset=asset, tenor_minutes=tenor)
            and _price_in_trade_band(settings, best_ask)
        ):
            signal.update(
                {
                    "signal_type": "TAKE_YES",
                    "mode": "taker",
                    "edge": round(yes_edge, 6),
                    "eligible": True,
                    "reason": "yes taker edge clears threshold",
                }
            )
        elif no_edge >= taker_threshold and _allow_taker_no(settings, asset=asset) and _price_in_trade_band(settings, no_ask):
            signal.update(
                {
                    "signal_type": "TAKE_NO",
                    "mode": "taker",
                    "edge": round(no_edge, 6),
                    "eligible": True,
                    "reason": "no taker edge clears threshold",
                }
            )
        elif settings.allow_maker_improve and yes_improve_edge >= maker_threshold and _price_in_trade_band(settings, yes_improve_price):
            signal.update(
                {
                    "signal_type": "MAKE_YES_IMPROVE",
                    "mode": "maker",
                    "edge": round(yes_improve_edge, 6),
                    "eligible": True,
                    "reason": "yes maker improve edge clears threshold",
                    "order_price": round(yes_improve_price, 6),
                }
            )
        elif settings.allow_maker_join and yes_join_edge >= maker_threshold and _price_in_trade_band(settings, best_bid):
            signal.update(
                {
                    "signal_type": "MAKE_YES_JOIN",
                    "mode": "maker",
                    "edge": round(yes_join_edge, 6),
                    "eligible": True,
                    "reason": "yes maker join edge clears threshold",
                    "order_price": round(best_bid, 6),
                }
            )
        elif settings.allow_maker_improve and no_improve_edge >= maker_threshold and _price_in_trade_band(settings, no_improve_price):
            signal.update(
                {
                    "signal_type": "MAKE_NO_IMPROVE",
                    "mode": "maker",
                    "edge": round(no_improve_edge, 6),
                    "eligible": True,
                    "reason": "no maker improve edge clears threshold",
                    "order_price": round(no_improve_price, 6),
                }
            )
        elif settings.allow_maker_join and no_join_edge >= maker_threshold and _price_in_trade_band(settings, no_bid):
            signal.update(
                {
                    "signal_type": "MAKE_NO_JOIN",
                    "mode": "maker",
                    "edge": round(no_join_edge, 6),
                    "eligible": True,
                    "reason": "no maker join edge clears threshold",
                    "order_price": round(no_bid, 6),
                }
            )
        else:
            yes_enabled = _allow_taker_yes(settings, asset=asset, tenor_minutes=tenor)
            min_fair_yes = _min_fair_yes_for_tenor(settings, tenor)
            if not yes_enabled:
                _skip(signal, reason="live slice disabled", blocked_reason="live_slice_disabled", edge=yes_edge)
            elif fair_yes < min_fair_yes:
                _skip(signal, reason="fair yes below threshold", blocked_reason="min_fair_yes", edge=yes_edge)
            elif not _price_in_trade_band(settings, best_ask):
                _skip(signal, reason="yes price outside trade band", blocked_reason="trade_price_band", edge=yes_edge)
            elif yes_edge < taker_threshold:
                _skip(signal, reason="yes taker edge below threshold", blocked_reason="taker_edge", edge=yes_edge)
            elif settings.allow_taker_no and not _price_in_trade_band(settings, no_ask):
                _skip(signal, reason="no price outside trade band", blocked_reason="trade_price_band", edge=no_edge)
            elif settings.allow_taker_no and no_edge < taker_threshold:
                _skip(signal, reason="no taker edge below threshold", blocked_reason="taker_edge", edge=no_edge)
        results.append(signal)
    return results


def build_cex_latency_paper_signals(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
    fair_values: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not settings.cex_latency_paper_enabled:
        return []
    allowed_assets = {str(asset).lower() for asset in settings.cex_latency_paper_assets}
    markets = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    market_by_id = {str(item.get("market_id") or ""): item for item in markets if isinstance(item, dict)}
    cache_items = polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else []
    cache_by_id = {str(item.get("market_id") or ""): item for item in cache_items if isinstance(item, dict)}
    results: list[dict[str, Any]] = []
    for fair in fair_values:
        if not isinstance(fair, dict):
            continue
        market_id = str(fair.get("market_id") or "")
        market = market_by_id.get(market_id)
        cache = cache_by_id.get(market_id)
        if market is None or cache is None:
            continue
        asset = str(fair.get("asset") or "").lower()
        tenor = int(market.get("tenor_minutes") or 0)
        best_bid = float(cache.get("best_bid") or 0.0)
        best_ask = float(cache.get("best_ask") or 0.0)
        no_bid = float(cache.get("no_best_bid") or max(1.0 - best_ask, 0.0))
        no_ask = float(cache.get("no_best_ask") or max(1.0 - best_bid, 0.0))
        yes_entry_price = float(cache.get("ask_vwap") or best_ask)
        no_entry_price = float(cache.get("no_ask_vwap") or no_ask)
        yes_entry_depth_usdc = float(
            cache.get("ask_fillable_usdc") or cache.get("asks_depth_usdc") or cache.get("min_depth_usdc") or 0.0
        )
        no_entry_depth_usdc = float(
            cache.get("no_ask_fillable_usdc") or cache.get("no_asks_depth_usdc") or cache.get("min_depth_usdc") or 0.0
        )
        yes_mid = (best_bid + best_ask) / 2.0 if best_bid > 0.0 and best_ask > 0.0 else 0.5
        spread = max(best_ask - best_bid, 0.0) if best_ask > 0.0 and best_bid > 0.0 else 1.0
        min_depth_usdc = float(cache.get("min_depth_usdc") or 0.0)
        book_age_ms = float(cache.get("book_age_ms") or 0.0)
        raw_fair_yes = float(fair.get("fair_yes") or 0.5)
        reference_price = float(fair.get("reference_price") or 0.0)
        volatility = float(fair.get("volatility") or 0.0)
        seconds_left = float(fair.get("time_to_expiry_sec") or 0.0)
        model_fair_yes = _shadow_model_fair_yes(
            settings.cex_latency_paper_model,
            fair_yes=raw_fair_yes,
            market_mid=yes_mid,
            seconds_left=seconds_left,
            min_depth_usdc=min_depth_usdc,
            spread=spread,
        )
        fair_yes = model_fair_yes
        fair_no = 1.0 - fair_yes
        yes_edge = fair_yes - (yes_entry_price + settings.taker_fee_per_share + settings.taker_slippage_per_share)
        no_edge = fair_no - (no_entry_price + settings.taker_fee_per_share + settings.taker_slippage_per_share)
        side = "YES" if yes_edge >= no_edge else "NO"
        entry_price = yes_entry_price if side == "YES" else no_entry_price
        entry_depth_usdc = yes_entry_depth_usdc if side == "YES" else no_entry_depth_usdc
        edge = yes_edge if side == "YES" else no_edge
        min_seconds_left = (
            settings.cex_latency_paper_min_seconds_left_5m
            if tenor <= 5
            else settings.cex_latency_paper_min_seconds_left_15m
        )
        signal = {
            "market_id": market_id,
            "asset": asset,
            "tenor_minutes": tenor,
            "fair_yes": fair_yes,
            "fair_no": fair_no,
            "yes_bid": best_bid,
            "yes_ask": best_ask,
            "no_bid": no_bid,
            "no_ask": no_ask,
            "min_depth_usdc": entry_depth_usdc,
            "book_age_ms": book_age_ms,
            "seconds_left": seconds_left,
            "reference_price": reference_price,
            "volatility": volatility,
            "signal_type": "CEX_LATENCY_SKIP",
            "mode": "cex_latency_paper",
            "edge": round(max(edge, 0.0), 6),
            "eligible": False,
            "reason": "cex-latency edge below threshold",
            "blocked_reason": "edge",
            "side": side,
            "order_price": entry_price,
        }
        if asset not in allowed_assets:
            signal.update({"reason": "asset disabled", "blocked_reason": "asset", "edge": 0.0})
        elif seconds_left <= settings.cex_latency_paper_force_exit_seconds:
            signal.update({"reason": "too close to expiry", "blocked_reason": "time_to_expiry", "edge": 0.0})
        elif seconds_left < min_seconds_left:
            signal.update({"reason": "entry too close to expiry", "blocked_reason": "entry_time_to_expiry", "edge": 0.0})
        elif seconds_left > settings.cex_latency_paper_max_seconds_left:
            signal.update({"reason": "too far from expiry", "blocked_reason": "max_time_to_expiry", "edge": 0.0})
        elif entry_depth_usdc < settings.cex_latency_paper_min_depth_usdc:
            signal.update({"reason": "insufficient visible depth", "blocked_reason": "min_book_depth", "edge": 0.0})
        elif book_age_ms > settings.cex_latency_paper_max_book_age_ms:
            signal.update({"reason": "book snapshot too stale", "blocked_reason": "book_age", "edge": 0.0})
        elif entry_price < settings.cex_latency_paper_min_trade_price or entry_price > settings.cex_latency_paper_max_trade_price:
            signal.update({"reason": "price outside cex-latency band", "blocked_reason": "trade_price_band"})
        elif edge >= settings.cex_latency_paper_min_edge_per_share:
            signal.update(
                {
                    "signal_type": f"CEX_LATENCY_TAKE_{side}",
                    "edge": round(edge, 6),
                    "eligible": True,
                    "reason": "cex-latency edge clears threshold",
                    "blocked_reason": "",
                }
            )
        results.append(signal)
    return results


def _book_microprice(best_bid: float, best_ask: float, bid_depth: float, ask_depth: float) -> tuple[float, float]:
    if best_bid <= 0.0 or best_ask <= 0.0:
        return 0.5, 0.0
    total_depth = max(float(bid_depth) + float(ask_depth), 0.0)
    mid = (best_bid + best_ask) / 2.0
    if total_depth <= 0.0:
        return mid, 0.0
    imbalance = (float(bid_depth) - float(ask_depth)) / total_depth
    spread = max(best_ask - best_bid, 0.0)
    return _clamp_probability(mid + imbalance * (spread / 2.0)), imbalance


def _estimate_maker_fill_probability(
    *,
    best_bid: float,
    best_ask: float,
    quote_price: float,
    edge: float,
    seconds_left: float,
    book_age_ms: float,
    max_book_age_ms: float,
) -> float:
    if best_bid <= 0.0 or best_ask <= 0.0 or quote_price <= 0.0 or best_ask <= best_bid:
        return 0.0
    spread = max(best_ask - best_bid, 0.01)
    distance_to_touch = max(best_ask - quote_price, 0.0)
    proximity = 1.0 - min(max(distance_to_touch / spread, 0.0), 1.0)
    time_multiplier = 1.10 if seconds_left <= 180.0 else 0.95 if seconds_left >= 900.0 else 1.0
    staleness = min(max(book_age_ms / max(max_book_age_ms, 1.0), 0.0), 2.0)
    staleness_multiplier = max(0.35, 1.0 - 0.35 * staleness)
    edge_multiplier = 1.0 + min(max(edge, 0.0), 0.20)
    probability = (0.015 + 0.70 * proximity) * time_multiplier * staleness_multiplier * edge_multiplier
    return round(min(max(probability, 0.0), 0.85), 6)


def _temporal_expected_value_usdc(
    *,
    edge: float,
    quote_price: float,
    quote_notional: float,
    fill_probability: float,
    spread: float,
) -> float:
    if edge <= 0.0 or quote_price <= 0.0 or quote_notional <= 0.0 or fill_probability <= 0.0:
        return 0.0
    shares = quote_notional / quote_price
    expected_edge_value = edge * shares * fill_probability
    adverse_penalty = min(max(spread, 0.0) * 0.10, 0.025) * shares * fill_probability
    return round(expected_edge_value - adverse_penalty, 6)


def build_btc_fair_value_paper_signals(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
    fair_values: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not settings.btc_fair_value_paper_enabled:
        return []
    allowed_assets = {str(asset).lower() for asset in settings.btc_fair_value_paper_assets}
    markets = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    market_by_id = {str(item.get("market_id") or ""): item for item in markets if isinstance(item, dict)}
    cache_items = polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else []
    cache_by_id = {str(item.get("market_id") or ""): item for item in cache_items if isinstance(item, dict)}
    raw_market_weight = max(float(settings.btc_fair_value_paper_market_weight), 0.0)
    raw_micro_weight = max(float(settings.btc_fair_value_paper_microprice_weight), 0.0)
    raw_binance_weight = max(float(settings.btc_fair_value_paper_binance_weight), 0.0)
    weight_sum = raw_market_weight + raw_micro_weight + raw_binance_weight
    if weight_sum <= 0.0:
        raw_market_weight, raw_micro_weight, raw_binance_weight, weight_sum = 0.45, 0.25, 0.30, 1.0
    market_weight = raw_market_weight / weight_sum
    micro_weight = raw_micro_weight / weight_sum
    binance_weight = raw_binance_weight / weight_sum
    results: list[dict[str, Any]] = []
    for fair in fair_values:
        if not isinstance(fair, dict):
            continue
        market_id = str(fair.get("market_id") or "")
        market = market_by_id.get(market_id)
        cache = cache_by_id.get(market_id)
        if market is None or cache is None:
            continue
        asset = str(fair.get("asset") or "").lower()
        tenor = int(market.get("tenor_minutes") or 0)
        best_bid = float(cache.get("best_bid") or 0.0)
        best_ask = float(cache.get("best_ask") or 0.0)
        no_bid = float(cache.get("no_best_bid") or max(1.0 - best_ask, 0.0))
        no_ask = float(cache.get("no_best_ask") or max(1.0 - best_bid, 0.0))
        yes_entry_price = float(cache.get("ask_vwap") or best_ask)
        no_entry_price = float(cache.get("no_ask_vwap") or no_ask)
        yes_entry_depth_usdc = float(cache.get("ask_fillable_usdc") or cache.get("asks_depth_usdc") or 0.0)
        no_entry_depth_usdc = float(cache.get("no_ask_fillable_usdc") or cache.get("no_asks_depth_usdc") or 0.0)
        bid_depth = float(cache.get("bids_depth_usdc") or cache.get("bid_depth_usdc") or 0.0)
        ask_depth = float(cache.get("asks_depth_usdc") or cache.get("ask_depth_usdc") or 0.0)
        yes_mid = (best_bid + best_ask) / 2.0 if best_bid > 0.0 and best_ask > 0.0 else 0.5
        micro_yes, imbalance = _book_microprice(best_bid, best_ask, bid_depth, ask_depth)
        raw_fair_yes = _clamp_probability(float(fair.get("fair_yes") or 0.5))
        # The Binance component is the external directional prior; if the
        # current fair model is weak it naturally shrinks back toward 50/50.
        binance_directional_fair = _clamp_probability(0.5 + 0.85 * (raw_fair_yes - 0.5))
        fair_yes = _clamp_probability(
            market_weight * yes_mid + micro_weight * micro_yes + binance_weight * binance_directional_fair
        )
        fair_no = 1.0 - fair_yes
        yes_edge = fair_yes - (yes_entry_price + settings.taker_fee_per_share + settings.taker_slippage_per_share)
        no_edge = fair_no - (no_entry_price + settings.taker_fee_per_share + settings.taker_slippage_per_share)
        side = "YES" if yes_edge >= no_edge else "NO"
        entry_price = yes_entry_price if side == "YES" else no_entry_price
        entry_depth_usdc = yes_entry_depth_usdc if side == "YES" else no_entry_depth_usdc
        edge = yes_edge if side == "YES" else no_edge
        confidence = abs(fair_yes - 0.5) + 0.25 * abs(imbalance)
        book_age_ms = float(cache.get("book_age_ms") or 0.0)
        seconds_left = float(fair.get("time_to_expiry_sec") or 0.0)
        reference_price = float(fair.get("reference_price") or 0.0)
        volatility = float(fair.get("volatility") or 0.0)
        signal = {
            "market_id": market_id,
            "asset": asset,
            "tenor_minutes": tenor,
            "fair_yes": fair_yes,
            "fair_no": fair_no,
            "yes_bid": best_bid,
            "yes_ask": best_ask,
            "no_bid": no_bid,
            "no_ask": no_ask,
            "min_depth_usdc": entry_depth_usdc,
            "book_age_ms": book_age_ms,
            "seconds_left": seconds_left,
            "reference_price": reference_price,
            "volatility": volatility,
            "signal_type": "BTC_FAIR_VALUE_SKIP",
            "mode": "btc_fair_value_paper",
            "edge": round(max(edge, 0.0), 6),
            "eligible": False,
            "reason": "btc fair-value edge below threshold",
            "blocked_reason": "edge",
            "side": side,
            "order_price": entry_price,
        }
        if asset not in allowed_assets:
            signal.update({"reason": "asset disabled", "blocked_reason": "asset", "edge": 0.0})
        elif seconds_left <= settings.btc_fair_value_paper_force_exit_seconds:
            signal.update({"reason": "too close to expiry", "blocked_reason": "time_to_expiry", "edge": 0.0})
        elif seconds_left < settings.btc_fair_value_paper_min_seconds_left:
            signal.update({"reason": "entry too close to expiry", "blocked_reason": "entry_time_to_expiry", "edge": 0.0})
        elif seconds_left > settings.btc_fair_value_paper_max_seconds_left:
            signal.update({"reason": "too far from expiry", "blocked_reason": "max_time_to_expiry", "edge": 0.0})
        elif confidence < settings.btc_fair_value_paper_min_model_confidence:
            signal.update({"reason": "model confidence below threshold", "blocked_reason": "model_confidence", "edge": 0.0})
        elif entry_depth_usdc < settings.btc_fair_value_paper_min_depth_usdc:
            signal.update({"reason": "insufficient visible depth", "blocked_reason": "min_book_depth", "edge": 0.0})
        elif book_age_ms > settings.btc_fair_value_paper_max_book_age_ms:
            signal.update({"reason": "book snapshot too stale", "blocked_reason": "book_age", "edge": 0.0})
        elif entry_price < settings.btc_fair_value_paper_min_trade_price or entry_price > settings.btc_fair_value_paper_max_trade_price:
            signal.update({"reason": "price outside btc fair-value band", "blocked_reason": "trade_price_band"})
        elif edge >= settings.btc_fair_value_paper_min_edge_per_share:
            signal.update(
                {
                    "signal_type": f"BTC_FAIR_VALUE_TAKE_{side}",
                    "edge": round(edge, 6),
                    "eligible": True,
                    "reason": "btc fair-value edge clears threshold",
                    "blocked_reason": "",
                }
            )
        results.append(signal)
    return results


def build_temporal_inventory_maker_paper_signals(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
    fair_values: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not settings.temporal_inventory_maker_paper_enabled:
        return []
    markets = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    market_by_id = {str(item.get("market_id") or ""): item for item in markets if isinstance(item, dict)}
    cache_items = polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else []
    cache_by_id = {str(item.get("market_id") or ""): item for item in cache_items if isinstance(item, dict)}
    fair_by_asset_tenor: dict[tuple[str, int], list[float]] = {}
    for fair in fair_values:
        if not isinstance(fair, dict):
            continue
        market = market_by_id.get(str(fair.get("market_id") or ""))
        if not market:
            continue
        asset = str(fair.get("asset") or market.get("asset") or "").lower()
        tenor = int(market.get("tenor_minutes") or 0)
        fair_by_asset_tenor.setdefault((asset, tenor), []).append(_clamp_probability(float(fair.get("fair_yes") or 0.5)))

    max_book_age_ms = max(float(settings.feed_max_polymarket_staleness_seconds) * 1000.0, 1.0)
    min_edge = max(float(settings.temporal_inventory_maker_paper_min_net_edge), 0.0)
    quote_notional = max(float(settings.temporal_inventory_maker_paper_base_order_usdc), 0.0)
    results: list[dict[str, Any]] = []
    for fair in fair_values:
        if not isinstance(fair, dict):
            continue
        market_id = str(fair.get("market_id") or "")
        market = market_by_id.get(market_id)
        cache = cache_by_id.get(market_id)
        if market is None or cache is None:
            continue
        asset = str(fair.get("asset") or market.get("asset") or "").lower()
        tenor = int(market.get("tenor_minutes") or 0)
        best_bid = float(cache.get("best_bid") or 0.0)
        best_ask = float(cache.get("best_ask") or 0.0)
        no_bid = float(cache.get("no_best_bid") or max(1.0 - best_ask, 0.0))
        no_ask = float(cache.get("no_best_ask") or max(1.0 - best_bid, 0.0))
        bid_depth = float(cache.get("bids_depth_usdc") or cache.get("bid_depth_usdc") or 0.0)
        ask_depth = float(cache.get("asks_depth_usdc") or cache.get("ask_depth_usdc") or 0.0)
        yes_mid = (best_bid + best_ask) / 2.0 if best_bid > 0.0 and best_ask > 0.0 else 0.5
        micro_yes, imbalance = _book_microprice(best_bid, best_ask, bid_depth, ask_depth)
        raw_fair_yes = _clamp_probability(float(fair.get("fair_yes") or 0.5))
        spread = max(best_ask - best_bid, 0.0) if best_ask > 0.0 and best_bid > 0.0 else 1.0
        book_age_ms = float(cache.get("book_age_ms") or 0.0)
        staleness = min(max(book_age_ms / max_book_age_ms, 0.0), 2.0)
        related_conflict = 0.0
        related_values: list[float] = []
        for (other_asset, other_tenor), values in fair_by_asset_tenor.items():
            if other_asset == asset and other_tenor != tenor:
                related_values.extend(values)
        if related_values:
            related_avg = sum(related_values) / len(related_values)
            related_conflict = abs(raw_fair_yes - related_avg)
        shrink = min(0.55, 0.18 * staleness + 2.0 * max(spread - 0.02, 0.0) + 0.55 * max(related_conflict - 0.06, 0.0))
        blended_yes = _clamp_probability(0.56 * raw_fair_yes + 0.24 * micro_yes + 0.20 * yes_mid)
        fair_yes = _clamp_probability(0.5 + (1.0 - shrink) * (blended_yes - 0.5))
        fair_no = 1.0 - fair_yes
        uncertainty = 0.008 + 0.35 * spread + 0.010 * min(staleness, 1.5) + 0.25 * related_conflict
        adverse_buffer = max(0.004, spread * 0.25) + float(settings.taker_fee_per_share or 0.0)
        seconds_left = float(fair.get("time_to_expiry_sec") or 0.0)

        def maker_quote(best_bid_value: float, best_ask_value: float, side_fair: float) -> tuple[float, float, str, float, float]:
            if side_fair <= 0.0 or best_bid_value <= 0.0:
                return 0.0, -1.0, "INVALID", 0.0, 0.0
            tick = 0.01
            post_only_ceiling = best_ask_value - tick if best_ask_value > tick else best_bid_value
            fair_ceiling = side_fair - min_edge - adverse_buffer - uncertainty
            if post_only_ceiling <= 0.0 or fair_ceiling <= 0.0:
                return 0.0, -1.0, "INVALID", 0.0, 0.0

            def candidate(price: float, style: str) -> tuple[float, float, str, float, float]:
                quote_price = round(max(min(price, post_only_ceiling, fair_ceiling), 0.0), 4)
                if quote_price <= 0.0:
                    return 0.0, -1.0, style, 0.0, 0.0
                net_edge = side_fair - quote_price - adverse_buffer - uncertainty
                fill_probability = _estimate_maker_fill_probability(
                    best_bid=best_bid_value,
                    best_ask=best_ask_value,
                    quote_price=quote_price,
                    edge=net_edge,
                    seconds_left=seconds_left,
                    book_age_ms=book_age_ms,
                    max_book_age_ms=max_book_age_ms,
                )
                expected_value = _temporal_expected_value_usdc(
                    edge=net_edge,
                    quote_price=quote_price,
                    quote_notional=quote_notional,
                    fill_probability=fill_probability,
                    spread=max(best_ask_value - best_bid_value, 0.0),
                )
                return quote_price, net_edge, style, fill_probability, expected_value

            passive = candidate(best_bid_value + tick, "PASSIVE")
            midpoint = candidate((best_bid_value + best_ask_value) / 2.0, "MID_AGGRESSIVE")
            near_touch = candidate(post_only_ceiling, "NEAR_TOUCH")
            if near_touch[1] >= float(settings.temporal_inventory_maker_paper_near_touch_min_edge):
                return near_touch
            if midpoint[1] >= float(settings.temporal_inventory_maker_paper_mid_aggressive_min_edge):
                return midpoint
            if passive[1] >= min_edge:
                return passive
            return max((passive, midpoint, near_touch), key=lambda item: item[1])

        yes_quote, yes_edge, yes_style, yes_fill_probability, yes_expected_value = maker_quote(best_bid, best_ask, fair_yes)
        no_quote, no_edge, no_style, no_fill_probability, no_expected_value = maker_quote(no_bid, no_ask, fair_no)
        side = "YES" if yes_edge >= no_edge else "NO"
        order_price = yes_quote if side == "YES" else no_quote
        edge = yes_edge if side == "YES" else no_edge
        quote_style = yes_style if side == "YES" else no_style
        fill_probability = yes_fill_probability if side == "YES" else no_fill_probability
        expected_value_usdc = yes_expected_value if side == "YES" else no_expected_value
        side_fair = fair_yes if side == "YES" else fair_no
        size = quote_notional / order_price if order_price > 0.0 and quote_notional > 0.0 else 0.0
        signal = {
            "market_id": market_id,
            "asset": asset,
            "tenor_minutes": tenor,
            "fair_yes": fair_yes,
            "fair_no": fair_no,
            "yes_bid": best_bid,
            "yes_ask": best_ask,
            "no_bid": no_bid,
            "no_ask": no_ask,
            "min_depth_usdc": float(cache.get("min_depth_usdc") or 0.0),
            "book_age_ms": book_age_ms,
            "seconds_left": seconds_left,
            "reference_price": float(fair.get("reference_price") or 0.0),
            "volatility": float(fair.get("volatility") or 0.0),
            "signal_type": "TEMPORAL_INVENTORY_SKIP",
            "mode": "temporal_inventory_maker_paper",
            "edge": round(max(edge, 0.0), 6),
            "eligible": False,
            "reason": "temporal inventory maker edge below threshold",
            "blocked_reason": "edge",
            "side": side,
            "order_price": order_price,
            "quote_size": size,
            "side_fair": side_fair,
            "quote_style": quote_style,
            "fill_probability": fill_probability,
            "expected_value_usdc": expected_value_usdc,
            "adverse_selection_buffer": round(adverse_buffer, 6),
            "model_uncertainty": round(uncertainty, 6),
            "book_imbalance": round(imbalance, 6),
            "related_conflict": round(related_conflict, 6),
        }
        if seconds_left <= settings.temporal_inventory_maker_paper_force_exit_seconds:
            signal.update({"reason": "too close to expiry", "blocked_reason": "time_to_expiry", "edge": 0.0})
        elif book_age_ms > max_book_age_ms:
            signal.update({"reason": "book snapshot too stale", "blocked_reason": "book_age", "edge": 0.0})
        elif best_bid <= 0.0 or best_ask <= 0.0 or no_bid <= 0.0 or no_ask <= 0.0:
            signal.update({"reason": "incomplete YES/NO book", "blocked_reason": "book"})
        elif order_price <= 0.0 or size <= 0.0:
            signal.update({"reason": "no valid post-only quote price", "blocked_reason": "quote_price", "edge": 0.0})
        elif fill_probability < float(settings.temporal_inventory_maker_paper_min_fill_probability):
            signal.update({"reason": "estimated maker fill probability below threshold", "blocked_reason": "fill_probability"})
        elif expected_value_usdc < float(settings.temporal_inventory_maker_paper_min_expected_value_usdc):
            signal.update({"reason": "expected maker quote value below threshold", "blocked_reason": "expected_value"})
        elif edge >= min_edge:
            signal.update(
                {
                    "signal_type": f"TEMPORAL_INVENTORY_MAKE_{side}",
                    "edge": round(edge, 6),
                    "eligible": True,
                    "reason": "temporal inventory maker edge clears threshold",
                    "blocked_reason": "",
                }
            )
        results.append(signal)
    return results


def build_late_resolution_capture_paper_signals(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
    fair_values: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not settings.late_resolution_capture_paper_enabled:
        return []
    markets = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    market_by_id = {str(item.get("market_id") or ""): item for item in markets if isinstance(item, dict)}
    cache_items = polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else []
    cache_by_id = {str(item.get("market_id") or ""): item for item in cache_items if isinstance(item, dict)}
    results: list[dict[str, Any]] = []
    for fair in fair_values:
        if not isinstance(fair, dict):
            continue
        market_id = str(fair.get("market_id") or "")
        market = market_by_id.get(market_id)
        cache = cache_by_id.get(market_id)
        if market is None or cache is None:
            continue
        asset = str(fair.get("asset") or market.get("asset") or "").lower()
        tenor = int(market.get("tenor_minutes") or 0)
        fair_yes = _clamp_probability(float(fair.get("fair_yes") or 0.5))
        fair_no = 1.0 - fair_yes
        reference_price = float(fair.get("reference_price") or 0.0)
        current_price = float(fair.get("current_price") or 0.0)
        boundary_distance_bps = (
            abs(current_price - reference_price) / reference_price * 10000.0
            if reference_price > 0.0 and current_price > 0.0
            else 0.0
        )
        side = "YES" if fair_yes >= fair_no else "NO"
        official_confidence = max(fair_yes, fair_no)
        best_bid = float(cache.get("best_bid") or 0.0)
        best_ask = float(cache.get("best_ask") or 0.0)
        no_bid = float(cache.get("no_best_bid") or max(1.0 - best_ask, 0.0))
        no_ask = float(cache.get("no_best_ask") or max(1.0 - best_bid, 0.0))
        order_price = float(cache.get("ask_vwap") or best_ask) if side == "YES" else float(cache.get("no_ask_vwap") or no_ask)
        min_depth = float(cache.get("ask_fillable_usdc") or cache.get("asks_depth_usdc") or 0.0) if side == "YES" else float(cache.get("no_ask_fillable_usdc") or cache.get("no_asks_depth_usdc") or 0.0)
        selected_bid = best_bid if side == "YES" else no_bid
        book_age_ms = float(cache.get("book_age_ms") or 0.0)
        seconds_left = float(fair.get("time_to_expiry_sec") or 0.0)
        edge = official_confidence - order_price - float(settings.taker_slippage_per_share or 0.0) - float(settings.taker_fee_per_share or 0.0)
        signal = {
            "market_id": market_id,
            "asset": asset,
            "tenor_minutes": tenor,
            "side": side,
            "signal_type": "LATE_RESOLUTION_SKIP",
            "mode": "late_resolution_capture_paper",
            "edge": round(max(edge, 0.0), 6),
            "order_price": order_price,
            "fair_yes": fair_yes,
            "fair_no": fair_no,
            "yes_bid": best_bid,
            "yes_ask": best_ask,
            "no_bid": no_bid,
            "no_ask": no_ask,
            "official_confidence": official_confidence,
            "boundary_distance_bps": boundary_distance_bps,
            "seconds_left": seconds_left,
            "min_depth_usdc": min_depth,
            "book_age_ms": book_age_ms,
            "eligible": False,
            "reason": "late-resolution edge below threshold",
            "blocked_reason": "edge",
        }
        if seconds_left < settings.late_resolution_capture_paper_min_seconds_left:
            signal.update({"reason": "too close to expiry for capture entry", "blocked_reason": "min_time", "edge": 0.0})
        elif seconds_left > settings.late_resolution_capture_paper_max_seconds_left:
            signal.update({"reason": "too far from expiry for late capture", "blocked_reason": "max_time", "edge": 0.0})
        elif book_age_ms > float(settings.late_resolution_capture_paper_max_book_age_ms):
            signal.update({"reason": "late-resolution book snapshot too stale", "blocked_reason": "book_age", "edge": 0.0})
        elif official_confidence < settings.late_resolution_capture_paper_min_official_confidence:
            signal.update({"reason": "official confidence below threshold", "blocked_reason": "official_confidence", "edge": 0.0})
        elif boundary_distance_bps < settings.late_resolution_capture_paper_min_boundary_distance_bps:
            signal.update({"reason": "boundary distance below threshold", "blocked_reason": "boundary_distance", "edge": 0.0})
        elif min_depth < float(settings.late_resolution_capture_paper_min_depth_usdc):
            signal.update({"reason": "late-resolution entry depth below threshold", "blocked_reason": "min_depth", "edge": 0.0})
        elif selected_bid < float(settings.late_resolution_capture_paper_min_exit_bid):
            signal.update({"reason": "late-resolution exit bid below threshold", "blocked_reason": "exit_bid", "edge": 0.0})
        elif order_price <= 0.0:
            signal.update({"reason": "missing entry price", "blocked_reason": "price", "edge": 0.0})
        elif edge >= settings.late_resolution_capture_paper_min_edge:
            signal.update(
                {
                    "signal_type": f"LATE_RESOLUTION_CAPTURE_{side}",
                    "edge": round(edge, 6),
                    "eligible": True,
                    "reason": "late-resolution capture edge clears threshold",
                    "blocked_reason": "",
                }
            )
        results.append(signal)
    return results


def _wallet_trade_ts(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        value = float(raw)
        if value > 10_000_000_000:
            value /= 1000.0
        try:
            return datetime.fromtimestamp(value, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    text = str(raw).strip()
    if not text:
        return None
    if text.isdigit():
        return _wallet_trade_ts(float(text))
    text = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _wallet_trade_outcome(raw: Any) -> str:
    text = str(raw or "").strip().lower()
    if text in {"up", "yes"}:
        return "YES"
    if text in {"down", "no"}:
        return "NO"
    return ""


def build_wallet_teacher_sniper_signals(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
    wallet_trades_payload: dict[str, Any],
    ts: datetime | None = None,
) -> list[dict[str, Any]]:
    if not settings.wallet_teacher_sniper_enabled:
        return []
    now = (ts or datetime.now(timezone.utc)).astimezone(timezone.utc)
    allowed_assets = {str(asset).lower() for asset in settings.wallet_teacher_sniper_assets}
    markets = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    market_by_slug = {
        str(item.get("slug") or "").lower(): item
        for item in markets
        if isinstance(item, dict) and str(item.get("slug") or "")
    }
    cache_items = polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else []
    cache_by_id = {str(item.get("market_id") or ""): item for item in cache_items if isinstance(item, dict)}
    trades = wallet_trades_payload.get("items", []) if isinstance(wallet_trades_payload.get("items"), list) else []
    lookback = max(float(settings.wallet_teacher_sniper_trade_lookback_seconds or 0), 1.0)
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        if str(trade.get("side") or "").upper() != "BUY":
            continue
        slug = str(trade.get("slug") or "").lower()
        market = market_by_slug.get(slug)
        if market is None:
            continue
        trade_ts = _wallet_trade_ts(trade.get("timestamp") or trade.get("created_at") or trade.get("time"))
        if trade_ts is None:
            continue
        age_seconds = (now - trade_ts).total_seconds()
        if age_seconds < 0 or age_seconds > lookback:
            continue
        side = _wallet_trade_outcome(trade.get("outcome"))
        if not side:
            side = "YES" if int(trade.get("outcome_index") or -1) == 0 else "NO" if int(trade.get("outcome_index") or -1) == 1 else ""
        if side not in {"YES", "NO"}:
            continue
        try:
            price = float(trade.get("price") or 0.0)
            size = float(trade.get("size") or 0.0)
        except (TypeError, ValueError):
            continue
        notional = price * size
        if notional < float(settings.wallet_teacher_sniper_min_teacher_notional_usdc):
            continue
        market_id = str(market.get("market_id") or "")
        key = (market_id, side)
        bucket = grouped.setdefault(
            key,
            {
                "market": market,
                "side": side,
                "teacher_notional": 0.0,
                "teacher_size": 0.0,
                "teacher_trade_count": 0,
                "latest_trade_ts": trade_ts,
                "weighted_price_sum": 0.0,
            },
        )
        bucket["teacher_notional"] += notional
        bucket["teacher_size"] += size
        bucket["teacher_trade_count"] += 1
        bucket["weighted_price_sum"] += price * notional
        if trade_ts > bucket["latest_trade_ts"]:
            bucket["latest_trade_ts"] = trade_ts

    results: list[dict[str, Any]] = []
    for (market_id, side), bucket in grouped.items():
        market = bucket["market"]
        cache = cache_by_id.get(market_id)
        if cache is None:
            continue
        asset = str(market.get("asset") or "").lower()
        tenor = int(market.get("tenor_minutes") or 0)
        best_bid = float(cache.get("best_bid") or 0.0)
        best_ask = float(cache.get("best_ask") or 0.0)
        no_bid = float(cache.get("no_best_bid") or max(1.0 - best_ask, 0.0))
        no_ask = float(cache.get("no_best_ask") or max(1.0 - best_bid, 0.0))
        entry_price = float(cache.get("ask_vwap") or best_ask) if side == "YES" else float(cache.get("no_ask_vwap") or no_ask)
        entry_depth_usdc = (
            float(cache.get("ask_fillable_usdc") or cache.get("asks_depth_usdc") or cache.get("min_depth_usdc") or 0.0)
            if side == "YES"
            else float(cache.get("no_ask_fillable_usdc") or cache.get("no_asks_depth_usdc") or cache.get("min_depth_usdc") or 0.0)
        )
        seconds_left = 0.0
        raw_expiry = str(market.get("expiry_ts") or "")
        if raw_expiry:
            expiry_text = raw_expiry[:-1] + "+00:00" if raw_expiry.endswith("Z") else raw_expiry
            try:
                expiry = datetime.fromisoformat(expiry_text)
                if expiry.tzinfo is None:
                    expiry = expiry.replace(tzinfo=timezone.utc)
                seconds_left = max((expiry.astimezone(timezone.utc) - now).total_seconds(), 0.0)
            except ValueError:
                seconds_left = 0.0
        book_age_ms = float(cache.get("book_age_ms") or 0.0)
        teacher_notional = float(bucket.get("teacher_notional") or 0.0)
        teacher_size = float(bucket.get("teacher_size") or 0.0)
        teacher_avg_price = (
            float(bucket.get("weighted_price_sum") or 0.0) / teacher_notional if teacher_notional > 0.0 else 0.0
        )
        teacher_score = min(teacher_notional / max(float(settings.wallet_teacher_sniper_notional_usdc), 1.0), 10.0)
        signal = {
            "market_id": market_id,
            "asset": asset,
            "tenor_minutes": tenor,
            "fair_yes": 0.5,
            "fair_no": 0.5,
            "yes_bid": best_bid,
            "yes_ask": best_ask,
            "no_bid": no_bid,
            "no_ask": no_ask,
            "min_depth_usdc": entry_depth_usdc,
            "book_age_ms": book_age_ms,
            "seconds_left": seconds_left,
            "reference_price": teacher_avg_price,
            "volatility": teacher_size,
            "signal_type": "WALLET_TEACHER_SKIP",
            "mode": "wallet_teacher_sniper",
            "edge": round(teacher_score, 6),
            "eligible": False,
            "reason": "wallet teacher candidate blocked",
            "blocked_reason": "unknown",
            "side": side,
            "order_price": entry_price,
        }
        if asset not in allowed_assets:
            signal.update({"reason": "asset disabled", "blocked_reason": "asset", "edge": 0.0})
        elif tenor > 5:
            signal.update({"reason": "teacher market not 5m", "blocked_reason": "tenor", "edge": 0.0})
        elif seconds_left <= float(settings.wallet_teacher_sniper_force_exit_seconds):
            signal.update({"reason": "too close to expiry", "blocked_reason": "time_to_expiry", "edge": 0.0})
        elif seconds_left < float(settings.wallet_teacher_sniper_min_seconds_left):
            signal.update({"reason": "entry too close to expiry", "blocked_reason": "entry_time_to_expiry", "edge": 0.0})
        elif seconds_left > float(settings.wallet_teacher_sniper_max_seconds_left):
            signal.update({"reason": "too far from expiry", "blocked_reason": "max_time_to_expiry", "edge": 0.0})
        elif entry_depth_usdc < float(settings.wallet_teacher_sniper_min_depth_usdc):
            signal.update({"reason": "insufficient visible depth", "blocked_reason": "min_book_depth", "edge": 0.0})
        elif book_age_ms > float(settings.wallet_teacher_sniper_max_book_age_ms):
            signal.update({"reason": "book snapshot too stale", "blocked_reason": "book_age", "edge": 0.0})
        elif entry_price < float(settings.wallet_teacher_sniper_min_trade_price) or entry_price > float(settings.wallet_teacher_sniper_max_trade_price):
            signal.update({"reason": "price outside wallet-teacher band", "blocked_reason": "trade_price_band"})
        else:
            signal.update(
                {
                    "signal_type": f"WALLET_TEACHER_TAKE_{side}",
                    "eligible": True,
                    "reason": "wallet teacher recent buy matched current market",
                    "blocked_reason": "",
                }
            )
        results.append(signal)
    return results


def build_shadow_btc_no_signals(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
    fair_values: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not settings.shadow_allow_btc_no_taker:
        return []
    markets = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    market_by_id = {str(item.get("market_id") or ""): item for item in markets if isinstance(item, dict)}
    cache_items = polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else []
    cache_by_id = {str(item.get("market_id") or ""): item for item in cache_items if isinstance(item, dict)}
    results: list[dict[str, Any]] = []
    for fair in fair_values:
        if not isinstance(fair, dict):
            continue
        market_id = str(fair.get("market_id") or "")
        market = market_by_id.get(market_id)
        cache = cache_by_id.get(market_id)
        if market is None or cache is None:
            continue
        asset = str(fair.get("asset") or "").lower()
        tenor = int(market.get("tenor_minutes") or 0)
        if asset != "btc" or tenor > 5:
            continue
        best_bid = float(cache.get("best_bid") or 0.0)
        best_ask = float(cache.get("best_ask") or 0.0)
        min_depth_usdc = float(cache.get("min_depth_usdc") or 0.0)
        book_age_ms = float(cache.get("book_age_ms") or 0.0)
        fair_yes = float(fair.get("fair_yes") or 0.5)
        fair_no = float(fair.get("fair_no") or 0.5)
        reference_price = float(fair.get("reference_price") or 0.0)
        volatility = float(fair.get("volatility") or 0.0)
        seconds_left = float(fair.get("time_to_expiry_sec") or 0.0)
        force_exit_seconds = settings.force_exit_seconds_5m
        no_ask = max(1.0 - best_bid, 0.0)
        no_edge = fair_no - (no_ask + settings.taker_fee_per_share + settings.taker_slippage_per_share)
        signal = {
            "market_id": market_id,
            "asset": asset,
            "tenor_minutes": tenor,
            "fair_yes": fair_yes,
            "fair_no": fair_no,
            "yes_bid": best_bid,
            "yes_ask": best_ask,
            "no_ask": no_ask,
            "no_bid": max(1.0 - best_ask, 0.0),
            "min_depth_usdc": min_depth_usdc,
            "book_age_ms": book_age_ms,
            "seconds_left": seconds_left,
            "reference_price": reference_price,
            "volatility": volatility,
            "signal_type": "SHADOW_SKIP",
            "mode": "shadow",
            "edge": 0.0,
            "eligible": False,
            "reason": "edge below thresholds",
            "blocked_reason": "",
        }
        if seconds_left <= force_exit_seconds:
            signal.update({"reason": "too close to expiry", "blocked_reason": "time_to_expiry"})
        elif seconds_left < settings.min_taker_entry_seconds_left_5m:
            signal.update({"reason": "entry too close to expiry", "blocked_reason": "entry_time_to_expiry"})
        elif min_depth_usdc < settings.min_book_depth_usdc:
            signal.update({"reason": "insufficient visible depth", "blocked_reason": "min_book_depth"})
        elif book_age_ms > settings.max_book_age_ms:
            signal.update({"reason": "book snapshot too stale", "blocked_reason": "book_age"})
        elif fair_no < settings.shadow_taker_min_fair_no_5m:
            _skip(signal, reason="fair no below threshold", blocked_reason="min_fair_no", edge=no_edge)
        elif no_edge >= settings.shadow_taker_min_edge_5m and _price_in_trade_band(settings, no_ask):
            signal.update(
                {
                    "signal_type": "SHADOW_TAKE_NO",
                    "mode": "shadow_taker",
                    "edge": round(no_edge, 6),
                    "eligible": True,
                    "reason": "shadow no taker edge clears threshold",
                }
            )
        elif not _price_in_trade_band(settings, no_ask):
            _skip(signal, reason="no price outside trade band", blocked_reason="trade_price_band", edge=no_edge)
        else:
            _skip(signal, reason="no taker edge below threshold", blocked_reason="taker_edge", edge=no_edge)
        results.append(signal)
    return results


def build_shadow_btc_yes_variant_signals(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    polymarket_cache: dict[str, Any],
    fair_values: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    floors = tuple(sorted({round(float(value), 4) for value in settings.shadow_variant_fair_floors}))
    models = tuple(dict.fromkeys(str(model).strip().lower() for model in settings.shadow_variant_models if str(model).strip()))
    assets = set(settings.shadow_variant_assets)
    sides = {str(side).upper() for side in settings.shadow_variant_sides}
    tenors = set(int(tenor) for tenor in settings.shadow_variant_tenors_minutes)
    if not floors or not models or not assets or not sides or not tenors:
        return []
    markets = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    market_by_id = {str(item.get("market_id") or ""): item for item in markets if isinstance(item, dict)}
    cache_items = polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else []
    cache_by_id = {str(item.get("market_id") or ""): item for item in cache_items if isinstance(item, dict)}
    results: list[dict[str, Any]] = []
    for fair in fair_values:
        if not isinstance(fair, dict):
            continue
        market_id = str(fair.get("market_id") or "")
        market = market_by_id.get(market_id)
        cache = cache_by_id.get(market_id)
        if market is None or cache is None:
            continue
        asset = str(fair.get("asset") or "").lower()
        tenor = int(market.get("tenor_minutes") or 0)
        tenor_bucket = 5 if tenor <= 5 else 15
        if asset not in assets or tenor_bucket not in tenors:
            continue
        best_bid = float(cache.get("best_bid") or 0.0)
        best_ask = float(cache.get("best_ask") or 0.0)
        yes_market_mid = (best_bid + best_ask) / 2.0 if best_bid > 0.0 and best_ask > 0.0 else 0.5
        spread = max(best_ask - best_bid, 0.0) if best_ask > 0.0 and best_bid > 0.0 else 1.0
        min_depth_usdc = float(cache.get("min_depth_usdc") or 0.0)
        book_age_ms = float(cache.get("book_age_ms") or 0.0)
        raw_fair_yes = float(fair.get("fair_yes") or 0.5)
        reference_price = float(fair.get("reference_price") or 0.0)
        volatility = float(fair.get("volatility") or 0.0)
        seconds_left = float(fair.get("time_to_expiry_sec") or 0.0)
        force_exit_seconds = settings.force_exit_seconds_5m if tenor_bucket == 5 else settings.force_exit_seconds_15m
        edge_thresholds = settings.shadow_variant_edge_thresholds_5m if tenor_bucket == 5 else settings.shadow_variant_edge_thresholds_15m
        entry_seconds_options = settings.shadow_variant_entry_seconds_5m if tenor_bucket == 5 else settings.shadow_variant_entry_seconds_15m

        for side in sorted(sides):
            if side not in {"YES", "NO"}:
                continue
            entry_price = best_ask if side == "YES" else max(1.0 - best_bid, 0.0)
            side_mid = yes_market_mid if side == "YES" else 1.0 - yes_market_mid
            raw_side_fair = raw_fair_yes if side == "YES" else 1.0 - raw_fair_yes
            no_ask = max(1.0 - best_bid, 0.0)
            no_bid = max(1.0 - best_ask, 0.0)
            for model in models:
                model_side_fair = raw_side_fair
                if model != "fair":
                    model_side_fair = _shadow_model_fair_yes(
                        model,
                        fair_yes=raw_side_fair,
                        market_mid=side_mid,
                        seconds_left=seconds_left,
                        min_depth_usdc=min_depth_usdc,
                        spread=spread,
                    )
                fair_yes = model_side_fair if side == "YES" else 1.0 - model_side_fair
                fair_no = 1.0 - fair_yes
                side_fair = fair_yes if side == "YES" else fair_no
                side_edge = side_fair - (entry_price + settings.taker_fee_per_share + settings.taker_slippage_per_share)
                for floor in floors:
                    for edge_threshold in edge_thresholds:
                        for min_entry_seconds in entry_seconds_options:
                            variant_id = (
                                f"{asset}{tenor_bucket}_{side.lower()}_{model}"
                                f"_f{floor:.2f}_e{float(edge_threshold):.2f}_t{int(min_entry_seconds)}"
                            )
                            signal = {
                                "variant_id": variant_id,
                                "market_id": market_id,
                                "asset": asset,
                                "tenor_minutes": tenor_bucket,
                                "fair_yes": fair_yes,
                                "fair_no": fair_no,
                                "yes_bid": best_bid,
                                "yes_ask": best_ask,
                                "no_ask": no_ask,
                                "no_bid": no_bid,
                                "min_depth_usdc": min_depth_usdc,
                                "book_age_ms": book_age_ms,
                                "seconds_left": seconds_left,
                                "reference_price": reference_price,
                                "volatility": volatility,
                                "signal_type": "SHADOW_VARIANT_SKIP",
                                "mode": "shadow_taker_variant",
                                "edge": round(max(side_edge, 0.0), 6),
                                "eligible": False,
                                "reason": f"{side.lower()} taker edge below threshold",
                                "blocked_reason": "taker_edge",
                                "side": side,
                                "order_price": entry_price,
                                "fair_floor": floor,
                                "edge_threshold": float(edge_threshold),
                                "min_entry_seconds": int(min_entry_seconds),
                            }
                            if seconds_left <= force_exit_seconds:
                                signal.update({"reason": "too close to expiry", "blocked_reason": "time_to_expiry", "edge": 0.0})
                            elif seconds_left < int(min_entry_seconds):
                                signal.update({"reason": "entry too close to expiry", "blocked_reason": "entry_time_to_expiry", "edge": 0.0})
                            elif min_depth_usdc < settings.min_book_depth_usdc:
                                signal.update({"reason": "insufficient visible depth", "blocked_reason": "min_book_depth", "edge": 0.0})
                            elif book_age_ms > settings.max_book_age_ms:
                                signal.update({"reason": "book snapshot too stale", "blocked_reason": "book_age", "edge": 0.0})
                            elif side_fair < floor:
                                _skip(signal, reason=f"fair {side.lower()} below variant floor", blocked_reason="variant_min_fair", edge=side_edge)
                            elif not _price_in_trade_band(settings, entry_price):
                                _skip(signal, reason=f"{side.lower()} price outside trade band", blocked_reason="trade_price_band", edge=side_edge)
                            elif side_edge >= float(edge_threshold):
                                signal.update(
                                    {
                                        "signal_type": f"SHADOW_TAKE_{side}",
                                        "mode": "shadow_taker_variant",
                                        "edge": round(side_edge, 6),
                                        "eligible": True,
                                        "reason": f"shadow {side.lower()} variant edge clears threshold",
                                        "blocked_reason": "",
                                    }
                                )
                            else:
                                _skip(signal, reason=f"{side.lower()} taker edge below threshold", blocked_reason="taker_edge", edge=side_edge)
                            results.append(signal)
    return results
