from __future__ import annotations

import math
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
