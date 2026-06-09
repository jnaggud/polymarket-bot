from __future__ import annotations

from dataclasses import replace
import html
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .config import LatencyBotSettings
from .storage import (
    init_latency_bot_db,
    latency_bot_capital_usage,
    latency_bot_equity_curve,
    latency_bot_complete_set_arb_stats,
    latency_bot_kalshi_arb_sim,
    latency_bot_live_strategy_equity_curves,
    latency_bot_live_complete_set_arb_pilot_stats,
    latency_bot_opportunity_stats,
    latency_bot_performance_stats,
    latency_bot_polymarket_us_arb_sim,
    latency_bot_signal_feature_stats,
    latency_bot_portfolio_summary,
    latency_bot_promoted_variant_performance_stats,
    latency_bot_realistic_complete_set_arb_sim,
    latency_bot_shadow_performance_stats,
    latency_bot_shadow_portfolio_summary,
    latency_bot_shadow_variant_performance_stats,
    latest_rows_since,
    latest_shadow_rows_since,
    load_open_positions,
    load_shadow_open_positions,
    summarize_latency_bot_db,
    latency_bot_threshold_relaxation_stats,
)


DISPLAY_TZ = ZoneInfo("America/Chicago")
_SHADOW_VARIANT_CACHE_TTL_SECONDS = 60.0
_SHADOW_VARIANT_CACHE_LOCK = threading.Lock()
_SHADOW_VARIANT_CACHE: dict[str, Any] = {"key": "", "loaded_at": 0.0, "payload": None}


def _load_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _fmt_money(value: Any) -> str:
    try:
        return f"${float(value):,.2f}"
    except (TypeError, ValueError):
        return "$0.00"


def _fmt_num(value: Any) -> str:
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "0"


def _parse_ts(raw: Any) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(DISPLAY_TZ)
    except ValueError:
        return None


def _fmt_ts(raw: Any) -> str:
    parsed = _parse_ts(raw)
    if parsed is None:
        text = str(raw or "").strip()
        return text or "-"
    return parsed.strftime("%Y-%m-%d %H:%M:%S CT")


def _fmt_curve_ts(raw: Any) -> str:
    parsed = _parse_ts(raw)
    if parsed is None:
        text = str(raw or "").strip()
        if not text:
            return "-"
        return text
    return parsed.strftime("%H:%M:%S")


def _table(headers: list[str], rows: list[list[str]]) -> str:
    head = "".join(f"<th>{html.escape(header)}</th>" for header in headers)
    if not rows:
        body = f"<tr><td colspan='{len(headers)}'>No data</td></tr>"
    else:
        body = "".join("<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>" for row in rows)
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def _render_equity_curve(points: list[dict[str, Any]], bankroll_usdc: float) -> str:
    if not points:
        return "<div class='sub'>No closed trades yet.</div>"
    width = 960
    height = 260
    pad_left = 78
    pad_right = 18
    pad_top = 18
    pad_bottom = 42
    values = [bankroll_usdc] + [float(point.get("equity_usdc") or bankroll_usdc) for point in points]
    min_value = min(values)
    max_value = max(values)
    if max_value == min_value:
        max_value += 1.0
        min_value -= 1.0
    axis_values = [max_value, (max_value + min_value) / 2.0, min_value]

    times: list[datetime] = []
    for point in points:
        parsed = _parse_ts(point.get("ts"))
        times.append(parsed or datetime.now(DISPLAY_TZ))
    min_ts = min(times)
    max_ts = max(times)
    span_seconds = max((max_ts - min_ts).total_seconds(), 1.0)

    def x_for(idx: int) -> float:
        if len(points) == 1:
            return (pad_left + width - pad_right) / 2
        seconds = (times[idx - 1] - min_ts).total_seconds()
        return pad_left + (seconds / span_seconds) * (width - pad_left - pad_right)

    def y_for(value: float) -> float:
        ratio = (value - min_value) / (max_value - min_value)
        return height - pad_bottom - (ratio * (height - pad_top - pad_bottom))

    polyline = " ".join(f"{x_for(idx):.1f},{y_for(float(point.get('equity_usdc') or bankroll_usdc)):.1f}" for idx, point in enumerate(points, start=1))
    baseline = y_for(bankroll_usdc)
    last = points[-1]
    first = points[0]
    mid_ts = min_ts + timedelta(seconds=(span_seconds / 2.0))
    mid_x = pad_left + ((span_seconds / 2.0) / span_seconds) * (width - pad_left - pad_right)
    grid_lines = "".join(
        f"<line x1='{pad_left}' y1='{y_for(value):.1f}' x2='{width - pad_right}' y2='{y_for(value):.1f}' class='curve-grid' />"
        for value in axis_values
    )
    y_ticks = "".join(
        (
            f"<line x1='{pad_left - 6}' y1='{y_for(value):.1f}' x2='{pad_left}' y2='{y_for(value):.1f}' class='curve-axis' />"
            f"<text x='{pad_left - 10}' y='{y_for(value) + 4:.1f}' text-anchor='end' class='curve-label'>{html.escape(_fmt_money(value))}</text>"
        )
        for value in axis_values
    )
    x_axis_y = height - pad_bottom
    return (
        f"<svg viewBox='0 0 {width} {height}' class='curve' role='img' aria-label='Latency bot equity curve'>"
        f"{grid_lines}"
        f"<line x1='{pad_left}' y1='{pad_top}' x2='{pad_left}' y2='{x_axis_y}' class='curve-axis' />"
        f"<line x1='{pad_left}' y1='{x_axis_y}' x2='{width - pad_right}' y2='{x_axis_y}' class='curve-axis' />"
        f"<line x1='{pad_left}' y1='{baseline:.1f}' x2='{width - pad_right}' y2='{baseline:.1f}' class='curve-baseline' />"
        f"{y_ticks}"
        f"<text x='{pad_left}' y='{height - 12}' text-anchor='start' class='curve-label'>{html.escape(_fmt_curve_ts(first.get('ts')))}</text>"
        f"<text x='{mid_x:.1f}' y='{height - 12}' text-anchor='middle' class='curve-label'>{html.escape(mid_ts.strftime('%H:%M:%S'))}</text>"
        f"<text x='{width - pad_right}' y='{height - 12}' text-anchor='end' class='curve-label'>{html.escape(_fmt_curve_ts(last.get('ts')))}</text>"
        f"<polyline points='{polyline}' class='curve-line' />"
        f"<circle cx='{x_for(len(points)):.1f}' cy='{y_for(float(last.get('equity_usdc') or bankroll_usdc)):.1f}' r='4' class='curve-dot' />"
        "</svg>"
    )


def _render_variant_equity_curves(points: list[dict[str, Any]], bankroll_usdc: float) -> str:
    if not points:
        return "<div class='sub'>No live-paper strategy closes yet.</div>"
    by_variant: dict[str, list[dict[str, Any]]] = {}
    for point in points:
        variant_id = str(point.get("variant_id") or "variant")
        by_variant.setdefault(variant_id, []).append(point)
    rendered = []
    for variant_id, variant_points in sorted(by_variant.items()):
        rendered.append(f"<h3>{html.escape(variant_id)}</h3>{_render_equity_curve(variant_points, bankroll_usdc)}")
    return "".join(rendered)


def _render_shadow_variant_table(items: list[dict[str, Any]]) -> str:
    return _table(
        [
            "Variant",
            "Signals",
            "Eligible",
            "Open",
            "Closed",
            "Markets",
            "Win Rate",
            "Net PnL",
            "Recent 10",
            "Max DD",
            "Promotion",
            "Reason",
            "Avg Edge",
            "Avg Fair",
            "Avg Entry",
        ],
        [
            [
                html.escape(str(item.get("variant_id", ""))),
                html.escape(_fmt_num(item.get("signals", 0))),
                html.escape(_fmt_num(item.get("eligible", 0))),
                html.escape(_fmt_num(item.get("open", 0))),
                html.escape(_fmt_num(item.get("closed", 0))),
                html.escape(_fmt_num(item.get("unique_markets", 0))),
                html.escape(f"{100.0 * float(item.get('win_rate', 0.0)):.1f}%"),
                html.escape(_fmt_money(item.get("net_pnl", 0.0))),
                html.escape(_fmt_money(item.get("recent_10_pnl", 0.0))),
                html.escape(_fmt_money(item.get("max_drawdown", 0.0))),
                html.escape(str(item.get("promotion_status", ""))),
                html.escape(str(item.get("promotion_reason", ""))),
                html.escape(f"{float(item.get('avg_edge', 0.0)):.4f}"),
                html.escape(f"{float(item.get('avg_fair_yes', 0.0)):.4f}"),
                html.escape(f"{float(item.get('avg_entry_price', 0.0)):.4f}"),
            ]
            for item in items
        ],
    )


def _render_shadow_variant_family_table(items: list[dict[str, Any]]) -> str:
    return _table(
        [
            "Family",
            "Variants",
            "Positive",
            "Candidates",
            "Representative",
            "Rep Closed",
            "Rep Markets",
            "Rep Win Rate",
            "Rep Net PnL",
            "Rep Recent 10",
            "Rep Max DD",
            "Rep Status",
            "Rep Reason",
        ],
        [
            [
                html.escape(str(item.get("family", ""))),
                html.escape(_fmt_num(item.get("variants", 0))),
                html.escape(_fmt_num(item.get("positive_variants", 0))),
                html.escape(_fmt_num(item.get("candidate_variants", 0))),
                html.escape(str(item.get("representative", ""))),
                html.escape(_fmt_num(item.get("rep_closed", 0))),
                html.escape(_fmt_num(item.get("rep_unique_markets", 0))),
                html.escape(f"{100.0 * float(item.get('rep_win_rate', 0.0)):.1f}%"),
                html.escape(_fmt_money(item.get("rep_net_pnl", 0.0))),
                html.escape(_fmt_money(item.get("rep_recent_10_pnl", 0.0))),
                html.escape(_fmt_money(item.get("rep_max_drawdown", 0.0))),
                html.escape(str(item.get("rep_promotion_status", ""))),
                html.escape(str(item.get("rep_promotion_reason", ""))),
            ]
            for item in items
        ],
    )


def _shadow_variant_family_id(variant_id: str) -> str:
    text = str(variant_id or "")
    if "_f" in text:
        return text.split("_f", 1)[0]
    return text


def _shadow_variant_sleeve_id(variant_id: str) -> str:
    parts = _shadow_variant_family_id(variant_id).split("_")
    if len(parts) < 2:
        return _shadow_variant_family_id(variant_id) or "unknown"
    return f"{parts[0]}_{parts[1]}"


def _variant_quality_score(item: dict[str, Any]) -> float:
    closed = float(item.get("closed", 0) or 0)
    win_rate = float(item.get("win_rate", 0.0) or 0.0)
    net_pnl = float(item.get("net_pnl", 0.0) or 0.0)
    recent_10 = float(item.get("recent_10_pnl", 0.0) or 0.0)
    max_drawdown = float(item.get("max_drawdown", 0.0) or 0.0)
    avg_edge = float(item.get("avg_edge", 0.0) or 0.0)

    score = min(15.0, (closed / 30.0) * 15.0)
    score += max(0.0, min(25.0, ((win_rate - 0.50) / 0.25) * 25.0))
    score += max(0.0, min(20.0, (net_pnl / 1000.0) * 20.0))
    score += min(15.0, (recent_10 / 200.0) * 15.0) if recent_10 > 0.0 else max(-20.0, recent_10 / 10.0)
    score += 15.0 if max_drawdown >= -150.0 else max(-25.0, 15.0 - ((abs(max_drawdown) - 150.0) / 5.0))
    score += max(0.0, min(10.0, (avg_edge / 0.10) * 10.0))
    return max(0.0, min(100.0, round(score, 1)))


def _variant_consensus_action(item: dict[str, Any], score: float) -> str:
    status = str(item.get("promotion_status") or "")
    reason = str(item.get("promotion_reason") or "")
    if status == "candidate" and score >= 75.0:
        return "primary voter / size-up eligible"
    if status == "candidate":
        return "candidate voter"
    if "recent 10" in reason or "drawdown" in reason:
        return "confirmation only"
    if status == "collecting":
        return "collect more closes"
    return "shadow only"


def _consensus_meta_strategy_data(shadow_variant_stats: dict[str, Any]) -> dict[str, Any]:
    candidates: dict[str, dict[str, Any]] = {}
    for bucket in ("watchlist", "top_raw_pnl", "variants"):
        for item in shadow_variant_stats.get(bucket, []) if isinstance(shadow_variant_stats.get(bucket), list) else []:
            if not isinstance(item, dict):
                continue
            variant_id = str(item.get("variant_id") or "")
            if not variant_id:
                continue
            candidates[variant_id] = dict(item)

    voters: list[dict[str, Any]] = []
    for item in candidates.values():
        variant_id = str(item.get("variant_id") or "")
        score = _variant_quality_score(item)
        voters.append(
            {
                **item,
                "family": _shadow_variant_family_id(variant_id),
                "sleeve": _shadow_variant_sleeve_id(variant_id),
                "consensus_score": score,
                "consensus_action": _variant_consensus_action(item, score),
            }
        )
    voters.sort(
        key=lambda item: (
            float(item.get("consensus_score", 0.0) or 0.0),
            float(item.get("net_pnl", 0.0) or 0.0),
            int(item.get("closed", 0) or 0),
        ),
        reverse=True,
    )

    sleeve_map: dict[str, list[dict[str, Any]]] = {}
    for item in voters:
        sleeve_map.setdefault(str(item.get("sleeve") or "unknown"), []).append(item)
    sleeves = []
    for sleeve, items in sleeve_map.items():
        representative = items[0]
        sleeves.append(
            {
                "sleeve": sleeve,
                "voters": len(items),
                "ready_voters": sum(
                    1
                    for item in items
                    if str(item.get("promotion_status") or "") == "candidate"
                    and float(item.get("consensus_score", 0.0) or 0.0) >= 65.0
                ),
                "representative": str(representative.get("variant_id") or ""),
                "score": float(representative.get("consensus_score", 0.0) or 0.0),
                "net_pnl": float(representative.get("net_pnl", 0.0) or 0.0),
                "recent_10_pnl": float(representative.get("recent_10_pnl", 0.0) or 0.0),
                "max_drawdown": float(representative.get("max_drawdown", 0.0) or 0.0),
                "action": str(representative.get("consensus_action") or ""),
            }
        )
    sleeves.sort(key=lambda item: (float(item["score"]), float(item["net_pnl"])), reverse=True)
    ready_sleeves = sum(1 for item in sleeves if int(item.get("ready_voters", 0) or 0) > 0)
    high_confidence_voters = sum(1 for item in voters if float(item.get("consensus_score", 0.0) or 0.0) >= 75.0)
    return {
        "voters": voters[:12],
        "sleeves": sleeves[:8],
        "ready_sleeves": ready_sleeves,
        "high_confidence_voters": high_confidence_voters,
    }


def _render_consensus_voter_table(items: list[dict[str, Any]]) -> str:
    return _table(
        [
            "Variant",
            "Sleeve",
            "Score",
            "Closed",
            "Win Rate",
            "Net PnL",
            "Recent 10",
            "Max DD",
            "Status",
            "Action",
        ],
        [
            [
                html.escape(str(item.get("variant_id", ""))),
                html.escape(str(item.get("sleeve", ""))),
                html.escape(f"{float(item.get('consensus_score', 0.0)):.1f}"),
                html.escape(_fmt_num(item.get("closed", 0))),
                html.escape(f"{100.0 * float(item.get('win_rate', 0.0)):.1f}%"),
                html.escape(_fmt_money(item.get("net_pnl", 0.0))),
                html.escape(_fmt_money(item.get("recent_10_pnl", 0.0))),
                html.escape(_fmt_money(item.get("max_drawdown", 0.0))),
                html.escape(str(item.get("promotion_status", ""))),
                html.escape(str(item.get("consensus_action", ""))),
            ]
            for item in items
        ],
    )


def _render_consensus_sleeve_table(items: list[dict[str, Any]]) -> str:
    return _table(
        ["Sleeve", "Voters", "Ready", "Representative", "Score", "Net PnL", "Recent 10", "Max DD", "Action"],
        [
            [
                html.escape(str(item.get("sleeve", ""))),
                html.escape(_fmt_num(item.get("voters", 0))),
                html.escape(_fmt_num(item.get("ready_voters", 0))),
                html.escape(str(item.get("representative", ""))),
                html.escape(f"{float(item.get('score', 0.0)):.1f}"),
                html.escape(_fmt_money(item.get("net_pnl", 0.0))),
                html.escape(_fmt_money(item.get("recent_10_pnl", 0.0))),
                html.escape(_fmt_money(item.get("max_drawdown", 0.0))),
                html.escape(str(item.get("action", ""))),
            ]
            for item in items
        ],
    )


def _cached_shadow_variant_performance_stats(settings: LatencyBotSettings) -> dict[str, Any]:
    key = str(settings.db_path.resolve())
    now = time.monotonic()
    with _SHADOW_VARIANT_CACHE_LOCK:
        if (
            _SHADOW_VARIANT_CACHE.get("key") == key
            and _SHADOW_VARIANT_CACHE.get("payload") is not None
            and now - float(_SHADOW_VARIANT_CACHE.get("loaded_at") or 0.0) < _SHADOW_VARIANT_CACHE_TTL_SECONDS
        ):
            return _SHADOW_VARIANT_CACHE["payload"]
        payload = latency_bot_shadow_variant_performance_stats(settings)
        _SHADOW_VARIANT_CACHE.update({"key": key, "loaded_at": now, "payload": payload})
        return payload


def build_latency_bot_dashboard_state(settings: LatencyBotSettings) -> dict[str, Any]:
    init_latency_bot_db(settings)
    status = _load_json(settings.status_path)
    run_metadata = _load_json(settings.db_path.parent / "latency_bot_run.json")
    markets = _load_json(settings.markets_path)
    polymarket_cache = _load_json(settings.polymarket_cache_path)
    binance_cache = _load_json(settings.binance_cache_path)
    db_summary = summarize_latency_bot_db(settings, 60)
    since_ts = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat().replace("+00:00", "Z")
    recent_signals = latest_rows_since(settings, "signals", since_ts=since_ts, limit=12)
    recent_fair_values = latest_rows_since(settings, "fair_values", since_ts=since_ts, limit=12)
    recent_fills = latest_rows_since(settings, "fills", since_ts=since_ts, limit=12)
    recent_orders = latest_rows_since(settings, "orders", since_ts=since_ts, limit=12)
    open_positions = load_open_positions(settings)
    recent_shadow_signals = latest_shadow_rows_since(settings, "shadow_signals", since_ts=since_ts, limit=12)
    recent_shadow_events = latest_shadow_rows_since(settings, "shadow_position_events", since_ts=since_ts, limit=12)
    open_shadow_positions = load_shadow_open_positions(settings)
    portfolio = latency_bot_portfolio_summary(
        settings,
        cache_items=polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else [],
    )
    promoted_variant_stats = latency_bot_promoted_variant_performance_stats(
        settings,
        cache_items=polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else [],
    )
    signal_stats = latency_bot_signal_feature_stats(settings)
    enabled_signal_stats = latency_bot_signal_feature_stats(settings, live_enabled_only=True)
    shadow_signal_stats = latency_bot_signal_feature_stats(settings, shadow=True)
    live_opportunity = latency_bot_opportunity_stats(settings, shadow=False, minutes=15, live_enabled_only=True)
    shadow_opportunity = latency_bot_opportunity_stats(settings, shadow=True, minutes=15)
    threshold_relaxation = latency_bot_threshold_relaxation_stats(settings)
    shadow_yes_variant_stats = _cached_shadow_variant_performance_stats(settings)
    live_stats = latency_bot_performance_stats(settings)
    shadow_portfolio = latency_bot_shadow_portfolio_summary(
        settings,
        cache_items=polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else [],
    )
    equity_curve = latency_bot_equity_curve(settings)
    live_strategy_equity_curves = latency_bot_live_strategy_equity_curves(settings)
    shadow_stats = latency_bot_shadow_performance_stats(settings)
    complete_set_arb_stats = latency_bot_complete_set_arb_stats(settings)
    realistic_complete_set_arb = latency_bot_realistic_complete_set_arb_sim(settings)
    realistic_complete_set_arb_all_time = latency_bot_realistic_complete_set_arb_sim(
        replace(settings, realistic_complete_set_arb_lookback_hours=24 * 365 * 20)
    )
    live_complete_set_arb_pilot = latency_bot_live_complete_set_arb_pilot_stats(settings)
    polymarket_us_arb = latency_bot_polymarket_us_arb_sim(settings)
    kalshi_arb = latency_bot_kalshi_arb_sim(settings)
    capital_usage = latency_bot_capital_usage(settings)
    return {
        "bankroll_usdc": settings.bankroll_usdc,
        "status": status,
        "run_metadata": run_metadata,
        "markets": markets,
        "polymarket_cache": polymarket_cache,
        "binance_cache": binance_cache,
        "db_summary": db_summary,
        "recent_signals": recent_signals,
        "recent_fair_values": recent_fair_values,
        "recent_fills": recent_fills,
        "recent_orders": recent_orders,
        "recent_shadow_signals": recent_shadow_signals,
        "recent_shadow_events": recent_shadow_events,
        "open_positions": open_positions,
        "open_shadow_positions": open_shadow_positions,
        "portfolio": portfolio,
        "promoted_variant_stats": promoted_variant_stats,
        "signal_stats": signal_stats,
        "enabled_signal_stats": enabled_signal_stats,
        "live_opportunity": live_opportunity,
        "threshold_relaxation": threshold_relaxation,
        "shadow_yes_variant_stats": shadow_yes_variant_stats,
        "live_stats": live_stats,
        "shadow_signal_stats": shadow_signal_stats,
        "shadow_opportunity": shadow_opportunity,
        "shadow_portfolio": shadow_portfolio,
        "equity_curve": equity_curve,
        "live_strategy_equity_curves": live_strategy_equity_curves,
        "shadow_stats": shadow_stats,
        "complete_set_arb_stats": complete_set_arb_stats,
        "realistic_complete_set_arb": realistic_complete_set_arb,
        "realistic_complete_set_arb_all_time": realistic_complete_set_arb_all_time,
        "live_complete_set_arb_pilot": live_complete_set_arb_pilot,
        "polymarket_us_arb": polymarket_us_arb,
        "kalshi_arb": kalshi_arb,
        "capital_usage": capital_usage,
    }


def render_latency_bot_dashboard_html(state: dict[str, Any]) -> str:
    page_generated_at = datetime.now(DISPLAY_TZ).strftime("%Y-%m-%d %H:%M:%S CT")
    status = state.get("status", {}) if isinstance(state.get("status"), dict) else {}
    run_metadata = state.get("run_metadata", {}) if isinstance(state.get("run_metadata"), dict) else {}
    last_cycle_result = status.get("last_cycle_result", {}) if isinstance(status.get("last_cycle_result"), dict) else {}
    execution_result = last_cycle_result.get("execution", {}) if isinstance(last_cycle_result.get("execution"), dict) else {}
    promoted_result = last_cycle_result.get("promoted_variants", {}) if isinstance(last_cycle_result.get("promoted_variants"), dict) else {}
    promoted_execution_result = promoted_result.get("execution", {}) if isinstance(promoted_result.get("execution"), dict) else {}
    db_summary = state.get("db_summary", {}) if isinstance(state.get("db_summary"), dict) else {}
    markets = state.get("markets", {}) if isinstance(state.get("markets"), dict) else {}
    polymarket_cache = state.get("polymarket_cache", {}) if isinstance(state.get("polymarket_cache"), dict) else {}
    binance_cache = state.get("binance_cache", {}) if isinstance(state.get("binance_cache"), dict) else {}
    recent_counts = db_summary.get("recent_counts", {}) if isinstance(db_summary.get("recent_counts"), dict) else {}
    latest_cycle = db_summary.get("latest_cycle", {}) if isinstance(db_summary.get("latest_cycle"), dict) else {}
    recent_signals = state.get("recent_signals", []) if isinstance(state.get("recent_signals"), list) else []
    recent_fair_values = state.get("recent_fair_values", []) if isinstance(state.get("recent_fair_values"), list) else []
    recent_fills = state.get("recent_fills", []) if isinstance(state.get("recent_fills"), list) else []
    recent_orders = state.get("recent_orders", []) if isinstance(state.get("recent_orders"), list) else []
    recent_shadow_signals = state.get("recent_shadow_signals", []) if isinstance(state.get("recent_shadow_signals"), list) else []
    recent_shadow_events = state.get("recent_shadow_events", []) if isinstance(state.get("recent_shadow_events"), list) else []
    open_positions = state.get("open_positions", []) if isinstance(state.get("open_positions"), list) else []
    open_shadow_positions = state.get("open_shadow_positions", []) if isinstance(state.get("open_shadow_positions"), list) else []
    portfolio = state.get("portfolio", {}) if isinstance(state.get("portfolio"), dict) else {}
    promoted_variant_stats = state.get("promoted_variant_stats", {}) if isinstance(state.get("promoted_variant_stats"), dict) else {}
    signal_stats = state.get("signal_stats", {}) if isinstance(state.get("signal_stats"), dict) else {}
    enabled_signal_stats = state.get("enabled_signal_stats", {}) if isinstance(state.get("enabled_signal_stats"), dict) else {}
    live_opportunity = state.get("live_opportunity", {}) if isinstance(state.get("live_opportunity"), dict) else {}
    threshold_relaxation = state.get("threshold_relaxation", {}) if isinstance(state.get("threshold_relaxation"), dict) else {}
    shadow_yes_variant_stats = state.get("shadow_yes_variant_stats", {}) if isinstance(state.get("shadow_yes_variant_stats"), dict) else {}
    live_stats = state.get("live_stats", {}) if isinstance(state.get("live_stats"), dict) else {}
    shadow_signal_stats = state.get("shadow_signal_stats", {}) if isinstance(state.get("shadow_signal_stats"), dict) else {}
    shadow_opportunity = state.get("shadow_opportunity", {}) if isinstance(state.get("shadow_opportunity"), dict) else {}
    shadow_portfolio = state.get("shadow_portfolio", {}) if isinstance(state.get("shadow_portfolio"), dict) else {}
    equity_curve = state.get("equity_curve", []) if isinstance(state.get("equity_curve"), list) else []
    live_strategy_equity_curves = state.get("live_strategy_equity_curves", []) if isinstance(state.get("live_strategy_equity_curves"), list) else []
    shadow_stats = state.get("shadow_stats", {}) if isinstance(state.get("shadow_stats"), dict) else {}
    complete_set_arb_stats = state.get("complete_set_arb_stats", {}) if isinstance(state.get("complete_set_arb_stats"), dict) else {}
    realistic_complete_set_arb = state.get("realistic_complete_set_arb", {}) if isinstance(state.get("realistic_complete_set_arb"), dict) else {}
    realistic_complete_set_arb_all_time = (
        state.get("realistic_complete_set_arb_all_time", {})
        if isinstance(state.get("realistic_complete_set_arb_all_time"), dict)
        else {}
    )
    polymarket_us_arb = state.get("polymarket_us_arb", {}) if isinstance(state.get("polymarket_us_arb"), dict) else {}
    kalshi_arb = state.get("kalshi_arb", {}) if isinstance(state.get("kalshi_arb"), dict) else {}
    capital_usage = state.get("capital_usage", {}) if isinstance(state.get("capital_usage"), dict) else {}
    consensus_meta = _consensus_meta_strategy_data(shadow_yes_variant_stats)
    bankroll_usdc = float(state.get("bankroll_usdc") or 10000.0)
    last_cycle_completed = _fmt_ts(status.get("last_cycle_completed_at"))
    run_label = str(run_metadata.get("label") or "").strip()
    run_started_at = _fmt_ts(run_metadata.get("started_at")) if str(run_metadata.get("started_at") or "").strip() else ""
    archive_path = str(run_metadata.get("baseline_archive") or "").strip()
    run_meta_parts = []
    if run_label:
        run_meta_parts.append(f"<strong>Run:</strong> {html.escape(run_label)}")
    if run_started_at:
        run_meta_parts.append(f"<strong>Started:</strong> {html.escape(run_started_at)}")
    if archive_path:
        run_meta_parts.append(f"<strong>Archived Baseline:</strong> {html.escape(archive_path)}")
    run_meta_html = f"<div class=\"meta\">{' | '.join(run_meta_parts)}</div>" if run_meta_parts else ""
    rows = [
        ["Runner Status", html.escape(str(status.get("runner_status", "unknown")))],
        ["Phase", html.escape(str(status.get("phase", "bootstrap")))],
        ["Tracked Markets", html.escape(_fmt_num(status.get("tracked_markets_count", 0)))],
        ["Open Orders", html.escape(_fmt_num(status.get("open_orders_count", 0)))],
        ["Open Positions", html.escape(_fmt_num(status.get("open_positions_count", 0)))],
        ["Capital Currently In Use", html.escape(_fmt_money(capital_usage.get("total_current_capital_usdc", 0.0)))],
        ["Current Capital / Bankroll", html.escape(f"{100.0 * float(capital_usage.get('total_current_bankroll_fraction', 0.0)):.1f}%")],
        ["Realized PnL", html.escape(_fmt_money(portfolio.get("realized_pnl_usdc", status.get("realized_pnl_usdc", 0.0))))],
        ["Dollars / Day Realized", html.escape(_fmt_money(portfolio.get("realized_usdc_per_day", 0.0)))],
        ["Projected Monthly Revenue", html.escape(_fmt_money(portfolio.get("projected_monthly_revenue_usdc", 0.0)))],
        ["Projected Yearly Revenue", html.escape(_fmt_money(portfolio.get("projected_yearly_revenue_usdc", 0.0)))],
        ["Unrealized PnL", html.escape(_fmt_money(portfolio.get("unrealized_pnl_usdc", status.get("unrealized_pnl_usdc", 0.0))))],
        ["Equity", html.escape(_fmt_money(portfolio.get("equity_usdc", bankroll_usdc)))],
        ["Risk State", html.escape(str(status.get("risk_state", "unknown")))],
        ["Last Cycle Started (CT)", html.escape(_fmt_ts(status.get("last_cycle_started_at")))],
        ["Last Cycle Completed (CT)", html.escape(_fmt_ts(status.get("last_cycle_completed_at")))],
        ["Last Error", html.escape(str(status.get("last_error") or "-"))],
    ]
    capital_usage_rows = [
        ["Configured Paper Bankroll", html.escape(_fmt_money(capital_usage.get("bankroll_usdc", bankroll_usdc)))],
        ["Total Capital Currently In Use", html.escape(_fmt_money(capital_usage.get("total_current_capital_usdc", 0.0)))],
        ["Current Capital / Bankroll", html.escape(f"{100.0 * float(capital_usage.get('total_current_bankroll_fraction', 0.0)):.1f}%")],
        ["Open Live Positions", html.escape(_fmt_num(capital_usage.get("live_position_count", 0)))],
        ["Live Position Capital", html.escape(_fmt_money(capital_usage.get("live_position_capital_usdc", 0.0)))],
        ["Open Orders", html.escape(_fmt_num(capital_usage.get("open_order_count", 0)))],
        ["Open Order Reserved Capital", html.escape(_fmt_money(capital_usage.get("open_order_capital_usdc", 0.0)))],
        ["Open Complete Sets", html.escape(_fmt_num(capital_usage.get("complete_set_open_count", 0)))],
        ["Complete-Set Locked Capital", html.escape(_fmt_money(capital_usage.get("complete_set_locked_capital_usdc", 0.0)))],
        ["Directional Notional / Trade", html.escape(_fmt_money(capital_usage.get("directional_trade_notional_usdc", 0.0)))],
        ["Complete-Set Notional / Trade", html.escape(_fmt_money(capital_usage.get("complete_set_trade_notional_usdc", 0.0)))],
        ["Complete-Set Max Sets / Cycle", html.escape(_fmt_num(capital_usage.get("complete_set_max_sets_per_cycle", 0)))],
        ["Complete-Set Max Cycle Notional", html.escape(_fmt_money(capital_usage.get("complete_set_max_cycle_notional_usdc", 0.0)))],
        ["Max Cycle Notional / Bankroll", html.escape(f"{100.0 * float(capital_usage.get('complete_set_max_cycle_bankroll_fraction', 0.0)):.1f}%")],
    ]
    db_rows = [
        ["DB Present", html.escape(str(bool(db_summary.get("db_present"))).lower())],
        ["Engine Cycles (60m)", html.escape(_fmt_num(recent_counts.get("engine_cycles", 0)))],
        ["Signals (60m)", html.escape(_fmt_num(recent_counts.get("signals", 0)))],
        ["Book Snapshots (60m)", html.escape(_fmt_num(recent_counts.get("polymarket_books", 0)))],
        ["Binance Ticks (60m)", html.escape(_fmt_num(recent_counts.get("binance_ticks", 0)))],
        ["Fills (60m)", html.escape(_fmt_num(recent_counts.get("fills", 0)))],
        ["Shadow Signals (60m)", html.escape(_fmt_num(recent_counts.get("shadow_signals", 0)))],
        ["Shadow Variant Signals (60m)", html.escape(_fmt_num(recent_counts.get("shadow_variant_signals", 0)))],
        ["Complete-Set Arb Signals (60m)", html.escape(_fmt_num(recent_counts.get("complete_set_arb_signals", 0)))],
        ["Complete-Set Arb Closes (60m)", html.escape(_fmt_num(recent_counts.get("complete_set_arb_closes", 0)))],
        ["Live Arb Pilot Attempts (60m)", html.escape(_fmt_num(recent_counts.get("live_complete_set_arb_pilot_attempts", 0)))],
        ["Polymarket US Arb Ticks (60m)", html.escape(_fmt_num(recent_counts.get("polymarket_us_arb_ticks", 0)))],
        ["Kalshi Arb Ticks (60m)", html.escape(_fmt_num(recent_counts.get("kalshi_arb_ticks", 0)))],
        ["Missed Opportunities (60m)", html.escape(_fmt_num(recent_counts.get("missed_opportunities", 0)))],
    ]
    latest_cycle_rows = [
        ["Timestamp (CT)", html.escape(_fmt_ts(latest_cycle.get("ts")))],
        ["Phase", html.escape(str(latest_cycle.get("phase") or "-"))],
        ["Markets Tracked", html.escape(_fmt_num(latest_cycle.get("markets_tracked", 0)))],
        ["Signals Seen", html.escape(_fmt_num(latest_cycle.get("signals_seen", 0)))],
        ["Orders Open", html.escape(_fmt_num(latest_cycle.get("orders_open", 0)))],
        ["Positions Open", html.escape(_fmt_num(latest_cycle.get("positions_open", 0)))],
        ["Risk State", html.escape(str(latest_cycle.get("risk_state") or "-"))],
        ["Notes", html.escape(str(latest_cycle.get("notes") or "-"))],
    ]
    notes = status.get("notes", []) if isinstance(status.get("notes"), list) else []
    notes_html = "".join(f"<li>{html.escape(str(note))}</li>" for note in notes) if notes else "<li>No notes</li>"
    market_items = markets.get("items", []) if isinstance(markets.get("items"), list) else []
    source = markets.get("source", {}) if isinstance(markets.get("source"), dict) else {}
    markets_table = _table(
        ["Question", "Asset", "Tenor", "Hours", "Expiry (CT)", "Status"],
        [
            [
                html.escape(str(item.get("question", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("tenor_minutes", ""))),
                html.escape(f"{float(item.get('hours_to_expiry', 0.0)):.2f}"),
                html.escape(_fmt_ts(item.get("expiry_ts"))),
                html.escape(str(item.get("status", ""))),
            ]
            for item in market_items[:15]
        ],
    )
    discovery_rows = [
        ["Source", html.escape(str(source.get("name", "-")))],
        ["Tag Candidates", html.escape(_fmt_num(source.get("tag_candidate_count", 0)))],
        ["Fetched Candidates", html.escape(_fmt_num(source.get("fetched_count", 0)))],
        ["Error", html.escape(str(source.get("error") or "-"))],
    ]
    feed_rows = [
        ["Polymarket Cache Source", html.escape(str(polymarket_cache.get("source") or "-"))],
        ["Polymarket Cache Updated (CT)", html.escape(_fmt_ts(polymarket_cache.get("updated_at")))],
        ["Polymarket Cache Rows", html.escape(_fmt_num(polymarket_cache.get("count", 0)))],
        ["Polymarket Errors", html.escape(_fmt_num(len(polymarket_cache.get("errors", [])) if isinstance(polymarket_cache.get("errors"), list) else 0))],
        ["Binance Cache Source", html.escape(str(binance_cache.get("source") or "-"))],
        ["Binance Cache Updated (CT)", html.escape(_fmt_ts(binance_cache.get("updated_at")))],
        ["Binance Cache Rows", html.escape(_fmt_num(binance_cache.get("count", 0)))],
        ["Binance Errors", html.escape(_fmt_num(len(binance_cache.get("errors", [])) if isinstance(binance_cache.get("errors"), list) else 0))],
    ]
    polymarket_items = polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else []
    tracked_cache_table = _table(
        ["Question", "Asset", "Hours", "Mid", "Bid", "Ask", "Depth", "Seen (CT)"],
        [
            [
                html.escape(str(item.get("question", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(f"{float(item.get('hours_to_expiry', 0.0)):.2f}"),
                html.escape(f"{float(item.get('midpoint', 0.0)):.3f}"),
                html.escape(f"{float(item.get('best_bid', 0.0)):.3f}"),
                html.escape(f"{float(item.get('best_ask', 0.0)):.3f}"),
                html.escape(_fmt_money(item.get("min_depth_usdc", 0.0))),
                html.escape(_fmt_ts(item.get("seen_at"))),
            ]
            for item in polymarket_items[:15]
        ],
    )
    fair_value_table = _table(
        ["Market", "Asset", "Fair Yes", "Fair No", "Ref Px", "Vol", "Secs Left"],
        [
            [
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(f"{float(item.get('fair_yes', 0.0)):.3f}"),
                html.escape(f"{float(item.get('fair_no', 0.0)):.3f}"),
                html.escape(f"{float(item.get('reference_price', 0.0)):.2f}"),
                html.escape(f"{float(item.get('volatility', 0.0)):.6f}"),
                html.escape(f"{float(item.get('time_to_expiry_sec', 0.0)):.1f}"),
            ]
            for item in recent_fair_values
        ],
    )
    signal_table = _table(
        ["Market", "Signal", "Mode", "Edge", "Eligible", "Reason"],
        [
            [
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("signal_type", ""))),
                html.escape(str(item.get("mode", ""))),
                html.escape(f"{float(item.get('edge', 0.0)):.4f}"),
                html.escape("yes" if bool(item.get("eligible")) else "no"),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in recent_signals
        ],
    )
    orders_table = _table(
        ["Created (CT)", "Market", "Side", "Price", "Size", "Mode", "Status", "Reprices", "Cancel Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts_created"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape(f"{float(item.get('price', 0.0)):.4f}"),
                html.escape(f"{float(item.get('size', 0.0)):.4f}"),
                html.escape(str(item.get("mode", ""))),
                html.escape(str(item.get("status", ""))),
                html.escape(str(item.get("reprices", ""))),
                html.escape(str(item.get("cancel_reason", ""))),
            ]
            for item in recent_orders
        ],
    )
    fills_table = _table(
        ["Time (CT)", "Market", "Side", "Price", "Size", "Type"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape(f"{float(item.get('price', 0.0)):.4f}"),
                html.escape(f"{float(item.get('size', 0.0)):.4f}"),
                html.escape(str(item.get("fill_type", ""))),
            ]
            for item in recent_fills
        ],
    )
    positions_table = _table(
        ["Market", "Asset", "Side", "Entry", "Size", "Mode", "Opened (CT)"],
        [
            [
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape(f"{float(item.get('entry_price', 0.0)):.4f}"),
                html.escape(f"{float(item.get('size', 0.0)):.4f}"),
                html.escape(str(item.get("mode", ""))),
                html.escape(_fmt_ts(item.get("entry_ts"))),
            ]
            for item in open_positions
        ],
    )
    promoted_summary = promoted_variant_stats.get("summary", {}) if isinstance(promoted_variant_stats.get("summary"), dict) else {}
    promoted_variants = promoted_variant_stats.get("variants", []) if isinstance(promoted_variant_stats.get("variants"), list) else []
    promoted_summary_rows = [
        ["Enabled Variants", html.escape(", ".join(str(item) for item in promoted_summary.get("enabled_variant_ids", [])) or "-")],
        ["Open Positions", html.escape(_fmt_num(promoted_summary.get("open", 0)))],
        ["Closed Trades", html.escape(_fmt_num(promoted_summary.get("closed", 0)))],
        ["Net PnL", html.escape(_fmt_money(promoted_summary.get("net_pnl", 0.0)))],
        ["24h Net Revenue", html.escape(_fmt_money(promoted_summary.get("realized_pnl_24h_usdc", 0.0)))],
        ["Dollars / Day", html.escape(_fmt_money(promoted_summary.get("realized_usdc_per_day", 0.0)))],
        ["Projected Monthly Revenue", html.escape(_fmt_money(promoted_summary.get("projected_monthly_revenue_usdc", 0.0)))],
        ["Projected Yearly Revenue", html.escape(_fmt_money(promoted_summary.get("projected_yearly_revenue_usdc", 0.0)))],
        ["Unrealized PnL", html.escape(_fmt_money(promoted_summary.get("unrealized_pnl_usdc", 0.0)))],
        ["Last Cycle Opens", html.escape(_fmt_num(promoted_execution_result.get("opened_positions_count", 0)))],
        ["Last Cycle Closes", html.escape(_fmt_num(promoted_execution_result.get("closed_positions_count", 0)))],
    ]
    promoted_variant_table = _table(
        [
            "Variant",
            "Open",
            "Closed",
            "Markets",
            "Win Rate",
            "Net PnL",
            "24h Net",
            "Monthly",
            "Yearly",
            "Unrealized",
            "Recent 10",
            "Max DD",
        ],
        [
            [
                html.escape(str(item.get("variant_id", ""))),
                html.escape(_fmt_num(item.get("open", 0))),
                html.escape(_fmt_num(item.get("closed", 0))),
                html.escape(_fmt_num(item.get("unique_markets", 0))),
                html.escape(f"{100.0 * float(item.get('win_rate', 0.0)):.1f}%"),
                html.escape(_fmt_money(item.get("net_pnl", 0.0))),
                html.escape(_fmt_money(item.get("realized_pnl_24h_usdc", 0.0))),
                html.escape(_fmt_money(item.get("projected_monthly_revenue_usdc", 0.0))),
                html.escape(_fmt_money(item.get("projected_yearly_revenue_usdc", 0.0))),
                html.escape(_fmt_money(item.get("unrealized_pnl_usdc", 0.0))),
                html.escape(_fmt_money(item.get("recent_10_pnl", 0.0))),
                html.escape(_fmt_money(item.get("max_drawdown", 0.0))),
            ]
            for item in promoted_variants
        ],
    )
    promoted_entry_block_table = _table(
        ["Variant", "Market", "Reason"],
        [
            [
                html.escape(str(item.get("variant_id", ""))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in promoted_execution_result.get("entry_blocks", [])
            if isinstance(item, dict)
        ],
    )
    complete_set_summary = complete_set_arb_stats.get("summary", {}) if isinstance(complete_set_arb_stats.get("summary"), dict) else {}
    complete_set_execution_result = last_cycle_result.get("complete_set_arb", {}) if isinstance(last_cycle_result.get("complete_set_arb"), dict) else {}
    complete_set_execution = complete_set_execution_result.get("execution", {}) if isinstance(complete_set_execution_result.get("execution"), dict) else {}
    complete_set_summary_rows = [
        ["Mode", "Paper-only complete-set arbitrage prototype"],
        ["Enabled", html.escape(str(bool(complete_set_summary.get("enabled"))).lower())],
        ["Execution Policy", html.escape(str(complete_set_summary.get("execution_policy") or complete_set_execution.get("execution_policy") or "paired_fok_batch"))],
        ["Atomicity Model", html.escape(str(complete_set_summary.get("simulated_atomicity") or complete_set_execution.get("simulated_atomicity") or "paired FOK batch"))],
        ["Closed Sets", html.escape(_fmt_num(complete_set_summary.get("closed", 0)))],
        ["Unique Markets", html.escape(_fmt_num(complete_set_summary.get("unique_markets", 0)))],
        ["Net PnL", html.escape(_fmt_money(complete_set_summary.get("net_pnl", 0.0)))],
        ["24h Net Revenue", html.escape(_fmt_money(complete_set_summary.get("realized_pnl_24h_usdc", 0.0)))],
        ["Projected Monthly Revenue", html.escape(_fmt_money(complete_set_summary.get("projected_monthly_revenue_usdc", 0.0)))],
        ["Projected Yearly Revenue", html.escape(_fmt_money(complete_set_summary.get("projected_yearly_revenue_usdc", 0.0)))],
        ["Avg PnL / Set", html.escape(_fmt_money(complete_set_summary.get("avg_pnl", 0.0)))],
        ["Max Drawdown", html.escape(_fmt_money(complete_set_summary.get("max_drawdown", 0.0)))],
        ["Signals 60m", html.escape(_fmt_num(complete_set_summary.get("signals_60m", 0)))],
        ["Eligible 60m", html.escape(_fmt_num(complete_set_summary.get("eligible_60m", 0)))],
        ["Eligible Rate 60m", html.escape(f"{100.0 * float(complete_set_summary.get('eligible_rate_60m', 0.0)):.1f}%")],
        ["Best Net Edge 60m", html.escape(f"{float(complete_set_summary.get('best_net_edge_60m', 0.0)):.4f}")],
        ["Min Edge / Share", html.escape(f"{float(complete_set_summary.get('min_profit_per_share', 0.0)):.4f}")],
        ["Paper Notional", html.escape(_fmt_money(complete_set_summary.get("notional_usdc", 0.0)))],
        ["Last Cycle Opens", html.escape(_fmt_num(complete_set_execution.get("opened_positions_count", 0)))],
        ["Last Cycle Closes", html.escape(_fmt_num(complete_set_execution.get("closed_positions_count", 0)))],
    ]
    complete_set_signal_table = _table(
        ["Time (CT)", "Market", "Asset", "Tenor", "YES Ask", "NO Ask", "Cost", "Net Edge", "Depth", "Secs Left", "Eligible", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("tenor_minutes", ""))),
                html.escape(f"{float(item.get('yes_ask', 0.0)):.4f}"),
                html.escape(f"{float(item.get('no_ask', 0.0)):.4f}"),
                html.escape(f"{float(item.get('total_cost', 0.0)):.4f}"),
                html.escape(f"{float(item.get('net_edge', 0.0)):.4f}"),
                html.escape(_fmt_money(item.get("executable_depth_usdc", 0.0))),
                html.escape(f"{float(item.get('seconds_left', 0.0)):.1f}"),
                html.escape("yes" if bool(item.get("eligible")) else "no"),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in complete_set_arb_stats.get("recent_signals", [])
        ],
    )
    complete_set_reason_table = _table(
        ["Reason", "Count", "Max Net Edge"],
        [
            [
                html.escape(str(item.get("reason", ""))),
                html.escape(_fmt_num(item.get("count", 0))),
                html.escape(f"{float(item.get('max_net_edge', 0.0)):.4f}"),
            ]
            for item in complete_set_arb_stats.get("reason_breakdown", [])
        ],
    )
    complete_set_recent_closes_table = _table(
        ["Time (CT)", "Market", "Asset", "Tenor", "YES", "NO", "Cost", "Size", "Net Edge", "PnL", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("tenor_minutes", ""))),
                html.escape(f"{float(item.get('yes_entry_price', 0.0)):.4f}"),
                html.escape(f"{float(item.get('no_entry_price', 0.0)):.4f}"),
                html.escape(f"{float(item.get('total_cost', 0.0)):.4f}"),
                html.escape(f"{float(item.get('size', 0.0)):.2f}"),
                html.escape(f"{float(item.get('net_edge', 0.0)):.4f}"),
                html.escape(_fmt_money(item.get("pnl", 0.0))),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in complete_set_arb_stats.get("recent_closes", [])
        ],
    )
    complete_set_entry_block_table = _table(
        ["Market", "Reason"],
        [
            [
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in complete_set_execution.get("entry_blocks", [])
            if isinstance(item, dict)
        ],
    )
    realistic_complete_set_summary = (
        realistic_complete_set_arb.get("summary", {})
        if isinstance(realistic_complete_set_arb.get("summary"), dict)
        else {}
    )
    realistic_complete_set_all_time_summary = (
        realistic_complete_set_arb_all_time.get("summary", {})
        if isinstance(realistic_complete_set_arb_all_time.get("summary"), dict)
        else {}
    )
    realistic_complete_set_summary_rows = [
        ["Mode", html.escape(str(realistic_complete_set_summary.get("mode", "Tick-replay realistic complete-set arb simulation")))],
        ["Enabled", html.escape(str(bool(realistic_complete_set_summary.get("enabled"))).lower())],
        ["Tick Poll Source", html.escape(str(realistic_complete_set_summary.get("tick_poll_source", "")))],
        ["Lookback Hours", html.escape(_fmt_num(realistic_complete_set_summary.get("lookback_hours", 0)))],
        ["Ticks Replayed", html.escape(_fmt_num(realistic_complete_set_summary.get("ticks_replayed", 0)))],
        ["First Tick In Window (CT)", html.escape(_fmt_ts(realistic_complete_set_summary.get("first_tick_ts")))],
        ["Last Tick (CT)", html.escape(_fmt_ts(realistic_complete_set_summary.get("last_tick_ts")))],
        ["Simulated Capital", html.escape(_fmt_money(realistic_complete_set_summary.get("simulated_capital_usdc", 0.0)))],
        ["Target Notional / Set", html.escape(_fmt_money(realistic_complete_set_summary.get("target_notional_usdc", 0.0)))],
        ["Minimum Partial Notional", html.escape(_fmt_money(realistic_complete_set_summary.get("min_notional_usdc", 0.0)))],
        ["Current Locked Capital", html.escape(_fmt_money(realistic_complete_set_summary.get("current_locked_capital_usdc", 0.0)))],
        ["Peak Locked Capital", html.escape(_fmt_money(realistic_complete_set_summary.get("peak_locked_capital_usdc", 0.0)))],
        ["Peak Capital Fraction", html.escape(f"{100.0 * float(realistic_complete_set_summary.get('peak_capital_fraction', 0.0)):.1f}%")],
        ["Locked Sets", html.escape(_fmt_num(realistic_complete_set_summary.get("locked_sets", 0)))],
        ["Unique Markets", html.escape(_fmt_num(realistic_complete_set_summary.get("unique_markets", 0)))],
        ["Paired Fill Failures", html.escape(_fmt_num(realistic_complete_set_summary.get("paired_fill_failures", 0)))],
        ["Expected Operational Loss", html.escape(_fmt_money(realistic_complete_set_summary.get("expected_operational_loss_usdc", 0.0)))],
        ["Capital-Blocked Ticks", html.escape(_fmt_num(realistic_complete_set_summary.get("capital_blocked", 0)))],
        ["Depth/Latency-Blocked Ticks", html.escape(_fmt_num(realistic_complete_set_summary.get("depth_latency_blocked", 0)))],
        ["Original Skips", html.escape(_fmt_num(realistic_complete_set_summary.get("original_skips", 0)))],
        ["Net PnL", html.escape(_fmt_money(realistic_complete_set_summary.get("net_pnl", 0.0)))],
        ["All-Time Replay Net PnL", html.escape(_fmt_money(realistic_complete_set_all_time_summary.get("net_pnl", 0.0)))],
        ["All-Time Locked Sets", html.escape(_fmt_num(realistic_complete_set_all_time_summary.get("locked_sets", 0)))],
        ["All-Time Ticks Replayed", html.escape(_fmt_num(realistic_complete_set_all_time_summary.get("ticks_replayed", 0)))],
        ["All-Time First Tick (CT)", html.escape(_fmt_ts(realistic_complete_set_all_time_summary.get("first_tick_ts")))],
        ["24h Net Revenue", html.escape(_fmt_money(realistic_complete_set_summary.get("realized_pnl_24h_usdc", 0.0)))],
        ["Projected Monthly Revenue", html.escape(_fmt_money(realistic_complete_set_summary.get("projected_monthly_revenue_usdc", 0.0)))],
        ["Projected Yearly Revenue", html.escape(_fmt_money(realistic_complete_set_summary.get("projected_yearly_revenue_usdc", 0.0)))],
        ["Avg PnL / Locked Set", html.escape(_fmt_money(realistic_complete_set_summary.get("avg_pnl_per_locked_set", 0.0)))],
        ["Max Drawdown", html.escape(_fmt_money(realistic_complete_set_summary.get("max_drawdown", 0.0)))],
        ["Signals 60m", html.escape(_fmt_num(realistic_complete_set_summary.get("signals_60m", 0)))],
        ["Eligible 60m", html.escape(_fmt_num(realistic_complete_set_summary.get("eligible_60m", 0)))],
        ["Eligible Rate 60m", html.escape(f"{100.0 * float(realistic_complete_set_summary.get('eligible_rate_60m', 0.0)):.1f}%")],
        ["Best Adjusted Edge 60m", html.escape(f"{float(realistic_complete_set_summary.get('best_adjusted_edge_60m', 0.0)):.4f}")],
        ["Min Edge / Share", html.escape(f"{float(realistic_complete_set_summary.get('min_edge_per_share', 0.0)):.4f}")],
        ["Latency Assumption", html.escape(f"{float(realistic_complete_set_summary.get('latency_ms', 0.0)):.0f} ms")],
        ["Depth Haircut", html.escape(f"{100.0 * float(realistic_complete_set_summary.get('depth_haircut', 0.0)):.1f}%")],
        ["Extra Slippage / Share", html.escape(f"{float(realistic_complete_set_summary.get('extra_slippage_per_share', 0.0)):.4f}")],
        ["Latency Decay / Share", html.escape(f"{float(realistic_complete_set_summary.get('latency_decay_per_share', 0.0)):.4f}")],
        ["Partial Fill Fraction", html.escape(f"{100.0 * float(realistic_complete_set_summary.get('partial_fill_fraction', 0.0)):.1f}%")],
        ["Failed-Leg Loss Fraction", html.escape(f"{100.0 * float(realistic_complete_set_summary.get('failed_leg_loss_fraction', 0.0)):.1f}%")],
        ["Operational Failure Rate", html.escape(f"{100.0 * float(realistic_complete_set_summary.get('operational_failure_rate', 0.0)):.1f}%")],
        ["Redeem Lag", html.escape(f"{_fmt_num(realistic_complete_set_summary.get('redeem_lag_seconds', 0))}s")],
    ]
    realistic_complete_set_event_table = _table(
        ["Time (CT)", "Market", "Asset", "Decision", "Reason", "Net Edge", "Adjusted Edge", "Depth", "Effective Depth", "Notional", "PnL", "Locked Capital"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("decision", ""))),
                html.escape(str(item.get("reason", ""))),
                html.escape(f"{float(item.get('net_edge', 0.0)):.4f}"),
                html.escape(f"{float(item.get('adjusted_edge', 0.0)):.4f}"),
                html.escape(_fmt_money(item.get("depth", 0.0))),
                html.escape(_fmt_money(item.get("effective_depth", 0.0))),
                html.escape(_fmt_money(item.get("notional_usdc", 0.0))),
                html.escape(_fmt_money(item.get("pnl", 0.0))),
                html.escape(_fmt_money(item.get("locked_capital_usdc", 0.0))),
            ]
            for item in realistic_complete_set_arb.get("recent_events", [])
        ],
    )
    realistic_complete_set_reason_table = _table(
        ["Reason", "Count", "Max Adjusted Edge"],
        [
            [
                html.escape(str(item.get("reason", ""))),
                html.escape(_fmt_num(item.get("count", 0))),
                html.escape(f"{float(item.get('max_adjusted_edge', 0.0)):.4f}"),
            ]
            for item in realistic_complete_set_arb.get("reason_breakdown", [])
        ],
    )
    live_complete_set_arb_pilot = state.get("live_complete_set_arb_pilot", {}) if isinstance(state.get("live_complete_set_arb_pilot"), dict) else {}
    live_complete_set_pilot_summary = (
        live_complete_set_arb_pilot.get("summary", {})
        if isinstance(live_complete_set_arb_pilot.get("summary"), dict)
        else {}
    )
    live_complete_set_pilot_summary_rows = [
        ["Mode", html.escape(str(live_complete_set_pilot_summary.get("mode", "Guarded live complete-set arb pilot")))],
        ["Enabled", html.escape(str(bool(live_complete_set_pilot_summary.get("enabled"))).lower())],
        ["Pilot Mode", html.escape(str(live_complete_set_pilot_summary.get("pilot_mode", "dry_run")))],
        ["Armed For Live Orders", html.escape(str(bool(live_complete_set_pilot_summary.get("armed_for_live_orders"))).lower())],
        ["Confirmation Required", html.escape(str(live_complete_set_pilot_summary.get("confirmation_required", "")))],
        ["Pilot Capital", html.escape(_fmt_money(live_complete_set_pilot_summary.get("simulated_or_live_capital_usdc", 0.0)))],
        ["Target Notional / Set", html.escape(_fmt_money(live_complete_set_pilot_summary.get("target_notional_usdc", 0.0)))],
        ["Submitted Sets", html.escape(_fmt_num(live_complete_set_pilot_summary.get("submitted_sets", 0)))],
        ["Dry-Run Candidates", html.escape(_fmt_num(live_complete_set_pilot_summary.get("dry_run_candidates", 0)))],
        ["Blocked Attempts", html.escape(_fmt_num(live_complete_set_pilot_summary.get("blocked_attempts", 0)))],
        ["Failed Attempts", html.escape(_fmt_num(live_complete_set_pilot_summary.get("failed_attempts", 0)))],
        ["Unique Markets Submitted", html.escape(_fmt_num(live_complete_set_pilot_summary.get("unique_markets_submitted", 0)))],
        ["Expected Locked PnL", html.escape(_fmt_money(live_complete_set_pilot_summary.get("expected_locked_pnl_usdc", 0.0)))],
        ["Dry-Run Candidate PnL", html.escape(_fmt_money(live_complete_set_pilot_summary.get("dry_run_candidate_pnl_usdc", 0.0)))],
        ["Net PnL", html.escape(_fmt_money(live_complete_set_pilot_summary.get("net_pnl", 0.0)))],
        ["24h Net Revenue", html.escape(_fmt_money(live_complete_set_pilot_summary.get("realized_pnl_24h_usdc", 0.0)))],
        ["24h Candidate PnL", html.escape(_fmt_money(live_complete_set_pilot_summary.get("expected_candidate_pnl_24h_usdc", 0.0)))],
        ["Projected Monthly Revenue", html.escape(_fmt_money(live_complete_set_pilot_summary.get("projected_monthly_revenue_usdc", 0.0)))],
        ["Projected Yearly Revenue", html.escape(_fmt_money(live_complete_set_pilot_summary.get("projected_yearly_revenue_usdc", 0.0)))],
        ["Max Drawdown", html.escape(_fmt_money(live_complete_set_pilot_summary.get("max_drawdown", 0.0)))],
        ["Attempts 60m", html.escape(_fmt_num(live_complete_set_pilot_summary.get("attempts_60m", 0)))],
        ["Eligible 60m", html.escape(_fmt_num(live_complete_set_pilot_summary.get("eligible_60m", 0)))],
        ["Eligible Rate 60m", html.escape(f"{100.0 * float(live_complete_set_pilot_summary.get('eligible_rate_60m', 0.0)):.1f}%")],
        ["Best Adjusted Edge 60m", html.escape(f"{float(live_complete_set_pilot_summary.get('best_adjusted_edge_60m', 0.0)):.4f}")],
        ["Min Edge / Share", html.escape(f"{float(live_complete_set_pilot_summary.get('min_edge_per_share', 0.0)):.4f}")],
        ["Min Depth", html.escape(_fmt_money(live_complete_set_pilot_summary.get("min_depth_usdc", 0.0)))],
        ["Depth Haircut", html.escape(f"{100.0 * float(live_complete_set_pilot_summary.get('depth_haircut', 0.0)):.1f}%")],
        ["Extra Slippage / Share", html.escape(f"{float(live_complete_set_pilot_summary.get('extra_slippage_per_share', 0.0)):.4f}")],
        ["Max Sets / Cycle", html.escape(_fmt_num(live_complete_set_pilot_summary.get("max_sets_per_cycle", 0)))],
        ["Max Open Sets", html.escape(_fmt_num(live_complete_set_pilot_summary.get("max_open_sets", 0)))],
        ["Daily Loss Limit", html.escape(_fmt_money(live_complete_set_pilot_summary.get("daily_loss_limit_usdc", 0.0)))],
        ["Allow Sequential Orders", html.escape(str(bool(live_complete_set_pilot_summary.get("allow_sequential_orders"))).lower())],
        ["Require FOK", html.escape(str(bool(live_complete_set_pilot_summary.get("require_fok"))).lower())],
        ["Same-Market Cooldown", html.escape(f"{_fmt_num(live_complete_set_pilot_summary.get('same_market_cooldown_seconds', 0))}s")],
    ]
    live_complete_set_pilot_attempt_table = _table(
        ["Time (CT)", "Market", "Asset", "Decision", "Reason", "YES", "NO", "Cost", "Adj Edge", "Depth", "Effective Depth", "Notional", "Expected PnL", "Realized PnL", "Error"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("decision", ""))),
                html.escape(str(item.get("reason", ""))),
                html.escape(f"{float(item.get('yes_price') or 0.0):.4f}"),
                html.escape(f"{float(item.get('no_price') or 0.0):.4f}"),
                html.escape(f"{float(item.get('total_cost') or 0.0):.4f}"),
                html.escape(f"{float(item.get('adjusted_edge') or 0.0):.4f}"),
                html.escape(_fmt_money(item.get("executable_depth_usdc", 0.0))),
                html.escape(_fmt_money(item.get("effective_depth_usdc", 0.0))),
                html.escape(_fmt_money(item.get("notional_usdc", 0.0))),
                html.escape(_fmt_money(item.get("expected_pnl_usdc", 0.0))),
                html.escape(_fmt_money(item.get("realized_pnl_usdc", 0.0))),
                html.escape(str(item.get("error", ""))[:160]),
            ]
            for item in live_complete_set_arb_pilot.get("recent_attempts", [])
        ],
    )
    live_complete_set_pilot_reason_table = _table(
        ["Decision", "Reason", "Count", "Max Adjusted Edge"],
        [
            [
                html.escape(str(item.get("decision", ""))),
                html.escape(str(item.get("reason", ""))),
                html.escape(_fmt_num(item.get("count", 0))),
                html.escape(f"{float(item.get('max_adjusted_edge') or 0.0):.4f}"),
            ]
            for item in live_complete_set_arb_pilot.get("reason_breakdown", [])
        ],
    )
    polymarket_us_arb_summary = (
        polymarket_us_arb.get("summary", {})
        if isinstance(polymarket_us_arb.get("summary"), dict)
        else {}
    )
    polymarket_us_arb_summary_rows = [
        ["Mode", html.escape(str(polymarket_us_arb_summary.get("mode", "Research-only Polymarket US crossed-book arb simulation")))],
        ["Enabled", html.escape(str(bool(polymarket_us_arb_summary.get("enabled"))).lower())],
        ["Data Source", html.escape(str(polymarket_us_arb_summary.get("data_source", "")))],
        ["Symbols Configured", html.escape(_fmt_num(polymarket_us_arb_summary.get("symbols_configured", 0)))],
        ["Symbols", html.escape(str(polymarket_us_arb_summary.get("symbols") or "-"))],
        ["Lookback Hours", html.escape(_fmt_num(polymarket_us_arb_summary.get("lookback_hours", 0)))],
        ["Ticks Replayed", html.escape(_fmt_num(polymarket_us_arb_summary.get("ticks_replayed", 0)))],
        ["First Tick (CT)", html.escape(_fmt_ts(polymarket_us_arb_summary.get("first_tick_ts")))],
        ["Last Tick (CT)", html.escape(_fmt_ts(polymarket_us_arb_summary.get("last_tick_ts")))],
        ["Source Errors", html.escape(_fmt_num(polymarket_us_arb_summary.get("source_errors", 0)))],
        ["Simulated Capital", html.escape(_fmt_money(polymarket_us_arb_summary.get("simulated_capital_usdc", 0.0)))],
        ["Target Notional / Trade", html.escape(_fmt_money(polymarket_us_arb_summary.get("target_notional_usdc", 0.0)))],
        ["Peak In-Flight Capital", html.escape(_fmt_money(polymarket_us_arb_summary.get("peak_in_flight_capital_usdc", 0.0)))],
        ["Peak Capital Fraction", html.escape(f"{100.0 * float(polymarket_us_arb_summary.get('peak_capital_fraction', 0.0)):.1f}%")],
        ["Simulated Trades", html.escape(_fmt_num(polymarket_us_arb_summary.get("simulated_trades", 0)))],
        ["Unique Symbols Traded", html.escape(_fmt_num(polymarket_us_arb_summary.get("unique_symbols_traded", 0)))],
        ["Expected Operational Loss", html.escape(_fmt_money(polymarket_us_arb_summary.get("expected_operational_loss_usdc", 0.0)))],
        ["Depth/Latency-Blocked Ticks", html.escape(_fmt_num(polymarket_us_arb_summary.get("depth_latency_blocked", 0)))],
        ["Capital-Blocked Ticks", html.escape(_fmt_num(polymarket_us_arb_summary.get("capital_blocked", 0)))],
        ["Net PnL", html.escape(_fmt_money(polymarket_us_arb_summary.get("net_pnl", 0.0)))],
        ["24h Net Revenue", html.escape(_fmt_money(polymarket_us_arb_summary.get("realized_pnl_24h_usdc", 0.0)))],
        ["Projected Monthly Revenue", html.escape(_fmt_money(polymarket_us_arb_summary.get("projected_monthly_revenue_usdc", 0.0)))],
        ["Projected Yearly Revenue", html.escape(_fmt_money(polymarket_us_arb_summary.get("projected_yearly_revenue_usdc", 0.0)))],
        ["Avg PnL / Trade", html.escape(_fmt_money(polymarket_us_arb_summary.get("avg_pnl_per_trade", 0.0)))],
        ["Max Drawdown", html.escape(_fmt_money(polymarket_us_arb_summary.get("max_drawdown", 0.0)))],
        ["Ticks 60m", html.escape(_fmt_num(polymarket_us_arb_summary.get("ticks_60m", 0)))],
        ["Eligible 60m", html.escape(_fmt_num(polymarket_us_arb_summary.get("eligible_60m", 0)))],
        ["Eligible Rate 60m", html.escape(f"{100.0 * float(polymarket_us_arb_summary.get('eligible_rate_60m', 0.0)):.1f}%")],
        ["Best Adjusted Edge 60m", html.escape(f"{float(polymarket_us_arb_summary.get('best_adjusted_edge_60m', 0.0)):.4f}")],
        ["Min Edge / Share", html.escape(f"{float(polymarket_us_arb_summary.get('min_edge_per_share', 0.0)):.4f}")],
        ["Min Depth", html.escape(_fmt_money(polymarket_us_arb_summary.get("min_depth_usdc", 0.0)))],
        ["Latency Assumption", html.escape(f"{float(polymarket_us_arb_summary.get('latency_ms', 0.0)):.0f} ms")],
        ["Depth Haircut", html.escape(f"{100.0 * float(polymarket_us_arb_summary.get('depth_haircut', 0.0)):.1f}%")],
        ["Extra Slippage / Share", html.escape(f"{float(polymarket_us_arb_summary.get('extra_slippage_per_share', 0.0)):.4f}")],
        ["Latency Decay / Share", html.escape(f"{float(polymarket_us_arb_summary.get('latency_decay_per_share', 0.0)):.4f}")],
        ["Fee / Share", html.escape(f"{float(polymarket_us_arb_summary.get('fee_per_share', 0.0)):.4f}")],
        ["Operational Failure Rate", html.escape(f"{100.0 * float(polymarket_us_arb_summary.get('operational_failure_rate', 0.0)):.1f}%")],
        ["Failed-Leg Loss Fraction", html.escape(f"{100.0 * float(polymarket_us_arb_summary.get('failed_leg_loss_fraction', 0.0)):.1f}%")],
        ["Same-Symbol Cooldown", html.escape(f"{_fmt_num(polymarket_us_arb_summary.get('same_symbol_cooldown_seconds', 0))}s")],
    ]
    polymarket_us_arb_event_table = _table(
        ["Time", "Symbol", "Decision", "Bid", "Ask", "Adj Edge", "Depth", "Notional", "PnL", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("symbol", ""))),
                html.escape(str(item.get("decision", ""))),
                html.escape(f"{float(item.get('best_bid') or 0.0):.4f}"),
                html.escape(f"{float(item.get('best_ask') or 0.0):.4f}"),
                html.escape(f"{float(item.get('adjusted_edge') or 0.0):.4f}"),
                html.escape(_fmt_money(item.get("depth", 0.0))),
                html.escape(_fmt_money(item.get("notional_usdc", 0.0))),
                html.escape(_fmt_money(item.get("pnl", 0.0))),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in polymarket_us_arb.get("recent_events", [])
        ],
    )
    polymarket_us_arb_reason_table = _table(
        ["Reason", "Count", "Max Adjusted Edge"],
        [
            [
                html.escape(str(item.get("reason", ""))),
                html.escape(_fmt_num(item.get("count", 0))),
                html.escape(f"{float(item.get('max_adjusted_edge') or 0.0):.4f}"),
            ]
            for item in polymarket_us_arb.get("reason_breakdown", [])
        ],
    )
    kalshi_arb_summary = (
        kalshi_arb.get("summary", {})
        if isinstance(kalshi_arb.get("summary"), dict)
        else {}
    )
    kalshi_arb_summary_rows = [
        ["Mode", html.escape(str(kalshi_arb_summary.get("mode", "Research-only Kalshi complete-set arb simulation")))],
        ["Enabled", html.escape(str(bool(kalshi_arb_summary.get("enabled"))).lower())],
        ["Data Source", html.escape(str(kalshi_arb_summary.get("data_source", "")))],
        ["Auto Discover", html.escape(str(bool(kalshi_arb_summary.get("auto_discover"))).lower())],
        ["Assets", html.escape(str(kalshi_arb_summary.get("assets") or "-"))],
        ["Tickers Configured", html.escape(_fmt_num(kalshi_arb_summary.get("tickers_configured", 0)))],
        ["Lookback Hours", html.escape(_fmt_num(kalshi_arb_summary.get("lookback_hours", 0)))],
        ["Ticks Replayed", html.escape(_fmt_num(kalshi_arb_summary.get("ticks_replayed", 0)))],
        ["First Tick (CT)", html.escape(_fmt_ts(kalshi_arb_summary.get("first_tick_ts")))],
        ["Last Tick (CT)", html.escape(_fmt_ts(kalshi_arb_summary.get("last_tick_ts")))],
        ["Source Errors", html.escape(_fmt_num(kalshi_arb_summary.get("source_errors", 0)))],
        ["Simulated Capital", html.escape(_fmt_money(kalshi_arb_summary.get("simulated_capital_usdc", 0.0)))],
        ["Target Notional / Set", html.escape(_fmt_money(kalshi_arb_summary.get("target_notional_usdc", 0.0)))],
        ["Current Locked Capital", html.escape(_fmt_money(kalshi_arb_summary.get("current_locked_capital_usdc", 0.0)))],
        ["Peak Locked Capital", html.escape(_fmt_money(kalshi_arb_summary.get("peak_locked_capital_usdc", 0.0)))],
        ["Peak Capital Fraction", html.escape(f"{100.0 * float(kalshi_arb_summary.get('peak_capital_fraction', 0.0)):.1f}%")],
        ["Simulated Sets", html.escape(_fmt_num(kalshi_arb_summary.get("simulated_sets", 0)))],
        ["Unique Tickers Traded", html.escape(_fmt_num(kalshi_arb_summary.get("unique_tickers_traded", 0)))],
        ["Expected Operational Loss", html.escape(_fmt_money(kalshi_arb_summary.get("expected_operational_loss_usdc", 0.0)))],
        ["Capital-Blocked Ticks", html.escape(_fmt_num(kalshi_arb_summary.get("capital_blocked", 0)))],
        ["Depth/Latency-Blocked Ticks", html.escape(_fmt_num(kalshi_arb_summary.get("depth_latency_blocked", 0)))],
        ["Net PnL", html.escape(_fmt_money(kalshi_arb_summary.get("net_pnl", 0.0)))],
        ["24h Net Revenue", html.escape(_fmt_money(kalshi_arb_summary.get("realized_pnl_24h_usdc", 0.0)))],
        ["Projected Monthly Revenue", html.escape(_fmt_money(kalshi_arb_summary.get("projected_monthly_revenue_usdc", 0.0)))],
        ["Projected Yearly Revenue", html.escape(_fmt_money(kalshi_arb_summary.get("projected_yearly_revenue_usdc", 0.0)))],
        ["Avg PnL / Set", html.escape(_fmt_money(kalshi_arb_summary.get("avg_pnl_per_set", 0.0)))],
        ["Max Drawdown", html.escape(_fmt_money(kalshi_arb_summary.get("max_drawdown", 0.0)))],
        ["Ticks 60m", html.escape(_fmt_num(kalshi_arb_summary.get("ticks_60m", 0)))],
        ["Eligible 60m", html.escape(_fmt_num(kalshi_arb_summary.get("eligible_60m", 0)))],
        ["Eligible Rate 60m", html.escape(f"{100.0 * float(kalshi_arb_summary.get('eligible_rate_60m', 0.0)):.1f}%")],
        ["Best Adjusted Edge 60m", html.escape(f"{float(kalshi_arb_summary.get('best_adjusted_edge_60m', 0.0)):.4f}")],
        ["Min Edge / Share", html.escape(f"{float(kalshi_arb_summary.get('min_edge_per_share', 0.0)):.4f}")],
        ["Min Depth", html.escape(_fmt_money(kalshi_arb_summary.get("min_depth_usdc", 0.0)))],
        ["Latency Assumption", html.escape(f"{float(kalshi_arb_summary.get('latency_ms', 0.0)):.0f} ms")],
        ["Depth Haircut", html.escape(f"{100.0 * float(kalshi_arb_summary.get('depth_haircut', 0.0)):.1f}%")],
        ["Extra Slippage / Share", html.escape(f"{float(kalshi_arb_summary.get('extra_slippage_per_share', 0.0)):.4f}")],
        ["Latency Decay / Share", html.escape(f"{float(kalshi_arb_summary.get('latency_decay_per_share', 0.0)):.4f}")],
        ["Fee / Share", html.escape(f"{float(kalshi_arb_summary.get('fee_per_share', 0.0)):.4f}")],
        ["Operational Failure Rate", html.escape(f"{100.0 * float(kalshi_arb_summary.get('operational_failure_rate', 0.0)):.1f}%")],
        ["Failed-Leg Loss Fraction", html.escape(f"{100.0 * float(kalshi_arb_summary.get('failed_leg_loss_fraction', 0.0)):.1f}%")],
        ["Same-Ticker Cooldown", html.escape(f"{_fmt_num(kalshi_arb_summary.get('same_ticker_cooldown_seconds', 0))}s")],
        ["Settlement Lag", html.escape(f"{_fmt_num(kalshi_arb_summary.get('settlement_lag_seconds', 0))}s")],
    ]
    kalshi_arb_event_table = _table(
        ["Time", "Ticker", "Asset", "Decision", "Cost", "Adj Edge", "Depth", "Sec Left", "Notional", "PnL", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("ticker", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("decision", ""))),
                html.escape(f"{float(item.get('total_cost') or 0.0):.4f}"),
                html.escape(f"{float(item.get('adjusted_edge') or 0.0):.4f}"),
                html.escape(_fmt_money(item.get("depth", 0.0))),
                html.escape(f"{float(item.get('seconds_left') or 0.0):.0f}"),
                html.escape(_fmt_money(item.get("notional_usdc", 0.0))),
                html.escape(_fmt_money(item.get("pnl", 0.0))),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in kalshi_arb.get("recent_events", [])
        ],
    )
    kalshi_arb_reason_table = _table(
        ["Reason", "Count", "Max Adjusted Edge"],
        [
            [
                html.escape(str(item.get("reason", ""))),
                html.escape(_fmt_num(item.get("count", 0))),
                html.escape(f"{float(item.get('max_adjusted_edge') or 0.0):.4f}"),
            ]
            for item in kalshi_arb.get("reason_breakdown", [])
        ],
    )
    consensus_summary_rows = [
        ["Mode", "Read-only research view; no trading behavior changed"],
        ["Strategy Thesis", "Use variants as voters, trade one representative per sleeve, size up only on cross-family agreement"],
        ["Ready Sleeves", html.escape(_fmt_num(consensus_meta.get("ready_sleeves", 0)))],
        ["High-Confidence Voters", html.escape(_fmt_num(consensus_meta.get("high_confidence_voters", 0)))],
        ["Pile-In Rule", "Only consider 3x-5x when 2+ independent sleeves score >=75, recent 10 > 0, and drawdown is inside limit"],
    ]
    consensus_sizing_table = _table(
        ["Score Band", "Action", "Size"],
        [
            ["<50", "Skip; signal is weak or unhealthy", "0x"],
            ["50-64", "Track only or tiny exploratory paper size", "0.25x-0.5x"],
            ["65-74", "Single-sleeve candidate", "1x"],
            ["75-84", "Strong voter; require no conflict", "2x"],
            [">=85 + 2 ready sleeves", "Rare consensus pile-in candidate", "3x-5x capped"],
        ],
    )
    consensus_sleeve_table = _render_consensus_sleeve_table(
        consensus_meta.get("sleeves", []) if isinstance(consensus_meta.get("sleeves"), list) else []
    )
    consensus_voter_table = _render_consensus_voter_table(
        consensus_meta.get("voters", []) if isinstance(consensus_meta.get("voters"), list) else []
    )
    live_summary_rows = [
        ["Live Strategy", "BTC 5m YES taker + promoted variant paper basket"],
        ["Live Closed Trades", html.escape(_fmt_num(live_stats.get("total_closed", 0)))],
        ["Live Win Rate", html.escape(f"{100.0 * float(live_stats.get('win_rate', 0.0)):.1f}%")],
        ["Live Avg Win", html.escape(_fmt_money(live_stats.get("avg_win", 0.0)))],
        ["Live Avg Loss", html.escape(_fmt_money(live_stats.get("avg_loss", 0.0)))],
        ["Live Net PnL", html.escape(_fmt_money(live_stats.get("net_pnl", 0.0)))],
    ]
    live_streak_rows = [
        ["Current Streak", html.escape(f"{str(live_stats.get('current_streak_direction') or 'flat')} x{int(live_stats.get('current_streak_length', 0))}")],
        ["Last 5 Trades Net", html.escape(_fmt_money((live_stats.get("recent_5") or {}).get("net_pnl", 0.0)))],
        ["Last 5 Stop Losses", html.escape(_fmt_num((live_stats.get("recent_5") or {}).get("stop_losses", 0)))],
        ["Last 10 Trades Net", html.escape(_fmt_money((live_stats.get("recent_10") or {}).get("net_pnl", 0.0)))],
        ["Last 10 Stop Losses", html.escape(_fmt_num((live_stats.get("recent_10") or {}).get("stop_losses", 0)))],
        ["Duplicate Close Positions", html.escape(_fmt_num(live_stats.get("duplicate_close_positions", 0)))],
        ["Duplicate Close Events", html.escape(_fmt_num(live_stats.get("duplicate_close_events", 0)))],
    ]
    live_entry_block_table = _table(
        ["Market", "Reason"],
        [
            [
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in execution_result.get("entry_blocks", [])
            if isinstance(item, dict)
        ],
    )
    live_recent_closes_table = _table(
        ["Time (CT)", "Position", "Mark", "PnL", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("position_id", ""))),
                html.escape(f"{float(item.get('mark', 0.0)):.4f}"),
                html.escape(f"{float(item.get('pnl', 0.0)):.4f}"),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in live_stats.get("recent_closes", [])
        ],
    )
    live_reason_breakdown_table = _table(
        ["Reason", "Count", "Net PnL"],
        [
            [
                html.escape(str(item.get("reason", ""))),
                html.escape(_fmt_num(item.get("count", 0))),
                html.escape(_fmt_money(item.get("pnl", 0.0))),
            ]
            for item in live_stats.get("exit_reason_breakdown", [])
        ],
    )
    live_close_slice_table = _table(
        ["Slice", "Closed", "Win Rate", "Net PnL", "Avg Edge"],
        [
            [
                html.escape(str(item.get("band", ""))),
                html.escape(_fmt_num(item.get("closed", 0))),
                html.escape(f"{100.0 * float(item.get('win_rate', 0.0)):.1f}%"),
                html.escape(_fmt_money(item.get("net_pnl", 0.0))),
                html.escape(f"{float(item.get('avg_edge', 0.0)):.4f}"),
            ]
            for item in live_stats.get("slice_breakdown", [])
        ],
    )
    live_close_price_band_table = _table(
        ["Entry Price Band", "Closed", "Win Rate", "Net PnL", "Avg Edge"],
        [
            [
                html.escape(str(item.get("band", ""))),
                html.escape(_fmt_num(item.get("closed", 0))),
                html.escape(f"{100.0 * float(item.get('win_rate', 0.0)):.1f}%"),
                html.escape(_fmt_money(item.get("net_pnl", 0.0))),
                html.escape(f"{float(item.get('avg_edge', 0.0)):.4f}"),
            ]
            for item in live_stats.get("price_band_breakdown", [])
        ],
    )
    live_close_edge_band_table = _table(
        ["Entry Edge Band", "Closed", "Win Rate", "Net PnL", "Avg Edge"],
        [
            [
                html.escape(str(item.get("band", ""))),
                html.escape(_fmt_num(item.get("closed", 0))),
                html.escape(f"{100.0 * float(item.get('win_rate', 0.0)):.1f}%"),
                html.escape(_fmt_money(item.get("net_pnl", 0.0))),
                html.escape(f"{float(item.get('avg_edge', 0.0)):.4f}"),
            ]
            for item in live_stats.get("edge_band_breakdown", [])
        ],
    )
    live_close_time_band_table = _table(
        ["Entry Seconds Left", "Closed", "Win Rate", "Net PnL", "Avg Edge"],
        [
            [
                html.escape(str(item.get("band", ""))),
                html.escape(_fmt_num(item.get("closed", 0))),
                html.escape(f"{100.0 * float(item.get('win_rate', 0.0)):.1f}%"),
                html.escape(_fmt_money(item.get("net_pnl", 0.0))),
                html.escape(f"{float(item.get('avg_edge', 0.0)):.4f}"),
            ]
            for item in live_stats.get("seconds_band_breakdown", [])
        ],
    )
    duplicate_close_table = _table(
        ["Position", "Close Rows", "First Close (CT)", "Last Close (CT)"],
        [
            [
                html.escape(str(item.get("position_id", ""))),
                html.escape(_fmt_num(item.get("close_count", 0))),
                html.escape(_fmt_ts(item.get("first_close_ts"))),
                html.escape(_fmt_ts(item.get("last_close_ts"))),
            ]
            for item in live_stats.get("duplicate_close_rows", [])
        ],
    )
    shadow_summary_rows = [
        ["Shadow Strategy", "BTC 5m NO taker"],
        ["Shadow Realized PnL", html.escape(_fmt_money(shadow_portfolio.get("realized_pnl_usdc", 0.0)))],
        ["Shadow Unrealized PnL", html.escape(_fmt_money(shadow_portfolio.get("unrealized_pnl_usdc", 0.0)))],
        ["Shadow Equity", html.escape(_fmt_money(shadow_portfolio.get("equity_usdc", bankroll_usdc)))],
        ["Shadow Open Positions", html.escape(_fmt_num(len(open_shadow_positions)))],
        ["Shadow Closed Trades", html.escape(_fmt_num(shadow_stats.get("total_closed", 0)))],
        ["Shadow Win Rate", html.escape(f"{100.0 * float(shadow_stats.get('win_rate', 0.0)):.1f}%")],
        ["Shadow Avg Win", html.escape(_fmt_money(shadow_stats.get("avg_win", 0.0)))],
        ["Shadow Avg Loss", html.escape(_fmt_money(shadow_stats.get("avg_loss", 0.0)))],
        ["Shadow Net PnL", html.escape(_fmt_money(shadow_stats.get("net_pnl", 0.0)))],
    ]
    shadow_signal_table = _table(
        ["Market", "Signal", "Mode", "Edge", "Eligible", "Reason"],
        [
            [
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("signal_type", ""))),
                html.escape(str(item.get("mode", ""))),
                html.escape(f"{float(item.get('edge', 0.0)):.4f}"),
                html.escape("yes" if bool(item.get("eligible")) else "no"),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in recent_shadow_signals
        ],
    )
    shadow_positions_table = _table(
        ["Market", "Asset", "Side", "Entry", "Size", "Mode", "Opened (CT)"],
        [
            [
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape(f"{float(item.get('entry_price', 0.0)):.4f}"),
                html.escape(f"{float(item.get('size', 0.0)):.4f}"),
                html.escape(str(item.get("mode", ""))),
                html.escape(_fmt_ts(item.get("entry_ts"))),
            ]
            for item in open_shadow_positions
        ],
    )
    shadow_events_table = _table(
        ["Time (CT)", "Position", "Type", "Mark", "PnL", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("position_id", ""))),
                html.escape(str(item.get("event_type", ""))),
                html.escape(f"{float(item.get('mark', 0.0)):.4f}"),
                html.escape(f"{float(item.get('pnl', 0.0)):.4f}"),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in recent_shadow_events
        ],
    )
    shadow_recent_closes_table = _table(
        ["Time (CT)", "Position", "Mark", "PnL", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("position_id", ""))),
                html.escape(f"{float(item.get('mark', 0.0)):.4f}"),
                html.escape(f"{float(item.get('pnl', 0.0)):.4f}"),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in shadow_stats.get("recent_closes", [])
        ],
    )
    shadow_reason_breakdown_table = _table(
        ["Reason", "Count", "Net PnL"],
        [
            [
                html.escape(str(item.get("reason", ""))),
                html.escape(_fmt_num(item.get("count", 0))),
                html.escape(_fmt_money(item.get("pnl", 0.0))),
            ]
            for item in shadow_stats.get("exit_reason_breakdown", [])
        ],
    )
    live_slice_table = _table(
        ["Slice", "Signals", "Eligible", "Eligible Rate", "Avg Edge"],
        [
            [
                html.escape(str(item.get("band", ""))),
                html.escape(_fmt_num(item.get("total", 0))),
                html.escape(_fmt_num(item.get("eligible", 0))),
                html.escape(f"{100.0 * float(item.get('eligible_rate', 0.0)):.1f}%"),
                html.escape(f"{float(item.get('avg_edge', 0.0)):.4f}"),
            ]
            for item in signal_stats.get("slice_breakdown", [])
        ],
    )
    live_reason_table = _table(
        ["Reason", "Count"],
        [
            [
                html.escape(str(item.get("reason", ""))),
                html.escape(_fmt_num(item.get("count", 0))),
            ]
            for item in signal_stats.get("reason_breakdown", [])
        ],
    )
    enabled_live_reason_table = _table(
        ["Reason", "Count"],
        [
            [
                html.escape(str(item.get("reason", ""))),
                html.escape(_fmt_num(item.get("count", 0))),
            ]
            for item in enabled_signal_stats.get("reason_breakdown", [])
        ],
    )
    threshold_relaxation_table = _table(
        ["Fair YES Floor", "Signals", "Markets", "Avg Edge", "Avg Fair YES", "Avg Entry"],
        [
            [
                html.escape(f"{float(item.get('fair_yes_floor', 0.0)):.2f}"),
                html.escape(_fmt_num(item.get("signals", 0))),
                html.escape(_fmt_num(item.get("markets", 0))),
                html.escape(f"{float(item.get('avg_edge', 0.0)):.4f}"),
                html.escape(f"{float(item.get('avg_fair_yes', 0.0)):.4f}"),
                html.escape(f"{float(item.get('avg_entry_price', 0.0)):.4f}"),
            ]
            for item in threshold_relaxation.get("threshold_breakdown", [])
        ],
    )
    top_blocked_table = _table(
        ["Time (CT)", "Market", "Edge", "Fair YES", "Ask", "Secs Left", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(f"{float(item.get('edge', 0.0)):.4f}"),
                html.escape(f"{float(item.get('fair_yes', 0.0)):.4f}"),
                html.escape(f"{float(item.get('yes_ask', 0.0)):.4f}"),
                html.escape(f"{float(item.get('seconds_left', 0.0)):.1f}"),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in threshold_relaxation.get("top_blocked", [])
        ],
    )
    shadow_yes_variant_top_raw_pnl_table = _render_shadow_variant_table(
        shadow_yes_variant_stats.get("top_raw_pnl", []) if isinstance(shadow_yes_variant_stats.get("top_raw_pnl"), list) else []
    )
    shadow_yes_variant_family_table = _render_shadow_variant_family_table(
        shadow_yes_variant_stats.get("families", []) if isinstance(shadow_yes_variant_stats.get("families"), list) else []
    )
    shadow_yes_variant_watchlist_table = _render_shadow_variant_table(
        shadow_yes_variant_stats.get("watchlist", []) if isinstance(shadow_yes_variant_stats.get("watchlist"), list) else []
    )
    shadow_yes_variant_table = _render_shadow_variant_table(
        shadow_yes_variant_stats.get("variants", []) if isinstance(shadow_yes_variant_stats.get("variants"), list) else []
    )
    shadow_yes_variant_market_table = _table(
        ["Market", "Closed", "Variants", "Net PnL", "Worst", "Best", "Variant IDs"],
        [
            [
                html.escape(str(item.get("market_id", ""))),
                html.escape(_fmt_num(item.get("closed", 0))),
                html.escape(_fmt_num(item.get("variants", 0))),
                html.escape(_fmt_money(item.get("net_pnl", 0.0))),
                html.escape(_fmt_money(item.get("worst_pnl", 0.0))),
                html.escape(_fmt_money(item.get("best_pnl", 0.0))),
                html.escape(str(item.get("variant_ids", ""))),
            ]
            for item in shadow_yes_variant_stats.get("market_breakdown", [])
        ],
    )
    shadow_yes_variant_calibration_table = _table(
        ["Variant", "Fair Band", "Closed", "Win Rate", "Avg Fair", "Avg Edge", "Net PnL", "Brier vs Win"],
        [
            [
                html.escape(str(item.get("variant_id", ""))),
                html.escape(str(item.get("fair_band", ""))),
                html.escape(_fmt_num(item.get("closed", 0))),
                html.escape(f"{100.0 * (float(item.get('wins', 0.0)) / max(float(item.get('closed', 0.0)), 1.0)):.1f}%"),
                html.escape(f"{float(item.get('avg_fair_yes', 0.0)):.4f}"),
                html.escape(f"{float(item.get('avg_edge', 0.0)):.4f}"),
                html.escape(_fmt_money(item.get("net_pnl", 0.0))),
                html.escape(f"{float(item.get('brier_vs_win', 0.0)):.4f}"),
            ]
            for item in shadow_yes_variant_stats.get("calibration", [])
        ],
    )
    shadow_yes_variant_recent_table = _table(
        ["Time (CT)", "Variant", "Position", "Market", "Entry", "Mark", "PnL", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("variant_id", ""))),
                html.escape(str(item.get("position_id", ""))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(f"{float(item.get('entry_price', 0.0)):.4f}"),
                html.escape(f"{float(item.get('mark', 0.0)):.4f}"),
                html.escape(f"{float(item.get('pnl', 0.0)):.4f}"),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in shadow_yes_variant_stats.get("recent_closes", [])
        ],
    )
    shadow_yes_variant_reason_table = _table(
        ["Variant", "Reason", "Count", "Max Edge", "Max Side Fair"],
        [
            [
                html.escape(str(item.get("variant_id", ""))),
                html.escape(str(item.get("reason", ""))),
                html.escape(_fmt_num(item.get("count", 0))),
                html.escape(f"{float(item.get('max_edge', 0.0)):.4f}"),
                html.escape(f"{float(item.get('max_fair_yes', 0.0)):.4f}"),
            ]
            for item in shadow_yes_variant_stats.get("reason_breakdown", [])
        ],
    )
    live_opportunity_rows = [
        ["State", html.escape(str(live_opportunity.get("state") or "-"))],
        ["Window", html.escape(f"{int(live_opportunity.get('window_minutes', 15))}m")],
        ["Signals", html.escape(_fmt_num(live_opportunity.get("signals", 0)))],
        ["Eligible", html.escape(_fmt_num(live_opportunity.get("eligible", 0)))],
        ["Eligible Rate", html.escape(f"{100.0 * float(live_opportunity.get('eligible_rate', 0.0)):.1f}%")],
        ["Top Edge", html.escape(f"{float(live_opportunity.get('top_edge', 0.0)):.4f}")],
        ["Top Market", html.escape(str(live_opportunity.get("top_market") or "-"))],
        ["Primary Blocker", html.escape(str(live_opportunity.get("top_reason") or "-"))],
        ["Last Fill (CT)", html.escape(_fmt_ts(live_opportunity.get("last_activity_ts")))],
        ["Relaxable Signals (60m)", html.escape(_fmt_num(threshold_relaxation.get("operational_candidate_signals", 0)))],
    ]
    shadow_signal_slice_table = _table(
        ["Slice", "Signals", "Eligible", "Eligible Rate", "Avg Edge"],
        [
            [
                html.escape(str(item.get("band", ""))),
                html.escape(_fmt_num(item.get("total", 0))),
                html.escape(_fmt_num(item.get("eligible", 0))),
                html.escape(f"{100.0 * float(item.get('eligible_rate', 0.0)):.1f}%"),
                html.escape(f"{float(item.get('avg_edge', 0.0)):.4f}"),
            ]
            for item in shadow_signal_stats.get("slice_breakdown", [])
        ],
    )
    shadow_opportunity_rows = [
        ["State", html.escape(str(shadow_opportunity.get("state") or "-"))],
        ["Window", html.escape(f"{int(shadow_opportunity.get('window_minutes', 15))}m")],
        ["Signals", html.escape(_fmt_num(shadow_opportunity.get("signals", 0)))],
        ["Eligible", html.escape(_fmt_num(shadow_opportunity.get("eligible", 0)))],
        ["Eligible Rate", html.escape(f"{100.0 * float(shadow_opportunity.get('eligible_rate', 0.0)):.1f}%")],
        ["Top Edge", html.escape(f"{float(shadow_opportunity.get('top_edge', 0.0)):.4f}")],
        ["Top Market", html.escape(str(shadow_opportunity.get("top_market") or "-"))],
        ["Primary Blocker", html.escape(str(shadow_opportunity.get("top_reason") or "-"))],
        ["Last Shadow Event (CT)", html.escape(_fmt_ts(shadow_opportunity.get("last_activity_ts")))],
    ]
    live_price_band_table = _table(
        ["Price Band", "Signals", "Eligible", "Eligible Rate", "Avg Edge"],
        [
            [
                html.escape(str(item.get("band", ""))),
                html.escape(_fmt_num(item.get("total", 0))),
                html.escape(_fmt_num(item.get("eligible", 0))),
                html.escape(f"{100.0 * float(item.get('eligible_rate', 0.0)):.1f}%"),
                html.escape(f"{float(item.get('avg_edge', 0.0)):.4f}"),
            ]
            for item in signal_stats.get("price_band_breakdown", [])
        ],
    )
    live_edge_band_table = _table(
        ["Edge Band", "Signals", "Eligible", "Eligible Rate", "Avg Edge"],
        [
            [
                html.escape(str(item.get("band", ""))),
                html.escape(_fmt_num(item.get("total", 0))),
                html.escape(_fmt_num(item.get("eligible", 0))),
                html.escape(f"{100.0 * float(item.get('eligible_rate', 0.0)):.1f}%"),
                html.escape(f"{float(item.get('avg_edge', 0.0)):.4f}"),
            ]
            for item in signal_stats.get("edge_band_breakdown", [])
        ],
    )
    live_time_band_table = _table(
        ["Seconds Left", "Signals", "Eligible", "Eligible Rate", "Avg Edge"],
        [
            [
                html.escape(str(item.get("band", ""))),
                html.escape(_fmt_num(item.get("total", 0))),
                html.escape(_fmt_num(item.get("eligible", 0))),
                html.escape(f"{100.0 * float(item.get('eligible_rate', 0.0)):.1f}%"),
                html.escape(f"{float(item.get('avg_edge', 0.0)):.4f}"),
            ]
            for item in signal_stats.get("seconds_band_breakdown", [])
        ],
    )
    shadow_reason_table = _table(
        ["Reason", "Count"],
        [
            [
                html.escape(str(item.get("reason", ""))),
                html.escape(_fmt_num(item.get("count", 0))),
            ]
            for item in shadow_signal_stats.get("reason_breakdown", [])
        ],
    )
    shadow_price_band_table = _table(
        ["Entry Price Band", "Closed", "Win Rate", "Net PnL", "Avg Edge"],
        [
            [
                html.escape(str(item.get("band", ""))),
                html.escape(_fmt_num(item.get("closed", 0))),
                html.escape(f"{100.0 * float(item.get('win_rate', 0.0)):.1f}%"),
                html.escape(_fmt_money(item.get("net_pnl", 0.0))),
                html.escape(f"{float(item.get('avg_edge', 0.0)):.4f}"),
            ]
            for item in shadow_stats.get("price_band_breakdown", [])
        ],
    )
    shadow_edge_band_table = _table(
        ["Edge Band", "Closed", "Win Rate", "Net PnL", "Avg Edge"],
        [
            [
                html.escape(str(item.get("band", ""))),
                html.escape(_fmt_num(item.get("closed", 0))),
                html.escape(f"{100.0 * float(item.get('win_rate', 0.0)):.1f}%"),
                html.escape(_fmt_money(item.get("net_pnl", 0.0))),
                html.escape(f"{float(item.get('avg_edge', 0.0)):.4f}"),
            ]
            for item in shadow_stats.get("edge_band_breakdown", [])
        ],
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="10">
  <title>Latency Bot Dashboard</title>
  <style>
    body {{ font-family: ui-sans-serif, system-ui, sans-serif; margin: 24px; color: #111827; background: #f8fafc; }}
    h1, h2 {{ margin: 0 0 12px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; }}
    .panel {{ background: white; border: 1px solid #dbe4ee; border-radius: 14px; padding: 16px; box-shadow: 0 1px 2px rgba(0,0,0,0.04); }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ text-align: left; padding: 8px; border-bottom: 1px solid #e5e7eb; vertical-align: top; }}
    th {{ font-size: 12px; letter-spacing: 0.03em; text-transform: uppercase; color: #475569; }}
    ul {{ margin: 0; padding-left: 18px; }}
    .meta {{ color: #64748b; margin-bottom: 16px; }}
    .meta strong {{ color: #0f172a; }}
    .sub {{ color: #64748b; }}
    .curve {{ width: 100%; height: auto; display: block; }}
    .curve-grid {{ stroke: #e2e8f0; stroke-width: 1; }}
    .curve-axis {{ stroke: #94a3b8; stroke-width: 1; }}
    .curve-baseline {{ stroke: #cbd5e1; stroke-width: 1; stroke-dasharray: 4 4; }}
    .curve-line {{ fill: none; stroke: #0f766e; stroke-width: 3; stroke-linecap: round; stroke-linejoin: round; }}
    .curve-dot {{ fill: #0f766e; }}
    .curve-label {{ fill: #475569; font-size: 11px; font-family: ui-sans-serif, system-ui, sans-serif; }}
  </style>
</head>
<body>
  <h1>Latency Bot</h1>
  <div class="meta">Paper-only BTC/ETH/SOL short-horizon latency bot scaffold.</div>
  {run_meta_html}
  <div class="meta">
    <strong>Page Updated:</strong> {html.escape(page_generated_at)} |
    <strong>Last Cycle Completed:</strong> {html.escape(last_cycle_completed)} |
    <strong>Auto-Refresh:</strong> every 10s
  </div>
  <div class="grid">
    <div class="panel"><h2>Status</h2>{_table(["Metric", "Value"], rows)}</div>
    <div class="panel"><h2>Capital Usage</h2>{_table(["Metric", "Value"], capital_usage_rows)}</div>
    <div class="panel"><h2>Database</h2>{_table(["Metric", "Value"], db_rows)}</div>
    <div class="panel"><h2>Latest Cycle</h2>{_table(["Metric", "Value"], latest_cycle_rows)}</div>
    <div class="panel"><h2>Notes</h2><ul>{notes_html}</ul></div>
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Paper PnL Curve</h2>
    <div class="sub">Actual live-paper execution only. Shadow/research variants are excluded from this curve.</div>
    {_render_equity_curve(equity_curve, bankroll_usdc)}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Paper Strategy PnL Curves</h2>
    <div class="sub">One curve per enabled promoted/live-paper strategy. The promoted set is the current top raw-PnL strategy basket, and these are the curves to compare when deciding what performs best live.</div>
    {_render_variant_equity_curves(live_strategy_equity_curves, bankroll_usdc)}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Complete-Set Arb Prototype</h2>
    <div class="sub">Paper-only YES+NO paired-entry scanner. It treats qualifying pairs as locked complete sets and books net edge after a slippage buffer; real execution still requires atomic leg handling.</div>
    {_table(["Metric", "Value"], complete_set_summary_rows)}
    <h3>Complete-Set PnL Curve</h3>
    {_render_equity_curve(complete_set_arb_stats.get("equity_curve", []) if isinstance(complete_set_arb_stats.get("equity_curve"), list) else [], bankroll_usdc)}
    <h3>Recent Opportunities</h3>
    {complete_set_signal_table}
    <h3>Skip Reasons</h3>
    {complete_set_reason_table}
    <h3>Recent Locked Sets</h3>
    {complete_set_recent_closes_table}
    <h3>Entry Blocks</h3>
    {complete_set_entry_block_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Realistic Complete-Set Arb Simulation</h2>
    <div class="sub">Research-only tick replay. This pane uses every recorded complete-set signal tick, then applies paired-leg failure stress, depth haircuts, latency/slippage decay, operational expected loss, and capital lockup until expiry plus redeem lag. The original arb pane above is unchanged.</div>
    {_table(["Metric", "Value"], realistic_complete_set_summary_rows)}
    <h3>Realistic PnL Curve</h3>
    {_render_equity_curve(realistic_complete_set_arb.get("equity_curve", []) if isinstance(realistic_complete_set_arb.get("equity_curve"), list) else [], float(realistic_complete_set_summary.get("simulated_capital_usdc", bankroll_usdc) or bankroll_usdc))}
    <h3>Recent Simulated Tick Events</h3>
    {realistic_complete_set_event_table}
    <h3>Realistic Skip / Failure Reasons</h3>
    {realistic_complete_set_reason_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Complete-Set Arb Pilot</h2>
    <div class="sub">Separate guarded live-pilot tracker for the $50 Polymarket test bankroll. Dry-run candidates, safety blocks, submitted paired attempts, and failures are isolated from all paper/research bot metrics.</div>
    {_table(["Metric", "Value"], live_complete_set_pilot_summary_rows)}
    <h3>Live Pilot PnL Curve</h3>
    {_render_equity_curve(live_complete_set_arb_pilot.get("equity_curve", []) if isinstance(live_complete_set_arb_pilot.get("equity_curve"), list) else [], float(live_complete_set_pilot_summary.get("simulated_or_live_capital_usdc", bankroll_usdc) or bankroll_usdc))}
    <h3>Recent Live Pilot Attempts</h3>
    {live_complete_set_pilot_attempt_table}
    <h3>Live Pilot Blocks / Failures</h3>
    {live_complete_set_pilot_reason_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Polymarket US Crossed-Book Arb Simulation</h2>
    <div class="sub">Research-only US probe. A separate daemon records Polymarket US order-book ticks for configured symbols, then this pane simulates FOK-style buy-YES-at-ask and sell/short-YES-at-bid trades when crossed-book edge survives depth, latency, slippage, fees, and operational failure assumptions. No live orders are submitted.</div>
    {_table(["Metric", "Value"], polymarket_us_arb_summary_rows)}
    <h3>US Simulated PnL Curve</h3>
    {_render_equity_curve(polymarket_us_arb.get("equity_curve", []) if isinstance(polymarket_us_arb.get("equity_curve"), list) else [], float(polymarket_us_arb_summary.get("simulated_capital_usdc", bankroll_usdc) or bankroll_usdc))}
    <h3>Recent US Tick / Trade Events</h3>
    {polymarket_us_arb_event_table}
    <h3>US Skip / Failure Reasons</h3>
    {polymarket_us_arb_reason_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Kalshi Complete-Set Arb Simulation</h2>
    <div class="sub">Research-only Kalshi probe. A separate daemon auto-discovers BTC/ETH/SOL short-horizon crypto markets, records YES/NO order-book ticks, and simulates buying complete sets when derived cost is below $1 after depth, latency, fee, capital-lock, and operational failure assumptions. No live orders are submitted.</div>
    {_table(["Metric", "Value"], kalshi_arb_summary_rows)}
    <h3>Kalshi Simulated PnL Curve</h3>
    {_render_equity_curve(kalshi_arb.get("equity_curve", []) if isinstance(kalshi_arb.get("equity_curve"), list) else [], float(kalshi_arb_summary.get("simulated_capital_usdc", bankroll_usdc) or bankroll_usdc))}
    <h3>Recent Kalshi Tick / Set Events</h3>
    {kalshi_arb_event_table}
    <h3>Kalshi Skip / Failure Reasons</h3>
    {kalshi_arb_reason_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Discovery</h2>
    {_table(["Metric", "Value"], discovery_rows)}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Feed Cache</h2>
    {_table(["Metric", "Value"], feed_rows)}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Tracked Markets</h2>
    {markets_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Tracked Market Cache</h2>
    {tracked_cache_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Fair Values</h2>
    {fair_value_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Signals</h2>
    {signal_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Signal Slices</h2>
    {live_slice_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Opportunity</h2>
    {_table(["Metric", "Value"], live_opportunity_rows)}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Skip Reasons</h2>
    {live_reason_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Enabled Live Skip Reasons</h2>
    {enabled_live_reason_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>BTC 5m YES Threshold Candidates</h2>
    {threshold_relaxation_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Top Blocked BTC 5m YES Candidates</h2>
    {top_blocked_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Consensus Meta-Strategy</h2>
    <div class="sub">Research-only view for combining variants into one coherent strategy. Variants are treated as voters; duplicate same-market signals should increase confidence, not duplicate exposure.</div>
    {_table(["Metric", "Value"], consensus_summary_rows)}
    <h3>Sleeve Representatives</h3>
    {consensus_sleeve_table}
    <h3>Top Voters</h3>
    {consensus_voter_table}
    <h3>Confidence Sizing Ladder</h3>
    {consensus_sizing_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow Taker Strategy Families</h2>
    <div class="sub">One representative per asset/tenor/side/model family. This is the primary table for reducing duplicate variants.</div>
    {shadow_yes_variant_family_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow Taker Top Raw PnL</h2>
    {shadow_yes_variant_top_raw_pnl_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow Taker Watchlist</h2>
    {shadow_yes_variant_watchlist_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow Taker Variant Grid</h2>
    <div class="sub">Showing top {html.escape(_fmt_num(len(shadow_yes_variant_stats.get("variants", []))))} of {html.escape(_fmt_num(shadow_yes_variant_stats.get("variants_total", 0)))} variants by raw PnL.</div>
    {shadow_yes_variant_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow Taker Unique Market PnL</h2>
    {shadow_yes_variant_market_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow Taker Fair Calibration</h2>
    {shadow_yes_variant_calibration_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow Taker Research Rankings</h2>
    <div class="sub">Shadow variants are research signals, not live-paper execution. Their executable PnL curves are intentionally hidden; promote a variant to live paper to generate comparable live curves above.</div>
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow Taker Variant Skip Reasons</h2>
    <div class="sub">Showing top {html.escape(_fmt_num(len(shadow_yes_variant_stats.get("reason_breakdown", []))))} of {html.escape(_fmt_num(shadow_yes_variant_stats.get("reason_breakdown_total", 0)))} reason rows.</div>
    {shadow_yes_variant_reason_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow Taker Variant Recent Closes</h2>
    {shadow_yes_variant_recent_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Price Bands</h2>
    {live_price_band_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Edge Bands</h2>
    {live_edge_band_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Time Bands</h2>
    {live_time_band_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Orders</h2>
    {orders_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Open Positions</h2>
    {positions_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Top Promoted Strategy Revenue</h2>
    <div class="sub">Enabled promoted strategies write to live paper positions. This panel shows live-paper net PnL plus 24h, monthly, and yearly revenue estimates for each promoted strategy.</div>
    {_table(["Metric", "Value"], promoted_summary_rows)}
    <h3>Per Variant Revenue</h3>
    {promoted_variant_table}
    <h3>Last Cycle Entry Blocks</h3>
    {promoted_entry_block_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Performance</h2>
    {_table(["Metric", "Value"], live_summary_rows)}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Streak Diagnostics</h2>
    {_table(["Metric", "Value"], live_streak_rows)}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Entry Blocks</h2>
    {live_entry_block_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Recent Closes</h2>
    {live_recent_closes_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Exit Reasons</h2>
    {live_reason_breakdown_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Close Slices</h2>
    {live_close_slice_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Close Price Bands</h2>
    {live_close_price_band_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Close Edge Bands</h2>
    {live_close_edge_band_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Close Time Bands</h2>
    {live_close_time_band_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Integrity</h2>
    {duplicate_close_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Recent Fills</h2>
    {fills_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow NO Summary</h2>
    {_table(["Metric", "Value"], shadow_summary_rows)}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow NO Research Status</h2>
    <div class="sub">Shadow NO is not live-paper execution, so no executable PnL curve is shown here.</div>
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow NO Signals</h2>
    {shadow_signal_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow NO Signal Slices</h2>
    {shadow_signal_slice_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow NO Opportunity</h2>
    {_table(["Metric", "Value"], shadow_opportunity_rows)}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow NO Skip Reasons</h2>
    {shadow_reason_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow NO Open Positions</h2>
    {shadow_positions_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow NO Price Bands</h2>
    {shadow_price_band_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow NO Edge Bands</h2>
    {shadow_edge_band_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow NO Recent Closes</h2>
    {shadow_recent_closes_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow NO Exit Reasons</h2>
    {shadow_reason_breakdown_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Shadow NO Events</h2>
    {shadow_events_table}
  </div>
</body>
</html>"""


def serve_latency_bot_dashboard(settings: LatencyBotSettings, host: str, port: int) -> None:
    settings.ensure_dirs()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            state = build_latency_bot_dashboard_state(settings)
            body = render_latency_bot_dashboard_html(state).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer((host, port), Handler)
    print(f"latency bot dashboard listening on http://{host}:{port}")
    server.serve_forever()
