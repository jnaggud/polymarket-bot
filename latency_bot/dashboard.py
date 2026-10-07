from __future__ import annotations

from dataclasses import replace
import html
import json
import math
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

from .config import LatencyBotSettings
from .strategy.related_market_arb import build_related_market_constraint_graph
from .strategy_truth import build_strategy_truth_rows
from .storage import (
    connect_latency_bot_db,
    init_latency_bot_db,
    latency_bot_capital_usage,
    latency_bot_btc_fair_value_paper_stats,
    latency_bot_cex_latency_paper_stats,
    latency_bot_equity_curve,
    latency_bot_complete_set_arb_stats,
    latency_bot_kalshi_arb_sim,
    latency_bot_live_strategy_equity_curves,
    latency_bot_live_complete_set_arb_pilot_stats,
    latency_bot_live_temporal_inventory_maker_stats,
    latency_bot_opportunity_stats,
    latency_bot_performance_stats,
    latency_bot_polymarket_account_reconciliation,
    latency_bot_late_resolution_capture_paper_stats,
    latency_bot_polymarket_us_arb_sim,
    latency_bot_preowned_inventory_arb_sim,
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
    latency_bot_temporal_inventory_maker_paper_stats,
    latency_bot_wallet_teacher_sniper_stats,
)


DISPLAY_TZ = ZoneInfo("America/Chicago")
_SHADOW_VARIANT_CACHE_TTL_SECONDS = 60.0
_SHADOW_VARIANT_CACHE_LOCK = threading.Lock()
_SHADOW_VARIANT_CACHE: dict[str, Any] = {"key": "", "loaded_at": 0.0, "payload": None}
_DASHBOARD_STATE_CACHE_TTL_SECONDS = 5.0
_DASHBOARD_STATE_CACHE_LOCK = threading.Lock()
_DASHBOARD_STATE_CACHE: dict[str, Any] = {"key": "", "loaded_at": 0.0, "payload": None}


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


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else 0.0
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return str(value)


def _dict_value(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list_value(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _compact_market_item(item: Any) -> dict[str, Any]:
    row = _dict_value(item)
    return {
        "question": row.get("question") or row.get("title") or "",
        "asset": row.get("asset") or "",
        "tenor_minutes": row.get("tenor_minutes", 0),
        "hours_to_expiry": row.get("hours_to_expiry", 0.0),
        "best_bid": row.get("best_bid", row.get("bid", 0.0)),
        "best_ask": row.get("best_ask", row.get("ask", 0.0)),
        "midpoint": row.get("midpoint", row.get("mid", 0.0)),
        "min_depth_usdc": row.get("min_depth_usdc", row.get("depth_usdc", 0.0)),
        "seen_at": row.get("seen_at") or row.get("updated_at") or "",
    }


def _compact_binance_item(item: Any) -> dict[str, Any]:
    row = _dict_value(item)
    return {
        "symbol": row.get("symbol") or "",
        "asset": row.get("asset") or "",
        "bid": row.get("bid", 0.0),
        "ask": row.get("ask", 0.0),
        "mid": row.get("mid", 0.0),
        "seen_at": row.get("seen_at") or row.get("updated_at") or "",
        "source_latency_ms": row.get("source_latency_ms", 0.0),
    }


def _build_interactive_dashboard_payload(state: dict[str, Any]) -> dict[str, Any]:
    status = _dict_value(state.get("status"))
    db_summary = _dict_value(state.get("db_summary"))
    recent_counts = _dict_value(db_summary.get("recent_counts"))
    latest_cycle = _dict_value(db_summary.get("latest_cycle"))
    temporal = _dict_value(state.get("temporal_inventory_maker_paper"))
    temporal_summary = _dict_value(temporal.get("summary"))
    live_temporal = _dict_value(state.get("live_temporal_inventory_maker"))
    live_temporal_summary = _dict_value(live_temporal.get("summary"))
    late_resolution = _dict_value(state.get("late_resolution_capture_paper"))
    late_resolution_summary = _dict_value(late_resolution.get("summary"))
    cex_latency = _dict_value(state.get("cex_latency_paper"))
    cex_latency_summary = _dict_value(cex_latency.get("summary"))
    btc_fair = _dict_value(state.get("btc_fair_value_paper"))
    btc_fair_summary = _dict_value(btc_fair.get("summary"))
    portfolio = _dict_value(state.get("portfolio"))
    capital_usage = _dict_value(state.get("capital_usage"))
    polymarket_cache = _dict_value(state.get("polymarket_cache"))
    binance_cache = _dict_value(state.get("binance_cache"))
    market_items = [_compact_market_item(item) for item in _list_value(polymarket_cache.get("items"))[:60]]
    binance_items = [_compact_binance_item(item) for item in _list_value(binance_cache.get("items"))[:20]]
    last_cycle_result = _dict_value(status.get("last_cycle_result"))
    live_temporal_result = _dict_value(last_cycle_result.get("live_temporal_inventory_maker"))
    late_resolution_result = _dict_value(last_cycle_result.get("late_resolution_capture_paper"))
    temporal_result = _dict_value(last_cycle_result.get("temporal_inventory_maker_paper"))
    return _json_safe(
        {
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "generated_at_ct": datetime.now(DISPLAY_TZ).strftime("%H:%M:%S CT"),
            "fast_mode": bool(state.get("fast_mode")),
            "served_from_cache": bool(state.get("served_from_cache")),
            "strategy_truth": _list_value(state.get("strategy_truth")),
            "related_market_graph": _dict_value(state.get("related_market_graph")),
            "data_availability": _dict_value(state.get("data_availability")),
            "status": {
                "runner_status": status.get("runner_status", "unknown"),
                "phase": status.get("phase", "bootstrap"),
                "risk_state": status.get("risk_state", "unknown"),
                "tracked_markets_count": status.get("tracked_markets_count", 0),
                "open_orders_count": status.get("open_orders_count", 0),
                "open_positions_count": status.get("open_positions_count", 0),
                "last_cycle_started_at": status.get("last_cycle_started_at"),
                "last_cycle_completed_at": status.get("last_cycle_completed_at"),
                "last_error": status.get("last_error") or "",
                "notes": _list_value(status.get("notes"))[:8],
            },
            "latest_cycle": latest_cycle,
            "counts": recent_counts,
            "portfolio": {
                "realized_pnl_usdc": portfolio.get("realized_pnl_usdc", status.get("realized_pnl_usdc", 0.0)),
                "unrealized_pnl_usdc": portfolio.get("unrealized_pnl_usdc", status.get("unrealized_pnl_usdc", 0.0)),
                "equity_usdc": portfolio.get("equity_usdc", state.get("bankroll_usdc", 0.0)),
                "realized_usdc_per_day": portfolio.get("realized_usdc_per_day", 0.0),
                "projected_monthly_revenue_usdc": portfolio.get("projected_monthly_revenue_usdc", 0.0),
            },
            "capital": {
                "bankroll_usdc": capital_usage.get("bankroll_usdc", state.get("bankroll_usdc", 0.0)),
                "total_current_capital_usdc": capital_usage.get("total_current_capital_usdc", 0.0),
                "total_current_bankroll_fraction": capital_usage.get("total_current_bankroll_fraction", 0.0),
                "open_order_capital_usdc": capital_usage.get("open_order_capital_usdc", 0.0),
                "live_position_capital_usdc": capital_usage.get("live_position_capital_usdc", 0.0),
            },
            "temporal": {
                "summary": temporal_summary,
                "execution": _dict_value(temporal_result.get("execution")),
                "equity_curve": _list_value(temporal.get("equity_curve"))[-120:],
                "markets": _list_value(temporal.get("markets"))[:20],
                "recent_events": _list_value(temporal.get("recent_events"))[:30],
                "recent_quotes": _list_value(temporal.get("recent_quotes"))[:30],
                "quote_style_breakdown": _list_value(temporal.get("quote_style_breakdown"))[:12],
                "exit_reason_breakdown": _list_value(temporal.get("exit_reason_breakdown"))[:12],
            },
            "live_temporal": {
                "summary": live_temporal_summary,
                "execution": _dict_value(live_temporal_result.get("execution")),
                "recent_orders": _list_value(live_temporal.get("recent_orders"))[:30],
            },
            "late_resolution": {
                "summary": late_resolution_summary,
                "execution": _dict_value(late_resolution_result.get("execution")),
                "equity_curve": _list_value(late_resolution.get("equity_curve"))[-120:],
                "open_positions": _list_value(late_resolution.get("open_positions"))[:20],
                "recent_closes": _list_value(late_resolution.get("recent_closes"))[:20],
                "recent_signals": _list_value(late_resolution.get("recent_signals"))[:30],
                "reason_breakdown": _list_value(late_resolution.get("reason_breakdown"))[:12],
            },
            "research": {
                "cex_latency": cex_latency_summary,
                "btc_fair": btc_fair_summary,
            },
            "markets": {
                "updated_at": polymarket_cache.get("updated_at"),
                "source": polymarket_cache.get("source"),
                "count": polymarket_cache.get("count", len(market_items)),
                "items": market_items,
            },
            "binance": {
                "updated_at": binance_cache.get("updated_at"),
                "source": binance_cache.get("source"),
                "count": binance_cache.get("count", 0),
                "items": binance_items,
            },
            "recent_signals": _list_value(state.get("recent_signals"))[:18],
            "recent_fair_values": _list_value(state.get("recent_fair_values"))[:18],
            "recent_fills": _list_value(state.get("recent_fills"))[:18],
            "recent_orders": _list_value(state.get("recent_orders"))[:18],
        }
    )


def _serialize_interactive_dashboard_payload(state: dict[str, Any]) -> str:
    return json.dumps(
        _build_interactive_dashboard_payload(state),
        allow_nan=False,
        separators=(",", ":"),
    ).replace("</", "<\\/")


def _render_interactive_cockpit(state: dict[str, Any]) -> str:
    payload_json = _serialize_interactive_dashboard_payload(state)
    template = r"""
  <style>
    .cockpit {
      --ink: #111827;
      --muted: #64748b;
      --line: #d8e0eb;
      --panel: rgba(255, 255, 255, 0.92);
      --good: #079669;
      --warn: #d97706;
      --bad: #dc2626;
      --cyan: #0891b2;
      --violet: #7c3aed;
      margin: 18px 0 20px;
      color: var(--ink);
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    .cockpit * { box-sizing: border-box; }
    .ops-shell {
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
      background: linear-gradient(180deg, #eef3f8 0%, #f8fbfd 100%);
      box-shadow: 0 12px 28px rgba(15, 23, 42, 0.08);
    }
    .ops-topbar {
      display: grid;
      grid-template-columns: minmax(260px, 1fr) auto;
      gap: 12px;
      align-items: center;
      padding: 10px 14px;
      border-bottom: 1px solid var(--line);
      background: #e8eef5;
    }
    .ops-brand {
      display: flex;
      align-items: baseline;
      gap: 12px;
      min-width: 0;
    }
    .ops-mark {
      width: 26px;
      height: 26px;
      border: 2px solid #d18a00;
      border-radius: 7px;
      color: #d18a00;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      font-weight: 800;
    }
    .ops-title {
      font-weight: 850;
      letter-spacing: .08em;
      font-size: 15px;
      white-space: nowrap;
    }
    .ops-subtitle {
      color: var(--muted);
      font-size: 11px;
      letter-spacing: .16em;
      text-transform: uppercase;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .ops-clock {
      font-variant-numeric: tabular-nums;
      font-size: 23px;
      letter-spacing: .05em;
      text-align: right;
      min-width: 190px;
    }
    .ops-tape {
      display: grid;
      grid-template-columns: repeat(6, minmax(120px, 1fr));
      gap: 0;
      border-bottom: 1px solid var(--line);
      background: #f6f9fc;
    }
    .tape-item {
      padding: 7px 12px;
      border-right: 1px solid var(--line);
      min-width: 0;
      font-size: 12px;
    }
    .tape-label { color: var(--muted); font-weight: 750; letter-spacing: .09em; }
    .tape-value { font-variant-numeric: tabular-nums; font-weight: 800; margin-left: 6px; }
    .pos { color: var(--good); }
    .neg { color: var(--bad); }
    .warn { color: var(--warn); }
    .ops-hero {
      display: grid;
      grid-template-columns: minmax(280px, .92fr) minmax(260px, .72fr) minmax(360px, 1.36fr);
      min-height: 252px;
      border-bottom: 1px solid var(--line);
    }
    .hero-card {
      padding: 14px;
      background: var(--panel);
      border-right: 1px solid var(--line);
      min-width: 0;
    }
    .hero-card:last-child { border-right: 0; }
    .panel-kicker {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
      color: var(--muted);
      font-size: 11px;
      font-weight: 800;
      letter-spacing: .13em;
      text-transform: uppercase;
      margin-bottom: 8px;
    }
    .big-money {
      font-size: clamp(34px, 5vw, 72px);
      line-height: .95;
      font-weight: 900;
      color: #d88300;
      font-variant-numeric: tabular-nums;
      margin: 8px 0 5px;
    }
    .hero-stats {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
      margin-top: 14px;
    }
    .micro-stat {
      border-top: 3px solid #dbe5ee;
      padding-top: 7px;
      min-width: 0;
    }
    .micro-label {
      color: var(--muted);
      font-size: 10px;
      font-weight: 800;
      letter-spacing: .11em;
      text-transform: uppercase;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .micro-value {
      font-size: 21px;
      font-weight: 850;
      font-variant-numeric: tabular-nums;
      margin-top: 2px;
      overflow-wrap: anywhere;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: #fff;
      color: #334155;
      font-size: 11px;
      font-weight: 800;
      padding: 4px 8px;
      letter-spacing: .04em;
      text-transform: uppercase;
      white-space: nowrap;
    }
    .badge.good { color: var(--good); border-color: rgba(7,150,105,.28); background: #ecfdf5; }
    .badge.warn { color: var(--warn); border-color: rgba(217,119,6,.25); background: #fffbeb; }
    .badge.bad { color: var(--bad); border-color: rgba(220,38,38,.24); background: #fef2f2; }
    .sparkline {
      width: 100%;
      height: 116px;
      display: block;
      border: 1px solid #e1e9f2;
      border-radius: 8px;
      background: #fbfdff;
    }
    .strategy-stack {
      display: grid;
      gap: 8px;
      margin-top: 8px;
    }
    .strategy-row {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 10px;
      align-items: center;
      padding: 9px;
      border: 1px solid #dfe7f0;
      border-radius: 8px;
      background: #fff;
      min-width: 0;
    }
    .strategy-name { font-weight: 850; font-size: 13px; }
    .strategy-meta { color: var(--muted); font-size: 11px; margin-top: 2px; overflow-wrap: anywhere; }
    .ops-cycle {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 8px;
      padding: 11px 14px;
      border-bottom: 1px solid var(--line);
      background: #f9fbfd;
    }
    .cycle-step {
      position: relative;
      min-height: 42px;
      padding: 8px 10px 8px 28px;
      border: 1px solid #dae3ee;
      border-radius: 8px;
      background: #fff;
      font-size: 11px;
      color: var(--muted);
      font-weight: 800;
      letter-spacing: .08em;
      text-transform: uppercase;
      overflow: hidden;
    }
    .cycle-step::before {
      content: "";
      position: absolute;
      left: 10px;
      top: 15px;
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: #9ca3af;
      box-shadow: 0 0 0 4px rgba(148,163,184,.12);
    }
    .cycle-step.active { border-color: rgba(8,145,178,.42); box-shadow: inset 0 -3px 0 rgba(8,145,178,.25); color: #0f172a; }
    .cycle-step.active::before { background: var(--cyan); box-shadow: 0 0 0 5px rgba(8,145,178,.13); }
    .ops-tabs {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
      background: #eef4f9;
    }
    .ops-tab {
      border: 1px solid #cfdae7;
      border-radius: 8px;
      background: #fff;
      color: #334155;
      padding: 7px 11px;
      font-size: 12px;
      font-weight: 850;
      cursor: pointer;
    }
    .ops-tab[aria-selected="true"] { background: #0f172a; color: #fff; border-color: #0f172a; }
    .ops-controls { margin-left: auto; display: flex; gap: 10px; align-items: center; color: var(--muted); font-size: 12px; }
    .ops-body {
      display: grid;
      grid-template-columns: minmax(360px, 1.18fr) minmax(300px, .82fr);
      gap: 0;
      min-height: 450px;
    }
    .ops-main, .ops-side { padding: 14px; min-width: 0; }
    .ops-main { border-right: 1px solid var(--line); }
    .cockpit-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
    .ops-panel {
      border: 1px solid #dbe5ef;
      border-radius: 8px;
      background: rgba(255,255,255,.88);
      overflow: hidden;
      min-width: 0;
    }
    .ops-panel.full { grid-column: 1 / -1; }
    .ops-panel h3 {
      margin: 0;
      padding: 10px 11px;
      font-size: 12px;
      letter-spacing: .11em;
      text-transform: uppercase;
      background: #f4f8fb;
      border-bottom: 1px solid #dbe5ef;
    }
    .ops-panel-content { padding: 10px; }
    .agent-canvas, .scatter-canvas {
      width: 100%;
      height: 270px;
      display: block;
      background: #fbfdff;
    }
    .mini-table { width: 100%; border-collapse: collapse; font-size: 12px; }
    .mini-table th, .mini-table td { padding: 7px; border-bottom: 1px solid #e6edf5; vertical-align: top; }
    .mini-table th { color: var(--muted); font-size: 10px; letter-spacing: .09em; text-transform: uppercase; }
    .activity-feed { display: grid; gap: 7px; max-height: 360px; overflow: auto; padding-right: 3px; }
    .activity-item {
      border: 1px solid #e1e9f2;
      border-left: 4px solid #94a3b8;
      border-radius: 8px;
      padding: 8px 9px;
      background: #fff;
      font-size: 12px;
    }
    .activity-item.fill, .activity-item.locked_pair { border-left-color: var(--good); }
    .activity-item.cancel, .activity-item.sell { border-left-color: var(--warn); }
    .activity-item.expire, .activity-item.failed { border-left-color: var(--bad); }
    .activity-title { display: flex; justify-content: space-between; gap: 10px; font-weight: 850; }
    .activity-meta { color: var(--muted); margin-top: 3px; line-height: 1.35; overflow-wrap: anywhere; }
    .heatmap { display: grid; gap: 6px; }
    .heat-row { display: grid; grid-template-columns: 52px 1fr 58px; align-items: center; gap: 8px; font-size: 12px; }
    .heat-bar { height: 20px; border-radius: 5px; background: #edf2f7; overflow: hidden; }
    .heat-fill { height: 100%; min-width: 2px; background: linear-gradient(90deg, #0891b2, #10b981); }
    .tab-pane { display: none; }
    .tab-pane.active { display: block; }
    .data-cards { display: grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap: 9px; margin-bottom: 12px; }
    .data-card { border: 1px solid #dfe7f0; border-radius: 8px; padding: 9px; background: #fff; min-width: 0; }
    .data-card .label { color: var(--muted); font-size: 10px; font-weight: 850; letter-spacing: .1em; text-transform: uppercase; }
    .data-card .value { font-size: 20px; font-weight: 900; font-variant-numeric: tabular-nums; margin-top: 3px; overflow-wrap: anywhere; }
    details { margin-top: 12px; border-top: 1px solid var(--line); padding-top: 12px; }
    details summary { cursor: pointer; color: var(--ink); }
    @media (max-width: 1100px) {
      .ops-topbar, .ops-hero, .ops-body { grid-template-columns: 1fr; }
      .ops-main { border-right: 0; border-bottom: 1px solid var(--line); }
      .ops-tape { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .ops-cycle { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .cockpit-grid, .data-cards { grid-template-columns: 1fr; }
      .ops-controls { width: 100%; margin-left: 0; justify-content: space-between; }
    }
  </style>
  <section class="cockpit" id="interactive-cockpit" data-dashboard-cockpit>
    <div class="ops-shell">
      <div class="ops-topbar">
        <div class="ops-brand">
          <span class="ops-mark">Q</span>
          <span class="ops-title">POLYMARKET LATENCY OPS</span>
          <span class="ops-subtitle">PROMOTED 5M BASKET / TEMPORAL MAKER / LIVE GATES</span>
        </div>
        <div class="ops-clock" id="ops-clock">--:--:--</div>
      </div>
      <div class="ops-tape" id="ops-tape"></div>
      <div class="ops-hero">
        <div class="hero-card">
          <div class="panel-kicker"><span>Truth Leader</span><span id="temporal-state-badge" class="badge">SYNC</span></div>
          <div class="big-money" id="hero-equity">$0.00</div>
          <div class="ops-subtitle" id="hero-subtitle">paper inventory maker</div>
          <svg class="sparkline" id="temporal-spark" viewBox="0 0 420 116" preserveAspectRatio="none"></svg>
          <div class="hero-stats">
            <div class="micro-stat"><div class="micro-label">Selected PnL</div><div class="micro-value" id="hero-realized">$0.00</div></div>
            <div class="micro-stat"><div class="micro-label">Closed</div><div class="micro-value" id="hero-marked">0</div></div>
            <div class="micro-stat"><div class="micro-label">Validation</div><div class="micro-value" id="hero-fillrate">blocked</div></div>
          </div>
        </div>
        <div class="hero-card">
          <div class="panel-kicker"><span>Strategy Stack</span><span id="live-gate-badge" class="badge">DRY</span></div>
          <div class="strategy-stack" id="strategy-stack"></div>
        </div>
        <div class="hero-card">
          <div class="panel-kicker"><span>Market/Execution Map</span><span id="graph-health" class="badge">LIVE</span></div>
          <canvas class="agent-canvas" id="agent-canvas"></canvas>
        </div>
      </div>
      <div class="ops-cycle" id="ops-cycle"></div>
      <div class="ops-tabs" role="tablist">
        <button class="ops-tab" type="button" data-tab="command" aria-selected="true">Command</button>
        <button class="ops-tab" type="button" data-tab="truth">Strategy Truth</button>
        <button class="ops-tab" type="button" data-tab="temporal">Temporal</button>
        <button class="ops-tab" type="button" data-tab="live">Live Maker</button>
        <button class="ops-tab" type="button" data-tab="markets">Markets</button>
        <div class="ops-controls">
          <label><input type="checkbox" id="ops-freeze"> freeze</label>
          <span id="ops-refresh-status">waiting</span>
        </div>
      </div>
      <div class="ops-body">
        <div class="ops-main">
          <div class="tab-pane active" data-pane="command">
            <div class="data-cards" id="command-cards"></div>
            <div class="cockpit-grid">
              <div class="ops-panel full"><h3>Quote Style Diagnostics</h3><div class="ops-panel-content" id="style-table"></div></div>
              <div class="ops-panel"><h3>Asset Heatmap</h3><div class="ops-panel-content" id="asset-heatmap"></div></div>
              <div class="ops-panel"><h3>Quote Scatter</h3><canvas class="scatter-canvas" id="scatter-canvas"></canvas></div>
            </div>
          </div>
          <div class="tab-pane" data-pane="truth">
            <div class="ops-panel full">
              <div class="panel-kicker"><span>Canonical Strategy Results</span><span>model / paper / rebates / wallet</span></div>
              <div class="ops-tabs" id="truth-timeframes">
                <button class="ops-tab" type="button" data-truth-period="all" aria-selected="true">All</button>
                <button class="ops-tab" type="button" data-truth-period="30d">30d</button>
                <button class="ops-tab" type="button" data-truth-period="7d">7d</button>
                <button class="ops-tab" type="button" data-truth-period="24h">24h</button>
              </div>
              <div class="ops-tabs" id="truth-views">
                <button class="ops-tab" type="button" data-truth-view="active" aria-selected="true">Active</button>
                <button class="ops-tab" type="button" data-truth-view="archive">Archive</button>
                <button class="ops-tab" type="button" data-truth-view="all">All</button>
              </div>
              <div class="ops-panel-content" id="strategy-truth-table"></div>
              <div class="ops-panel-content" id="promoted-basket-detail"></div>
            </div>
            <div class="ops-panel full" id="constraint-graph-panel"><h3>Related-Market Constraint Graph</h3><div class="ops-panel-content" id="constraint-graph-table"></div></div>
          </div>
          <div class="tab-pane" data-pane="temporal">
            <div class="data-cards" id="temporal-cards"></div>
            <div class="cockpit-grid">
              <div class="ops-panel full"><h3>Recent Lifecycle Markets</h3><div class="ops-panel-content" id="temporal-markets"></div></div>
              <div class="ops-panel full"><h3>Recent Simulated Maker Quotes</h3><div class="ops-panel-content" id="temporal-quotes"></div></div>
            </div>
          </div>
          <div class="tab-pane" data-pane="live">
            <div class="data-cards" id="live-cards"></div>
            <div class="ops-panel"><h3>Recent Live Maker Decisions</h3><div class="ops-panel-content" id="live-orders"></div></div>
          </div>
          <div class="tab-pane" data-pane="markets">
            <div class="data-cards" id="market-cards"></div>
            <div class="ops-panel"><h3>Tracked Market Books</h3><div class="ops-panel-content" id="market-table"></div></div>
          </div>
        </div>
        <aside class="ops-side">
          <div class="ops-panel"><h3>Recent Lifecycle Events</h3><div class="ops-panel-content"><div class="activity-feed" id="activity-feed"></div></div></div>
        </aside>
      </div>
    </div>
  </section>
  <script>
    window.__LATENCY_DASHBOARD_INITIAL__ = __INITIAL_STATE__;
    (function () {
      const initial = window.__LATENCY_DASHBOARD_INITIAL__ || {};
      let state = initial;
      let activeTab = "command";
      let truthTimeframe = "all";
      let truthView = "active";
      const qs = new URLSearchParams(window.location.search);
      const mode = qs.get("mode") || (initial.fast_mode ? "fast" : "full");
      const $ = (id) => document.getElementById(id);
      const n = (value) => {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : 0;
      };
      const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
      const money = (value) => `${n(value) < 0 ? "-" : ""}$${Math.abs(n(value)).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
      const maybeMoney = (value) => value === null || value === undefined ? "—" : money(value);
      const num = (value) => Math.round(n(value)).toLocaleString();
      const pct = (value, places = 1) => `${(100 * n(value)).toFixed(places)}%`;
      const px = (value) => n(value).toFixed(4);
      const posClass = (value) => n(value) > 0 ? "pos" : (n(value) < 0 ? "neg" : "");
      const setHTML = (id, html) => {
        const el = $(id);
        if (el) el.innerHTML = html;
      };
      const setText = (id, text) => {
        const el = $(id);
        if (el) el.textContent = text;
      };
      function table(headers, rows) {
        if (!rows.length) return `<div class="activity-meta">No data</div>`;
        return `<table class="mini-table"><thead><tr>${headers.map((h) => `<th>${esc(h)}</th>`).join("")}</tr></thead><tbody>${rows.map((row) => `<tr>${row.map((cell) => `<td>${cell}</td>`).join("")}</tr>`).join("")}</tbody></table>`;
      }
      function cards(items) {
        return items.map((item) => `<div class="data-card"><div class="label">${esc(item.label)}</div><div class="value ${item.className || ""}">${item.value}</div></div>`).join("");
      }
      function badgeClass(ok, warn) {
        if (ok) return "badge good";
        if (warn) return "badge warn";
        return "badge bad";
      }
      function phaseIndex(phase) {
        const normalized = String(phase || "").toLowerCase();
        if (normalized.includes("discovery") || normalized.includes("scan")) return 0;
        if (normalized.includes("fair") || normalized.includes("signal") || normalized.includes("detect")) return 1;
        if (normalized.includes("risk") || normalized.includes("validate")) return 2;
        if (normalized.includes("size") || normalized.includes("capital")) return 3;
        if (normalized.includes("execution") || normalized.includes("fill")) return 4;
        if (normalized.includes("settle") || normalized.includes("close")) return 5;
        return 1;
      }
      function promotedBasket(data) {
        return (data.strategy_truth || []).find((item) => item.strategy_id === "promoted:directional_basket:v1") || {};
      }
      function renderSparkline(points, bankroll) {
        const svg = $("temporal-spark");
        if (!svg) return;
        const values = (points || []).map((point) => n(point.equity_usdc || point.equity || bankroll)).filter((value) => Number.isFinite(value));
        const series = values.length ? [n(bankroll), ...values] : [n(bankroll), n(bankroll)];
        let min = Math.min(...series);
        let max = Math.max(...series);
        if (min === max) { min -= 1; max += 1; }
        const w = 420, h = 116, pad = 10;
        const pointFor = (value, index) => {
          const x = pad + (index / Math.max(series.length - 1, 1)) * (w - pad * 2);
          const y = h - pad - ((value - min) / (max - min)) * (h - pad * 2);
          return `${x.toFixed(1)},${y.toFixed(1)}`;
        };
        const path = series.map(pointFor).join(" ");
        const baseline = h - pad - ((n(bankroll) - min) / (max - min)) * (h - pad * 2);
        svg.innerHTML = `<line x1="0" y1="${baseline.toFixed(1)}" x2="${w}" y2="${baseline.toFixed(1)}" stroke="#cbd5e1" stroke-dasharray="4 4"/><polyline points="${path}" fill="none" stroke="#0f766e" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/><circle cx="${pointFor(series[series.length - 1], series.length - 1).split(",")[0]}" cy="${pointFor(series[series.length - 1], series.length - 1).split(",")[1]}" r="4" fill="#0f766e"/>`;
      }
      function assetAggregates(items) {
        const groups = new Map();
        (items || []).forEach((item) => {
          const asset = String(item.asset || "UNK").toUpperCase();
          const group = groups.get(asset) || { asset, count: 0, depth: 0, mid: 0, bestEdge: 0 };
          group.count += 1;
          group.depth += n(item.min_depth_usdc || item.depth_usdc || 0);
          group.mid += n(item.midpoint || item.mid || 0);
          group.bestEdge = Math.max(group.bestEdge, Math.abs(n(item.edge || 0)));
          groups.set(asset, group);
        });
        return Array.from(groups.values()).map((group) => ({ ...group, mid: group.count ? group.mid / group.count : 0 })).sort((a, b) => b.depth - a.depth);
      }
      function renderTape(data) {
        const temporal = data.temporal?.summary || {};
        const live = data.live_temporal?.summary || {};
        const basket = promotedBasket(data);
        const basketStats = basket.timeframes?.all || {};
        const counts = data.counts || {};
        const assets = assetAggregates(data.markets?.items || []).slice(0, 3);
        const items = [
          ["LIVE", `${data.status?.runner_status || "unknown"} / ${data.status?.phase || "-"}`],
          ["BASKET PNL", money(basketStats.net_pnl_usdc || 0), n(basketStats.net_pnl_usdc || 0)],
          ["TEMP PNL", money(temporal.marked_pnl_usdc || temporal.realized_pnl_usdc || 0), n(temporal.marked_pnl_usdc || temporal.realized_pnl_usdc || 0)],
          ["LIVE MAKER", `${live.pilot_mode || "dry_run"} / ${num(live.dry_run_24h || 0)} dry`, 0],
          ["BOOKS 60M", num(counts.polymarket_books || 0), 0],
          [assets[0]?.asset || "MARKETS", `${num(data.markets?.count || 0)} tracked`, 0],
        ];
        setHTML("ops-tape", items.map(([label, value, score]) => `<div class="tape-item"><span class="tape-label">${esc(label)}</span><span class="tape-value ${posClass(score)}">${esc(value)}</span></div>`).join(""));
      }
      function renderHero(data) {
        const temporal = data.temporal?.summary || {};
        const live = data.live_temporal?.summary || {};
        const basket = promotedBasket(data);
        const basketStats = basket.timeframes?.all || {};
        const truthRows = (data.strategy_truth || []).filter((item) => item.data_status === "loaded" && item.execution_tier === "executable_paper" && Boolean(item.enabled));
        const leader = basket.enabled ? basket : (truthRows.sort((a, b) => n(b.timeframes?.all?.net_pnl_usdc) - n(a.timeframes?.all?.net_pnl_usdc))[0] || {});
        const leaderStats = leader.timeframes?.all || {};
        setText("ops-clock", new Date().toLocaleTimeString([], { hour12: false }));
        setText("hero-equity", money(leaderStats.net_pnl_usdc || 0));
        setHTML("hero-realized", `<span class="${posClass(leaderStats.net_pnl_usdc)}">${money(leaderStats.net_pnl_usdc || 0)}</span>`);
        setText("hero-marked", num(leaderStats.closed || 0));
        setText("hero-fillrate", leader.data_status === "no_observations" ? "collecting" : (leader.validation?.status || "blocked"));
        setText("hero-subtitle", `${leader.label || "No validated strategy"} / ${leader.execution_tier || "no data"}`);
        const temporalBadge = $("temporal-state-badge");
        if (temporalBadge) {
          temporalBadge.className = badgeClass(Boolean(leader.validation?.passed), Boolean(leader.enabled));
          temporalBadge.textContent = leader.validation?.passed ? "PASSED" : (leader.data_status === "no_observations" ? "COLLECTING" : "PAPER");
        }
        const liveBadge = $("live-gate-badge");
        if (liveBadge) {
          const armed = Boolean(live.armed_for_live_orders);
          liveBadge.className = badgeClass(armed, live.pilot_mode === "dry_run");
          liveBadge.textContent = armed ? "ARMED" : (live.pilot_mode || "DRY");
        }
        renderSparkline([], leaderStats.net_pnl_usdc || 0);
        setHTML("strategy-stack", [
          ["Promoted 5m Basket", basket.data_status === "no_observations" ? "collecting" : "paper", `${num(basket.member_count || 0)} ETH/BTC variants / ${num(basketStats.closed || 0)} closes / ${money(basketStats.net_pnl_usdc || 0)}`, Boolean(basket.enabled)],
          ["Temporal Maker", temporal.enabled ? "enabled" : "disabled", `${money(temporal.realized_pnl_usdc || 0)} realized / ${pct(temporal.quote_fill_rate || 0)} fills`, temporal.enabled],
          ["Live Maker", live.pilot_mode || "dry_run", `${num(live.dry_run_24h || 0)} dry-run / ${num(live.submitted_24h || 0)} submitted`, Boolean(live.armed_for_live_orders)],
        ].map(([name, tag, meta, good]) => `<div class="strategy-row"><div><div class="strategy-name">${esc(name)}</div><div class="strategy-meta">${esc(meta)}</div></div><span class="${badgeClass(good, tag === "dry_run" || tag === "collecting")}">${esc(tag)}</span></div>`).join(""));
        const graphBadge = $("graph-health");
        if (graphBadge) {
          const stale = Boolean(data.status?.last_error);
          graphBadge.className = badgeClass(!stale, false);
          graphBadge.textContent = stale ? "ERROR" : "LIVE";
        }
      }
      function renderTruth(data) {
        const allRows = data.strategy_truth || [];
        const rows = allRows.filter((item) => {
          if (truthView === "active") return Boolean(item.enabled) && item.execution_tier === "executable_paper" && !item.parent_strategy_id;
          if (truthView === "archive") return !item.enabled || item.execution_tier !== "executable_paper";
          return true;
        });
        setHTML("strategy-truth-table", table(
          ["Strategy", "Tier", "Status", "Closed", "Win", "PnL", "Max DD", "95% EV", "Gate"],
          rows.map((item) => {
            const stats = item.timeframes?.[truthTimeframe] || {};
            return [
              esc(item.label || item.strategy_id || ""),
              esc(item.execution_tier || ""),
              esc(item.data_status === "not_loaded_fast" ? "not loaded in fast mode" : item.data_status || ""),
              num(stats.closed || 0),
              pct(stats.win_rate || 0),
              `<span class="${posClass(stats.net_pnl_usdc)}">${money(stats.net_pnl_usdc || 0)}</span>`,
              money(stats.max_drawdown_usdc || 0),
              money(stats.lower_95_trade_ev_usdc || 0),
              `<span class="${badgeClass(Boolean(item.validation?.passed), false)}" title="${esc((item.validation?.reasons || []).join('; '))}">${esc(item.validation?.status || "blocked")}</span>`,
            ];
          })
        ));
        const basketChildren = allRows.filter((item) => item.parent_strategy_id === "promoted:directional_basket:v1");
        setHTML("promoted-basket-detail", truthView === "archive" ? "" : `<details><summary><strong>Promoted basket variants (${num(basketChildren.length)})</strong> — fresh execution-realistic results</summary>${table(
          ["Variant", "Status", "Closed", "Win", "PnL", "Gate"],
          basketChildren.map((item) => {
            const stats = item.timeframes?.[truthTimeframe] || {};
            return [esc(item.label || ""), esc(item.data_status || ""), num(stats.closed || 0), pct(stats.win_rate || 0), `<span class="${posClass(stats.net_pnl_usdc)}">${money(stats.net_pnl_usdc || 0)}</span>`, esc(item.validation?.status || "blocked")];
          })
        )}</details>`);
        const graph = data.related_market_graph || {};
        const graphPanel = $("constraint-graph-panel");
        if (graphPanel) graphPanel.style.display = truthView === "active" ? "none" : "block";
        setHTML("constraint-graph-table", table(
          ["Group", "Legs", "Cost", "Edge", "Capacity", "Execution"],
          (graph.opportunities || []).map((item) => [
            esc(item.group_id || ""),
            num((item.legs || []).length),
            px(item.total_cost || 0),
            px(item.edge_per_share || 0),
            money(item.max_notional_usdc || 0),
            esc(item.requires_atomic_or_preowned_execution ? "atomic/pre-owned required" : "research"),
          ])
        ) + `<div class="activity-meta">${esc(graph.warning || "")}</div>`);
      }
      function renderCycle(data) {
        const steps = ["Scan", "Detect", "Validate", "Size", "Fill", "Settle"];
        const active = phaseIndex(data.status?.phase);
        setHTML("ops-cycle", steps.map((step, idx) => `<div class="cycle-step ${idx === active ? "active" : ""}"><span>0${idx + 1}</span><br>${esc(step)}</div>`).join(""));
      }
      function renderCommand(data) {
        const temporal = data.temporal?.summary || {};
        const live = data.live_temporal?.summary || {};
        const basket = promotedBasket(data);
        const basketStats = basket.timeframes?.all || {};
        setHTML("command-cards", cards([
          { label: "Promoted Basket", value: basket.data_status === "no_observations" ? "collecting" : money(basketStats.net_pnl_usdc || 0) },
          { label: "Basket Variants", value: num(basket.member_count || 0) },
          { label: "Basket Closes", value: num(basketStats.closed || 0) },
          { label: "Tracked", value: num(data.status?.tracked_markets_count || 0) },
          { label: "Temporal Active", value: num(temporal.active_inventory_markets || 0) },
          { label: "Open Quotes", value: num(temporal.quote_open || 0) },
          { label: "Live Submitted", value: num(live.submitted_24h || 0) },
          { label: "Capital Used", value: money(data.capital?.total_current_capital_usdc || 0) },
          { label: "Last Cycle", value: esc(data.generated_at_ct || "-") },
        ]));
        const styles = data.temporal?.quote_style_breakdown || [];
        setHTML("style-table", table(["Style", "Quotes", "Filled", "Open", "Fill", "Avg Edge", "Fill P", "EV"], styles.map((item) => [
          esc(item.quote_style || "-"),
          num(item.quotes || 0),
          num(item.filled || 0),
          num(item.open || 0),
          pct(n(item.filled || 0) / Math.max(n(item.quotes || 0), 1)),
          px(item.avg_edge || 0),
          n(item.avg_fill_probability || 0).toFixed(3),
          money(item.expected_value_usdc || 0),
        ])));
        renderHeatmap(data.markets?.items || []);
        drawScatter(data.temporal?.recent_quotes || []);
      }
      function renderHeatmap(items) {
        const groups = assetAggregates(items).slice(0, 10);
        const maxDepth = Math.max(...groups.map((group) => group.depth), 1);
        setHTML("asset-heatmap", `<div class="heatmap">${groups.map((group) => `<div class="heat-row"><strong>${esc(group.asset)}</strong><div class="heat-bar"><div class="heat-fill" style="width:${Math.max(3, 100 * group.depth / maxDepth).toFixed(1)}%"></div></div><span>${num(group.count)}</span></div>`).join("") || "No market cache"}</div>`);
      }
      function renderTemporal(data) {
        const temporal = data.temporal?.summary || {};
        setHTML("temporal-cards", cards([
          { label: "Equity", value: money(temporal.equity_usdc || 0) },
          { label: "Realized", value: money(temporal.realized_pnl_usdc || 0), className: posClass(temporal.realized_pnl_usdc) },
          { label: "Marked", value: money(temporal.marked_pnl_usdc || 0), className: posClass(temporal.marked_pnl_usdc) },
          { label: "Locked Pairs", value: n(temporal.locked_pair_shares || 0).toFixed(4) },
          { label: "Pair Cost", value: n(temporal.average_pair_cost || 0).toFixed(4) },
          { label: "Unpaired Exposure", value: money(temporal.unpaired_exposure_usdc || 0) },
          { label: "Adverse Loss", value: money(temporal.adverse_selection_loss_usdc || 0), className: "neg" },
          { label: "Max DD", value: money(temporal.max_drawdown || 0), className: "neg" },
          { label: "Worst Exit", value: esc(temporal.worst_exit_reason || "-") },
          { label: "Worst Exit PnL", value: money(temporal.worst_exit_reason_pnl_usdc || 0), className: "neg" },
        ]));
        setHTML("temporal-markets", table(["Updated", "Market", "Asset", "State", "YES", "NO", "Pair", "Realized"], (data.temporal?.markets || []).map((item) => [
          esc(shortTs(item.updated_ts)),
          esc(item.market_id || ""),
          esc(item.asset || ""),
          esc(item.state || ""),
          n(item.yes_shares || 0).toFixed(3),
          n(item.no_shares || 0).toFixed(3),
          n(item.locked_pair_cost || 0).toFixed(4),
          money(item.realized_pnl_usdc || 0),
        ])));
        setHTML("temporal-quotes", table(["Created", "Market", "Style", "Side", "Px", "Size", "Status", "Edge", "Fill P", "EV"], (data.temporal?.recent_quotes || []).map((item) => [
          esc(shortTs(item.ts_created)),
          esc(item.market_id || ""),
          esc(item.quote_style || ""),
          esc(item.side || ""),
          px(item.price || 0),
          n(item.size || 0).toFixed(3),
          esc(item.status || ""),
          px(item.edge || 0),
          n(item.fill_probability || 0).toFixed(3),
          money(item.expected_value_usdc || 0),
        ])));
      }
      function renderLive(data) {
        const live = data.live_temporal?.summary || {};
        setHTML("live-cards", cards([
          { label: "Pilot Mode", value: esc(live.pilot_mode || "dry_run") },
          { label: "Armed", value: esc(Boolean(live.armed_for_live_orders)) },
          { label: "Open Orders", value: num(live.open_orders || 0) },
          { label: "Open Notional", value: money(live.open_notional_usdc || 0) },
          { label: "Dry-Run 24h", value: num(live.dry_run_24h || 0) },
          { label: "Submitted 24h", value: num(live.submitted_24h || 0) },
          { label: "Blocked 24h", value: num(live.blocked_24h || 0) },
          { label: "Cancel-All", value: esc(Boolean(live.last_heartbeat_cancel_all_ok)) },
          { label: "Reward Scoring", value: num(live.reward_scoring_orders || 0) },
          { label: "Actual Rebates", value: money(live.actual_rebate_pnl_usdc || 0) },
        ]));
        setHTML("live-orders", table(["Created", "Market", "Side", "Px", "Size", "Status", "Decision", "Edge", "Reason"], (data.live_temporal?.recent_orders || []).map((item) => [
          esc(shortTs(item.ts_created)),
          esc(item.market_id || ""),
          esc(item.side || ""),
          px(item.price || 0),
          n(item.size || 0).toFixed(3),
          esc(item.status || ""),
          esc(item.decision || ""),
          px(item.edge || 0),
          esc(item.reason || item.error || ""),
        ])));
      }
      function renderMarkets(data) {
        const items = data.markets?.items || [];
        setHTML("market-cards", cards([
          { label: "Cache Rows", value: num(data.markets?.count || items.length) },
          { label: "Cache Source", value: esc(data.markets?.source || "-") },
          { label: "Binance Rows", value: num(data.binance?.count || 0) },
          { label: "Updated", value: esc(shortTs(data.markets?.updated_at)) },
        ]));
        setHTML("market-table", table(["Question", "Asset", "Hours", "Bid", "Ask", "Mid", "Depth", "Seen"], items.slice(0, 30).map((item) => [
          esc(item.question || item.title || ""),
          esc(item.asset || ""),
          n(item.hours_to_expiry || 0).toFixed(2),
          px(item.best_bid || item.bid || 0),
          px(item.best_ask || item.ask || 0),
          px(item.midpoint || item.mid || 0),
          money(item.min_depth_usdc || item.depth_usdc || 0),
          esc(shortTs(item.seen_at || item.updated_at)),
        ])));
      }
      function renderActivity(data) {
        const events = data.temporal?.recent_events || [];
        const orders = (data.live_temporal?.recent_orders || []).slice(0, 6).map((item) => ({ ...item, event_type: `LIVE_${item.status || item.decision || "ORDER"}`, ts: item.ts_created }));
        const feed = [...events, ...orders].slice(0, 32);
        setHTML("activity-feed", feed.map((item) => {
          const type = String(item.event_type || item.status || "EVENT").toLowerCase();
          const title = esc(item.event_type || item.status || "EVENT");
          const when = esc(shortTs(item.ts || item.ts_created));
          const meta = `${esc(item.market_id || "")} ${esc(item.side || "")} ${item.price ? `@ ${px(item.price)}` : ""} ${item.pnl_usdc ? ` / ${money(item.pnl_usdc)}` : ""}`;
          const reason = esc(item.reason || item.cancel_reason || item.decision || "");
          return `<div class="activity-item ${type}"><div class="activity-title"><span>${title}</span><span>${when}</span></div><div class="activity-meta">${meta}<br>${reason}</div></div>`;
        }).join("") || `<div class="activity-meta">No recent lifecycle events</div>`);
      }
      function shortTs(raw) {
        if (!raw) return "-";
        const parsed = new Date(raw);
        if (Number.isNaN(parsed.getTime())) return String(raw).slice(0, 19);
        return parsed.toLocaleTimeString([], { hour12: false });
      }
      function drawAgentGraph(data) {
        const canvas = $("agent-canvas");
        if (!canvas) return;
        const rect = canvas.getBoundingClientRect();
        const scale = window.devicePixelRatio || 1;
        canvas.width = Math.max(1, Math.floor(rect.width * scale));
        canvas.height = Math.max(1, Math.floor(rect.height * scale));
        const ctx = canvas.getContext("2d");
        ctx.setTransform(scale, 0, 0, scale, 0, 0);
        const w = rect.width, h = rect.height;
        ctx.clearRect(0, 0, w, h);
        ctx.fillStyle = "#fbfdff";
        ctx.fillRect(0, 0, w, h);
        const summary = data.temporal?.summary || {};
        const live = data.live_temporal?.summary || {};
        const counts = data.counts || {};
        const stages = [
          { name: "Books", value: n(counts.polymarket_books || 0), valueLabel: num(counts.polymarket_books || 0), detail: "book snapshots" },
          { name: "Signals", value: n(counts.signals || 0), valueLabel: num(counts.signals || 0), detail: "modeled signals" },
          { name: "Quotes", value: n(summary.quotes_60m || counts.temporal_inventory_quotes || 0), valueLabel: num(summary.quotes_60m || counts.temporal_inventory_quotes || 0), detail: "paper maker quotes" },
          { name: "Fills", value: n(summary.quote_fills_60m || 0), valueLabel: num(summary.quote_fills_60m || 0), detail: "simulated fills" },
          { name: "PnL", value: Math.max(0, Math.abs(n(summary.realized_pnl_24h_usdc || summary.marked_pnl_usdc || 0))), valueLabel: money(summary.marked_pnl_usdc || 0), detail: "marked PnL" },
        ];
        const maxFlow = Math.max(...stages.slice(0, 4).map((stage) => stage.value), 1);
        const y = Math.max(96, h * .48);
        const nodeW = Math.max(80, Math.min(122, w * .16));
        const nodeH = 62;
        const xs = stages.map((_, idx) => 22 + idx * ((w - 44 - nodeW) / Math.max(stages.length - 1, 1)));
        const flowWidth = (value) => Math.max(4, Math.min(30, 4 + 26 * n(value) / maxFlow));
        const roundedRect = (x, y, width, height, radius) => {
          const r = Math.min(radius, width / 2, height / 2);
          ctx.beginPath();
          ctx.moveTo(x + r, y);
          ctx.lineTo(x + width - r, y);
          ctx.quadraticCurveTo(x + width, y, x + width, y + r);
          ctx.lineTo(x + width, y + height - r);
          ctx.quadraticCurveTo(x + width, y + height, x + width - r, y + height);
          ctx.lineTo(x + r, y + height);
          ctx.quadraticCurveTo(x, y + height, x, y + height - r);
          ctx.lineTo(x, y + r);
          ctx.quadraticCurveTo(x, y, x + r, y);
          ctx.closePath();
        };
        const drawConnector = (fromIdx, toIdx, value, color) => {
          const x1 = xs[fromIdx] + nodeW;
          const x2 = xs[toIdx];
          const width = flowWidth(value);
          ctx.save();
          ctx.strokeStyle = color;
          ctx.lineWidth = width;
          ctx.lineCap = "round";
          ctx.globalAlpha = .32;
          ctx.beginPath();
          ctx.moveTo(x1, y);
          ctx.bezierCurveTo(x1 + (x2 - x1) * .45, y, x1 + (x2 - x1) * .55, y, x2, y);
          ctx.stroke();
          ctx.restore();
          ctx.save();
          ctx.strokeStyle = color;
          ctx.lineWidth = 1.2;
          ctx.globalAlpha = .75;
          ctx.beginPath();
          ctx.moveTo(x1, y);
          ctx.bezierCurveTo(x1 + (x2 - x1) * .45, y, x1 + (x2 - x1) * .55, y, x2, y);
          ctx.stroke();
          ctx.restore();
        };
        drawConnector(0, 1, Math.min(stages[0].value, stages[1].value), "#0891b2");
        drawConnector(1, 2, Math.min(stages[1].value, stages[2].value), "#0891b2");
        drawConnector(2, 3, Math.min(stages[2].value, stages[3].value), "#10b981");
        drawConnector(3, 4, stages[3].value || stages[4].value, n(summary.marked_pnl_usdc || 0) >= 0 ? "#10b981" : "#dc2626");
        const cancelled = n(summary.quote_cancelled || 0);
        const liveBlocked = n(live.blocked_24h || 0);
        const branchY = Math.min(h - 56, y + 70);
        if (cancelled > 0 || liveBlocked > 0) {
          const branchX = xs[2] + nodeW * .5;
          ctx.save();
          ctx.strokeStyle = "#d97706";
          ctx.lineWidth = Math.min(18, Math.max(3, flowWidth(cancelled + liveBlocked) * .65));
          ctx.lineCap = "round";
          ctx.globalAlpha = .25;
          ctx.beginPath();
          ctx.moveTo(branchX, y + nodeH * .5);
          ctx.bezierCurveTo(branchX + 18, branchY - 42, xs[3] - 22, branchY - 18, xs[3], branchY);
          ctx.stroke();
          ctx.restore();
          ctx.fillStyle = "#d97706";
          ctx.beginPath();
          ctx.arc(xs[3], branchY, 6, 0, Math.PI * 2);
          ctx.fill();
          ctx.fillStyle = "#475569";
          ctx.font = "700 10px ui-sans-serif, system-ui";
          ctx.textAlign = "center";
          ctx.fillText(`${num(cancelled + liveBlocked)} stopped`, xs[3], branchY + 22);
        }
        stages.forEach((stage, idx) => {
          const x = xs[idx];
          const top = y - nodeH / 2;
          const fill = idx === 4 ? (n(summary.marked_pnl_usdc || 0) >= 0 ? "#ecfdf5" : "#fef2f2") : "#ffffff";
          const stroke = idx === 3 ? "#10b981" : (idx === 4 ? (n(summary.marked_pnl_usdc || 0) >= 0 ? "#10b981" : "#dc2626") : "#cbd5e1");
          roundedRect(x, top, nodeW, nodeH, 8);
          ctx.fillStyle = fill;
          ctx.fill();
          ctx.strokeStyle = stroke;
          ctx.lineWidth = 1.4;
          ctx.stroke();
          ctx.fillStyle = idx === 4 ? (n(summary.marked_pnl_usdc || 0) >= 0 ? "#079669" : "#dc2626") : "#0f172a";
          ctx.font = "850 18px ui-sans-serif, system-ui";
          ctx.textAlign = "center";
          ctx.fillText(stage.valueLabel, x + nodeW / 2, top + 26);
          ctx.fillStyle = "#64748b";
          ctx.font = "750 10px ui-sans-serif, system-ui";
          ctx.fillText(stage.detail, x + nodeW / 2, top + 45);
          ctx.fillStyle = "#475569";
          ctx.font = "850 12px ui-sans-serif, system-ui";
          ctx.fillText(stage.name, x + nodeW / 2, h - 14);
        });
        ctx.fillStyle = "#64748b";
        ctx.font = "700 11px ui-sans-serif, system-ui";
        ctx.textAlign = "left";
        ctx.fillText("60m stage flow; PnL node uses current temporal marked PnL", 18, 22);
      }
      function drawScatter(quotes) {
        const canvas = $("scatter-canvas");
        if (!canvas) return;
        const rect = canvas.getBoundingClientRect();
        const scale = window.devicePixelRatio || 1;
        canvas.width = Math.max(1, Math.floor(rect.width * scale));
        canvas.height = Math.max(1, Math.floor(rect.height * scale));
        const ctx = canvas.getContext("2d");
        ctx.setTransform(scale, 0, 0, scale, 0, 0);
        const w = rect.width, h = rect.height;
        ctx.clearRect(0, 0, w, h);
        ctx.fillStyle = "#fbfdff";
        ctx.fillRect(0, 0, w, h);
        ctx.strokeStyle = "#e2e8f0";
        ctx.lineWidth = 1;
        for (let i = 1; i < 5; i += 1) {
          const x = (w / 5) * i;
          const y = (h / 5) * i;
          ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, h); ctx.stroke();
          ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w, y); ctx.stroke();
        }
        const maxEdge = Math.max(...(quotes || []).map((q) => Math.abs(n(q.edge || 0))), .01);
        (quotes || []).forEach((quote, idx) => {
          const x = 22 + (Math.abs(n(quote.edge || 0)) / maxEdge) * (w - 44);
          const y = h - 22 - n(quote.fill_probability || 0) * (h - 44);
          const status = String(quote.status || "").toUpperCase();
          ctx.fillStyle = status === "FILLED" ? "#10b981" : (status === "OPEN" ? "#0891b2" : "#d97706");
          ctx.beginPath();
          ctx.arc(x, y, 4 + (idx % 3), 0, Math.PI * 2);
          ctx.fill();
        });
        ctx.fillStyle = "#64748b";
        ctx.font = "700 11px ui-sans-serif, system-ui";
        ctx.fillText("edge ->", 12, h - 8);
        ctx.save(); ctx.translate(10, h - 18); ctx.rotate(-Math.PI / 2); ctx.fillText("fill probability ->", 0, 0); ctx.restore();
      }
      function render(data) {
        state = data || state;
        renderTape(state);
        renderHero(state);
        renderCycle(state);
        renderCommand(state);
        renderTruth(state);
        renderTemporal(state);
        renderLive(state);
        renderMarkets(state);
        renderActivity(state);
        drawAgentGraph(state);
      }
      async function refresh() {
        if ($("ops-freeze")?.checked) return;
        try {
          setText("ops-refresh-status", "refreshing");
          const response = await fetch(`/api/state?mode=${encodeURIComponent(mode)}`, { cache: "no-store" });
          if (!response.ok) throw new Error(`HTTP ${response.status}`);
          const data = await response.json();
          render(data);
          setText("ops-refresh-status", `updated ${data.generated_at_ct || ""}`);
        } catch (error) {
          setText("ops-refresh-status", `refresh failed: ${error.message || error}`);
        }
      }
      document.querySelectorAll(".ops-tab").forEach((button) => {
        if (button.dataset.truthPeriod || button.dataset.truthView) return;
        button.addEventListener("click", () => {
          activeTab = button.dataset.tab || "command";
          document.querySelectorAll("[data-tab]").forEach((tab) => tab.setAttribute("aria-selected", String(tab === button)));
          document.querySelectorAll(".tab-pane").forEach((pane) => pane.classList.toggle("active", pane.dataset.pane === activeTab));
          drawAgentGraph(state);
          drawScatter(state.temporal?.recent_quotes || []);
        });
      });
      document.querySelectorAll("[data-truth-period]").forEach((button) => {
        button.addEventListener("click", () => {
          truthTimeframe = button.dataset.truthPeriod || "all";
          document.querySelectorAll("[data-truth-period]").forEach((item) => item.setAttribute("aria-selected", String(item === button)));
          renderTruth(state);
        });
      });
      document.querySelectorAll("[data-truth-view]").forEach((button) => {
        button.addEventListener("click", () => {
          truthView = button.dataset.truthView || "active";
          document.querySelectorAll("[data-truth-view]").forEach((item) => item.setAttribute("aria-selected", String(item === button)));
          renderTruth(state);
        });
      });
      window.addEventListener("resize", () => {
        drawAgentGraph(state);
        drawScatter(state.temporal?.recent_quotes || []);
      });
      render(initial);
      setInterval(() => setText("ops-clock", new Date().toLocaleTimeString([], { hour12: false })), 1000);
      setInterval(refresh, 3000);
    })();
  </script>
"""
    return template.replace("__INITIAL_STATE__", payload_json)


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


def _summarize_latency_bot_db_fast(settings: LatencyBotSettings, minutes: int = 60) -> dict[str, Any]:
    if not settings.db_path.exists():
        return {
            "db_present": False,
            "db_path": str(settings.db_path),
            "lookback_minutes": minutes,
        }
    since = (datetime.now(timezone.utc) - timedelta(minutes=max(minutes, 1))).isoformat().replace("+00:00", "Z")
    recent_counts: dict[str, int] = {}
    with connect_latency_bot_db(settings) as conn:
        def count_recent(table: str, ts_column: str = "ts") -> int:
            try:
                row = conn.execute(
                    f"SELECT COUNT(*) AS count FROM {table} WHERE {ts_column} >= ?",
                    (since,),
                ).fetchone()
                return int(row["count"]) if row else 0
            except Exception:
                return 0

        for key, table in (
            ("binance_ticks", "binance_ticks"),
            ("polymarket_books", "polymarket_books"),
            ("fair_values", "fair_values"),
            ("signals", "signals"),
            ("shadow_signals", "shadow_signals"),
            ("shadow_variant_signals", "shadow_variant_signals"),
            ("cex_latency_paper_signals", "cex_latency_paper_signals"),
            ("temporal_inventory_events", "temporal_inventory_events"),
            ("late_resolution_capture_signals", "late_resolution_capture_signals"),
            ("complete_set_arb_signals", "complete_set_arb_signals"),
            ("live_complete_set_arb_pilot_attempts", "live_complete_set_arb_pilot_attempts"),
            ("polymarket_us_arb_ticks", "polymarket_us_arb_ticks"),
            ("kalshi_arb_ticks", "kalshi_arb_ticks"),
            ("fills", "fills"),
            ("missed_opportunities", "missed_opportunities"),
            ("engine_cycles", "engine_cycles"),
        ):
            recent_counts[key] = count_recent(table)
        recent_counts["temporal_inventory_quotes"] = count_recent("temporal_inventory_quotes", "ts_created")
        recent_counts["live_temporal_inventory_maker_orders"] = count_recent("live_temporal_inventory_maker_orders", "ts_created")
        try:
            close_row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM complete_set_arb_events
                WHERE ts >= ? AND event_type = 'close'
                """,
                (since,),
            ).fetchone()
            recent_counts["complete_set_arb_closes"] = int(close_row["count"]) if close_row else 0
        except Exception:
            recent_counts["complete_set_arb_closes"] = 0
        latest_cycle = conn.execute(
            """
            SELECT ts, phase, markets_tracked, signals_seen, orders_open, positions_open, risk_state, notes
            FROM engine_cycles
            ORDER BY ts DESC
            LIMIT 1
            """
        ).fetchone()
    return {
        "db_present": True,
        "db_path": str(settings.db_path),
        "lookback_minutes": minutes,
        "recent_counts": recent_counts,
        "totals": {},
        "latest_cycle": dict(latest_cycle) if latest_cycle else {},
    }


def _cached_dashboard_state(settings: LatencyBotSettings, *, fast: bool) -> dict[str, Any] | None:
    key = f"{settings.db_path.resolve()}:{'fast' if fast else 'full'}"
    now = time.monotonic()
    with _DASHBOARD_STATE_CACHE_LOCK:
        if (
            _DASHBOARD_STATE_CACHE.get("key") == key
            and _DASHBOARD_STATE_CACHE.get("payload") is not None
            and now - float(_DASHBOARD_STATE_CACHE.get("loaded_at") or 0.0) < _DASHBOARD_STATE_CACHE_TTL_SECONDS
        ):
            payload = _DASHBOARD_STATE_CACHE["payload"]
            return dict(payload) if isinstance(payload, dict) else None
    return None


def _store_dashboard_state_cache(settings: LatencyBotSettings, *, fast: bool, state: dict[str, Any]) -> None:
    key = f"{settings.db_path.resolve()}:{'fast' if fast else 'full'}"
    with _DASHBOARD_STATE_CACHE_LOCK:
        _DASHBOARD_STATE_CACHE.update({"key": key, "loaded_at": time.monotonic(), "payload": dict(state)})


def build_latency_bot_dashboard_state(settings: LatencyBotSettings, *, fast: bool = False) -> dict[str, Any]:
    cached = _cached_dashboard_state(settings, fast=fast)
    if cached is not None:
        cached["served_from_cache"] = True
        return cached
    init_latency_bot_db(settings)
    status = _load_json(settings.status_path)
    run_metadata = _load_json(settings.db_path.parent / "latency_bot_run.json")
    markets = _load_json(settings.markets_path)
    polymarket_cache = _load_json(settings.polymarket_cache_path)
    binance_cache = _load_json(settings.binance_cache_path)
    db_summary = _summarize_latency_bot_db_fast(settings, 60) if fast else summarize_latency_bot_db(settings, 60)
    since_ts = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat().replace("+00:00", "Z")
    recent_signals = latest_rows_since(settings, "signals", since_ts=since_ts, limit=12)
    recent_fair_values = latest_rows_since(settings, "fair_values", since_ts=since_ts, limit=12)
    recent_fills = latest_rows_since(settings, "fills", since_ts=since_ts, limit=12)
    recent_orders = latest_rows_since(settings, "orders", since_ts=since_ts, limit=12)
    open_positions = load_open_positions(settings)
    recent_shadow_signals = [] if fast else latest_shadow_rows_since(settings, "shadow_signals", since_ts=since_ts, limit=12)
    recent_shadow_events = [] if fast else latest_shadow_rows_since(settings, "shadow_position_events", since_ts=since_ts, limit=12)
    open_shadow_positions = [] if fast else load_shadow_open_positions(settings)
    portfolio = latency_bot_portfolio_summary(
        settings,
        cache_items=polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else [],
    )
    strategy_truth = build_strategy_truth_rows(settings)
    related_market_graph = build_related_market_constraint_graph(
        settings,
        markets_payload=markets,
        polymarket_cache=polymarket_cache,
    )
    promoted_variant_stats = (
        {"data_status": "not_loaded_fast", "summary": {"data_status": "not_loaded_fast"}, "variants": []}
        if fast
        else latency_bot_promoted_variant_performance_stats(
            settings,
            cache_items=polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else [],
        )
    )
    signal_stats = {} if fast else latency_bot_signal_feature_stats(settings)
    enabled_signal_stats = {} if fast else latency_bot_signal_feature_stats(settings, live_enabled_only=True)
    shadow_signal_stats = {} if fast else latency_bot_signal_feature_stats(settings, shadow=True)
    live_opportunity = {} if fast else latency_bot_opportunity_stats(settings, shadow=False, minutes=15, live_enabled_only=True)
    shadow_opportunity = {} if fast else latency_bot_opportunity_stats(settings, shadow=True, minutes=15)
    threshold_relaxation = {} if fast else latency_bot_threshold_relaxation_stats(settings)
    shadow_yes_variant_stats = {} if fast else _cached_shadow_variant_performance_stats(settings)
    live_stats = {} if fast else latency_bot_performance_stats(settings)
    shadow_portfolio = (
        {}
        if fast
        else latency_bot_shadow_portfolio_summary(
            settings,
            cache_items=polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else [],
        )
    )
    equity_curve = [] if fast else latency_bot_equity_curve(settings)
    live_strategy_equity_curves = [] if fast else latency_bot_live_strategy_equity_curves(settings)
    shadow_stats = {} if fast else latency_bot_shadow_performance_stats(settings)
    complete_set_arb_stats = {} if fast else latency_bot_complete_set_arb_stats(settings)
    cex_latency_paper = {"data_status": "not_loaded_fast", "summary": {"data_status": "not_loaded_fast"}} if fast else latency_bot_cex_latency_paper_stats(settings)
    btc_fair_value_paper = {"data_status": "not_loaded_fast", "summary": {"data_status": "not_loaded_fast"}} if fast else latency_bot_btc_fair_value_paper_stats(settings)
    temporal_inventory_maker_paper = latency_bot_temporal_inventory_maker_paper_stats(settings)
    live_temporal_inventory_maker = latency_bot_live_temporal_inventory_maker_stats(settings)
    late_resolution_capture_paper = latency_bot_late_resolution_capture_paper_stats(settings)
    wallet_teacher_sniper = {} if fast else latency_bot_wallet_teacher_sniper_stats(settings)
    realistic_complete_set_arb = {} if fast else latency_bot_realistic_complete_set_arb_sim(settings)
    preowned_inventory_arb = {} if fast else latency_bot_preowned_inventory_arb_sim(settings)
    realistic_complete_set_arb_all_time = (
        {}
        if fast
        else latency_bot_realistic_complete_set_arb_sim(
            replace(settings, realistic_complete_set_arb_lookback_hours=24 * 365 * 20)
        )
    )
    live_complete_set_arb_pilot = {} if fast else latency_bot_live_complete_set_arb_pilot_stats(settings)
    polymarket_account_reconciliation = (
        {} if fast else latency_bot_polymarket_account_reconciliation(settings)
    )
    polymarket_us_arb = {} if fast else latency_bot_polymarket_us_arb_sim(settings)
    kalshi_arb = {} if fast else latency_bot_kalshi_arb_sim(settings)
    capital_usage = latency_bot_capital_usage(settings)
    state = {
        "bankroll_usdc": settings.bankroll_usdc,
        "fast_mode": bool(fast),
        "served_from_cache": False,
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
        "strategy_truth": strategy_truth,
        "related_market_graph": related_market_graph,
        "data_availability": {
            "promoted_variant_stats": "not_loaded_fast" if fast else "loaded",
            "cex_latency_paper": "not_loaded_fast" if fast else "loaded",
            "btc_fair_value_paper": "not_loaded_fast" if fast else "loaded",
            "shadow_research": "not_loaded_fast" if fast else "loaded",
            "complete_set_research": "not_loaded_fast" if fast else "loaded",
            "wallet_reconciliation": "not_loaded_fast" if fast else "loaded",
        },
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
        "cex_latency_paper": cex_latency_paper,
        "btc_fair_value_paper": btc_fair_value_paper,
        "temporal_inventory_maker_paper": temporal_inventory_maker_paper,
        "live_temporal_inventory_maker": live_temporal_inventory_maker,
        "late_resolution_capture_paper": late_resolution_capture_paper,
        "wallet_teacher_sniper": wallet_teacher_sniper,
        "realistic_complete_set_arb": realistic_complete_set_arb,
        "realistic_complete_set_arb_all_time": realistic_complete_set_arb_all_time,
        "preowned_inventory_arb": preowned_inventory_arb,
        "live_complete_set_arb_pilot": live_complete_set_arb_pilot,
        "polymarket_account_reconciliation": polymarket_account_reconciliation,
        "polymarket_us_arb": polymarket_us_arb,
        "kalshi_arb": kalshi_arb,
        "capital_usage": capital_usage,
    }
    _store_dashboard_state_cache(settings, fast=fast, state=state)
    return state


def _render_latency_bot_fast_html(state: dict[str, Any]) -> str:
    status = state.get("status", {}) if isinstance(state.get("status"), dict) else {}
    page_generated_at = datetime.now(DISPLAY_TZ).strftime("%Y-%m-%d %H:%M:%S CT")
    last_cycle_completed = _fmt_ts(status.get("last_cycle_completed_at"))
    cockpit = _render_interactive_cockpit(state)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Polymarket Latency Ops</title>
  <style>
    body {{ margin: 0; background: #eef3f8; color: #0f172a; font-family: ui-sans-serif, system-ui, sans-serif; }}
    .fast-header {{ max-width: 1440px; margin: 16px auto 0; padding: 0 16px; display: flex; gap: 12px; justify-content: space-between; align-items: center; color: #64748b; font-size: 12px; }}
    .fast-header a {{ color: #0f766e; font-weight: 800; text-decoration: none; }}
  </style>
</head>
<body>
  <div class="fast-header">
    <span>Updated {html.escape(page_generated_at)} · last cycle {html.escape(last_cycle_completed)}</span>
    <a href="?mode=full">Open archived research and legacy bots</a>
  </div>
  {cockpit}
</body>
</html>"""


def render_latency_bot_dashboard_html(state: dict[str, Any]) -> str:
    if bool(state.get("fast_mode")):
        return _render_latency_bot_fast_html(state)
    page_generated_at = datetime.now(DISPLAY_TZ).strftime("%Y-%m-%d %H:%M:%S CT")
    fast_mode = bool(state.get("fast_mode"))
    served_from_cache = bool(state.get("served_from_cache"))
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
    cex_latency_paper = state.get("cex_latency_paper", {}) if isinstance(state.get("cex_latency_paper"), dict) else {}
    btc_fair_value_paper = state.get("btc_fair_value_paper", {}) if isinstance(state.get("btc_fair_value_paper"), dict) else {}
    temporal_inventory_maker_paper = (
        state.get("temporal_inventory_maker_paper", {})
        if isinstance(state.get("temporal_inventory_maker_paper"), dict)
        else {}
    )
    live_temporal_inventory_maker = (
        state.get("live_temporal_inventory_maker", {})
        if isinstance(state.get("live_temporal_inventory_maker"), dict)
        else {}
    )
    late_resolution_capture_paper = (
        state.get("late_resolution_capture_paper", {})
        if isinstance(state.get("late_resolution_capture_paper"), dict)
        else {}
    )
    wallet_teacher_sniper = state.get("wallet_teacher_sniper", {}) if isinstance(state.get("wallet_teacher_sniper"), dict) else {}
    realistic_complete_set_arb = state.get("realistic_complete_set_arb", {}) if isinstance(state.get("realistic_complete_set_arb"), dict) else {}
    realistic_complete_set_arb_all_time = (
        state.get("realistic_complete_set_arb_all_time", {})
        if isinstance(state.get("realistic_complete_set_arb_all_time"), dict)
        else {}
    )
    preowned_inventory_arb = state.get("preowned_inventory_arb", {}) if isinstance(state.get("preowned_inventory_arb"), dict) else {}
    polymarket_account_reconciliation = (
        state.get("polymarket_account_reconciliation", {})
        if isinstance(state.get("polymarket_account_reconciliation"), dict)
        else {}
    )
    polymarket_us_arb = state.get("polymarket_us_arb", {}) if isinstance(state.get("polymarket_us_arb"), dict) else {}
    kalshi_arb = state.get("kalshi_arb", {}) if isinstance(state.get("kalshi_arb"), dict) else {}
    capital_usage = state.get("capital_usage", {}) if isinstance(state.get("capital_usage"), dict) else {}
    cex_latency_summary = cex_latency_paper.get("summary", {}) if isinstance(cex_latency_paper.get("summary"), dict) else {}
    btc_fair_value_summary = btc_fair_value_paper.get("summary", {}) if isinstance(btc_fair_value_paper.get("summary"), dict) else {}
    temporal_inventory_summary = (
        temporal_inventory_maker_paper.get("summary", {})
        if isinstance(temporal_inventory_maker_paper.get("summary"), dict)
        else {}
    )
    live_temporal_summary = (
        live_temporal_inventory_maker.get("summary", {})
        if isinstance(live_temporal_inventory_maker.get("summary"), dict)
        else {}
    )
    late_resolution_summary = (
        late_resolution_capture_paper.get("summary", {})
        if isinstance(late_resolution_capture_paper.get("summary"), dict)
        else {}
    )
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
        ["Legacy Shared Paper Realized PnL", html.escape(_fmt_money(portfolio.get("realized_pnl_usdc", status.get("realized_pnl_usdc", 0.0))))],
        ["Legacy Shared 24h PnL", html.escape(_fmt_money(portfolio.get("realized_usdc_per_day", 0.0)))],
        ["Legacy 24h x 30 Projection", html.escape(_fmt_money(portfolio.get("projected_monthly_revenue_usdc", 0.0)))],
        ["Legacy 24h x 365 Projection", html.escape(_fmt_money(portfolio.get("projected_yearly_revenue_usdc", 0.0)))],
        ["Legacy Shared Unrealized PnL", html.escape(_fmt_money(portfolio.get("unrealized_pnl_usdc", status.get("unrealized_pnl_usdc", 0.0))))],
        ["Legacy Shared Paper Equity", html.escape(_fmt_money(portfolio.get("equity_usdc", bankroll_usdc)))],
        ["CEX Paper Model", html.escape(str(cex_latency_summary.get("model") or "-"))],
        ["CEX Paper Equity", html.escape(_fmt_money(cex_latency_summary.get("equity_usdc", 0.0)))],
        ["CEX Paper Net PnL", html.escape(_fmt_money(cex_latency_summary.get("net_pnl", 0.0)))],
        [
            "CEX Paper Open / Closed",
            html.escape(
                f"{_fmt_num(cex_latency_summary.get('open', 0))} / {_fmt_num(cex_latency_summary.get('closed', 0))}"
            ),
        ],
        [
            "CEX Paper Eligible 60m",
            html.escape(
                f"{_fmt_num(cex_latency_summary.get('eligible_60m', 0))} / {_fmt_num(cex_latency_summary.get('signals_60m', 0))}"
            ),
        ],
        ["BTC Fair Paper Equity", html.escape(_fmt_money(btc_fair_value_summary.get("equity_usdc", 0.0)))],
        ["BTC Fair Paper Net PnL", html.escape(_fmt_money(btc_fair_value_summary.get("net_pnl", 0.0)))],
        [
            "BTC Fair Paper Open / Closed",
            html.escape(
                f"{_fmt_num(btc_fair_value_summary.get('open', 0))} / {_fmt_num(btc_fair_value_summary.get('closed', 0))}"
            ),
        ],
        [
            "BTC Fair Paper Eligible 60m",
            html.escape(
                f"{_fmt_num(btc_fair_value_summary.get('eligible_60m', 0))} / {_fmt_num(btc_fair_value_summary.get('signals_60m', 0))}"
            ),
        ],
        ["Temporal Inventory Equity", html.escape(_fmt_money(temporal_inventory_summary.get("equity_usdc", 0.0)))],
        ["Temporal Inventory Marked PnL", html.escape(_fmt_money(temporal_inventory_summary.get("marked_pnl_usdc", 0.0)))],
        [
            "Temporal Inventory Open / Quotes",
            html.escape(
                f"{_fmt_num(temporal_inventory_summary.get('open_markets', 0))} / {_fmt_num(temporal_inventory_summary.get('quote_count', 0))}"
            ),
        ],
        ["Temporal Quote Fill Rate", html.escape(f"{100.0 * float(temporal_inventory_summary.get('quote_fill_rate', 0.0)):.1f}%")],
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
        ["Eligible Shadow Variant Signals (60m)", html.escape(_fmt_num(recent_counts.get("shadow_variant_signals", 0)))],
        ["CEX Latency Paper Signals (60m)", html.escape(_fmt_num(recent_counts.get("cex_latency_paper_signals", 0)))],
        ["Temporal Inventory Events (60m)", html.escape(_fmt_num(recent_counts.get("temporal_inventory_events", 0)))],
        ["Temporal Inventory Quotes (60m)", html.escape(_fmt_num(recent_counts.get("temporal_inventory_quotes", 0)))],
        ["Live Temporal Maker Orders (60m)", html.escape(_fmt_num(recent_counts.get("live_temporal_inventory_maker_orders", 0)))],
        ["Late Resolution Signals (60m)", html.escape(_fmt_num(recent_counts.get("late_resolution_capture_signals", 0)))],
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
    if promoted_summary.get("data_status") == "not_loaded_fast":
        promoted_summary_rows = [["Data", "Not loaded in fast mode; use the Strategy Truth tab or ?mode=full"]]
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
    cex_latency_summary = cex_latency_paper.get("summary", {}) if isinstance(cex_latency_paper.get("summary"), dict) else {}
    cex_latency_execution_result = last_cycle_result.get("cex_latency_paper", {}) if isinstance(last_cycle_result.get("cex_latency_paper"), dict) else {}
    cex_latency_execution = cex_latency_execution_result.get("execution", {}) if isinstance(cex_latency_execution_result.get("execution"), dict) else {}
    btc_fair_value_execution_result = last_cycle_result.get("btc_fair_value_paper", {}) if isinstance(last_cycle_result.get("btc_fair_value_paper"), dict) else {}
    btc_fair_value_execution = btc_fair_value_execution_result.get("execution", {}) if isinstance(btc_fair_value_execution_result.get("execution"), dict) else {}
    temporal_inventory_execution_result = (
        last_cycle_result.get("temporal_inventory_maker_paper", {})
        if isinstance(last_cycle_result.get("temporal_inventory_maker_paper"), dict)
        else {}
    )
    temporal_inventory_execution = (
        temporal_inventory_execution_result.get("execution", {})
        if isinstance(temporal_inventory_execution_result.get("execution"), dict)
        else {}
    )
    temporal_inventory_summary_rows = [
        ["Mode", html.escape(str(temporal_inventory_summary.get("mode", "temporal_inventory_maker_paper")))],
        ["Enabled", html.escape(str(bool(temporal_inventory_summary.get("enabled"))).lower())],
        ["Starting Capital", html.escape(_fmt_money(temporal_inventory_summary.get("starting_capital_usdc", 0.0)))],
        ["Equity", html.escape(_fmt_money(temporal_inventory_summary.get("equity_usdc", 0.0)))],
        ["Realized PnL", html.escape(_fmt_money(temporal_inventory_summary.get("realized_pnl_usdc", 0.0)))],
        ["Marked PnL", html.escape(_fmt_money(temporal_inventory_summary.get("marked_pnl_usdc", 0.0)))],
        ["Locked-Pair PnL", html.escape(_fmt_money(temporal_inventory_summary.get("locked_pair_pnl_usdc", 0.0)))],
        ["Unpaired Marked PnL", html.escape(_fmt_money(temporal_inventory_summary.get("unpaired_marked_pnl_usdc", 0.0)))],
        ["Open Exposure", html.escape(_fmt_money(temporal_inventory_summary.get("open_exposure_usdc", 0.0)))],
        ["Unpaired Exposure", html.escape(_fmt_money(temporal_inventory_summary.get("unpaired_exposure_usdc", 0.0)))],
        ["Locked Pairs", html.escape(f"{float(temporal_inventory_summary.get('locked_pair_shares', 0.0)):.4f}")],
        ["Average Pair Cost", html.escape(f"{float(temporal_inventory_summary.get('average_pair_cost', 0.0)):.4f}")],
        ["Expired Inventory Cost", html.escape(_fmt_money(temporal_inventory_summary.get("expired_inventory_cost_usdc", 0.0)))],
        ["Quote Fill Rate", html.escape(f"{100.0 * float(temporal_inventory_summary.get('quote_fill_rate', 0.0)):.1f}%")],
        ["Quotes Filled / Total", html.escape(f"{_fmt_num(temporal_inventory_summary.get('quote_filled', 0))} / {_fmt_num(temporal_inventory_summary.get('quote_count', 0))}")],
        ["Quotes Open / Cancelled", html.escape(f"{_fmt_num(temporal_inventory_summary.get('quote_open', 0))} / {_fmt_num(temporal_inventory_summary.get('quote_cancelled', 0))}")],
        ["Adverse-Selection Loss", html.escape(_fmt_money(temporal_inventory_summary.get("adverse_selection_loss_usdc", 0.0)))],
        ["Win Rate", html.escape(f"{100.0 * float(temporal_inventory_summary.get('win_rate', 0.0)):.1f}%")],
        ["Max Drawdown", html.escape(_fmt_money(temporal_inventory_summary.get("max_drawdown", 0.0)))],
        ["24h Realized PnL", html.escape(_fmt_money(temporal_inventory_summary.get("realized_pnl_24h_usdc", 0.0)))],
        ["Projected Monthly Revenue", html.escape(_fmt_money(temporal_inventory_summary.get("projected_monthly_revenue_usdc", 0.0)))],
        ["Projected Yearly Revenue", html.escape(_fmt_money(temporal_inventory_summary.get("projected_yearly_revenue_usdc", 0.0)))],
        ["Active Inventory Markets", html.escape(_fmt_num(temporal_inventory_summary.get("active_inventory_markets", 0)))],
        ["Tracked Lifecycle Markets", html.escape(_fmt_num(temporal_inventory_summary.get("tracked_markets", temporal_inventory_summary.get("open_markets", 0))))],
        ["Closed Markets", html.escape(_fmt_num(temporal_inventory_summary.get("closed_markets", 0)))],
        ["Base Order", html.escape(_fmt_money(temporal_inventory_summary.get("base_order_usdc", 0.0)))],
        ["Max Market Exposure", html.escape(_fmt_money(temporal_inventory_summary.get("max_market_exposure_usdc", 0.0)))],
        ["Max Total Exposure", html.escape(_fmt_money(temporal_inventory_summary.get("max_total_exposure_usdc", 0.0)))],
        ["Min Net Edge", html.escape(f"{float(temporal_inventory_summary.get('min_net_edge', 0.0)):.4f}")],
        ["Max Pair Cost", html.escape(f"{float(temporal_inventory_summary.get('max_pair_cost', 0.0)):.4f}")],
        ["Quote TTL", html.escape(f"{_fmt_num(temporal_inventory_summary.get('quote_ttl_seconds', 0))}s")],
        ["High-Edge / Hedge TTL", html.escape(f"{_fmt_num(temporal_inventory_summary.get('high_edge_ttl_seconds', 0))}s / {_fmt_num(temporal_inventory_summary.get('hedge_ttl_seconds', 0))}s")],
        ["Aggressive Edge Floors", html.escape(f"mid {float(temporal_inventory_summary.get('mid_aggressive_min_edge', 0.0)):.4f} | touch {float(temporal_inventory_summary.get('near_touch_min_edge', 0.0)):.4f}")],
        ["Fill / EV Floors", html.escape(f"p {float(temporal_inventory_summary.get('min_fill_probability', 0.0)):.3f} | EV {_fmt_money(temporal_inventory_summary.get('min_expected_value_usdc', 0.0))}")],
        ["Unpaired Timeout", html.escape(f"{_fmt_num(temporal_inventory_summary.get('unpaired_timeout_seconds', 0))}s")],
        ["Force Exit", html.escape(f"{_fmt_num(temporal_inventory_summary.get('force_exit_seconds', 0))}s")],
        ["Daily Loss Limit", html.escape(_fmt_money(temporal_inventory_summary.get("daily_loss_limit_usdc", 0.0)))],
        ["Last Cycle Quotes Opened", html.escape(_fmt_num(temporal_inventory_execution.get("opened_quotes_count", 0)))],
        ["Last Cycle Quotes Filled", html.escape(_fmt_num(temporal_inventory_execution.get("filled_quotes_count", 0)))],
        ["Last Cycle Quotes Cancelled", html.escape(_fmt_num(temporal_inventory_execution.get("cancelled_quotes_count", 0)))],
    ]
    temporal_inventory_market_table = _table(
        ["Updated (CT)", "Market", "Asset", "State", "YES", "NO", "YES Cost", "NO Cost", "Pair Cost", "Locked PnL", "Realized"],
        [
            [
                html.escape(_fmt_ts(item.get("updated_ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("state", ""))),
                html.escape(f"{float(item.get('yes_shares', 0.0)):.4f}"),
                html.escape(f"{float(item.get('no_shares', 0.0)):.4f}"),
                html.escape(_fmt_money(item.get("yes_cost_usdc", 0.0))),
                html.escape(_fmt_money(item.get("no_cost_usdc", 0.0))),
                html.escape(f"{float(item.get('locked_pair_cost', 0.0)):.4f}"),
                html.escape(_fmt_money(item.get("locked_pair_pnl_usdc", 0.0))),
                html.escape(_fmt_money(item.get("realized_pnl_usdc", 0.0))),
            ]
            for item in temporal_inventory_maker_paper.get("markets", [])
        ],
    )
    temporal_inventory_event_table = _table(
        ["Time (CT)", "Market", "Event", "State", "Side", "Price", "Size", "Notional", "PnL", "Pair Cost", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("event_type", ""))),
                html.escape(str(item.get("state", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape(f"{float(item.get('price') or 0.0):.4f}"),
                html.escape(f"{float(item.get('size') or 0.0):.4f}"),
                html.escape(_fmt_money(item.get("notional_usdc", 0.0))),
                html.escape(_fmt_money(item.get("pnl_usdc", 0.0))),
                html.escape(f"{float(item.get('pair_cost') or 0.0):.4f}"),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in temporal_inventory_maker_paper.get("recent_events", [])
        ],
    )
    temporal_inventory_style_table = _table(
        ["Style", "Quotes", "Filled", "Open", "Fill Rate", "Avg Edge", "Avg Fill P", "EV", "Adverse Loss"],
        [
            [
                html.escape(str(item.get("quote_style", ""))),
                html.escape(_fmt_num(item.get("quotes", 0))),
                html.escape(_fmt_num(item.get("filled", 0))),
                html.escape(_fmt_num(item.get("open", 0))),
                html.escape(f"{100.0 * (float(item.get('filled') or 0.0) / max(float(item.get('quotes') or 0.0), 1.0)):.1f}%"),
                html.escape(f"{float(item.get('avg_edge') or 0.0):.4f}"),
                html.escape(f"{float(item.get('avg_fill_probability') or 0.0):.3f}"),
                html.escape(_fmt_money(item.get("expected_value_usdc", 0.0))),
                html.escape(_fmt_money(item.get("adverse_selection_loss_usdc", 0.0))),
            ]
            for item in temporal_inventory_maker_paper.get("quote_style_breakdown", [])
        ],
    )
    temporal_inventory_quote_table = _table(
        ["Created (CT)", "Market", "Style", "Side", "Price", "Size", "Status", "Edge", "Fill P", "EV", "Fill", "Adverse Loss", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts_created"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("quote_style", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape(f"{float(item.get('price', 0.0)):.4f}"),
                html.escape(f"{float(item.get('size', 0.0)):.4f}"),
                html.escape(str(item.get("status", ""))),
                html.escape(f"{float(item.get('edge') or 0.0):.4f}"),
                html.escape(f"{float(item.get('fill_probability') or 0.0):.3f}"),
                html.escape(_fmt_money(item.get("expected_value_usdc", 0.0))),
                html.escape(f"{float(item.get('fill_price') or 0.0):.4f} / {float(item.get('fill_size') or 0.0):.4f}"),
                html.escape(_fmt_money(item.get("adverse_selection_loss_usdc", 0.0))),
                html.escape(str(item.get("cancel_reason") or item.get("reason") or "")),
            ]
            for item in temporal_inventory_maker_paper.get("recent_quotes", [])
        ],
    )
    live_temporal_execution_result = (
        last_cycle_result.get("live_temporal_inventory_maker", {})
        if isinstance(last_cycle_result.get("live_temporal_inventory_maker"), dict)
        else {}
    )
    live_temporal_execution = (
        live_temporal_execution_result.get("execution", {})
        if isinstance(live_temporal_execution_result.get("execution"), dict)
        else {}
    )
    live_temporal_summary_rows = [
        ["Mode", html.escape(str(live_temporal_summary.get("mode", "Guarded live temporal inventory maker")))],
        ["Enabled", html.escape(str(bool(live_temporal_summary.get("enabled"))).lower())],
        ["Pilot Mode", html.escape(str(live_temporal_summary.get("pilot_mode", "dry_run")))],
        ["Armed For Live Orders", html.escape(str(bool(live_temporal_summary.get("armed_for_live_orders"))).lower())],
        ["Confirmation Required", html.escape(str(live_temporal_summary.get("confirmation_required", "")))],
        ["Capital Cap", html.escape(_fmt_money(live_temporal_summary.get("capital_usdc", 0.0)))],
        ["Base Order", html.escape(_fmt_money(live_temporal_summary.get("base_order_usdc", 0.0)))],
        ["Max Open Orders", html.escape(_fmt_num(live_temporal_summary.get("max_open_orders", 0)))],
        ["Max Orders / Cycle", html.escape(_fmt_num(live_temporal_summary.get("max_orders_per_cycle", 0)))],
        ["Min Edge", html.escape(f"{float(live_temporal_summary.get('min_edge', 0.0)):.4f}")],
        ["Min Seconds Left", html.escape(f"{_fmt_num(live_temporal_summary.get('min_seconds_left', 0))}s")],
        ["Heartbeat Timeout", html.escape(f"{_fmt_num(live_temporal_summary.get('heartbeat_timeout_seconds', 0))}s")],
        ["Max Order Age", html.escape(f"{_fmt_num(live_temporal_summary.get('max_order_age_seconds', 0))}s")],
        ["Daily Loss Limit", html.escape(_fmt_money(live_temporal_summary.get("daily_loss_limit_usdc", 0.0)))],
        ["Open Local Maker Orders", html.escape(_fmt_num(live_temporal_summary.get("open_orders", 0)))],
        ["Open Local Maker Notional", html.escape(_fmt_money(live_temporal_summary.get("open_notional_usdc", 0.0)))],
        ["Submitted / Dry-Run 24h", html.escape(f"{_fmt_num(live_temporal_summary.get('submitted_24h', 0))} / {_fmt_num(live_temporal_summary.get('dry_run_24h', 0))}")],
        ["Blocked / Failed 24h", html.escape(f"{_fmt_num(live_temporal_summary.get('blocked_24h', 0))} / {_fmt_num(live_temporal_summary.get('failed_24h', 0))}")],
        ["Cancelled 24h", html.escape(_fmt_num(live_temporal_summary.get("cancelled_24h", 0)))],
        ["Last Heartbeat (CT)", html.escape(_fmt_ts(live_temporal_summary.get("last_heartbeat_ts")))],
        ["Last Heartbeat Status", html.escape(str(live_temporal_summary.get("last_heartbeat_status", "")) or "-")],
        ["Cancel-All Available", html.escape(str(bool(live_temporal_summary.get("last_heartbeat_cancel_all_ok"))).lower())],
        ["Heartbeat Reason", html.escape(str(live_temporal_summary.get("last_heartbeat_reason", "")) or "-")],
        ["Last Cycle Submitted / Dry-Run", html.escape(f"{_fmt_num(live_temporal_execution.get('submitted_count', 0))} / {_fmt_num(live_temporal_execution.get('dry_run_count', 0))}")],
        ["Last Cycle Blocked / Cancelled / Failed", html.escape(f"{_fmt_num(live_temporal_execution.get('blocked_count', 0))} / {_fmt_num(live_temporal_execution.get('cancelled_count', 0))} / {_fmt_num(live_temporal_execution.get('failed_count', 0))}")],
        ["Last Cycle CLOB Cash", html.escape(_fmt_money(live_temporal_execution.get("clob_cash_usdc", 0.0)))],
        ["Last Cycle CLOB Open Orders", html.escape(_fmt_num(live_temporal_execution.get("clob_open_orders", 0)))],
        ["Last Cycle CLOB Open Positions", html.escape(_fmt_num(live_temporal_execution.get("clob_open_positions", 0)))],
        ["Last Cycle Recent CLOB Trades", html.escape(_fmt_num(live_temporal_execution.get("clob_recent_trades", 0)))],
    ]
    live_temporal_order_table = _table(
        ["Created (CT)", "Market", "Side", "Price", "Size", "Status", "Decision", "Edge", "CLOB Order", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts_created"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape(f"{float(item.get('price') or 0.0):.4f}"),
                html.escape(f"{float(item.get('size') or 0.0):.4f}"),
                html.escape(str(item.get("status", ""))),
                html.escape(str(item.get("decision", ""))),
                html.escape(f"{float(item.get('edge') or 0.0):.4f}"),
                html.escape((str(item.get("clob_order_id", ""))[:10] + "..." + str(item.get("clob_order_id", ""))[-6:]) if len(str(item.get("clob_order_id", ""))) > 20 else str(item.get("clob_order_id", ""))),
                html.escape(str(item.get("reason") or item.get("error") or "")),
            ]
            for item in live_temporal_inventory_maker.get("recent_orders", [])
        ],
    )
    late_resolution_execution_result = (
        last_cycle_result.get("late_resolution_capture_paper", {})
        if isinstance(last_cycle_result.get("late_resolution_capture_paper"), dict)
        else {}
    )
    late_resolution_execution = (
        late_resolution_execution_result.get("execution", {})
        if isinstance(late_resolution_execution_result.get("execution"), dict)
        else {}
    )
    late_resolution_summary_rows = [
        ["Mode", html.escape(str(late_resolution_summary.get("mode", "late_resolution_capture_paper")))],
        ["Enabled", html.escape(str(bool(late_resolution_summary.get("enabled"))).lower())],
        ["Starting Capital", html.escape(_fmt_money(late_resolution_summary.get("starting_capital_usdc", 0.0)))],
        ["Equity", html.escape(_fmt_money(late_resolution_summary.get("equity_usdc", 0.0)))],
        ["Net PnL", html.escape(_fmt_money(late_resolution_summary.get("net_pnl", 0.0)))],
        ["24h Realized PnL", html.escape(_fmt_money(late_resolution_summary.get("realized_pnl_24h_usdc", 0.0)))],
        ["Open / Closed", html.escape(f"{_fmt_num(late_resolution_summary.get('open', 0))} / {_fmt_num(late_resolution_summary.get('closed', 0))}")],
        ["Win Rate", html.escape(f"{100.0 * float(late_resolution_summary.get('win_rate', 0.0)):.1f}%")],
        ["Avg PnL", html.escape(_fmt_money(late_resolution_summary.get("avg_pnl", 0.0)))],
        ["Max Drawdown", html.escape(_fmt_money(late_resolution_summary.get("max_drawdown", 0.0)))],
        ["Capital In Use", html.escape(_fmt_money(late_resolution_summary.get("current_capital_in_use_usdc", 0.0)))],
        ["Signals / Eligible 60m", html.escape(f"{_fmt_num(late_resolution_summary.get('signals_60m', 0))} / {_fmt_num(late_resolution_summary.get('eligible_60m', 0))}")],
        ["Best Edge 60m", html.escape(f"{float(late_resolution_summary.get('best_edge_60m', 0.0)):.4f}")],
        ["Target Notional", html.escape(_fmt_money(late_resolution_summary.get("target_notional_usdc", 0.0)))],
        ["Max Market Exposure", html.escape(_fmt_money(late_resolution_summary.get("max_market_exposure_usdc", 0.0)))],
        ["Max Total Exposure", html.escape(_fmt_money(late_resolution_summary.get("max_total_exposure_usdc", 0.0)))],
        ["Entry Window", html.escape(f"{_fmt_num(late_resolution_summary.get('min_seconds_left', 0))}s - {_fmt_num(late_resolution_summary.get('max_seconds_left', 0))}s")],
        ["Min Official Confidence", html.escape(f"{float(late_resolution_summary.get('min_official_confidence', 0.0)):.4f}")],
        ["Min Boundary Distance", html.escape(f"{float(late_resolution_summary.get('min_boundary_distance_bps', 0.0)):.1f} bps")],
        ["Min Edge", html.escape(f"{float(late_resolution_summary.get('min_edge', 0.0)):.4f}")],
        ["Min Entry Depth", html.escape(_fmt_money(late_resolution_summary.get("min_depth_usdc", 0.0)))],
        ["Min Exit Bid", html.escape(f"{float(late_resolution_summary.get('min_exit_bid', 0.0)):.4f}")],
        ["Max Book Age", html.escape(f"{float(late_resolution_summary.get('max_book_age_ms', 0.0)):.0f} ms")],
        ["Daily Loss Limit", html.escape(_fmt_money(late_resolution_summary.get("daily_loss_limit_usdc", 0.0)))],
        ["Last Cycle Opened / Closed", html.escape(f"{_fmt_num(late_resolution_execution.get('opened_positions_count', 0))} / {_fmt_num(late_resolution_execution.get('closed_positions_count', 0))}")],
        ["Last Cycle Entry Blocks", html.escape(_fmt_num(late_resolution_execution.get("entry_blocks_count", 0)))],
    ]
    late_resolution_open_table = _table(
        ["Entry (CT)", "Market", "Asset", "Side", "Entry", "Size", "Notional", "Edge", "Confidence", "Boundary"],
        [
            [
                html.escape(_fmt_ts(item.get("entry_ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape(f"{float(item.get('entry_price') or 0.0):.4f}"),
                html.escape(f"{float(item.get('size') or 0.0):.4f}"),
                html.escape(_fmt_money(item.get("notional_usdc", 0.0))),
                html.escape(f"{float(item.get('entry_edge') or 0.0):.4f}"),
                html.escape(f"{float(item.get('entry_official_confidence') or 0.0):.4f}"),
                html.escape(f"{float(item.get('entry_boundary_distance_bps') or 0.0):.1f} bps"),
            ]
            for item in late_resolution_capture_paper.get("open_positions", [])
        ],
    )
    late_resolution_close_table = _table(
        ["Time (CT)", "Market", "Side", "Entry", "Exit", "Size", "Notional", "PnL", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape(f"{float(item.get('entry_price') or 0.0):.4f}"),
                html.escape(f"{float(item.get('exit_price') or 0.0):.4f}"),
                html.escape(f"{float(item.get('size') or 0.0):.4f}"),
                html.escape(_fmt_money(item.get("notional_usdc", 0.0))),
                html.escape(_fmt_money(item.get("pnl", 0.0))),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in late_resolution_capture_paper.get("recent_closes", [])
        ],
    )
    late_resolution_signal_table = _table(
        ["Time (CT)", "Market", "Side", "Eligible", "Edge", "Price", "Confidence", "Boundary", "Seconds", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape("yes" if bool(item.get("eligible")) else "no"),
                html.escape(f"{float(item.get('edge') or 0.0):.4f}"),
                html.escape(f"{float(item.get('order_price') or 0.0):.4f}"),
                html.escape(f"{float(item.get('official_confidence') or 0.0):.4f}"),
                html.escape(f"{float(item.get('boundary_distance_bps') or 0.0):.1f} bps"),
                html.escape(f"{float(item.get('seconds_left') or 0.0):.1f}"),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in late_resolution_capture_paper.get("recent_signals", [])
        ],
    )
    late_resolution_reason_table = _table(
        ["Reason", "Count", "Max Edge"],
        [
            [
                html.escape(str(item.get("reason", ""))),
                html.escape(_fmt_num(item.get("count", 0))),
                html.escape(f"{float(item.get('max_edge') or 0.0):.4f}"),
            ]
            for item in late_resolution_capture_paper.get("reason_breakdown", [])
        ],
    )
    current_strategy_rows = [
        ["Iteration", html.escape("Temporal inventory maker + guarded live maker + separate late-resolution paper")],
        ["Primary Paper Bot", html.escape(f"{'enabled' if bool(temporal_inventory_summary.get('enabled')) else 'disabled'} | equity {_fmt_money(temporal_inventory_summary.get('equity_usdc', 0.0))} | marked PnL {_fmt_money(temporal_inventory_summary.get('marked_pnl_usdc', 0.0))}")],
        ["Paper Quotes", html.escape(f"open {_fmt_num(temporal_inventory_summary.get('quote_open', 0))} | filled {_fmt_num(temporal_inventory_summary.get('quote_filled', 0))} | fill rate {100.0 * float(temporal_inventory_summary.get('quote_fill_rate', 0.0)):.1f}%")],
        ["Live Maker Gate", html.escape(f"{'enabled' if bool(live_temporal_summary.get('enabled')) else 'disabled'} | mode {live_temporal_summary.get('pilot_mode', 'dry_run')} | armed {str(bool(live_temporal_summary.get('armed_for_live_orders'))).lower()}")],
        ["Live Maker Orders", html.escape(f"local open {_fmt_num(live_temporal_summary.get('open_orders', 0))} | 24h dry-run {_fmt_num(live_temporal_summary.get('dry_run_24h', 0))} | 24h submitted {_fmt_num(live_temporal_summary.get('submitted_24h', 0))} | 24h blocked {_fmt_num(live_temporal_summary.get('blocked_24h', 0))}")],
        ["Live Heartbeat", html.escape(f"{live_temporal_summary.get('last_heartbeat_status', '-') or '-'} | {live_temporal_summary.get('last_heartbeat_reason', '-') or '-'}")],
        ["Late Resolution Module", html.escape(f"{'enabled' if bool(late_resolution_summary.get('enabled')) else 'disabled'} | open {_fmt_num(late_resolution_summary.get('open', 0))} | net PnL {_fmt_money(late_resolution_summary.get('net_pnl', 0.0))} | eligible 60m {_fmt_num(late_resolution_summary.get('eligible_60m', 0))}")],
        ["Last Cycle Live Maker", html.escape(f"submitted {_fmt_num(live_temporal_execution.get('submitted_count', 0))} | dry-run {_fmt_num(live_temporal_execution.get('dry_run_count', 0))} | blocked {_fmt_num(live_temporal_execution.get('blocked_count', 0))} | cancelled {_fmt_num(live_temporal_execution.get('cancelled_count', 0))}")],
        ["Last Cycle Late Resolution", html.escape(f"opened {_fmt_num(late_resolution_execution.get('opened_positions_count', 0))} | closed {_fmt_num(late_resolution_execution.get('closed_positions_count', 0))} | blocks {_fmt_num(late_resolution_execution.get('entry_blocks_count', 0))}")],
    ]
    cex_latency_summary_rows = [
        ["Mode", html.escape(str(cex_latency_summary.get("mode", "CEX-latency directional paper bot")))],
        ["Data Sources", html.escape(str(cex_latency_summary.get("data_sources", "")))],
        ["Execution Pricing", html.escape(str(cex_latency_summary.get("execution_pricing", "")))],
        ["Enabled", html.escape(str(bool(cex_latency_summary.get("enabled"))).lower())],
        ["Starting Capital", html.escape(_fmt_money(cex_latency_summary.get("starting_capital_usdc", 0.0)))],
        ["Equity", html.escape(_fmt_money(cex_latency_summary.get("equity_usdc", 0.0)))],
        ["Net PnL", html.escape(_fmt_money(cex_latency_summary.get("net_pnl", 0.0)))],
        ["24h Net Revenue", html.escape(_fmt_money(cex_latency_summary.get("realized_pnl_24h_usdc", 0.0)))],
        ["Projected Monthly Revenue", html.escape(_fmt_money(cex_latency_summary.get("projected_monthly_revenue_usdc", 0.0)))],
        ["Projected Yearly Revenue", html.escape(_fmt_money(cex_latency_summary.get("projected_yearly_revenue_usdc", 0.0)))],
        ["Open Positions", html.escape(_fmt_num(cex_latency_summary.get("open", 0)))],
        ["Closed Trades", html.escape(_fmt_num(cex_latency_summary.get("closed", 0)))],
        ["Win Rate", html.escape(f"{100.0 * float(cex_latency_summary.get('win_rate', 0.0)):.1f}%")],
        ["Avg PnL / Trade", html.escape(_fmt_money(cex_latency_summary.get("avg_pnl", 0.0)))],
        ["Max Drawdown", html.escape(_fmt_money(cex_latency_summary.get("max_drawdown", 0.0)))],
        ["Current Capital In Use", html.escape(_fmt_money(cex_latency_summary.get("current_capital_in_use_usdc", 0.0)))],
        ["Current Capital / Bot Bankroll", html.escape(f"{100.0 * float(cex_latency_summary.get('current_capital_fraction', 0.0)):.1f}%")],
        ["Target Notional / Trade", html.escape(_fmt_money(cex_latency_summary.get("target_notional_usdc", 0.0)))],
        ["Max Open Positions", html.escape(_fmt_num(cex_latency_summary.get("max_open_positions", 0)))],
        ["Assets", html.escape(", ".join(str(item) for item in cex_latency_summary.get("assets", [])) or "-")],
        ["Model", html.escape(str(cex_latency_summary.get("model", "")))],
        ["Signals 60m", html.escape(_fmt_num(cex_latency_summary.get("signals_60m", 0)))],
        ["Eligible 60m", html.escape(_fmt_num(cex_latency_summary.get("eligible_60m", 0)))],
        ["Eligible Rate 60m", html.escape(f"{100.0 * float(cex_latency_summary.get('eligible_rate_60m', 0.0)):.1f}%")],
        ["Best Edge 60m", html.escape(f"{float(cex_latency_summary.get('best_edge_60m', 0.0)):.4f}")],
        ["Min Edge / Share", html.escape(f"{float(cex_latency_summary.get('min_edge_per_share', 0.0)):.4f}")],
        ["Min Depth", html.escape(_fmt_money(cex_latency_summary.get("min_depth_usdc", 0.0)))],
        ["Take Profit", html.escape(f"{100.0 * float(cex_latency_summary.get('take_profit_fraction', 0.0)):.1f}%")],
        ["Stop Loss", html.escape(f"{100.0 * float(cex_latency_summary.get('stop_loss_fraction', 0.0)):.1f}%")],
        ["Exit Edge Floor", html.escape(f"{float(cex_latency_summary.get('exit_edge_floor', 0.0)):.4f}")],
        ["Force Exit", html.escape(f"{_fmt_num(cex_latency_summary.get('force_exit_seconds', 0))}s")],
        ["Last Cycle Opens", html.escape(_fmt_num(cex_latency_execution.get("opened_positions_count", 0)))],
        ["Last Cycle Closes", html.escape(_fmt_num(cex_latency_execution.get("closed_positions_count", 0)))],
    ]
    if cex_latency_summary.get("data_status") == "not_loaded_fast":
        cex_latency_summary_rows = [["Data", "Not loaded in fast mode; canonical historical PnL remains in Strategy Truth"]]
    cex_latency_signal_table = _table(
        ["Time (CT)", "Market", "Asset", "Side", "Signal", "Edge", "Fair YES", "Fair NO", "Entry", "Depth", "Book Age", "Secs Left", "Eligible", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape(str(item.get("signal_type", ""))),
                html.escape(f"{float(item.get('edge', 0.0)):.4f}"),
                html.escape(f"{float(item.get('fair_yes', 0.0)):.3f}"),
                html.escape(f"{float(item.get('fair_no', 0.0)):.3f}"),
                html.escape(f"{float(item.get('order_price', 0.0)):.4f}"),
                html.escape(_fmt_money(item.get("min_depth_usdc", 0.0))),
                html.escape(f"{float(item.get('book_age_ms', 0.0)):.0f} ms"),
                html.escape(f"{float(item.get('seconds_left', 0.0)):.1f}"),
                html.escape("yes" if bool(item.get("eligible")) else "no"),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in cex_latency_paper.get("recent_signals", [])
        ],
    )
    cex_latency_open_table = _table(
        ["Opened (CT)", "Market", "Asset", "Side", "Entry", "Size", "Notional", "Entry Edge", "Secs Left", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("entry_ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape(f"{float(item.get('entry_price', 0.0)):.4f}"),
                html.escape(f"{float(item.get('size', 0.0)):.4f}"),
                html.escape(_fmt_money(item.get("notional_usdc", 0.0))),
                html.escape(f"{float(item.get('entry_edge', 0.0)):.4f}"),
                html.escape(f"{float(item.get('entry_seconds_left', 0.0)):.1f}"),
                html.escape(str(item.get("entry_reason", ""))),
            ]
            for item in cex_latency_paper.get("open_positions", [])
        ],
    )
    cex_latency_close_table = _table(
        ["Time (CT)", "Market", "Asset", "Side", "Entry", "Exit", "Size", "Notional", "Entry Edge", "PnL", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape(f"{float(item.get('entry_price', 0.0)):.4f}"),
                html.escape(f"{float(item.get('exit_price', 0.0)):.4f}"),
                html.escape(f"{float(item.get('size', 0.0)):.4f}"),
                html.escape(_fmt_money(item.get("notional_usdc", 0.0))),
                html.escape(f"{float(item.get('entry_edge', 0.0)):.4f}"),
                html.escape(_fmt_money(item.get("pnl", 0.0))),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in cex_latency_paper.get("recent_closes", [])
        ],
    )
    cex_latency_reason_table = _table(
        ["Reason", "Count", "Max Edge"],
        [
            [
                html.escape(str(item.get("reason", ""))),
                html.escape(_fmt_num(item.get("count", 0))),
                html.escape(f"{float(item.get('max_edge', 0.0)):.4f}"),
            ]
            for item in cex_latency_paper.get("reason_breakdown", [])
        ],
    )
    btc_fair_value_summary_rows = [
        ["Mode", html.escape(str(btc_fair_value_summary.get("mode", "BTC fair-value directional paper bot")))],
        ["Data Sources", html.escape(str(btc_fair_value_summary.get("data_sources", "")))],
        ["Execution Pricing", html.escape(str(btc_fair_value_summary.get("execution_pricing", "")))],
        ["Enabled", html.escape(str(bool(btc_fair_value_summary.get("enabled"))).lower())],
        ["Starting Capital", html.escape(_fmt_money(btc_fair_value_summary.get("starting_capital_usdc", 0.0)))],
        ["Equity", html.escape(_fmt_money(btc_fair_value_summary.get("equity_usdc", 0.0)))],
        ["Net PnL", html.escape(_fmt_money(btc_fair_value_summary.get("net_pnl", 0.0)))],
        ["24h Net Revenue", html.escape(_fmt_money(btc_fair_value_summary.get("realized_pnl_24h_usdc", 0.0)))],
        ["Projected Monthly Revenue", html.escape(_fmt_money(btc_fair_value_summary.get("projected_monthly_revenue_usdc", 0.0)))],
        ["Projected Yearly Revenue", html.escape(_fmt_money(btc_fair_value_summary.get("projected_yearly_revenue_usdc", 0.0)))],
        ["Open Positions", html.escape(_fmt_num(btc_fair_value_summary.get("open", 0)))],
        ["Closed Trades", html.escape(_fmt_num(btc_fair_value_summary.get("closed", 0)))],
        ["Win Rate", html.escape(f"{100.0 * float(btc_fair_value_summary.get('win_rate', 0.0)):.1f}%")],
        ["Avg PnL / Trade", html.escape(_fmt_money(btc_fair_value_summary.get("avg_pnl", 0.0)))],
        ["Max Drawdown", html.escape(_fmt_money(btc_fair_value_summary.get("max_drawdown", 0.0)))],
        ["Current Capital In Use", html.escape(_fmt_money(btc_fair_value_summary.get("current_capital_in_use_usdc", 0.0)))],
        ["Current Capital / Bot Bankroll", html.escape(f"{100.0 * float(btc_fair_value_summary.get('current_capital_fraction', 0.0)):.1f}%")],
        ["Target Notional / Trade", html.escape(_fmt_money(btc_fair_value_summary.get("target_notional_usdc", 0.0)))],
        ["Max Open Positions", html.escape(_fmt_num(btc_fair_value_summary.get("max_open_positions", 0)))],
        ["Assets", html.escape(", ".join(str(item) for item in btc_fair_value_summary.get("assets", [])) or "-")],
        ["Model Weights", html.escape(f"market={float(btc_fair_value_summary.get('market_weight', 0.0)):.2f}, micro={float(btc_fair_value_summary.get('microprice_weight', 0.0)):.2f}, binance={float(btc_fair_value_summary.get('binance_weight', 0.0)):.2f}")],
        ["Min Model Confidence", html.escape(f"{float(btc_fair_value_summary.get('min_model_confidence', 0.0)):.4f}")],
        ["Signals 60m", html.escape(_fmt_num(btc_fair_value_summary.get("signals_60m", 0)))],
        ["Eligible 60m", html.escape(_fmt_num(btc_fair_value_summary.get("eligible_60m", 0)))],
        ["Eligible Rate 60m", html.escape(f"{100.0 * float(btc_fair_value_summary.get('eligible_rate_60m', 0.0)):.1f}%")],
        ["Best Edge 60m", html.escape(f"{float(btc_fair_value_summary.get('best_edge_60m', 0.0)):.4f}")],
        ["Min Edge / Share", html.escape(f"{float(btc_fair_value_summary.get('min_edge_per_share', 0.0)):.4f}")],
        ["Min Depth", html.escape(_fmt_money(btc_fair_value_summary.get("min_depth_usdc", 0.0)))],
        ["Take Profit", html.escape(f"{100.0 * float(btc_fair_value_summary.get('take_profit_fraction', 0.0)):.1f}%")],
        ["Stop Loss", html.escape(f"{100.0 * float(btc_fair_value_summary.get('stop_loss_fraction', 0.0)):.1f}%")],
        ["Exit Edge Floor", html.escape(f"{float(btc_fair_value_summary.get('exit_edge_floor', 0.0)):.4f}")],
        ["Force Exit", html.escape(f"{_fmt_num(btc_fair_value_summary.get('force_exit_seconds', 0))}s")],
        ["Last Cycle Opens", html.escape(_fmt_num(btc_fair_value_execution.get("opened_positions_count", 0)))],
        ["Last Cycle Closes", html.escape(_fmt_num(btc_fair_value_execution.get("closed_positions_count", 0)))],
    ]
    if btc_fair_value_summary.get("data_status") == "not_loaded_fast":
        btc_fair_value_summary_rows = [["Data", "Not loaded in fast mode; canonical historical PnL remains in Strategy Truth"]]
    btc_fair_value_signal_table = _table(
        ["Time (CT)", "Market", "Asset", "Side", "Signal", "Edge", "Fair YES", "Fair NO", "Entry", "Depth", "Book Age", "Secs Left", "Eligible", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape(str(item.get("signal_type", ""))),
                html.escape(f"{float(item.get('edge', 0.0)):.4f}"),
                html.escape(f"{float(item.get('fair_yes', 0.0)):.3f}"),
                html.escape(f"{float(item.get('fair_no', 0.0)):.3f}"),
                html.escape(f"{float(item.get('order_price', 0.0)):.4f}"),
                html.escape(_fmt_money(item.get("min_depth_usdc", 0.0))),
                html.escape(f"{float(item.get('book_age_ms', 0.0)):.0f} ms"),
                html.escape(f"{float(item.get('seconds_left', 0.0)):.1f}"),
                html.escape("yes" if bool(item.get("eligible")) else "no"),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in btc_fair_value_paper.get("recent_signals", [])
        ],
    )
    btc_fair_value_open_table = _table(
        ["Opened (CT)", "Market", "Asset", "Side", "Entry", "Size", "Notional", "Entry Edge", "Secs Left", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("entry_ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape(f"{float(item.get('entry_price', 0.0)):.4f}"),
                html.escape(f"{float(item.get('size', 0.0)):.4f}"),
                html.escape(_fmt_money(item.get("notional_usdc", 0.0))),
                html.escape(f"{float(item.get('entry_edge', 0.0)):.4f}"),
                html.escape(f"{float(item.get('entry_seconds_left', 0.0)):.1f}"),
                html.escape(str(item.get("entry_reason", ""))),
            ]
            for item in btc_fair_value_paper.get("open_positions", [])
        ],
    )
    btc_fair_value_close_table = _table(
        ["Time (CT)", "Market", "Asset", "Side", "Entry", "Exit", "Size", "Notional", "Entry Edge", "PnL", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape(f"{float(item.get('entry_price', 0.0)):.4f}"),
                html.escape(f"{float(item.get('exit_price', 0.0)):.4f}"),
                html.escape(f"{float(item.get('size', 0.0)):.4f}"),
                html.escape(_fmt_money(item.get("notional_usdc", 0.0))),
                html.escape(f"{float(item.get('entry_edge', 0.0)):.4f}"),
                html.escape(_fmt_money(item.get("pnl", 0.0))),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in btc_fair_value_paper.get("recent_closes", [])
        ],
    )
    btc_fair_value_reason_table = _table(
        ["Reason", "Count", "Max Edge"],
        [
            [
                html.escape(str(item.get("reason", ""))),
                html.escape(_fmt_num(item.get("count", 0))),
                html.escape(f"{float(item.get('max_edge', 0.0)):.4f}"),
            ]
            for item in btc_fair_value_paper.get("reason_breakdown", [])
        ],
    )
    wallet_teacher_summary = (
        wallet_teacher_sniper.get("summary", {})
        if isinstance(wallet_teacher_sniper.get("summary"), dict)
        else {}
    )
    wallet_teacher_result = (
        last_cycle_result.get("wallet_teacher_sniper", {})
        if isinstance(last_cycle_result.get("wallet_teacher_sniper"), dict)
        else {}
    )
    wallet_teacher_execution = (
        wallet_teacher_result.get("execution", {})
        if isinstance(wallet_teacher_result.get("execution"), dict)
        else {}
    )
    wallet_teacher_summary_rows = [
        ["Mode", html.escape(str(wallet_teacher_summary.get("mode", "Target-wallet teacher paper bot")))],
        ["Data Sources", html.escape(str(wallet_teacher_summary.get("data_sources", "")))],
        ["Execution Pricing", html.escape(str(wallet_teacher_summary.get("execution_pricing", "")))],
        ["Enabled", html.escape(str(bool(wallet_teacher_summary.get("enabled"))).lower())],
        ["Target Wallet", html.escape(str(wallet_teacher_summary.get("target_wallet", "")))],
        ["Starting Capital", html.escape(_fmt_money(wallet_teacher_summary.get("starting_capital_usdc", 0.0)))],
        ["Equity", html.escape(_fmt_money(wallet_teacher_summary.get("equity_usdc", 0.0)))],
        ["Net PnL", html.escape(_fmt_money(wallet_teacher_summary.get("net_pnl", 0.0)))],
        ["24h Net Revenue", html.escape(_fmt_money(wallet_teacher_summary.get("realized_pnl_24h_usdc", 0.0)))],
        ["Projected Monthly Revenue", html.escape(_fmt_money(wallet_teacher_summary.get("projected_monthly_revenue_usdc", 0.0)))],
        ["Projected Yearly Revenue", html.escape(_fmt_money(wallet_teacher_summary.get("projected_yearly_revenue_usdc", 0.0)))],
        ["Open Positions", html.escape(_fmt_num(wallet_teacher_summary.get("open", 0)))],
        ["Closed Trades", html.escape(_fmt_num(wallet_teacher_summary.get("closed", 0)))],
        ["Win Rate", html.escape(f"{100.0 * float(wallet_teacher_summary.get('win_rate', 0.0)):.1f}%")],
        ["Avg PnL / Trade", html.escape(_fmt_money(wallet_teacher_summary.get("avg_pnl", 0.0)))],
        ["Max Drawdown", html.escape(_fmt_money(wallet_teacher_summary.get("max_drawdown", 0.0)))],
        ["Current Capital In Use", html.escape(_fmt_money(wallet_teacher_summary.get("current_capital_in_use_usdc", 0.0)))],
        ["Current Capital / Bot Bankroll", html.escape(f"{100.0 * float(wallet_teacher_summary.get('current_capital_fraction', 0.0)):.1f}%")],
        ["Target Notional / Trade", html.escape(_fmt_money(wallet_teacher_summary.get("target_notional_usdc", 0.0)))],
        ["Max Open Positions", html.escape(_fmt_num(wallet_teacher_summary.get("max_open_positions", 0)))],
        ["Assets", html.escape(", ".join(str(item) for item in wallet_teacher_summary.get("assets", [])) or "-")],
        ["Signals 60m", html.escape(_fmt_num(wallet_teacher_summary.get("signals_60m", 0)))],
        ["Eligible 60m", html.escape(_fmt_num(wallet_teacher_summary.get("eligible_60m", 0)))],
        ["Eligible Rate 60m", html.escape(f"{100.0 * float(wallet_teacher_summary.get('eligible_rate_60m', 0.0)):.1f}%")],
        ["Best Teacher Score 60m", html.escape(f"{float(wallet_teacher_summary.get('best_edge_60m', 0.0)):.4f}")],
        ["Teacher Trade Lookback", html.escape(f"{_fmt_num(wallet_teacher_summary.get('teacher_trade_lookback_seconds', 0))}s")],
        ["Min Teacher Trade Notional", html.escape(_fmt_money(wallet_teacher_summary.get("min_teacher_notional_usdc", 0.0)))],
        ["Min Depth", html.escape(_fmt_money(wallet_teacher_summary.get("min_depth_usdc", 0.0)))],
        ["Take Profit", html.escape(f"{100.0 * float(wallet_teacher_summary.get('take_profit_fraction', 0.0)):.1f}%")],
        ["Stop Loss", html.escape(f"{100.0 * float(wallet_teacher_summary.get('stop_loss_fraction', 0.0)):.1f}%")],
        ["Force Exit", html.escape(f"{_fmt_num(wallet_teacher_summary.get('force_exit_seconds', 0))}s")],
        ["Last Cycle Opens", html.escape(_fmt_num(wallet_teacher_execution.get("opened_positions_count", 0)))],
        ["Last Cycle Closes", html.escape(_fmt_num(wallet_teacher_execution.get("closed_positions_count", 0)))],
    ]
    wallet_teacher_signal_table = _table(
        ["Time (CT)", "Market", "Asset", "Side", "Signal", "Teacher Score", "Entry", "Depth", "Book Age", "Secs Left", "Eligible", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape(str(item.get("signal_type", ""))),
                html.escape(f"{float(item.get('edge', 0.0)):.4f}"),
                html.escape(f"{float(item.get('order_price', 0.0)):.4f}"),
                html.escape(_fmt_money(item.get("min_depth_usdc", 0.0))),
                html.escape(f"{float(item.get('book_age_ms', 0.0)):.0f} ms"),
                html.escape(f"{float(item.get('seconds_left', 0.0)):.1f}"),
                html.escape("yes" if bool(item.get("eligible")) else "no"),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in wallet_teacher_sniper.get("recent_signals", [])
            if isinstance(item, dict)
        ],
    )
    wallet_teacher_open_table = _table(
        ["Opened (CT)", "Market", "Asset", "Side", "Entry", "Size", "Notional", "Teacher Score", "Secs Left", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("entry_ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape(f"{float(item.get('entry_price', 0.0)):.4f}"),
                html.escape(f"{float(item.get('size', 0.0)):.4f}"),
                html.escape(_fmt_money(item.get("notional_usdc", 0.0))),
                html.escape(f"{float(item.get('entry_edge', 0.0)):.4f}"),
                html.escape(f"{float(item.get('entry_seconds_left', 0.0)):.1f}"),
                html.escape(str(item.get("entry_reason", ""))),
            ]
            for item in wallet_teacher_sniper.get("open_positions", [])
            if isinstance(item, dict)
        ],
    )
    wallet_teacher_close_table = _table(
        ["Time (CT)", "Market", "Asset", "Side", "Entry", "Exit", "Size", "Notional", "Teacher Score", "PnL", "Reason"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape(f"{float(item.get('entry_price', 0.0)):.4f}"),
                html.escape(f"{float(item.get('exit_price', 0.0)):.4f}"),
                html.escape(f"{float(item.get('size', 0.0)):.4f}"),
                html.escape(_fmt_money(item.get("notional_usdc", 0.0))),
                html.escape(f"{float(item.get('entry_edge', 0.0)):.4f}"),
                html.escape(_fmt_money(item.get("pnl", 0.0))),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in wallet_teacher_sniper.get("recent_closes", [])
            if isinstance(item, dict)
        ],
    )
    wallet_teacher_reason_table = _table(
        ["Reason", "Count", "Max Teacher Score"],
        [
            [
                html.escape(str(item.get("reason", ""))),
                html.escape(_fmt_num(item.get("count", 0))),
                html.escape(f"{float(item.get('max_edge', 0.0)):.4f}"),
            ]
            for item in wallet_teacher_sniper.get("reason_breakdown", [])
            if isinstance(item, dict)
        ],
    )
    cex_latency_threshold_table = _table(
        ["Edge Floor", "Raw Ticks", "Unique Markets", "Best Edge", "Avg Best Edge", "Avg Entry", "Indicative Edge $"],
        [
            [
                html.escape(f"{float(item.get('edge_floor', 0.0)):.3f}"),
                html.escape(_fmt_num(item.get("raw_ticks", 0))),
                html.escape(_fmt_num(item.get("unique_markets", 0))),
                html.escape(f"{float(item.get('best_edge', 0.0)):.4f}"),
                html.escape(f"{float(item.get('avg_best_edge', 0.0)):.4f}"),
                html.escape(f"{float(item.get('avg_entry', 0.0)):.4f}"),
                html.escape(_fmt_money(item.get("indicative_edge_usdc", 0.0))),
            ]
            for item in cex_latency_paper.get("threshold_sensitivity", [])
            if isinstance(item, dict)
        ],
    )
    cex_latency_asset_side_table = _table(
        ["Asset", "Side", "Closed", "Win Rate", "Net PnL", "Avg PnL", "Avg Edge", "Avg Entry"],
        [
            [
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape(_fmt_num(item.get("closed", 0))),
                html.escape(
                    f"{100.0 * (float(item.get('wins', 0) or 0.0) / float(item.get('closed', 0) or 1.0)):.1f}%"
                ),
                html.escape(_fmt_money(item.get("net_pnl", 0.0))),
                html.escape(_fmt_money(item.get("avg_pnl", 0.0))),
                html.escape(f"{float(item.get('avg_edge', 0.0)):.4f}"),
                html.escape(f"{float(item.get('avg_entry', 0.0)):.4f}"),
            ]
            for item in cex_latency_paper.get("pnl_by_asset_side", [])
            if isinstance(item, dict)
        ],
    )
    cex_latency_exit_reason_pnl_table = _table(
        ["Exit Reason", "Closed", "Win Rate", "Net PnL", "Avg PnL"],
        [
            [
                html.escape(str(item.get("reason", ""))),
                html.escape(_fmt_num(item.get("closed", 0))),
                html.escape(
                    f"{100.0 * (float(item.get('wins', 0) or 0.0) / float(item.get('closed', 0) or 1.0)):.1f}%"
                ),
                html.escape(_fmt_money(item.get("net_pnl", 0.0))),
                html.escape(_fmt_money(item.get("avg_pnl", 0.0))),
            ]
            for item in cex_latency_paper.get("pnl_by_exit_reason", [])
            if isinstance(item, dict)
        ],
    )
    cex_latency_price_band_pnl_table = _table(
        ["Entry Price Band", "Closed", "Win Rate", "Net PnL", "Avg PnL", "Avg Edge"],
        [
            [
                html.escape(str(item.get("band", ""))),
                html.escape(_fmt_num(item.get("closed", 0))),
                html.escape(
                    f"{100.0 * (float(item.get('wins', 0) or 0.0) / float(item.get('closed', 0) or 1.0)):.1f}%"
                ),
                html.escape(_fmt_money(item.get("net_pnl", 0.0))),
                html.escape(_fmt_money(item.get("avg_pnl", 0.0))),
                html.escape(f"{float(item.get('avg_edge', 0.0)):.4f}"),
            ]
            for item in cex_latency_paper.get("pnl_by_price_band", [])
            if isinstance(item, dict)
        ],
    )
    cex_latency_edge_band_pnl_table = _table(
        ["Entry Edge Band", "Closed", "Win Rate", "Net PnL", "Avg PnL", "Avg Entry"],
        [
            [
                html.escape(str(item.get("band", ""))),
                html.escape(_fmt_num(item.get("closed", 0))),
                html.escape(
                    f"{100.0 * (float(item.get('wins', 0) or 0.0) / float(item.get('closed', 0) or 1.0)):.1f}%"
                ),
                html.escape(_fmt_money(item.get("net_pnl", 0.0))),
                html.escape(_fmt_money(item.get("avg_pnl", 0.0))),
                html.escape(f"{float(item.get('avg_entry', 0.0)):.4f}"),
            ]
            for item in cex_latency_paper.get("pnl_by_edge_band", [])
            if isinstance(item, dict)
        ],
    )
    cex_latency_entry_block_table = _table(
        ["Market", "Reason"],
        [
            [
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("reason", ""))),
            ]
            for item in cex_latency_execution.get("entry_blocks", [])
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
    preowned_inventory_summary = (
        preowned_inventory_arb.get("summary", {})
        if isinstance(preowned_inventory_arb.get("summary"), dict)
        else {}
    )
    preowned_inventory_summary_rows = [
        ["Mode", html.escape(str(preowned_inventory_summary.get("mode", "Research-only pre-owned inventory complete-set arb simulation")))],
        ["Enabled", html.escape(str(bool(preowned_inventory_summary.get("enabled"))).lower())],
        ["Tick Poll Source", html.escape(str(preowned_inventory_summary.get("tick_poll_source", "")))],
        ["Lookback Hours", html.escape(_fmt_num(preowned_inventory_summary.get("lookback_hours", 0)))],
        ["Ticks Replayed", html.escape(_fmt_num(preowned_inventory_summary.get("ticks_replayed", 0)))],
        ["First Tick (CT)", html.escape(_fmt_ts(preowned_inventory_summary.get("first_tick_ts")))],
        ["Last Tick (CT)", html.escape(_fmt_ts(preowned_inventory_summary.get("last_tick_ts")))],
        ["Simulated Capital", html.escape(_fmt_money(preowned_inventory_summary.get("simulated_capital_usdc", 0.0)))],
        ["Target Notional / Set", html.escape(_fmt_money(preowned_inventory_summary.get("target_notional_usdc", 0.0)))],
        ["Seed Side Notional", html.escape(_fmt_money(preowned_inventory_summary.get("seed_side_notional_usdc", 0.0)))],
        ["Seed Max Complete-Set Cost", html.escape(f"{float(preowned_inventory_summary.get('seed_max_complete_set_cost', 0.0)):.4f}")],
        ["Seed Min Seconds Left", html.escape(f"{_fmt_num(preowned_inventory_summary.get('seed_min_seconds_left', 0))}s")],
        ["Seed Max Seconds Left", html.escape(f"{_fmt_num(preowned_inventory_summary.get('seed_max_seconds_left', 0))}s")],
        ["Seed Min Depth", html.escape(_fmt_money(preowned_inventory_summary.get("seed_min_depth_usdc", 0.0)))],
        ["Seed Requires Original Eligibility", html.escape(str(bool(preowned_inventory_summary.get("seed_require_original_eligible"))).lower())],
        ["Max Open Seeded Markets", html.escape(_fmt_num(preowned_inventory_summary.get("max_open_seeded_markets", 0)))],
        ["Per-Asset Time-Bucket Seed Cap", html.escape(_fmt_num(preowned_inventory_summary.get("per_asset_time_bucket_cap", 0)))],
        ["Time Bucket", html.escape(f"{_fmt_num(preowned_inventory_summary.get('time_bucket_seconds', 0))}s")],
        ["Seeded Markets", html.escape(_fmt_num(preowned_inventory_summary.get("seeded_markets", 0)))],
        ["Seed-to-Set Conversion Rate", html.escape(f"{100.0 * float(preowned_inventory_summary.get('seed_to_set_conversion_rate', 0.0)):.1f}%")],
        ["Profit Explanation", html.escape(str(preowned_inventory_summary.get("profit_explanation", "")))],
        ["Seed Spend", html.escape(_fmt_money(preowned_inventory_summary.get("seed_spend_usdc", 0.0)))],
        ["Expired Inventory Cost", html.escape(_fmt_money(preowned_inventory_summary.get("expired_inventory_cost_usdc", 0.0)))],
        ["Expired Inventory Guaranteed Payout", html.escape(_fmt_money(preowned_inventory_summary.get("expired_inventory_guaranteed_payout_usdc", 0.0)))],
        ["Expired Inventory Conservative PnL", html.escape(_fmt_money(preowned_inventory_summary.get("expired_inventory_conservative_pnl_usdc", 0.0)))],
        ["Unused Inventory Cost", html.escape(_fmt_money(preowned_inventory_summary.get("unused_inventory_cost_usdc", 0.0)))],
        ["Unused Inventory Guaranteed Payout", html.escape(_fmt_money(preowned_inventory_summary.get("unused_inventory_guaranteed_payout_usdc", 0.0)))],
        ["Unused Inventory Conservative Mark", html.escape(_fmt_money(preowned_inventory_summary.get("unused_inventory_conservative_pnl_usdc", 0.0)))],
        ["Unused Inventory Shares", html.escape(f"{float(preowned_inventory_summary.get('unused_inventory_shares', 0.0)):.4f}")],
        ["Consumed Inventory Cost", html.escape(_fmt_money(preowned_inventory_summary.get("consumed_inventory_cost_usdc", 0.0)))],
        ["Live Buy-Leg Spend", html.escape(_fmt_money(preowned_inventory_summary.get("live_buy_leg_spend_usdc", 0.0)))],
        ["Current Locked Capital", html.escape(_fmt_money(preowned_inventory_summary.get("current_locked_capital_usdc", 0.0)))],
        ["Peak Locked Capital", html.escape(_fmt_money(preowned_inventory_summary.get("peak_locked_capital_usdc", 0.0)))],
        ["Peak Capital Fraction", html.escape(f"{100.0 * float(preowned_inventory_summary.get('peak_capital_fraction', 0.0)):.1f}%")],
        ["Inventory-Paired Sets", html.escape(_fmt_num(preowned_inventory_summary.get("inventory_sets", 0)))],
        ["Unique Markets", html.escape(_fmt_num(preowned_inventory_summary.get("unique_markets", 0)))],
        ["Inventory-Blocked Ticks", html.escape(_fmt_num(preowned_inventory_summary.get("inventory_blocked", 0)))],
        ["Capital-Blocked Ticks", html.escape(_fmt_num(preowned_inventory_summary.get("capital_blocked", 0)))],
        ["Depth-Blocked Ticks", html.escape(_fmt_num(preowned_inventory_summary.get("depth_blocked", 0)))],
        ["Original Skips", html.escape(_fmt_num(preowned_inventory_summary.get("original_skips", 0)))],
        ["Completed-Pair Gross PnL", html.escape(_fmt_money(preowned_inventory_summary.get("completed_set_gross_pnl_usdc", 0.0)))],
        ["Wallet-Adjusted Net PnL", html.escape(_fmt_money(preowned_inventory_summary.get("net_pnl", 0.0)))],
        ["Marked Net PnL incl. Open Inventory", html.escape(_fmt_money(preowned_inventory_summary.get("marked_net_pnl", preowned_inventory_summary.get("net_pnl", 0.0))))],
        ["24h Net Revenue", html.escape(_fmt_money(preowned_inventory_summary.get("realized_pnl_24h_usdc", 0.0)))],
        ["Projected Monthly Revenue", html.escape(_fmt_money(preowned_inventory_summary.get("projected_monthly_revenue_usdc", 0.0)))],
        ["Projected Yearly Revenue", html.escape(_fmt_money(preowned_inventory_summary.get("projected_yearly_revenue_usdc", 0.0)))],
        ["Avg Gross PnL / Inventory Set", html.escape(_fmt_money(preowned_inventory_summary.get("avg_pnl_per_inventory_set", 0.0)))],
        ["Avg Wallet Net PnL / Inventory Set", html.escape(_fmt_money(preowned_inventory_summary.get("avg_net_pnl_per_inventory_set", 0.0)))],
        ["Max Drawdown", html.escape(_fmt_money(preowned_inventory_summary.get("max_drawdown", 0.0)))],
        ["Signals 60m", html.escape(_fmt_num(preowned_inventory_summary.get("signals_60m", 0)))],
        ["Eligible 60m", html.escape(_fmt_num(preowned_inventory_summary.get("eligible_60m", 0)))],
        ["Eligible Rate 60m", html.escape(f"{100.0 * float(preowned_inventory_summary.get('eligible_rate_60m', 0.0)):.1f}%")],
        ["Best Inventory Edge 60m", html.escape(f"{float(preowned_inventory_summary.get('best_inventory_edge_60m', 0.0)):.4f}")],
        ["Min Edge / Share", html.escape(f"{float(preowned_inventory_summary.get('min_edge_per_share', 0.0)):.4f}")],
        ["Min Seconds Left", html.escape(f"{_fmt_num(preowned_inventory_summary.get('min_seconds_left', 0))}s")],
        ["Min Depth", html.escape(_fmt_money(preowned_inventory_summary.get("min_depth_usdc", 0.0)))],
        ["Same-Market Cooldown", html.escape(f"{_fmt_num(preowned_inventory_summary.get('same_market_cooldown_seconds', 0))}s")],
        ["Depth Haircut", html.escape(f"{100.0 * float(preowned_inventory_summary.get('depth_haircut', 0.0)):.1f}%")],
        ["Extra Slippage / Share", html.escape(f"{float(preowned_inventory_summary.get('extra_slippage_per_share', 0.0)):.4f}")],
        ["Redeem Lag", html.escape(f"{_fmt_num(preowned_inventory_summary.get('redeem_lag_seconds', 0))}s")],
    ]
    preowned_inventory_event_table = _table(
        ["Time (CT)", "Market", "Asset", "Decision", "Reason", "Buy Side", "Inventory Side", "Cost", "Inv Edge", "Shares", "Buy Spend", "Inventory Cost", "PnL", "Locked Capital"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("decision", ""))),
                html.escape(str(item.get("reason", ""))),
                html.escape(str(item.get("side_bought", "")).upper()),
                html.escape(str(item.get("inventory_side", "")).upper()),
                html.escape(f"{float(item.get('total_cost') or 0.0):.4f}"),
                html.escape(f"{float(item.get('inventory_edge') or 0.0):.4f}"),
                html.escape(f"{float(item.get('shares') or 0.0):.4f}"),
                html.escape(_fmt_money(item.get("buy_spend_usdc", 0.0))),
                html.escape(_fmt_money(item.get("inventory_cost_usdc", 0.0))),
                html.escape(_fmt_money(item.get("pnl", 0.0))),
                html.escape(_fmt_money(item.get("locked_capital_usdc", 0.0))),
            ]
            for item in preowned_inventory_arb.get("recent_events", [])
        ],
    )
    preowned_inventory_reason_table = _table(
        ["Reason", "Count", "Max Inventory Edge"],
        [
            [
                html.escape(str(item.get("reason", ""))),
                html.escape(_fmt_num(item.get("count", 0))),
                html.escape(f"{float(item.get('max_inventory_edge', 0.0)):.4f}"),
            ]
            for item in preowned_inventory_arb.get("reason_breakdown", [])
        ],
    )
    account_reconciliation_summary = (
        polymarket_account_reconciliation.get("summary", {})
        if isinstance(polymarket_account_reconciliation.get("summary"), dict)
        else {}
    )
    account_reconciliation_summary_rows = [
        ["Mode", html.escape(str(account_reconciliation_summary.get("mode", "Polymarket account reconciliation")))],
        ["Source", html.escape(str(account_reconciliation_summary.get("source", "")))],
        ["Status", html.escape(str(account_reconciliation_summary.get("status", "")))],
        ["Error", html.escape(str(account_reconciliation_summary.get("error", "")) or "-")],
        ["Last Checked (CT)", html.escape(_fmt_ts(account_reconciliation_summary.get("last_checked_ts")))],
        ["Funder Address", html.escape(str(account_reconciliation_summary.get("funder_address", "")))],
        ["CLOB Cash / Collateral Balance", html.escape(_fmt_money(account_reconciliation_summary.get("clob_cash_usdc", 0.0)))],
        ["Pilot Starting Capital", html.escape(_fmt_money(account_reconciliation_summary.get("pilot_starting_capital_usdc", 0.0)))],
        ["Cash - Pilot Starting Capital", html.escape(_fmt_money(account_reconciliation_summary.get("cash_minus_pilot_capital_usdc", 0.0)))],
        ["CLOB Open Orders", html.escape(_fmt_num(account_reconciliation_summary.get("clob_open_orders", 0)))],
        ["CLOB Open Positions", html.escape(_fmt_num(account_reconciliation_summary.get("clob_open_positions", 0)))],
        ["Recent CLOB Trades Fetched", html.escape(_fmt_num(account_reconciliation_summary.get("clob_recent_trades", 0)))],
        ["Recent BUY Spend", html.escape(_fmt_money(account_reconciliation_summary.get("clob_recent_buy_spend_usdc", 0.0)))],
        ["Recent SELL Proceeds", html.escape(_fmt_money(account_reconciliation_summary.get("clob_recent_sell_proceeds_usdc", 0.0)))],
        ["Recent Net Trade Cashflow", html.escape(_fmt_money(account_reconciliation_summary.get("clob_recent_net_trade_cashflow_usdc", 0.0)))],
        ["Local Live Attempt Rows With Order IDs", html.escape(_fmt_num(account_reconciliation_summary.get("local_live_attempt_rows", 0)))],
        ["Local Live Maker Order Rows", html.escape(_fmt_num(account_reconciliation_summary.get("local_live_maker_order_rows", 0)))],
        ["Local Order IDs Recorded", html.escape(_fmt_num(account_reconciliation_summary.get("local_order_ids_recorded", 0)))],
        ["Local-vs-CLOB Order ID Match Rate", html.escape(f"{100.0 * float(account_reconciliation_summary.get('local_order_id_match_rate', 0.0)):.1f}%")],
        ["CLOB Trades Matching Local Order IDs", html.escape(_fmt_num(account_reconciliation_summary.get("clob_trades_matching_local_order_ids", 0)))],
        ["CLOB Trades Not In Local Order IDs", html.escape(_fmt_num(account_reconciliation_summary.get("clob_trades_not_in_local_order_ids", 0)))],
        ["Local Realized PnL", html.escape(_fmt_money(account_reconciliation_summary.get("local_realized_pnl_usdc", 0.0)))],
        ["CLOB Cashflow - Local PnL Gap", html.escape(_fmt_money(account_reconciliation_summary.get("local_vs_clob_cashflow_gap_usdc", 0.0)))],
        ["Note", html.escape(str(account_reconciliation_summary.get("note", "")))],
    ]
    account_reconciliation_trade_table = _table(
        ["Time (CT)", "Outcome", "Side", "Size", "Price", "Cash Amount", "Status", "Local Order?", "Order ID", "Tx"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("outcome", ""))),
                html.escape(str(item.get("side", ""))),
                html.escape(f"{float(item.get('size') or 0.0):.4f}"),
                html.escape(f"{float(item.get('price') or 0.0):.4f}"),
                html.escape(_fmt_money(item.get("cash_amount_usdc", 0.0))),
                html.escape(str(item.get("status", ""))),
                html.escape("yes" if bool(item.get("matched_local_order")) else "no"),
                html.escape((str(item.get("order_id", ""))[:10] + "..." + str(item.get("order_id", ""))[-6:]) if len(str(item.get("order_id", ""))) > 20 else str(item.get("order_id", ""))),
                html.escape((str(item.get("transaction_hash", ""))[:10] + "..." + str(item.get("transaction_hash", ""))[-6:]) if len(str(item.get("transaction_hash", ""))) > 20 else str(item.get("transaction_hash", ""))),
            ]
            for item in polymarket_account_reconciliation.get("recent_trades", [])
        ],
    )
    account_reconciliation_open_order_table = _table(
        ["Order ID", "Market", "Outcome", "Side", "Price", "Size", "Status"],
        [
            [
                html.escape((str(item.get("id") or item.get("order_id") or item.get("orderID") or "")[:10] + "..." + str(item.get("id") or item.get("order_id") or item.get("orderID") or "")[-6:]) if len(str(item.get("id") or item.get("order_id") or item.get("orderID") or "")) > 20 else str(item.get("id") or item.get("order_id") or item.get("orderID") or "")),
                html.escape(str(item.get("market") or item.get("market_id") or "")),
                html.escape(str(item.get("outcome") or item.get("asset_id") or "")),
                html.escape(str(item.get("side") or "")),
                html.escape(f"{float(item.get('price') or 0.0):.4f}"),
                html.escape(f"{float(item.get('size') or item.get('original_size') or 0.0):.4f}"),
                html.escape(str(item.get("status") or "")),
            ]
            for item in polymarket_account_reconciliation.get("open_orders", [])
            if isinstance(item, dict)
        ],
    )
    account_reconciliation_position_table = _table(
        ["Market", "Outcome", "Side", "Size", "Avg Price", "Current Value", "Raw Status"],
        [
            [
                html.escape(str(item.get("market") or item.get("market_id") or "")),
                html.escape(str(item.get("outcome") or item.get("asset") or item.get("asset_id") or "")),
                html.escape(str(item.get("side") or "")),
                html.escape(f"{float(item.get('size') or item.get('balance') or 0.0):.4f}"),
                html.escape(f"{float(item.get('avg_price') or item.get('average_price') or item.get('price') or 0.0):.4f}"),
                html.escape(_fmt_money(item.get("current_value") or item.get("value") or 0.0)),
                html.escape(str(item.get("status") or item.get("redeemable") or "")),
            ]
            for item in polymarket_account_reconciliation.get("open_positions", [])
            if isinstance(item, dict)
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
        ["Accounting Basis", html.escape(str(live_complete_set_pilot_summary.get("accounting_basis", "local matched-leg reconstruction; not Polymarket wallet/account cash")))],
        ["Enabled", html.escape(str(bool(live_complete_set_pilot_summary.get("enabled"))).lower())],
        ["Pilot Mode", html.escape(str(live_complete_set_pilot_summary.get("pilot_mode", "dry_run")))],
        ["Armed For Live Orders", html.escape(str(bool(live_complete_set_pilot_summary.get("armed_for_live_orders"))).lower())],
        ["Credentials Configured", html.escape(str(bool(live_complete_set_pilot_summary.get("credentials_configured"))).lower())],
        ["Confirmation Required", html.escape(str(live_complete_set_pilot_summary.get("confirmation_required", "")))],
        ["Pilot Capital", html.escape(_fmt_money(live_complete_set_pilot_summary.get("simulated_or_live_capital_usdc", 0.0)))],
        ["Target Notional / Set", html.escape(_fmt_money(live_complete_set_pilot_summary.get("target_notional_usdc", 0.0)))],
        ["Submitted Sets", html.escape(_fmt_num(live_complete_set_pilot_summary.get("submitted_sets", 0)))],
        ["Actual Locked Complete Sets", html.escape(_fmt_num(live_complete_set_pilot_summary.get("actual_locked_complete_sets", 0)))],
        ["Directional Exposure Warning", html.escape(str(live_complete_set_pilot_summary.get("directional_exposure_warning", "no")))],
        ["Dry-Run Candidates", html.escape(_fmt_num(live_complete_set_pilot_summary.get("dry_run_candidates", 0)))],
        ["Blocked Attempts", html.escape(_fmt_num(live_complete_set_pilot_summary.get("blocked_attempts", 0)))],
        ["Failed Attempts", html.escape(_fmt_num(live_complete_set_pilot_summary.get("failed_attempts", 0)))],
        ["Matched Live Legs", html.escape(_fmt_num(live_complete_set_pilot_summary.get("matched_live_legs", 0)))],
        ["Resolved Matched Live Legs", html.escape(_fmt_num(live_complete_set_pilot_summary.get("resolved_matched_live_legs", 0)))],
        ["Unresolved Matched Live Legs", html.escape(_fmt_num(live_complete_set_pilot_summary.get("unresolved_matched_live_legs", 0)))],
        ["Matched Leg Total Spend", html.escape(_fmt_money(live_complete_set_pilot_summary.get("matched_live_leg_spend_usdc", 0.0)))],
        ["Resolved Matched Leg Payout", html.escape(_fmt_money(live_complete_set_pilot_summary.get("resolved_matched_live_leg_payout_usdc", 0.0)))],
        ["Resolved Matched Leg PnL", html.escape(_fmt_money(live_complete_set_pilot_summary.get("resolved_matched_live_leg_pnl_usdc", 0.0)))],
        ["Unresolved Matched Leg Spend", html.escape(_fmt_money(live_complete_set_pilot_summary.get("unresolved_matched_live_leg_spend_usdc", 0.0)))],
        ["Unresolved Matched Leg Markets", html.escape(_fmt_num(live_complete_set_pilot_summary.get("unresolved_matched_live_leg_markets", 0)))],
        ["Matched Leg Resolution Source", html.escape(str(live_complete_set_pilot_summary.get("matched_leg_resolution_source", "")))],
        ["Matched Leg Resolution Errors", html.escape(_fmt_num(live_complete_set_pilot_summary.get("matched_leg_resolution_errors", 0)))],
        ["One-Leg Failures", html.escape(_fmt_num(live_complete_set_pilot_summary.get("one_leg_failures", 0)))],
        ["Resolved One-Leg Failures", html.escape(_fmt_num(live_complete_set_pilot_summary.get("resolved_one_leg_failures", 0)))],
        ["Unresolved One-Leg Failures", html.escape(_fmt_num(live_complete_set_pilot_summary.get("unresolved_one_leg_failures", 0)))],
        ["One-Leg Total Spend", html.escape(_fmt_money(live_complete_set_pilot_summary.get("one_leg_spend_usdc", 0.0)))],
        ["Resolved One-Leg Payout", html.escape(_fmt_money(live_complete_set_pilot_summary.get("resolved_one_leg_payout_usdc", 0.0)))],
        ["Resolved One-Leg PnL", html.escape(_fmt_money(live_complete_set_pilot_summary.get("resolved_one_leg_pnl_usdc", 0.0)))],
        ["Unresolved One-Leg Spend", html.escape(_fmt_money(live_complete_set_pilot_summary.get("unresolved_one_leg_spend_usdc", 0.0)))],
        ["Unresolved One-Leg Markets", html.escape(_fmt_num(live_complete_set_pilot_summary.get("unresolved_one_leg_markets", 0)))],
        ["One-Leg Resolution Source", html.escape(str(live_complete_set_pilot_summary.get("one_leg_resolution_source", "")))],
        ["One-Leg Resolution Errors", html.escape(_fmt_num(live_complete_set_pilot_summary.get("one_leg_resolution_errors", 0)))],
        ["Unique Markets Submitted", html.escape(_fmt_num(live_complete_set_pilot_summary.get("unique_markets_submitted", 0)))],
        ["Expected Locked PnL", html.escape(_fmt_money(live_complete_set_pilot_summary.get("expected_locked_pnl_usdc", 0.0)))],
        ["Dry-Run Candidate PnL", html.escape(_fmt_money(live_complete_set_pilot_summary.get("dry_run_candidate_pnl_usdc", 0.0)))],
        ["Local Matched-Leg PnL", html.escape(_fmt_money(live_complete_set_pilot_summary.get("net_pnl", 0.0)))],
        ["24h Local Matched-Leg PnL", html.escape(_fmt_money(live_complete_set_pilot_summary.get("realized_pnl_24h_usdc", 0.0)))],
        ["24h Candidate PnL", html.escape(_fmt_money(live_complete_set_pilot_summary.get("expected_candidate_pnl_24h_usdc", 0.0)))],
        ["Projected Monthly Local PnL", html.escape(_fmt_money(live_complete_set_pilot_summary.get("projected_monthly_revenue_usdc", 0.0)))],
        ["Projected Yearly Local PnL", html.escape(_fmt_money(live_complete_set_pilot_summary.get("projected_yearly_revenue_usdc", 0.0)))],
        ["Max Drawdown", html.escape(_fmt_money(live_complete_set_pilot_summary.get("max_drawdown", 0.0)))],
        ["Attempts 60m", html.escape(_fmt_num(live_complete_set_pilot_summary.get("attempts_60m", 0)))],
        ["Eligible 60m", html.escape(_fmt_num(live_complete_set_pilot_summary.get("eligible_60m", 0)))],
        ["Eligible Rate 60m", html.escape(f"{100.0 * float(live_complete_set_pilot_summary.get('eligible_rate_60m', 0.0)):.1f}%")],
        ["Best Adjusted Edge 60m", html.escape(f"{float(live_complete_set_pilot_summary.get('best_adjusted_edge_60m', 0.0)):.4f}")],
        ["Min Edge / Share", html.escape(f"{float(live_complete_set_pilot_summary.get('min_edge_per_share', 0.0)):.4f}")],
        ["Min Depth", html.escape(_fmt_money(live_complete_set_pilot_summary.get("min_depth_usdc", 0.0)))],
        ["Min Seconds Left", html.escape(f"{_fmt_num(live_complete_set_pilot_summary.get('min_seconds_left', 0))}s")],
        ["Min Leg Amount", html.escape(_fmt_money(live_complete_set_pilot_summary.get("min_leg_amount_usdc", 0.0)))],
        ["Depth Haircut", html.escape(f"{100.0 * float(live_complete_set_pilot_summary.get('depth_haircut', 0.0)):.1f}%")],
        ["Extra Slippage / Share", html.escape(f"{float(live_complete_set_pilot_summary.get('extra_slippage_per_share', 0.0)):.4f}")],
        ["Max Sets / Cycle", html.escape(_fmt_num(live_complete_set_pilot_summary.get("max_sets_per_cycle", 0)))],
        ["Max Open Sets", html.escape(_fmt_num(live_complete_set_pilot_summary.get("max_open_sets", 0)))],
        ["Daily Loss Limit", html.escape(_fmt_money(live_complete_set_pilot_summary.get("daily_loss_limit_usdc", 0.0)))],
        ["Allow Sequential Orders", html.escape(str(bool(live_complete_set_pilot_summary.get("allow_sequential_orders"))).lower())],
        ["Require FOK", html.escape(str(bool(live_complete_set_pilot_summary.get("require_fok"))).lower())],
        ["Rescue Enabled", html.escape(str(bool(live_complete_set_pilot_summary.get("rescue_enabled"))).lower())],
        ["Signature Type", html.escape(_fmt_num(live_complete_set_pilot_summary.get("signature_type", 0)))],
        ["CLOB Host", html.escape(str(live_complete_set_pilot_summary.get("host", "")))],
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
    live_complete_set_pilot_matched_leg_table = _table(
        ["Time (CT)", "Market", "Asset", "Attempt", "Side Held", "Winner", "Status", "Spend", "Shares", "Payout", "PnL", "Question"],
        [
            [
                html.escape(_fmt_ts(item.get("ts"))),
                html.escape(str(item.get("market_id", ""))),
                html.escape(str(item.get("asset", ""))),
                html.escape(str(item.get("decision", ""))),
                html.escape(str(item.get("side", ""))).upper(),
                html.escape(str(item.get("winner", ""))).upper(),
                html.escape(str(item.get("status", ""))),
                html.escape(_fmt_money(item.get("entry_spend_usdc", 0.0))),
                html.escape(f"{float(item.get('shares') or 0.0):.4f}"),
                html.escape(_fmt_money(item.get("payout_usdc", 0.0))),
                html.escape(_fmt_money(item.get("pnl_usdc", 0.0))),
                html.escape(str(item.get("question", ""))[:120]),
            ]
            for item in live_complete_set_arb_pilot.get("matched_leg_reconciliation", [])
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
    interactive_cockpit_html = _render_interactive_cockpit(state)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Latency Bot Dashboard</title>
  <style>
    body {{ font-family: ui-sans-serif, system-ui, sans-serif; margin: 24px; color: #111827; background: #f8fafc; }}
    h1, h2 {{ margin: 0 0 12px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; }}
    .panel {{ background: white; border: 1px solid #dbe4ee; border-radius: 14px; padding: 16px; box-shadow: 0 1px 2px rgba(0,0,0,0.04); }}
    .current-strategy {{ border-color: #0f766e; box-shadow: 0 1px 8px rgba(15, 118, 110, 0.12); }}
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
    <strong>Auto-Refresh:</strong> every 10s |
    <strong>Mode:</strong> {html.escape("live-fast" if fast_mode else "full-research")} |
    <strong>Cache:</strong> {html.escape("hit" if served_from_cache else "fresh")}
  </div>
  <div class="meta">
    {"Live-fast mode skips heavy research panes so real-money pilot status refreshes quickly. Open <a href='?mode=full'>full research mode</a> only when you need the historical simulations." if fast_mode else "Full research mode recomputes historical simulations and can take over a minute. Open <a href='?mode=fast'>live-fast mode</a> for monitoring."}
  </div>
  {interactive_cockpit_html}
  <div class="grid">
    <div class="panel"><h2>Status</h2>{_table(["Metric", "Value"], rows)}</div>
    <div class="panel"><h2>Capital Usage</h2>{_table(["Metric", "Value"], capital_usage_rows)}</div>
    <div class="panel"><h2>Database</h2>{_table(["Metric", "Value"], db_rows)}</div>
    <div class="panel"><h2>Latest Cycle</h2>{_table(["Metric", "Value"], latest_cycle_rows)}</div>
    <div class="panel"><h2>Notes</h2><ul>{notes_html}</ul></div>
  </div>
  <div class="panel current-strategy" style="margin-top:16px;">
    <h2>Current Strategy Iteration</h2>
    <div class="sub">This is the active paper-first maker system: temporal inventory accounting, guarded live maker shadow/live path, and isolated late-resolution capture paper module.</div>
    {_table(["Metric", "Value"], current_strategy_rows)}
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
    <h2>Temporal Inventory Maker Paper Bot</h2>
    <div class="sub">Paper-only inventory lifecycle and simulated post-only maker quoting. It seeds one side only when modeled edge survives conservative buffers, hedges only with actually owned opposite inventory, and counts locked pairs only when owned YES/NO shares match.</div>
    {_table(["Metric", "Value"], temporal_inventory_summary_rows)}
    <h3>Temporal Inventory PnL Curve</h3>
    {_render_equity_curve(temporal_inventory_maker_paper.get("equity_curve", []) if isinstance(temporal_inventory_maker_paper.get("equity_curve"), list) else [], float(temporal_inventory_summary.get("starting_capital_usdc", bankroll_usdc) or bankroll_usdc))}
    <h3>Open Lifecycle Markets</h3>
    {temporal_inventory_market_table}
    <h3>Recent Lifecycle Events</h3>
    <div class="sub">Expected labels include SEED, MAKER_QUOTE, MAKER_FILL, HEDGE, LOCKED_PAIR, ROTATE, SELL, EXPIRE, and RESOLVE. CANCEL is shown when a stale simulated maker quote is pulled.</div>
    {temporal_inventory_event_table}
    <h3>Quote Style Diagnostics</h3>
    {temporal_inventory_style_table}
    <h3>Recent Simulated Maker Quotes</h3>
    {temporal_inventory_quote_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Temporal Inventory Maker</h2>
    <div class="sub">Guarded live maker path for the temporal inventory strategy. Live orders require explicit arming, profitable paper posture, account reconciliation, heartbeat, cancel-all protection, and post-only quote checks. Dry-run mode records candidates without submitting orders.</div>
    {_table(["Metric", "Value"], live_temporal_summary_rows)}
    <h3>Recent Live Maker Decisions</h3>
    {live_temporal_order_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Late Resolution Capture Paper</h2>
    <div class="sub">Separate paper-only capped-risk module for very near-expiry markets. Its PnL is isolated from the temporal inventory maker because this strategy has different tail risk and promotion criteria.</div>
    {_table(["Metric", "Value"], late_resolution_summary_rows)}
    <h3>Late Resolution PnL Curve</h3>
    {_render_equity_curve(late_resolution_capture_paper.get("equity_curve", []) if isinstance(late_resolution_capture_paper.get("equity_curve"), list) else [], float(late_resolution_summary.get("starting_capital_usdc", bankroll_usdc) or bankroll_usdc))}
    <h3>Open Late Resolution Positions</h3>
    {late_resolution_open_table}
    <h3>Recent Late Resolution Closes</h3>
    {late_resolution_close_table}
    <h3>Recent Late Resolution Signals</h3>
    {late_resolution_signal_table}
    <h3>Late Resolution Skip Reasons</h3>
    {late_resolution_reason_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>CEX Latency Paper Bot</h2>
    <div class="sub">Research-only unless recent realized PnL and calibration justify promotion. It gathers Gamma markets, Polymarket CLOB YES/NO books, and Binance prices; entries/exits use book-level VWAP at target notional when available. It does not assume atomic YES+NO arbitrage.</div>
    {_table(["Metric", "Value"], cex_latency_summary_rows)}
    <h3>CEX Latency Paper PnL Curve</h3>
    {_render_equity_curve(cex_latency_paper.get("equity_curve", []) if isinstance(cex_latency_paper.get("equity_curve"), list) else [], float(cex_latency_summary.get("starting_capital_usdc", bankroll_usdc) or bankroll_usdc))}
    <h3>CEX Edge-Floor Sensitivity</h3>
    <div class="sub">Last 24h of recorded CEX signals. Raw ticks are signal observations; unique markets keeps only the best tick per market. Indicative edge dollars are model edge at target notional, not realized PnL.</div>
    {cex_latency_threshold_table}
    <h3>CEX Realized PnL Breakdown</h3>
    <div class="sub">Closed paper trades only. These tables show why the directional CEX paper bot is making or losing money.</div>
    <h4>By Asset / Side</h4>
    {cex_latency_asset_side_table}
    <h4>By Exit Reason</h4>
    {cex_latency_exit_reason_pnl_table}
    <h4>By Entry Price Band</h4>
    {cex_latency_price_band_pnl_table}
    <h4>By Entry Edge Band</h4>
    {cex_latency_edge_band_pnl_table}
    <h3>Open CEX Latency Paper Positions</h3>
    {cex_latency_open_table}
    <h3>Recent CEX Latency Paper Closes</h3>
    {cex_latency_close_table}
    <h3>Recent CEX Latency Paper Signals</h3>
    {cex_latency_signal_table}
    <h3>CEX Latency Skip Reasons</h3>
    {cex_latency_reason_table}
    <h3>Last Cycle Entry Blocks</h3>
    {cex_latency_entry_block_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>BTC Fair-Value Paper Bot</h2>
    <div class="sub">Research-only unless profitable in live paper. It blends Binance-derived fair value, Polymarket microprice, and order-book imbalance, then paper-trades one leg at CLOB VWAP. This does not assume atomic YES+NO execution.</div>
    {_table(["Metric", "Value"], btc_fair_value_summary_rows)}
    <h3>BTC Fair-Value Paper PnL Curve</h3>
    {_render_equity_curve(btc_fair_value_paper.get("equity_curve", []) if isinstance(btc_fair_value_paper.get("equity_curve"), list) else [], float(btc_fair_value_summary.get("starting_capital_usdc", bankroll_usdc) or bankroll_usdc))}
    <h3>Open BTC Fair-Value Paper Positions</h3>
    {btc_fair_value_open_table}
    <h3>Recent BTC Fair-Value Paper Closes</h3>
    {btc_fair_value_close_table}
    <h3>Recent BTC Fair-Value Paper Signals</h3>
    {btc_fair_value_signal_table}
    <h3>BTC Fair-Value Skip Reasons</h3>
    {btc_fair_value_reason_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Wallet Teacher Sniper Paper Bot</h2>
    <div class="sub">Separate $1,000 paper-only target-wallet copy bot. It watches public BUY trades from the configured wallet, matches them to currently tracked 5-minute Polymarket BTC/ETH/SOL markets, and paper-enters the same side only when current CLOB depth is available. This is directional and does not assume atomic YES+NO arbitrage.</div>
    {_table(["Metric", "Value"], wallet_teacher_summary_rows)}
    <h3>Wallet Teacher Paper PnL Curve</h3>
    {_render_equity_curve(wallet_teacher_sniper.get("equity_curve", []) if isinstance(wallet_teacher_sniper.get("equity_curve"), list) else [], float(wallet_teacher_summary.get("starting_capital_usdc", bankroll_usdc) or bankroll_usdc))}
    <h3>Open Wallet Teacher Paper Positions</h3>
    {wallet_teacher_open_table}
    <h3>Recent Wallet Teacher Paper Closes</h3>
    {wallet_teacher_close_table}
    <h3>Recent Wallet Teacher Signals</h3>
    {wallet_teacher_signal_table}
    <h3>Wallet Teacher Skip Reasons</h3>
    {wallet_teacher_reason_table}
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
    <h2>Pre-Owned Inventory Arb Paper Simulation</h2>
    <div class="sub">Research-only paper replay for an inventory-backed arb. It seeds small YES and NO buffers, then counts an arb only when one live buy can pair against already-owned opposite inventory. This avoids pretending paired FOK is atomic, but exposes how much capital gets trapped in unused inventory.</div>
    {_table(["Metric", "Value"], preowned_inventory_summary_rows)}
    <h3>Inventory PnL Curve</h3>
    {_render_equity_curve(preowned_inventory_arb.get("equity_curve", []) if isinstance(preowned_inventory_arb.get("equity_curve"), list) else [], float(preowned_inventory_summary.get("simulated_capital_usdc", bankroll_usdc) or bankroll_usdc))}
    <h3>Recent Inventory Sim Events</h3>
    {preowned_inventory_event_table}
    <h3>Inventory Skip / Block Reasons</h3>
    {preowned_inventory_reason_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Polymarket Account Reconciliation</h2>
    <div class="sub">Authoritative CLOB account probe for the live wallet. Use this pane to compare Polymarket cash and real CLOB trades against the local bot ledger. Polymarket UI P/L is still an exchange-side display and may include timing/settlement effects not present in local reconstruction.</div>
    {_table(["Metric", "Value"], account_reconciliation_summary_rows)}
    <h3>Recent CLOB Account Trades</h3>
    {account_reconciliation_trade_table}
    <h3>CLOB Open Orders</h3>
    {account_reconciliation_open_order_table}
    <h3>CLOB Open Positions</h3>
    {account_reconciliation_position_table}
  </div>
  <div class="panel" style="margin-top:16px;">
    <h2>Live Complete-Set Arb Pilot - Unsafe Non-Atomic Legacy</h2>
    <div class="sub">Legacy live-pilot tracker kept visible for reconciliation only. This path can create directional one-leg exposure when paired fills are not truly atomic; it is not the decision path for new live deployment. Polymarket account cash/P&amp;L is authoritative.</div>
    {_table(["Metric", "Value"], live_complete_set_pilot_summary_rows)}
    <h3>Local Matched-Leg PnL Curve (Not Account Value)</h3>
    {_render_equity_curve(live_complete_set_arb_pilot.get("equity_curve", []) if isinstance(live_complete_set_arb_pilot.get("equity_curve"), list) else [], float(live_complete_set_pilot_summary.get("simulated_or_live_capital_usdc", bankroll_usdc) or bankroll_usdc))}
    <h3>Recent Live Pilot Attempts</h3>
    {live_complete_set_pilot_attempt_table}
    <h3>Matched Live-Leg Reconciliation</h3>
    {live_complete_set_pilot_matched_leg_table}
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
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            requested_mode = str((query.get("mode") or [""])[0]).strip().lower()
            env_fast = str(os.getenv("LATENCY_BOT_DASHBOARD_FAST", "1")).strip().lower() not in {"0", "false", "no"}
            fast = requested_mode != "full" if requested_mode else env_fast
            state = build_latency_bot_dashboard_state(settings, fast=fast)
            if parsed.path.rstrip("/") == "/api/state":
                payload = _serialize_interactive_dashboard_payload(state).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            body = render_latency_bot_dashboard_html(state).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer((host, port), Handler)
    print(f"latency bot dashboard listening on http://{host}:{port}")
    server.serve_forever()
