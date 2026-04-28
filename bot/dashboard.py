from __future__ import annotations

import html
import json
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from statistics import mean
from typing import Any

from bot.accounting import dedupe_closed_trades, rebuild_closed_trades
from bot.config import Settings


def _json_load(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _fmt_money(value: float) -> str:
    return f"${value:,.2f}"


def _fmt_num(value: float | int) -> str:
    if isinstance(value, int) or float(value).is_integer():
        return f"{int(value)}"
    return f"{float(value):,.2f}"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.1f}%"


def _fmt_ts(value: str | None) -> str:
    if not value:
        return "-"
    return value


def _as_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _normalize_category(raw: str | None) -> str:
    if not raw:
        return "unknown"
    value = raw.strip().lower()
    if not value or len(value) > 32 or "-" in value:
        return "unknown"
    aliases = {
        "cryptocurrency": "crypto",
        "blockchain": "crypto",
        "economy": "macro",
        "economic": "macro",
        "finance": "macro",
        "business": "macro",
        "elections": "politics",
        "government": "politics",
        "technology": "tech",
        "entertainment": "culture",
    }
    return aliases.get(value, value.replace("/", " ").replace("_", " ").split()[0])


def _categorize_text(question: str, hint: str = "") -> str:
    text = f"{question} {hint}".lower()
    keyword_groups = {
        "crypto": ("bitcoin", "ethereum", "eth", "btc", "token", "airdrop", "solana", "fdv", "launch"),
        "politics": ("president", "senate", "house", "election", "minister", "government", "sentence", "court"),
        "macro": ("fed", "inflation", "cpi", "gdp", "treasury", "recession", "tariff", "oil", "gold", "stocks"),
        "sports": ("nba", "nfl", "mlb", "nhl", "championship", "world cup", "goal", "touchdown"),
        "tech": ("openai", "chatgpt", "ai", "tesla", "apple", "google", "meta"),
        "culture": ("movie", "oscar", "grammy", "celebrity"),
    }
    for category, keywords in keyword_groups.items():
        if any(keyword in text for keyword in keywords):
            return category
    return "unknown"


def _active_rotation_category(settings: Settings) -> str | None:
    if not settings.category_rotation:
        return None
    rotation_days = max(settings.category_rotation_days, 1)
    bucket = datetime.now(timezone.utc).toordinal() // rotation_days
    return settings.category_rotation[bucket % len(settings.category_rotation)]


def _trade_category(trade: dict[str, Any], opened_by_position: dict[str, dict[str, Any]]) -> str:
    direct = _normalize_category(str(trade.get("category", "") or ""))
    if direct != "unknown":
        return direct
    opened = opened_by_position.get(str(trade.get("position_id", "")), {})
    opened_category = _normalize_category(str(opened.get("category", "") or ""))
    if opened_category != "unknown":
        return opened_category
    raw_hint = str(trade.get("category", "") or opened.get("category", "") or "")
    return _categorize_text(str(trade.get("question", "")), raw_hint)


def _display_category(item: dict[str, Any]) -> str:
    direct = _normalize_category(str(item.get("category", "") or ""))
    if direct != "unknown":
        return direct
    return _categorize_text(str(item.get("question", "")), str(item.get("category", "")))


def _build_lifetime_stats(trades: list[dict[str, Any]], bankroll_usdc: float, unrealized_pnl_usdc: float) -> dict[str, Any]:
    opens = [trade for trade in trades if trade.get("type") == "OPEN"]
    closes = rebuild_closed_trades(trades)
    winners = [trade for trade in closes if _as_float(trade.get("pnl_usdc")) > 0]
    losers = [trade for trade in closes if _as_float(trade.get("pnl_usdc")) < 0]
    breakeven = [trade for trade in closes if _as_float(trade.get("pnl_usdc")) == 0]
    gross_profit = round(sum(_as_float(trade.get("pnl_usdc")) for trade in winners), 2)
    gross_loss = round(sum(abs(_as_float(trade.get("pnl_usdc"))) for trade in losers), 2)
    realized_pnl = round(sum(_as_float(trade.get("pnl_usdc")) for trade in closes), 2)
    closed_count = len(closes)
    win_rate = round((len(winners) / closed_count) * 100.0, 1) if closed_count else None
    profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else None
    avg_winner = round(mean(_as_float(trade.get("pnl_usdc")) for trade in winners), 2) if winners else None
    avg_loser = round(mean(_as_float(trade.get("pnl_usdc")) for trade in losers), 2) if losers else None
    best_trade = max(closes, key=lambda trade: _as_float(trade.get("pnl_usdc")), default=None)
    worst_trade = min(closes, key=lambda trade: _as_float(trade.get("pnl_usdc")), default=None)

    opened_by_position = {
        str(trade.get("position_id", "")): trade
        for trade in opens
        if trade.get("position_id")
    }
    holding_minutes: list[float] = []
    for close_trade in closes:
        opened = opened_by_position.get(str(close_trade.get("position_id", "")))
        opened_at = _parse_ts(opened.get("opened_at")) if opened else None
        closed_at = _parse_ts(close_trade.get("closed_at"))
        if opened_at and closed_at and closed_at >= opened_at:
            holding_minutes.append((closed_at - opened_at).total_seconds() / 60.0)

    curve_points = []
    running_pnl = 0.0
    sorted_closes = sorted(closes, key=lambda trade: _parse_ts(trade.get("closed_at")) or datetime.min.replace(tzinfo=timezone.utc))
    for idx, trade in enumerate(sorted_closes, start=1):
        running_pnl = round(running_pnl + _as_float(trade.get("pnl_usdc")), 2)
        curve_points.append(
            {
                "index": idx,
                "timestamp": trade.get("closed_at"),
                "equity_usdc": round(bankroll_usdc + running_pnl, 2),
                "realized_pnl_usdc": running_pnl,
                "question": trade.get("question", ""),
            }
        )

    return {
        "opened_count": len(opens),
        "closed_count": closed_count,
        "winners_count": len(winners),
        "losers_count": len(losers),
        "breakeven_count": len(breakeven),
        "win_rate_pct": win_rate,
        "gross_profit_usdc": gross_profit,
        "gross_loss_usdc": gross_loss,
        "realized_pnl_usdc": realized_pnl,
        "unrealized_pnl_usdc": round(unrealized_pnl_usdc, 2),
        "net_total_pnl_usdc": round(realized_pnl + unrealized_pnl_usdc, 2),
        "profit_factor": profit_factor,
        "avg_winner_usdc": avg_winner,
        "avg_loser_usdc": avg_loser,
        "avg_holding_minutes": round(mean(holding_minutes), 1) if holding_minutes else None,
        "best_trade": best_trade,
        "worst_trade": worst_trade,
        "equity_curve": curve_points,
    }


def _build_trade_analytics(trades: list[dict[str, Any]], positions: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    opens = [trade for trade in trades if trade.get("type") == "OPEN"]
    closes = rebuild_closed_trades(trades)
    opened_by_position = {
        str(trade.get("position_id", "")): trade
        for trade in opens
        if trade.get("position_id")
    }

    category_rows: dict[str, dict[str, Any]] = {}
    market_rows: dict[str, dict[str, Any]] = {}

    for position in positions:
        category = _trade_category(position, opened_by_position)
        category_rows.setdefault(
            category,
            {"category": category, "closed_trades": 0, "wins": 0, "realized_pnl_usdc": 0.0, "open_positions": 0},
        )
        category_rows[category]["open_positions"] += 1

    for trade in closes:
        pnl = _as_float(trade.get("pnl_usdc"), 0.0)
        category = _trade_category(trade, opened_by_position)
        category_entry = category_rows.setdefault(
            category,
            {"category": category, "closed_trades": 0, "wins": 0, "realized_pnl_usdc": 0.0, "open_positions": 0},
        )
        category_entry["closed_trades"] += 1
        category_entry["wins"] += 1 if pnl > 0 else 0
        category_entry["realized_pnl_usdc"] = round(category_entry["realized_pnl_usdc"] + pnl, 2)

        market_key = str(trade.get("question", "")).strip() or str(trade.get("market_id", ""))
        market_entry = market_rows.setdefault(
            market_key,
            {
                "question": market_key,
                "category": category,
                "closed_trades": 0,
                "wins": 0,
                "realized_pnl_usdc": 0.0,
            },
        )
        market_entry["closed_trades"] += 1
        market_entry["wins"] += 1 if pnl > 0 else 0
        market_entry["realized_pnl_usdc"] = round(market_entry["realized_pnl_usdc"] + pnl, 2)

    category_analytics = []
    for item in category_rows.values():
        closed_trades = int(item["closed_trades"])
        item["win_rate_pct"] = round((item["wins"] / closed_trades) * 100.0, 1) if closed_trades else None
        item["avg_pnl_usdc"] = round(item["realized_pnl_usdc"] / closed_trades, 2) if closed_trades else None
        category_analytics.append(item)
    category_analytics.sort(key=lambda item: (item["realized_pnl_usdc"], item["open_positions"]), reverse=True)

    market_analytics = []
    for item in market_rows.values():
        closed_trades = int(item["closed_trades"])
        item["win_rate_pct"] = round((item["wins"] / closed_trades) * 100.0, 1) if closed_trades else None
        item["avg_pnl_usdc"] = round(item["realized_pnl_usdc"] / closed_trades, 2) if closed_trades else None
        market_analytics.append(item)
    market_analytics.sort(key=lambda item: item["realized_pnl_usdc"], reverse=True)

    return {
        "categories": category_analytics[:12],
        "markets": market_analytics[:12],
    }


def _render_equity_curve(points: list[dict[str, Any]], bankroll_usdc: float) -> str:
    if not points:
        return "<div class='sub'>No closed trades yet.</div>"

    width = 960
    height = 220
    pad_x = 18
    pad_y = 18
    values = [bankroll_usdc] + [_as_float(point.get("equity_usdc"), bankroll_usdc) for point in points]
    min_value = min(values)
    max_value = max(values)
    if max_value == min_value:
        max_value += 1.0
        min_value -= 1.0

    def x_for(idx: int) -> float:
        if len(points) == 1:
            return width / 2
        return pad_x + ((idx - 1) / (len(points) - 1)) * (width - (pad_x * 2))

    def y_for(value: float) -> float:
        ratio = (value - min_value) / (max_value - min_value)
        return height - pad_y - (ratio * (height - (pad_y * 2)))

    polyline = " ".join(f"{x_for(idx):.1f},{y_for(_as_float(point.get('equity_usdc'), bankroll_usdc)):.1f}" for idx, point in enumerate(points, start=1))
    baseline = y_for(bankroll_usdc)
    last = points[-1]
    return (
        f"<svg viewBox='0 0 {width} {height}' class='curve' role='img' aria-label='Equity curve'>"
        f"<line x1='{pad_x}' y1='{baseline:.1f}' x2='{width - pad_x}' y2='{baseline:.1f}' class='curve-baseline' />"
        f"<polyline points='{polyline}' class='curve-line' />"
        f"<circle cx='{x_for(len(points)):.1f}' cy='{y_for(_as_float(last.get('equity_usdc'), bankroll_usdc)):.1f}' r='4' class='curve-dot' />"
        "</svg>"
    )


def _shadow_paths(suffix: str) -> dict[str, Path]:
    return {
        "positions": Path(f"state/{suffix}_positions.json"),
        "trades": Path(f"state/{suffix}_trades.json"),
        "marks": Path(f"state/{suffix}_marks.json"),
    }


def _build_shadow_portfolio_state(name: str, bankroll_usdc: float) -> dict[str, Any]:
    paths = _shadow_paths(name)
    positions = _json_load(paths["positions"], [])
    trades = _json_load(paths["trades"], [])
    marks = _json_load(paths["marks"], {"positions": [], "summary": {}})
    rebuilt_closes = rebuild_closed_trades(trades)
    realized_pnl = round(sum(_as_float(trade.get("pnl_usdc", 0.0)) for trade in rebuilt_closes), 2)
    unrealized_pnl = round(_as_float(marks.get("summary", {}).get("total_unrealized_pnl_usdc"), 0.0), 2)
    lifetime = _build_lifetime_stats(trades, bankroll_usdc, unrealized_pnl)
    return {
        "name": name,
        "positions": positions,
        "trades": trades,
        "realized_pnl_usdc": realized_pnl,
        "unrealized_pnl_usdc": unrealized_pnl,
        "equity_including_open_usdc": round(bankroll_usdc + realized_pnl + unrealized_pnl, 2),
        "open_positions": len(positions),
        "open_notional_usdc": round(sum(float(item.get("notional_usdc", 0.0)) for item in positions), 2),
        "lifetime": lifetime,
    }


def build_dashboard_state(settings: Settings) -> dict[str, Any]:
    positions = _json_load(settings.positions_path, [])
    trades = _json_load(settings.trades_path, [])
    queue = _json_load(settings.queue_path, [])
    theses = _json_load(settings.theses_path, [])
    marks = _json_load(settings.marks_path, {"positions": [], "summary": {}})
    missed = _json_load(settings.missed_opportunities_path, {"items": [], "updated_at": None})
    last_nonempty_queue = _json_load(settings.last_nonempty_queue_path, {"items": [], "updated_at": None})
    last_nonempty_theses = _json_load(settings.last_nonempty_theses_path, {"items": [], "updated_at": None})
    targets = _json_load(settings.targets_path, [])
    activity = _json_load(settings.target_activity_path, {"wallets": []})
    status = _json_load(settings.status_path, {})

    closed = dedupe_closed_trades(trades)
    rebuilt_closes = rebuild_closed_trades(trades)
    corrected_close_by_position = {
        str(trade.get("position_id", "")): trade
        for trade in rebuilt_closes
        if trade.get("position_id")
    }
    realized_pnl = round(sum(_as_float(trade.get("pnl_usdc", 0.0)) for trade in rebuilt_closes), 2)
    compounded_bankroll = round(max(settings.bankroll_usdc + realized_pnl, 0.0), 2)
    open_notional = round(sum(float(item.get("notional_usdc", 0.0)) for item in positions), 2)
    unrealized_pnl = round(_as_float(marks.get("summary", {}).get("total_unrealized_pnl_usdc"), 0.0), 2)
    equity_including_open = round(compounded_bankroll + unrealized_pnl, 2)
    marks_by_position = {item["position_id"]: item for item in marks.get("positions", [])}
    enriched_positions = []
    for item in positions:
        mark = marks_by_position.get(item["position_id"], {})
        enriched_positions.append({**item, **mark})

    latest_cycle = status.get("last_cycle_result", {})
    opened_this_cycle = [item for item in latest_cycle.get("trade_decisions", []) if item.get("action") == "OPEN"]
    scaled_this_cycle = [item for item in latest_cycle.get("trade_decisions", []) if item.get("action") == "SCALE"]
    lifetime = _build_lifetime_stats(trades, settings.bankroll_usdc, unrealized_pnl)
    analytics = _build_trade_analytics(trades, positions)
    active_rotation = _active_rotation_category(settings)
    ab_tests = {}
    if settings.ab_test_enabled and settings.ab_test_strategy == "wallet_copy":
        ab_tests["wallet_copy"] = _build_shadow_portfolio_state("ab_wallet_copy", settings.bankroll_usdc)
        ab_tests["wallet_copy_aggressive"] = _build_shadow_portfolio_state("ab_wallet_copy_aggressive", settings.bankroll_usdc)
    display_trades = []
    for trade in trades:
        if trade.get("type") == "CLOSE":
            corrected = corrected_close_by_position.get(str(trade.get("position_id", "")))
            if corrected:
                display_trades.append({**trade, **{k: v for k, v in corrected.items() if k in {"pnl_usdc", "shares", "notional_usdc", "entry_price", "category", "side"}}})
                continue
        display_trades.append(trade)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "mode": settings.bot_mode,
            "starting_bankroll_usdc": settings.bankroll_usdc,
            "bankroll_usdc": compounded_bankroll,
            "compounded_bankroll_usdc": compounded_bankroll,
            "equity_including_open_usdc": equity_including_open,
            "open_positions": len(positions),
            "closed_trades": len(rebuilt_closes),
            "realized_pnl_usdc": realized_pnl,
            "unrealized_pnl_usdc": unrealized_pnl,
            "open_notional_usdc": open_notional,
            "queue_count": len(queue),
            "thesis_count": len(theses),
            "last_nonempty_queue_count": len(last_nonempty_queue.get("items", [])),
            "last_nonempty_thesis_count": len(last_nonempty_theses.get("items", [])),
            "opened_this_cycle": len(opened_this_cycle),
            "scaled_this_cycle": len(scaled_this_cycle),
            "targets_count": len(targets),
            "tracked_wallets": len(activity.get("wallets", [])),
            "lifetime_win_rate_pct": lifetime["win_rate_pct"],
            "lifetime_profit_factor": lifetime["profit_factor"],
            "lifetime_net_total_pnl_usdc": lifetime["net_total_pnl_usdc"],
            "active_rotation_category": active_rotation,
            "market_cooldown_minutes": settings.market_cooldown_minutes,
            "max_position_usdc": round(compounded_bankroll * settings.max_position_fraction, 2),
            "max_portfolio_usdc": round(compounded_bankroll * settings.max_portfolio_fraction, 2),
            "scale_in_enabled": settings.scale_in_enabled,
        },
        "status": status,
        "positions": enriched_positions[-25:],
        "trades": display_trades[-50:],
        "queue": queue[:25],
        "missed": missed.get("items", latest_cycle.get("missed_opportunities", []))[:15],
        "theses": theses[:25],
        "last_nonempty_queue": last_nonempty_queue,
        "last_nonempty_theses": last_nonempty_theses,
        "targets": targets[:25],
        "lifetime": lifetime,
        "analytics": analytics,
        "ab_tests": ab_tests,
    }


def _table(headers: list[str], rows: list[list[str]]) -> str:
    head = "".join(f"<th>{html.escape(header)}</th>" for header in headers)
    body_rows = []
    for row in rows:
        body_rows.append("<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>")
    body = "".join(body_rows) or '<tr><td colspan="99">No data</td></tr>'
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def render_dashboard_html(state: dict[str, Any]) -> str:
    summary = state["summary"]
    status = state.get("status", {})
    lifetime = state["lifetime"]
    analytics = state["analytics"]
    ab_tests = state.get("ab_tests", {})
    cards = "".join(
        [
            f"<div class='card'><div class='label'>Mode</div><div class='value'>{html.escape(str(summary['mode']))}</div></div>",
            f"<div class='card'><div class='label'>Starting Bankroll</div><div class='value'>{_fmt_money(summary['starting_bankroll_usdc'])}</div></div>",
            f"<div class='card'><div class='label'>Compounded Bankroll</div><div class='value'>{_fmt_money(summary['compounded_bankroll_usdc'])}</div></div>",
            f"<div class='card'><div class='label'>Realized PnL</div><div class='value'>{_fmt_money(summary['realized_pnl_usdc'])}</div></div>",
            f"<div class='card'><div class='label'>Unrealized PnL</div><div class='value'>{_fmt_money(summary['unrealized_pnl_usdc'])}</div></div>",
            f"<div class='card'><div class='label'>Net Total PnL</div><div class='value'>{_fmt_money(summary['lifetime_net_total_pnl_usdc'])}</div></div>",
            f"<div class='card'><div class='label'>Equity incl. Open</div><div class='value'>{_fmt_money(summary['equity_including_open_usdc'])}</div></div>",
            f"<div class='card'><div class='label'>Open Capital</div><div class='value'>{_fmt_money(summary['open_notional_usdc'])}</div></div>",
            f"<div class='card'><div class='label'>Open Positions</div><div class='value'>{summary['open_positions']}</div></div>",
            f"<div class='card'><div class='label'>Latest Queue</div><div class='value'>{summary['queue_count']}</div></div>",
            f"<div class='card'><div class='label'>Latest Theses</div><div class='value'>{summary['thesis_count']}</div></div>",
            f"<div class='card'><div class='label'>Last Non-Empty Queue</div><div class='value'>{summary['last_nonempty_queue_count']}</div></div>",
            f"<div class='card'><div class='label'>Opened This Cycle</div><div class='value'>{summary['opened_this_cycle']}</div></div>",
            f"<div class='card'><div class='label'>Scaled This Cycle</div><div class='value'>{summary['scaled_this_cycle']}</div></div>",
            f"<div class='card'><div class='label'>Lifetime Win Rate</div><div class='value'>{html.escape(_fmt_pct(summary['lifetime_win_rate_pct']))}</div></div>",
            f"<div class='card'><div class='label'>Profit Factor</div><div class='value'>{html.escape(_fmt_num(summary['lifetime_profit_factor']) if summary['lifetime_profit_factor'] is not None else '-')}</div></div>",
            f"<div class='card'><div class='label'>Active Rotation</div><div class='value'>{html.escape(str(summary['active_rotation_category'] or 'all'))}</div></div>",
            f"<div class='card'><div class='label'>Cooldown</div><div class='value'>{html.escape(str(summary['market_cooldown_minutes']))}m</div></div>",
            f"<div class='card'><div class='label'>Max Position</div><div class='value'>{_fmt_money(summary['max_position_usdc'])}</div></div>",
            f"<div class='card'><div class='label'>Max Portfolio</div><div class='value'>{_fmt_money(summary['max_portfolio_usdc'])}</div></div>",
            f"<div class='card'><div class='label'>Scale-In</div><div class='value'>{'on' if summary['scale_in_enabled'] else 'off'}</div></div>",
            f"<div class='card'><div class='label'>Targets</div><div class='value'>{summary['targets_count']}</div></div>",
        ]
    )
    for key, label in [("wallet_copy", "A/B Wallet Copy"), ("wallet_copy_aggressive", "A/B Wallet Copy Aggressive")]:
        if key not in ab_tests:
            continue
        portfolio = ab_tests[key]
        cards += "".join(
            [
                f"<div class='card'><div class='label'>{label} PnL</div><div class='value'>{_fmt_money(portfolio['lifetime']['net_total_pnl_usdc'])}</div></div>",
                f"<div class='card'><div class='label'>{label} Win Rate</div><div class='value'>{html.escape(_fmt_pct(portfolio['lifetime']['win_rate_pct']))}</div></div>",
                f"<div class='card'><div class='label'>{label} Open</div><div class='value'>{portfolio['open_positions']}</div></div>",
            ]
        )

    lifetime_table = _table(
        ["Metric", "Value"],
        [
            ["Starting Bankroll", html.escape(_fmt_money(summary["starting_bankroll_usdc"]))],
            ["Compounded Bankroll", html.escape(_fmt_money(summary["compounded_bankroll_usdc"]))],
            ["Equity Including Open PnL", html.escape(_fmt_money(summary["equity_including_open_usdc"]))],
            ["Total Opens", html.escape(_fmt_num(lifetime["opened_count"]))],
            ["Total Closes", html.escape(_fmt_num(lifetime["closed_count"]))],
            ["Winners / Losers / Flat", html.escape(f"{lifetime['winners_count']} / {lifetime['losers_count']} / {lifetime['breakeven_count']}")],
            ["Win Rate", html.escape(_fmt_pct(lifetime["win_rate_pct"]))],
            ["Gross Profit", html.escape(_fmt_money(lifetime["gross_profit_usdc"]))],
            ["Gross Loss", html.escape(_fmt_money(-lifetime["gross_loss_usdc"]))],
            ["Profit Factor", html.escape(_fmt_num(lifetime["profit_factor"]) if lifetime["profit_factor"] is not None else "-")],
            ["Average Winner", html.escape(_fmt_money(lifetime["avg_winner_usdc"]) if lifetime["avg_winner_usdc"] is not None else "-")],
            ["Average Loser", html.escape(_fmt_money(lifetime["avg_loser_usdc"]) if lifetime["avg_loser_usdc"] is not None else "-")],
            ["Average Hold", html.escape(f"{_fmt_num(lifetime['avg_holding_minutes'])} min" if lifetime["avg_holding_minutes"] is not None else "-")],
            [
                "Best Trade",
                html.escape(
                    f"{lifetime['best_trade'].get('question', '')} ({_fmt_money(_as_float(lifetime['best_trade'].get('pnl_usdc')) )})"
                    if lifetime["best_trade"]
                    else "-"
                ),
            ],
            [
                "Worst Trade",
                html.escape(
                    f"{lifetime['worst_trade'].get('question', '')} ({_fmt_money(_as_float(lifetime['worst_trade'].get('pnl_usdc')) )})"
                    if lifetime["worst_trade"]
                    else "-"
                ),
            ],
        ],
    )

    positions_table = _table(
        ["Question", "Category", "Side", "Entry", "Mark", "uPnL", "Notional", "Opened"],
        [
            [
                html.escape(str(item.get("question", ""))),
                html.escape(_display_category(item)),
                html.escape(str(item.get("side", ""))),
                f"{float(item.get('entry_price', 0.0)):.3f}",
                f"{float(item.get('current_midpoint', item.get('entry_price', 0.0))):.3f}",
                _fmt_money(float(item.get("unrealized_pnl_usdc", 0.0))),
                _fmt_money(float(item.get("notional_usdc", 0.0))),
                html.escape(_fmt_ts(item.get("opened_at"))),
            ]
            for item in reversed(state["positions"])
        ],
    )
    trades_table = _table(
        ["Type", "Question", "Category", "Notional", "PnL", "Reason", "Time"],
        [
            [
                html.escape(str(item.get("type", ""))),
                html.escape(str(item.get("question", ""))),
                html.escape(_display_category(item)),
                _fmt_money(float(item.get("add_notional_usdc", item.get("notional_usdc", 0.0)) or 0.0)),
                _fmt_money(float(item.get("pnl_usdc", 0.0))),
                html.escape(str(item.get("reason", ""))),
                html.escape(_fmt_ts(item.get("closed_at") or item.get("scaled_at") or item.get("opened_at"))),
            ]
            for item in reversed(state["trades"])
        ],
    )
    queue_table = _table(
        ["Question", "Category", "Mid", "Depth", "Hours", "Spread", "Score"],
        [
            [
                html.escape(str(item.get("question", ""))),
                html.escape(_display_category(item)),
                f"{float(item.get('midpoint', 0.0)):.3f}",
                _fmt_money(min(float(item.get("bids_depth", 0.0)), float(item.get("asks_depth", 0.0)))),
                f"{float(item.get('hours_to_resolution', 0.0)):.1f}",
                f"{float(item.get('spread', 0.0)):.3f}",
                f"{float(item.get('priority_score', 0.0)):.2f}",
            ]
            for item in state["queue"]
        ],
    )
    last_nonempty_queue_table = _table(
        ["Question", "Category", "Mid", "Depth", "Hours", "Spread", "Score"],
        [
            [
                html.escape(str(item.get("question", ""))),
                html.escape(_display_category(item)),
                f"{float(item.get('midpoint', 0.0)):.3f}",
                _fmt_money(min(float(item.get("bids_depth", 0.0)), float(item.get("asks_depth", 0.0)))),
                f"{float(item.get('hours_to_resolution', 0.0)):.1f}",
                f"{float(item.get('spread', 0.0)):.3f}",
                f"{float(item.get('priority_score', 0.0)):.2f}",
            ]
            for item in state["last_nonempty_queue"].get("items", [])[:25]
        ],
    )
    missed_table = _table(
        ["Question", "Category", "Reason", "Mid", "Score"],
        [
            [
                html.escape(str(item.get("question", ""))),
                html.escape(_display_category(item)),
                html.escape(str(item.get("reason", ""))),
                f"{float(item.get('midpoint', 0.0)):.3f}",
                f"{float(item.get('priority_score', 0.0)):.2f}",
            ]
            for item in state["missed"]
        ],
    )
    category_analytics_table = _table(
        ["Category", "Closed", "Open", "Win Rate", "Realized", "Avg/Trade"],
        [
            [
                html.escape(str(item.get("category", "unknown"))),
                html.escape(_fmt_num(item.get("closed_trades", 0))),
                html.escape(_fmt_num(item.get("open_positions", 0))),
                html.escape(_fmt_pct(item.get("win_rate_pct"))),
                html.escape(_fmt_money(_as_float(item.get("realized_pnl_usdc"), 0.0))),
                html.escape(_fmt_money(_as_float(item.get("avg_pnl_usdc"), 0.0)) if item.get("avg_pnl_usdc") is not None else "-"),
            ]
            for item in analytics["categories"]
        ],
    )
    market_analytics_table = _table(
        ["Market", "Category", "Closed", "Win Rate", "Realized", "Avg/Trade"],
        [
            [
                html.escape(str(item.get("question", ""))),
                html.escape(str(item.get("category", "unknown"))),
                html.escape(_fmt_num(item.get("closed_trades", 0))),
                html.escape(_fmt_pct(item.get("win_rate_pct"))),
                html.escape(_fmt_money(_as_float(item.get("realized_pnl_usdc"), 0.0))),
                html.escape(_fmt_money(_as_float(item.get("avg_pnl_usdc"), 0.0)) if item.get("avg_pnl_usdc") is not None else "-"),
            ]
            for item in analytics["markets"]
        ],
    )
    targets_table = _table(
        ["Wallet", "Username", "PnL", "Volume", "Rank"],
        [
            [
                html.escape(str(item.get("wallet", ""))),
                html.escape(str(item.get("username", ""))),
                _fmt_money(float(item.get("total_pnl", 0.0))),
                _fmt_money(float(item.get("volume", item.get("notional_usdc", 0.0)))),
                html.escape(str(item.get("rank", ""))),
            ]
            for item in state["targets"]
        ],
    )
    ab_panels = []
    for key, label in [("wallet_copy", "A/B Wallet Copy"), ("wallet_copy_aggressive", "A/B Wallet Copy Aggressive")]:
        if key not in ab_tests:
            continue
        portfolio = ab_tests[key]
        table = _table(
            ["Metric", "Value"],
            [
                ["Strategy", key],
                ["Realized PnL", html.escape(_fmt_money(portfolio["realized_pnl_usdc"]))],
                ["Unrealized PnL", html.escape(_fmt_money(portfolio["unrealized_pnl_usdc"]))],
                ["Net Total PnL", html.escape(_fmt_money(portfolio["lifetime"]["net_total_pnl_usdc"]))],
                ["Open Positions", html.escape(_fmt_num(portfolio["open_positions"]))],
                ["Open Capital", html.escape(_fmt_money(portfolio["open_notional_usdc"]))],
                ["Win Rate", html.escape(_fmt_pct(portfolio["lifetime"]["win_rate_pct"]))],
                ["Profit Factor", html.escape(_fmt_num(portfolio["lifetime"]["profit_factor"]) if portfolio["lifetime"]["profit_factor"] is not None else "-")],
                ["Total Closes", html.escape(_fmt_num(portfolio["lifetime"]["closed_count"]))],
            ],
        )
        ab_panels.append(
            '<div class="panel"><h2>'
            + label
            + "</h2>"
            + table
            + _render_equity_curve(portfolio["lifetime"]["equity_curve"], summary["starting_bankroll_usdc"])
            + "</div>"
        )
    ab_panels_html = "".join(ab_panels)

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Polymarket Paper Bot</title>
  <meta http-equiv="refresh" content="10">
  <style>
    :root {{
      --bg: #f4f0e8;
      --ink: #18211f;
      --muted: #5a6762;
      --panel: #fffdf8;
      --line: #d8d0c2;
      --accent: #0d7a5f;
      --accent-2: #b85c38;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: Georgia, "Iowan Old Style", serif; color: var(--ink); background:
      radial-gradient(circle at top left, #fff7df 0, transparent 28%),
      radial-gradient(circle at top right, #dff5ee 0, transparent 24%),
      linear-gradient(180deg, #efe7d9, var(--bg)); }}
    .wrap {{ max-width: 1200px; margin: 0 auto; padding: 24px; }}
    h1, h2 {{ margin: 0 0 12px; }}
    .sub {{ color: var(--muted); margin-bottom: 24px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-bottom: 24px; }}
    .card, .panel {{ background: var(--panel); border: 1px solid var(--line); border-radius: 16px; padding: 16px; box-shadow: 0 6px 24px rgba(24,33,31,.06); }}
    .card .label {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .08em; }}
    .card .value {{ font-size: 28px; margin-top: 8px; }}
    .panels {{ display: grid; grid-template-columns: 1fr; gap: 16px; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ text-align: left; padding: 10px 8px; border-bottom: 1px solid var(--line); vertical-align: top; }}
    th {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .08em; }}
    .status {{ display: inline-block; padding: 6px 10px; border-radius: 999px; background: #e4f5ef; color: var(--accent); font-weight: 700; }}
    .error {{ color: var(--accent-2); white-space: pre-wrap; }}
    .curve {{ width: 100%; height: auto; display: block; margin-top: 8px; overflow: visible; }}
    .curve-line {{ fill: none; stroke: var(--accent); stroke-width: 3; stroke-linecap: round; stroke-linejoin: round; }}
    .curve-baseline {{ stroke: var(--line); stroke-dasharray: 6 4; stroke-width: 1.5; }}
    .curve-dot {{ fill: var(--accent-2); }}
    @media (max-width: 768px) {{
      .wrap {{ padding: 16px; }}
      .card .value {{ font-size: 24px; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Polymarket Paper Bot</h1>
    <div class="sub">Generated at {html.escape(state["generated_at"])}. Auto-refresh every 10 seconds.</div>
    <div class="grid">{cards}</div>
    <div class="panel" style="margin-bottom:16px;">
      <h2>Runner Status</h2>
      <div class="sub"><span class="status">{html.escape(str(status.get("status", "unknown")))}</span></div>
      <div>Last cycle started: {html.escape(_fmt_ts(status.get("last_cycle_started_at")))}</div>
      <div>Last cycle completed: {html.escape(_fmt_ts(status.get("last_cycle_completed_at")))}</div>
      <div>Latest queue / theses / opens / scales: {summary['queue_count']} / {summary['thesis_count']} / {summary['opened_this_cycle']} / {summary['scaled_this_cycle']}</div>
      <div>Rotation / cooldown: {html.escape(str(summary['active_rotation_category'] or 'all'))} / {summary['market_cooldown_minutes']}m</div>
      <div>Risk caps: max position {html.escape(_fmt_money(summary['max_position_usdc']))}, max portfolio {html.escape(_fmt_money(summary['max_portfolio_usdc']))}</div>
      <div>Last non-empty queue snapshot: {html.escape(_fmt_ts(state["last_nonempty_queue"].get("updated_at")))}</div>
      <div>Last error: <span class="error">{html.escape(str(status.get("last_error") or "-"))}</span></div>
    </div>
    <div class="panels">
      <div class="panel"><h2>Lifetime Stats</h2>{lifetime_table}</div>
      <div class="panel"><h2>Equity Curve</h2>{_render_equity_curve(lifetime["equity_curve"], summary["starting_bankroll_usdc"])}</div>
      {ab_panels_html}
      <div class="panel"><h2>Category Analytics</h2>{category_analytics_table}</div>
      <div class="panel"><h2>Market Analytics</h2>{market_analytics_table}</div>
      <div class="panel"><h2>Open Positions</h2>{positions_table}</div>
      <div class="panel"><h2>Recent Trades</h2>{trades_table}</div>
      <div class="panel"><h2>Latest Scan Queue</h2>{queue_table}</div>
      <div class="panel"><h2>Top Opportunities Missed</h2>{missed_table}</div>
      <div class="panel"><h2>Last Non-Empty Queue</h2>{last_nonempty_queue_table}</div>
      <div class="panel"><h2>Target Wallets</h2>{targets_table}</div>
    </div>
  </div>
</body>
</html>"""


def serve_dashboard(settings: Settings, host: str, port: int) -> None:
    class Handler(BaseHTTPRequestHandler):
        def _respond(self, code: int, content_type: str, body: bytes) -> None:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            state = build_dashboard_state(settings)
            if self.path == "/api/state":
                self._respond(200, "application/json; charset=utf-8", json.dumps(state, indent=2).encode("utf-8"))
                return
            if self.path == "/" or self.path.startswith("/?"):
                html_body = render_dashboard_html(state).encode("utf-8")
                self._respond(200, "text/html; charset=utf-8", html_body)
                return
            self._respond(404, "text/plain; charset=utf-8", b"not found")

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer((host, port), Handler)
    print(f"[{datetime.now(timezone.utc).isoformat()}] dashboard listening on http://{host}:{port}", flush=True)
    server.serve_forever()
