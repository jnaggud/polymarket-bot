from __future__ import annotations

import argparse
import html
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean
from typing import Any

from bot.accounting import effective_position_shares, rebuild_closed_trades, side_contract_price
from bot.config import Settings
from bot.core import _low_price_liquidity_factor, _parse_env_file
from bot.dashboard import _build_trade_analytics, _fmt_money, _fmt_pct, _json_load, _render_equity_curve


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


def _as_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _execution_haircut(contract_price: float) -> float:
    price = max(min(contract_price, 0.99), 0.01)
    if price < 0.10:
        return min(0.02, max(0.004, price * 0.25))
    if price < 0.25:
        return max(0.003, price * 0.08)
    return max(0.002, price * 0.03)


@dataclass(slots=True)
class ReplayPortfolio:
    name: str
    label: str
    trades: list[dict[str, Any]]
    positions: list[dict[str, Any]]
    marks: dict[str, Any]
    starting_bankroll_usdc: float


def _portfolio_sources(settings: Settings) -> list[ReplayPortfolio]:
    portfolios = [
        ReplayPortfolio(
            name="main",
            label="Main Bot",
            trades=_json_load(settings.trades_path, []),
            positions=_json_load(settings.positions_path, []),
            marks=_json_load(settings.marks_path, {"positions": [], "summary": {}}),
            starting_bankroll_usdc=settings.bankroll_usdc,
        ),
        ReplayPortfolio(
            name="wallet_copy",
            label="A/B Wallet Copy",
            trades=_json_load(Path("state/ab_wallet_copy_trades.json"), []),
            positions=_json_load(Path("state/ab_wallet_copy_positions.json"), []),
            marks=_json_load(Path("state/ab_wallet_copy_marks.json"), {"positions": [], "summary": {}}),
            starting_bankroll_usdc=settings.bankroll_usdc,
        ),
        ReplayPortfolio(
            name="wallet_copy_aggressive",
            label="A/B Wallet Copy Aggressive",
            trades=_json_load(Path("state/ab_wallet_copy_aggressive_trades.json"), []),
            positions=_json_load(Path("state/ab_wallet_copy_aggressive_positions.json"), []),
            marks=_json_load(Path("state/ab_wallet_copy_aggressive_marks.json"), {"positions": [], "summary": {}}),
            starting_bankroll_usdc=settings.bankroll_usdc,
        ),
    ]
    if settings.ab_test_verified_public_enabled:
        portfolios.append(
            ReplayPortfolio(
                name="verified_public",
                label="A/B Verified Public Traders",
                trades=_json_load(Path("state/ab_verified_public_trades.json"), []),
                positions=_json_load(Path("state/ab_verified_public_positions.json"), []),
                marks=_json_load(Path("state/ab_verified_public_marks.json"), {"positions": [], "summary": {}}),
                starting_bankroll_usdc=settings.bankroll_usdc,
            )
        )
    return portfolios


def _replay_closed_trade(trade: dict[str, Any]) -> dict[str, Any]:
    side = str(trade.get("side", "BUY")).upper()
    entry_contract_price = _as_float(trade.get("entry_contract_price"), 0.0)
    if entry_contract_price <= 0:
        entry_contract_price = side_contract_price(side, _as_float(trade.get("entry_price"), 0.5))
    exit_contract_price = _as_float(trade.get("exit_contract_price"), 0.0)
    if exit_contract_price <= 0:
        exit_contract_price = side_contract_price(side, _as_float(trade.get("exit_price"), _as_float(trade.get("entry_price"), 0.5)))

    fill_factor = min(_low_price_liquidity_factor(entry_contract_price), _low_price_liquidity_factor(exit_contract_price))
    base_shares = _as_float(trade.get("shares"), 0.0)
    replay_shares = round(base_shares * fill_factor, 6)

    replay_entry_contract = round(min(0.99, entry_contract_price + _execution_haircut(entry_contract_price)), 6)
    replay_exit_contract = round(max(0.01, exit_contract_price - _execution_haircut(exit_contract_price)), 6)
    replay_notional = round(replay_entry_contract * replay_shares, 2)
    replay_pnl = round((replay_exit_contract - replay_entry_contract) * replay_shares, 2)

    return {
        **trade,
        "shares": replay_shares,
        "notional_usdc": replay_notional,
        "entry_contract_price": replay_entry_contract,
        "exit_contract_price": replay_exit_contract,
        "pnl_usdc": replay_pnl,
        "replay_fill_factor": round(fill_factor, 6),
    }


def _replay_open_position(position: dict[str, Any], marks_by_position: dict[str, dict[str, Any]]) -> dict[str, Any]:
    side = str(position.get("side", "BUY")).upper()
    entry_contract_price = _as_float(position.get("entry_contract_price"), 0.0)
    if entry_contract_price <= 0:
        entry_contract_price = side_contract_price(side, _as_float(position.get("entry_price"), 0.5))

    mark = marks_by_position.get(str(position.get("position_id", "")), {})
    current_contract_price = _as_float(mark.get("liquidation_contract_price"), 0.0)
    if current_contract_price <= 0:
        current_contract_price = _as_float(mark.get("current_contract_price"), 0.0)
    if current_contract_price <= 0:
        current_contract_price = side_contract_price(side, _as_float(mark.get("current_midpoint"), _as_float(position.get("entry_price"), 0.5)))

    fill_factor = min(_low_price_liquidity_factor(entry_contract_price), _low_price_liquidity_factor(current_contract_price))
    replay_shares = round(effective_position_shares(position) * fill_factor, 6)
    replay_entry_contract = round(min(0.99, entry_contract_price + _execution_haircut(entry_contract_price)), 6)
    replay_current_contract = round(max(0.01, current_contract_price - _execution_haircut(current_contract_price)), 6)
    replay_notional = round(replay_entry_contract * replay_shares, 2)
    replay_mark_value = round(replay_current_contract * replay_shares, 2)
    replay_unrealized = round(replay_mark_value - replay_notional, 2)
    return {
        **position,
        "shares": replay_shares,
        "notional_usdc": replay_notional,
        "entry_contract_price": replay_entry_contract,
        "replay_current_contract_price": replay_current_contract,
        "replay_mark_value_usdc": replay_mark_value,
        "replay_unrealized_pnl_usdc": replay_unrealized,
        "replay_fill_factor": round(fill_factor, 6),
    }


def _build_window_stats(
    label: str,
    portfolio: ReplayPortfolio,
    window_start: datetime,
) -> dict[str, Any]:
    rebuilt_closes = rebuild_closed_trades(portfolio.trades)
    replay_closes = [_replay_closed_trade(trade) for trade in rebuilt_closes]
    replay_closes = [trade for trade in replay_closes if (_parse_ts(trade.get("closed_at")) or datetime.min.replace(tzinfo=timezone.utc)) >= window_start]
    replay_closes.sort(key=lambda trade: _parse_ts(trade.get("closed_at")) or datetime.min.replace(tzinfo=timezone.utc))

    marks_by_position = {
        str(item.get("position_id", "")): item
        for item in portfolio.marks.get("positions", [])
        if item.get("position_id")
    }
    replay_positions = [_replay_open_position(position, marks_by_position) for position in portfolio.positions]

    realized_pnl = round(sum(_as_float(trade.get("pnl_usdc")) for trade in replay_closes), 2)
    unrealized_pnl = round(sum(_as_float(position.get("replay_unrealized_pnl_usdc")) for position in replay_positions), 2)
    gross_profit = round(sum(_as_float(trade.get("pnl_usdc")) for trade in replay_closes if _as_float(trade.get("pnl_usdc")) > 0), 2)
    gross_loss = round(sum(abs(_as_float(trade.get("pnl_usdc"))) for trade in replay_closes if _as_float(trade.get("pnl_usdc")) < 0), 2)
    winners = [trade for trade in replay_closes if _as_float(trade.get("pnl_usdc")) > 0]
    losers = [trade for trade in replay_closes if _as_float(trade.get("pnl_usdc")) < 0]
    breakeven = [trade for trade in replay_closes if _as_float(trade.get("pnl_usdc")) == 0]
    opened_map = {
        str(trade.get("position_id", "")): trade
        for trade in portfolio.trades
        if trade.get("type") == "OPEN"
    }
    holding_minutes: list[float] = []
    for close_trade in replay_closes:
        opened = opened_map.get(str(close_trade.get("position_id", "")))
        opened_at = _parse_ts(opened.get("opened_at")) if opened else None
        closed_at = _parse_ts(close_trade.get("closed_at"))
        if opened_at and closed_at and closed_at >= opened_at:
            holding_minutes.append((closed_at - opened_at).total_seconds() / 60.0)

    curve_points = []
    running_pnl = 0.0
    peak_equity = portfolio.starting_bankroll_usdc
    max_drawdown = 0.0
    for idx, trade in enumerate(replay_closes, start=1):
        running_pnl = round(running_pnl + _as_float(trade.get("pnl_usdc")), 2)
        equity = round(portfolio.starting_bankroll_usdc + running_pnl, 2)
        peak_equity = max(peak_equity, equity)
        if peak_equity > 0:
            max_drawdown = min(max_drawdown, (equity - peak_equity) / peak_equity)
        curve_points.append(
            {
                "index": idx,
                "timestamp": trade.get("closed_at"),
                "equity_usdc": equity,
                "realized_pnl_usdc": running_pnl,
                "question": trade.get("question", ""),
            }
        )

    best_trade = max(replay_closes, key=lambda trade: _as_float(trade.get("pnl_usdc")), default=None)
    worst_trade = min(replay_closes, key=lambda trade: _as_float(trade.get("pnl_usdc")), default=None)
    open_capital = round(sum(_as_float(position.get("notional_usdc")) for position in replay_positions), 2)
    analytics = _build_trade_analytics(replay_closes, replay_positions)

    return {
        "label": label,
        "window_start": window_start.isoformat(),
        "window_days": None,
        "closed_count": len(replay_closes),
        "open_positions": len(replay_positions),
        "realized_pnl_usdc": realized_pnl,
        "unrealized_pnl_usdc": unrealized_pnl,
        "net_total_pnl_usdc": round(realized_pnl + unrealized_pnl, 2),
        "gross_profit_usdc": gross_profit,
        "gross_loss_usdc": gross_loss,
        "win_rate_pct": round((len(winners) / len(replay_closes)) * 100.0, 1) if replay_closes else None,
        "profit_factor": round(gross_profit / gross_loss, 2) if gross_loss > 0 else None,
        "avg_winner_usdc": round(mean(_as_float(trade.get("pnl_usdc")) for trade in winners), 2) if winners else None,
        "avg_loser_usdc": round(mean(_as_float(trade.get("pnl_usdc")) for trade in losers), 2) if losers else None,
        "breakeven_count": len(breakeven),
        "avg_holding_minutes": round(mean(holding_minutes), 1) if holding_minutes else None,
        "max_drawdown_pct": round(abs(max_drawdown) * 100.0, 2),
        "open_capital_usdc": open_capital,
        "equity_including_open_usdc": round(portfolio.starting_bankroll_usdc + realized_pnl + unrealized_pnl, 2),
        "equity_curve": curve_points,
        "best_trade": best_trade,
        "worst_trade": worst_trade,
        "top_winners": sorted(replay_closes, key=lambda trade: _as_float(trade.get("pnl_usdc")), reverse=True)[:10],
        "top_losers": sorted(replay_closes, key=lambda trade: _as_float(trade.get("pnl_usdc")))[:10],
        "analytics": analytics,
        "replay_positions": replay_positions,
    }


def _render_stats_table(stats: dict[str, Any]) -> str:
    rows = [
        ("Realized PnL", _fmt_money(stats["realized_pnl_usdc"])),
        ("Unrealized PnL", _fmt_money(stats["unrealized_pnl_usdc"])),
        ("Net Total PnL", _fmt_money(stats["net_total_pnl_usdc"])),
        ("Closed Trades", str(stats["closed_count"])),
        ("Open Positions", str(stats["open_positions"])),
        ("Open Capital", _fmt_money(stats["open_capital_usdc"])),
        ("Win Rate", _fmt_pct(stats["win_rate_pct"])),
        ("Profit Factor", "-" if stats["profit_factor"] is None else f"{stats['profit_factor']:.2f}"),
        ("Average Winner", "-" if stats["avg_winner_usdc"] is None else _fmt_money(stats["avg_winner_usdc"])),
        ("Average Loser", "-" if stats["avg_loser_usdc"] is None else _fmt_money(stats["avg_loser_usdc"])),
        ("Max Drawdown", f"{stats['max_drawdown_pct']:.2f}%"),
        ("Average Hold", "-" if stats["avg_holding_minutes"] is None else f"{stats['avg_holding_minutes']:.1f} min"),
    ]
    body = "".join(
        f"<tr><th>{html.escape(label)}</th><td>{html.escape(value)}</td></tr>"
        for label, value in rows
    )
    return f"<table><tbody>{body}</tbody></table>"


def _render_trade_list(title: str, trades: list[dict[str, Any]]) -> str:
    if not trades:
        return f"<div class='panel'><h3>{html.escape(title)}</h3><div class='sub'>No trades in window.</div></div>"
    rows = []
    for trade in trades:
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(trade.get('closed_at', '')))}</td>"
            f"<td>{html.escape(str(trade.get('question', '')))}</td>"
            f"<td>{html.escape(_fmt_money(_as_float(trade.get('pnl_usdc'))))}</td>"
            "</tr>"
        )
    return (
        f"<div class='panel'><h3>{html.escape(title)}</h3>"
        "<table><thead><tr><th>Closed</th><th>Question</th><th>PnL</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></div>"
    )


def render_backtest_html(results: list[dict[str, Any]], generated_at: str, days: int) -> str:
    sections = []
    for stats in results:
        best = stats["best_trade"]
        worst = stats["worst_trade"]
        meta = (
            f"<div class='sub'>Best: {html.escape(str(best.get('question', '-')))} "
            f"({_fmt_money(_as_float(best.get('pnl_usdc'))) if best else '-'})"
            "<br>"
            f"Worst: {html.escape(str(worst.get('question', '-')))} "
            f"({_fmt_money(_as_float(worst.get('pnl_usdc'))) if worst else '-'})"
            "</div>"
        )
        sections.append(
            "<section class='section'>"
            f"<h2>{html.escape(stats['label'])}</h2>"
            f"{meta}"
            f"<div class='grid'>{_render_stats_table(stats)}</div>"
            f"<div class='panel'><h3>Equity Curve</h3>{_render_equity_curve(stats['equity_curve'], 10000.0)}</div>"
            f"{_render_trade_list('Top Winners', stats['top_winners'][:5])}"
            f"{_render_trade_list('Top Losers', stats['top_losers'][:5])}"
            "</section>"
        )
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Polymarket Replay Backtest</title>
  <style>
    body {{ font-family: Georgia, serif; background: linear-gradient(135deg,#f8f3df,#edf4ee); color:#1e2b26; margin:0; }}
    .wrap {{ max-width: 1180px; margin: 0 auto; padding: 32px 24px 56px; }}
    h1, h2, h3 {{ margin: 0 0 12px; }}
    .sub {{ color:#5a655f; font-size: 14px; margin-bottom: 14px; }}
    .section {{ background:#fffdf8; border:1px solid #ddd5c7; border-radius:18px; padding:20px; margin-top:20px; box-shadow:0 12px 30px rgba(65,52,24,.06); }}
    .grid {{ display:grid; grid-template-columns: repeat(auto-fit,minmax(240px,1fr)); gap:16px; margin-bottom:16px; }}
    .panel {{ background:#fff; border:1px solid #e5dccd; border-radius:14px; padding:16px; margin-top:16px; }}
    table {{ width:100%; border-collapse:collapse; }}
    th, td {{ text-align:left; padding:8px 10px; border-bottom:1px solid #eee3d2; font-size:14px; vertical-align:top; }}
    .curve {{ width:100%; height:auto; }}
    .curve-line {{ fill:none; stroke:#2f8f72; stroke-width:3; stroke-linecap:round; stroke-linejoin:round; }}
    .curve-baseline {{ stroke:#c9c0b1; stroke-width:2; stroke-dasharray:6 6; }}
    .curve-dot {{ fill:#b35a3c; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Polymarket 14-Day Replay Backtest</h1>
    <div class="sub">Generated at {html.escape(generated_at)}. Window: last {days} days.
    <br>This is a conservative replay of recorded trade decisions, not a true historical order-book backtest.
    <br>Assumptions: adverse entry/exit price haircut plus low-price liquidity haircut using the new execution model’s constraints.</div>
    {''.join(sections)}
  </div>
</body>
</html>"""


def run_backtest_report(days: int = 14) -> dict[str, Any]:
    _parse_env_file(Path(".env"))
    settings = Settings.from_env()
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(days=days)
    results = []
    for portfolio in _portfolio_sources(settings):
        stats = _build_window_stats(portfolio.label, portfolio, window_start)
        stats["window_days"] = days
        results.append(stats)

    output_dir = Path("state/backtests")
    output_dir.mkdir(parents=True, exist_ok=True)
    html_path = output_dir / f"replay_{days}d.html"
    json_path = output_dir / f"replay_{days}d.json"
    generated_at = now.isoformat()
    html_path.write_text(render_backtest_html(results, generated_at, days), encoding="utf-8")
    json_path.write_text(json.dumps({"generated_at": generated_at, "days": days, "results": results}, indent=2), encoding="utf-8")
    return {
        "generated_at": generated_at,
        "days": days,
        "html_path": str(html_path),
        "json_path": str(json_path),
        "results": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=14)
    args = parser.parse_args()
    result = run_backtest_report(days=args.days)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
