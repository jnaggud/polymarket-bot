#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from latency_bot.config import LatencyBotSettings


def _money(value: float | int | None) -> str:
    return f"${float(value or 0.0):,.2f}"


def _pct(value: float | int | None) -> str:
    return f"{100.0 * float(value or 0.0):.1f}%"


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=1.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA busy_timeout = 1000")
    return conn


def _row_dict(row: sqlite3.Row | None) -> dict[str, object]:
    return dict(row) if row is not None else {}


def snapshot(settings: LatencyBotSettings, *, recent_limit: int) -> dict[str, object]:
    cutoff_60m = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat().replace("+00:00", "Z")
    cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat().replace("+00:00", "Z")
    with _connect(settings.db_path) as conn:
        signal_60m = _row_dict(
            conn.execute(
                """
                SELECT
                    COUNT(*) AS signals,
                    SUM(CASE WHEN eligible THEN 1 ELSE 0 END) AS eligible,
                    MAX(edge) AS best_edge
                FROM cex_latency_paper_signals
                WHERE ts >= ?
                """,
                (cutoff_60m,),
            ).fetchone()
        )
        totals = _row_dict(
            conn.execute(
                """
                SELECT
                    COUNT(*) AS closed,
                    SUM(CASE WHEN e.pnl > 0 THEN 1 ELSE 0 END) AS wins,
                    SUM(e.pnl) AS net_pnl,
                    SUM(CASE WHEN e.ts >= ? THEN e.pnl ELSE 0 END) AS pnl_24h
                FROM cex_latency_paper_events e
                WHERE e.event_type = 'close'
                """,
                (cutoff_24h,),
            ).fetchone()
        )
        open_summary = _row_dict(
            conn.execute(
                """
                SELECT COUNT(*) AS open, SUM(notional_usdc) AS capital
                FROM cex_latency_paper_positions
                WHERE status = 'open'
                """
            ).fetchone()
        )
        skip_reasons = [
            dict(row)
            for row in conn.execute(
                """
                SELECT reason, COUNT(*) AS count, MAX(edge) AS max_edge
                FROM cex_latency_paper_signals
                WHERE ts >= ?
                GROUP BY reason
                ORDER BY count DESC, reason ASC
                LIMIT 5
                """,
                (cutoff_60m,),
            ).fetchall()
        ]
        open_positions = [
            dict(row)
            for row in conn.execute(
                """
                SELECT entry_ts, market_id, asset, side, entry_price, notional_usdc, entry_edge, entry_seconds_left
                FROM cex_latency_paper_positions
                WHERE status = 'open'
                ORDER BY entry_ts DESC
                LIMIT ?
                """,
                (recent_limit,),
            ).fetchall()
        ]
        closes = [
            dict(row)
            for row in conn.execute(
                """
                SELECT e.ts, p.market_id, p.asset, p.side, p.entry_price, e.mark AS exit_price, e.pnl, e.reason
                FROM cex_latency_paper_events e
                JOIN cex_latency_paper_positions p ON p.position_id = e.position_id
                WHERE e.event_type = 'close'
                ORDER BY e.ts DESC, e.id DESC
                LIMIT ?
                """,
                (recent_limit,),
            ).fetchall()
        ]
        signals = [
            dict(row)
            for row in conn.execute(
                """
                SELECT ts, market_id, asset, side, signal_type, edge, eligible, reason
                FROM cex_latency_paper_signals
                ORDER BY ts DESC, id DESC
                LIMIT ?
                """,
                (recent_limit,),
            ).fetchall()
        ]

    closed = int(totals.get("closed") or 0)
    wins = int(totals.get("wins") or 0)
    net_pnl = float(totals.get("net_pnl") or 0.0)
    pnl_24h = float(totals.get("pnl_24h") or 0.0)
    return {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "model": settings.cex_latency_paper_model,
        "capital": float(settings.cex_latency_paper_capital_usdc),
        "equity": float(settings.cex_latency_paper_capital_usdc) + net_pnl,
        "net_pnl": net_pnl,
        "pnl_24h": pnl_24h,
        "closed": closed,
        "wins": wins,
        "win_rate": wins / closed if closed else 0.0,
        "open": int(open_summary.get("open") or 0),
        "open_capital": float(open_summary.get("capital") or 0.0),
        "signals_60m": int(signal_60m.get("signals") or 0),
        "eligible_60m": int(signal_60m.get("eligible") or 0),
        "best_edge_60m": float(signal_60m.get("best_edge") or 0.0),
        "skip_reasons": skip_reasons,
        "open_positions": open_positions,
        "recent_closes": closes,
        "recent_signals": signals,
    }


def print_snapshot(data: dict[str, object]) -> None:
    print(
        f"[{data['ts']}] model={data['model']} equity={_money(data['equity'])} "
        f"net={_money(data['net_pnl'])} 24h={_money(data['pnl_24h'])} "
        f"closed={data['closed']} win={_pct(data['win_rate'])} "
        f"open={data['open']} open_capital={_money(data['open_capital'])} "
        f"signals60={data['signals_60m']} eligible60={data['eligible_60m']} "
        f"best_edge60={float(data['best_edge_60m']):.4f}"
    )
    if data["open_positions"]:
        print("  open:")
        for row in data["open_positions"]:
            print(
                "   "
                f"{row['entry_ts']} {row['asset']} {row['side']} market={row['market_id']} "
                f"entry={float(row['entry_price'] or 0.0):.4f} "
                f"notional={_money(row['notional_usdc'])} edge={float(row['entry_edge'] or 0.0):.4f}"
            )
    if data["recent_closes"]:
        print("  recent closes:")
        for row in data["recent_closes"]:
            print(
                "   "
                f"{row['ts']} {row['asset']} {row['side']} market={row['market_id']} "
                f"entry={float(row['entry_price'] or 0.0):.4f} exit={float(row['exit_price'] or 0.0):.4f} "
                f"pnl={_money(row['pnl'])} {row['reason']}"
            )
    if data["skip_reasons"]:
        reasons = ", ".join(
            f"{row['reason']}={int(row['count'] or 0)}(max={float(row['max_edge'] or 0.0):.4f})"
            for row in data["skip_reasons"]
        )
        print(f"  skip60: {reasons}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Watch CEX latency paper bot performance from SQLite.")
    parser.add_argument("--interval", type=float, default=5.0, help="Refresh interval in seconds.")
    parser.add_argument("--limit", type=int, default=3, help="Recent rows to print.")
    parser.add_argument("--once", action="store_true", help="Print one snapshot and exit.")
    args = parser.parse_args()

    settings = LatencyBotSettings.from_env()
    while True:
        try:
            print_snapshot(snapshot(settings, recent_limit=max(args.limit, 0)))
        except Exception as exc:  # keep the watcher alive through transient DB locks
            print(f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] watcher error: {exc}")
        if args.once:
            return 0
        time.sleep(max(args.interval, 0.5))


if __name__ == "__main__":
    raise SystemExit(main())
