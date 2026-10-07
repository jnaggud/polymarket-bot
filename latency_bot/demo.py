from __future__ import annotations

import json
import shutil
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .config import LatencyBotSettings
from .storage import init_latency_bot_db


DEMO_SEED = 20261007
DEMO_VARIANTS = (
    "btc5_yes_calibrated_digital_f0.50_e0.03_t180",
    "eth5_no_calibrated_digital_f0.50_e0.03_t180",
)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def demo_settings(output_dir: Path, base: LatencyBotSettings | None = None) -> LatencyBotSettings:
    source = base or LatencyBotSettings.from_env()
    return replace(
        source,
        db_path=output_dir / "latency_bot_demo.sqlite3",
        status_path=output_dir / "latency_bot_status.json",
        markets_path=output_dir / "latency_bot_markets.json",
        polymarket_cache_path=output_dir / "latency_bot_polymarket_cache.json",
        binance_cache_path=output_dir / "latency_bot_binance_cache.json",
        bootstrap_intraday_book_tape_path=output_dir / "intraday_book_tape.json",
    )


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_database(settings: LatencyBotSettings, anchor: datetime) -> dict[str, Any]:
    init_latency_bot_db(settings)
    pnl_pattern = (7.40, -3.20, 5.10, 2.60, -2.80, 6.75, 4.20, -4.10, 3.85, 5.55, -2.45, 7.10)
    running_pnl = 0.0
    validation_records: list[dict[str, Any]] = []
    connection = sqlite3.connect(settings.db_path)
    try:
        for index in range(24):
            asset = "btc" if index % 2 == 0 else "eth"
            side = "YES" if index % 3 else "NO"
            market_id = f"demo-{asset}-5m-{index + 1:02d}"
            opened_at = anchor - timedelta(hours=24 - index, minutes=4)
            closed_at = opened_at + timedelta(minutes=3)
            entry_price = round(0.45 + ((index % 5) * 0.015), 4)
            pnl = round(pnl_pattern[index % len(pnl_pattern)] * (1.0 if asset == "btc" else 0.82), 4)
            running_pnl = round(running_pnl + pnl, 4)
            size = round(100.0 / entry_price, 6)
            fair = round(min(entry_price + 0.075, 0.91), 4)
            variant = DEMO_VARIANTS[index % len(DEMO_VARIANTS)]
            connection.execute(
                """
                INSERT INTO markets (
                    market_id, question, asset, tenor_minutes, expiry_ts, yes_token_id, no_token_id,
                    created_at, first_seen_at, status
                ) VALUES (?, ?, ?, 5, ?, ?, ?, ?, ?, 'closed')
                """,
                (
                    market_id,
                    f"Will {asset.upper()} finish the five-minute window higher?",
                    asset,
                    _iso(closed_at + timedelta(minutes=1)),
                    f"demo-{index}-yes",
                    f"demo-{index}-no",
                    _iso(opened_at - timedelta(minutes=2)),
                    _iso(opened_at - timedelta(minutes=2)),
                ),
            )
            connection.execute(
                "INSERT INTO binance_ticks (ts, asset, mid, bid, ask, last, source_latency_ms) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (_iso(opened_at), asset, 100_000.0 + index * 37.0, 99_999.5 + index * 37.0, 100_000.5 + index * 37.0, 100_000.0 + index * 37.0, 38.0),
            )
            connection.execute(
                """
                INSERT INTO polymarket_books (
                    ts, market_id, side, best_bid, best_ask, bid_depth, ask_depth, spread,
                    book_age_ms, no_best_bid, no_best_ask, no_bid_depth, no_ask_depth,
                    complete_set_cost, complete_set_edge
                ) VALUES (?, ?, 'YES', ?, ?, 520, 610, 0.02, 125, ?, ?, 480, 540, 1.01, -0.01)
                """,
                (_iso(opened_at), market_id, entry_price - 0.01, entry_price + 0.01, 0.98 - entry_price, 1.0 - entry_price),
            )
            connection.execute(
                """
                INSERT INTO fair_values (
                    ts, market_id, asset, fair_yes, fair_no, reference_price, volatility, time_to_expiry_sec
                ) VALUES (?, ?, ?, ?, ?, ?, 0.0018, 240)
                """,
                (_iso(opened_at), market_id, asset, fair, round(1.0 - fair, 4), 100_000.0 + index * 37.0),
            )
            connection.execute(
                """
                INSERT INTO signals (
                    ts, market_id, asset, side, tenor_minutes, signal_type, edge, mode,
                    fair_yes, fair_no, yes_bid, yes_ask, no_bid, no_ask, min_depth_usdc,
                    book_age_ms, seconds_left, reference_price, volatility, reason, eligible, blocked_reason
                ) VALUES (?, ?, ?, ?, 5, 'TAKE_YES', 0.055, 'paper', ?, ?, ?, ?, ?, ?, 480, 125, 240, ?, 0.0018, 'demo_signal', 1, '')
                """,
                (
                    _iso(opened_at), market_id, asset, side, fair, round(1.0 - fair, 4),
                    entry_price - 0.01, entry_price + 0.01, 0.98 - entry_price, 1.0 - entry_price,
                    100_000.0 + index * 37.0,
                ),
            )
            position_id = f"demo-primary-{index:02d}"
            connection.execute(
                """
                INSERT INTO positions (
                    position_id, market_id, asset, side, entry_ts, entry_price, size, mode, status,
                    execution_model, entry_signal_type, entry_edge, entry_fair_yes, entry_fair_no,
                    entry_seconds_left, entry_min_depth_usdc, entry_book_age_ms, entry_reference_price,
                    entry_volatility, entry_tenor_minutes, entry_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'paper', 'closed', 'vwap_latency_partial_fill_v1',
                          'TAKE_YES', 0.055, ?, ?, 240, 480, 125, ?, 0.0018, 5, 'demo_signal')
                """,
                (position_id, market_id, asset, side, _iso(opened_at), entry_price, size, fair, round(1.0 - fair, 4), 100_000.0 + index * 37.0),
            )
            connection.execute(
                "INSERT INTO position_events (ts, position_id, event_type, mark, edge, pnl, reason) VALUES (?, ?, 'close', ?, 0.0, ?, 'RESOLUTION')",
                (_iso(closed_at), position_id, 1.0 if pnl > 0 else 0.0, pnl),
            )
            connection.execute(
                "INSERT INTO equity_snapshots (ts, realized_pnl_usdc, unrealized_pnl_usdc, equity_usdc) VALUES (?, ?, 0, ?)",
                (_iso(closed_at), running_pnl, round(settings.bankroll_usdc + running_pnl, 4)),
            )
            cex_position_id = f"demo-cex-{index:02d}"
            connection.execute(
                """
                INSERT INTO cex_latency_paper_positions (
                    position_id, market_id, asset, side, entry_ts, entry_price, size, notional_usdc,
                    mode, status, entry_signal_type, entry_edge, entry_fair_yes, entry_fair_no,
                    entry_seconds_left, entry_min_depth_usdc, entry_book_age_ms, entry_reference_price,
                    entry_volatility, entry_tenor_minutes, entry_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 100, 'cex_latency_paper', 'closed', 'CEX_LATENCY',
                          0.055, ?, ?, 240, 480, 125, ?, 0.0018, 5, 'demo_signal')
                """,
                (cex_position_id, market_id, asset, side, _iso(opened_at), entry_price, size, fair, round(1.0 - fair, 4), 100_000.0 + index * 37.0),
            )
            connection.execute(
                "INSERT INTO cex_latency_paper_events (ts, position_id, event_type, mark, edge, pnl, reason) VALUES (?, ?, 'close', ?, 0.0, ?, 'RESOLUTION')",
                (_iso(closed_at), cex_position_id, 1.0 if pnl > 0 else 0.0, round(pnl * 0.88, 4)),
            )
            variant_position_id = f"demo-variant-{index:02d}"
            connection.execute(
                """
                INSERT INTO shadow_variant_positions (
                    position_id, variant_id, market_id, asset, side, entry_ts, entry_price, size, mode,
                    status, entry_signal_type, entry_edge, entry_fair_yes, entry_fair_no,
                    entry_seconds_left, entry_min_depth_usdc, entry_book_age_ms, entry_reference_price,
                    entry_volatility, entry_tenor_minutes, entry_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'executable_paper', 'closed', 'PROMOTED_VARIANT',
                          0.055, ?, ?, 240, 480, 125, ?, 0.0018, 5, 'demo_signal')
                """,
                (
                    variant_position_id, variant, market_id, asset, side, _iso(opened_at), entry_price,
                    size, fair, round(1.0 - fair, 4), 100_000.0 + index * 37.0,
                ),
            )
            connection.execute(
                "INSERT INTO shadow_variant_position_events (ts, variant_id, position_id, event_type, mark, edge, pnl, reason) VALUES (?, ?, ?, 'close', ?, 0.0, ?, 'RESOLUTION')",
                (_iso(closed_at), variant, variant_position_id, 1.0 if pnl > 0 else 0.0, round(pnl * 0.72, 4)),
            )
            connection.execute(
                "INSERT INTO engine_cycles (ts, phase, markets_tracked, signals_seen, orders_open, positions_open, risk_state, notes) VALUES (?, 'paper_execution', 12, 2, 0, 0, 'normal', 'credential-free demo cycle')",
                (_iso(closed_at),),
            )
            predicted_fill = round(0.35 + ((index % 6) * 0.10), 2)
            observed_fill = 1 if index % 5 not in (0, 4) else 0
            independent_price = round(entry_price + (0.002 if index % 4 else 0.006), 4)
            independent_size = round(size * (0.92 if observed_fill else 0.0), 6)
            validation_records.append(
                {
                    "strategy_id": variant,
                    "closed_at": _iso(closed_at),
                    "market_id": market_id,
                    "gross_pnl_usdc": pnl,
                    "baseline_pnl_usdc": round(pnl * 0.18 - 0.75, 4),
                    "notional_usdc": 100.0,
                    "fee_usdc": 0.35,
                    "observed_latency_ms": 425.0 + (index % 4) * 125.0,
                    "available_depth_usdc": 160.0 + (index % 5) * 55.0,
                    "requested_notional_usdc": 100.0,
                    "predicted_fill_probability": predicted_fill,
                    "observed_fill": observed_fill,
                    "paper_fill_price": entry_price,
                    "independent_fill_price": independent_price if observed_fill else None,
                    "paper_fill_size": size,
                    "independent_fill_size": independent_size,
                }
            )
        for variant in DEMO_VARIANTS:
            connection.execute(
                "INSERT OR REPLACE INTO shadow_variant_signal_summary (variant_id, signals, eligible) VALUES (?, 120, 36)",
                (variant,),
            )
            connection.execute(
                "INSERT OR REPLACE INTO shadow_variant_reason_summary (variant_id, reason, count, max_edge, max_side_fair) VALUES (?, 'demo_signal', 120, 0.085, 0.64)",
                (variant,),
            )
        connection.commit()
    finally:
        connection.close()
    return {"validation_records": validation_records, "net_pnl_usdc": running_pnl, "closed_trades": 24}


def create_demo_workspace(
    output_dir: Path,
    *,
    force: bool = False,
    anchor: datetime | None = None,
    base_settings: LatencyBotSettings | None = None,
) -> dict[str, Any]:
    output_dir = output_dir.resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        if not force:
            raise FileExistsError(f"demo directory is not empty: {output_dir}; pass --force to replace it")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = (anchor or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(microsecond=0)
    settings = demo_settings(output_dir, base_settings)
    seeded = _seed_database(settings, generated_at)
    markets = [
        {
            "market_id": f"demo-{asset}-5m-live",
            "question": f"Will {asset.upper()} finish this five-minute window higher?",
            "asset": asset,
            "tenor_minutes": 5,
            "expiry_ts": _iso(generated_at + timedelta(minutes=5)),
            "status": "active",
        }
        for asset in ("btc", "eth")
    ]
    _write_json(
        settings.status_path,
        {
            "runner_status": "demo",
            "phase": "paper_execution",
            "last_cycle_started_at": _iso(generated_at - timedelta(seconds=3)),
            "last_cycle_completed_at": _iso(generated_at),
            "last_error": None,
            "tracked_markets_count": len(markets),
            "open_orders_count": 0,
            "open_positions_count": 0,
            "realized_pnl_usdc": seeded["net_pnl_usdc"],
            "unrealized_pnl_usdc": 0.0,
            "risk_state": "paper_only",
            "notes": ["Credential-free deterministic demonstration ledger.", "No live venue credentials are loaded."],
        },
    )
    _write_json(settings.markets_path, {"generated_at": _iso(generated_at), "count": len(markets), "items": markets})
    _write_json(
        settings.polymarket_cache_path,
        {
            "generated_at": _iso(generated_at),
            "items": [
                {"market_id": item["market_id"], "best_bid": 0.49, "best_ask": 0.51, "bid_depth": 500, "ask_depth": 550}
                for item in markets
            ],
        },
    )
    _write_json(
        settings.binance_cache_path,
        {
            "generated_at": _iso(generated_at),
            "items": [
                {"asset": "btc", "mid": 100_888.0, "bid": 100_887.5, "ask": 100_888.5},
                {"asset": "eth", "mid": 3_588.0, "bid": 3_587.9, "ask": 3_588.1},
            ],
        },
    )
    validation_path = output_dir / "validation_records.json"
    _write_json(validation_path, {"seed": DEMO_SEED, "records": seeded["validation_records"]})
    manifest = {
        "dataset": "polymarket-bot credential-free demo",
        "seed": DEMO_SEED,
        "generated_at": _iso(generated_at),
        "closed_trades": seeded["closed_trades"],
        "net_pnl_usdc": seeded["net_pnl_usdc"],
        "db_path": str(settings.db_path),
        "validation_path": str(validation_path),
        "contains_credentials": False,
        "contains_live_trades": False,
    }
    _write_json(output_dir / "manifest.json", manifest)
    return manifest
