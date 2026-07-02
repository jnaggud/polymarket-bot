from __future__ import annotations

import json
import sqlite3
import urllib.error
import urllib.request
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

from .config import LatencyBotSettings


_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS markets (
        market_id TEXT PRIMARY KEY,
        question TEXT NOT NULL,
        asset TEXT NOT NULL,
        tenor_minutes INTEGER NOT NULL,
        expiry_ts TEXT NOT NULL,
        yes_token_id TEXT,
        no_token_id TEXT,
        created_at TEXT NOT NULL,
        first_seen_at TEXT NOT NULL,
        status TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS binance_ticks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        asset TEXT NOT NULL,
        mid REAL NOT NULL,
        bid REAL,
        ask REAL,
        last REAL,
        source_latency_ms REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS polymarket_books (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        market_id TEXT NOT NULL,
        side TEXT NOT NULL,
        best_bid REAL,
        best_ask REAL,
        bid_depth REAL,
        ask_depth REAL,
        spread REAL,
        book_age_ms REAL,
        no_best_bid REAL,
        no_best_ask REAL,
        no_bid_depth REAL,
        no_ask_depth REAL,
        complete_set_cost REAL,
        complete_set_edge REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fair_values (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        market_id TEXT NOT NULL,
        asset TEXT NOT NULL,
        fair_yes REAL NOT NULL,
        fair_no REAL NOT NULL,
        reference_price REAL,
        volatility REAL,
        time_to_expiry_sec REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        market_id TEXT NOT NULL,
        asset TEXT,
        side TEXT,
        tenor_minutes INTEGER,
        signal_type TEXT NOT NULL,
        edge REAL NOT NULL,
        mode TEXT NOT NULL,
        fair_yes REAL,
        fair_no REAL,
        yes_bid REAL,
        yes_ask REAL,
        no_bid REAL,
        no_ask REAL,
        min_depth_usdc REAL,
        book_age_ms REAL,
        seconds_left REAL,
        reference_price REAL,
        volatility REAL,
        reason TEXT,
        eligible INTEGER NOT NULL,
        blocked_reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS orders (
        order_id TEXT PRIMARY KEY,
        ts_created TEXT NOT NULL,
        market_id TEXT NOT NULL,
        mode TEXT NOT NULL,
        side TEXT NOT NULL,
        price REAL NOT NULL,
        size REAL NOT NULL,
        status TEXT NOT NULL,
        reprices INTEGER NOT NULL,
        cancel_reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fills (
        fill_id TEXT PRIMARY KEY,
        order_id TEXT NOT NULL,
        ts TEXT NOT NULL,
        market_id TEXT NOT NULL,
        side TEXT NOT NULL,
        price REAL NOT NULL,
        size REAL NOT NULL,
        fill_type TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS positions (
        position_id TEXT PRIMARY KEY,
        market_id TEXT NOT NULL,
        asset TEXT NOT NULL,
        side TEXT NOT NULL,
        entry_ts TEXT NOT NULL,
        entry_price REAL NOT NULL,
        size REAL NOT NULL,
        mode TEXT NOT NULL,
        status TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS position_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        position_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        mark REAL,
        edge REAL,
        pnl REAL,
        reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS risk_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        event_type TEXT NOT NULL,
        reason TEXT,
        value REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS missed_opportunities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        market_id TEXT NOT NULL,
        asset TEXT NOT NULL,
        side TEXT NOT NULL,
        edge REAL NOT NULL,
        reason TEXT NOT NULL,
        book_snapshot_ref TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS engine_cycles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        phase TEXT NOT NULL,
        markets_tracked INTEGER NOT NULL,
        signals_seen INTEGER NOT NULL,
        orders_open INTEGER NOT NULL,
        positions_open INTEGER NOT NULL,
        risk_state TEXT NOT NULL,
        notes TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS equity_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        realized_pnl_usdc REAL NOT NULL,
        unrealized_pnl_usdc REAL NOT NULL,
        equity_usdc REAL NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shadow_signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        market_id TEXT NOT NULL,
        asset TEXT,
        tenor_minutes INTEGER,
        signal_type TEXT NOT NULL,
        edge REAL NOT NULL,
        mode TEXT NOT NULL,
        fair_yes REAL,
        fair_no REAL,
        yes_bid REAL,
        yes_ask REAL,
        no_bid REAL,
        no_ask REAL,
        min_depth_usdc REAL,
        book_age_ms REAL,
        seconds_left REAL,
        reference_price REAL,
        volatility REAL,
        reason TEXT,
        eligible INTEGER NOT NULL,
        blocked_reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shadow_positions (
        position_id TEXT PRIMARY KEY,
        market_id TEXT NOT NULL,
        asset TEXT NOT NULL,
        side TEXT NOT NULL,
        entry_ts TEXT NOT NULL,
        entry_price REAL NOT NULL,
        size REAL NOT NULL,
        mode TEXT NOT NULL,
        status TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shadow_position_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        position_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        mark REAL,
        edge REAL,
        pnl REAL,
        reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS cex_latency_paper_signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        market_id TEXT NOT NULL,
        asset TEXT,
        side TEXT,
        tenor_minutes INTEGER,
        signal_type TEXT NOT NULL,
        edge REAL NOT NULL,
        mode TEXT NOT NULL,
        fair_yes REAL,
        fair_no REAL,
        yes_bid REAL,
        yes_ask REAL,
        no_bid REAL,
        no_ask REAL,
        order_price REAL,
        min_depth_usdc REAL,
        book_age_ms REAL,
        seconds_left REAL,
        reference_price REAL,
        volatility REAL,
        reason TEXT,
        eligible INTEGER NOT NULL,
        blocked_reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS cex_latency_paper_positions (
        position_id TEXT PRIMARY KEY,
        market_id TEXT NOT NULL,
        asset TEXT NOT NULL,
        side TEXT NOT NULL,
        entry_ts TEXT NOT NULL,
        entry_price REAL NOT NULL,
        size REAL NOT NULL,
        notional_usdc REAL NOT NULL,
        mode TEXT NOT NULL,
        status TEXT NOT NULL,
        entry_signal_type TEXT,
        entry_edge REAL,
        entry_fair_yes REAL,
        entry_fair_no REAL,
        entry_seconds_left REAL,
        entry_min_depth_usdc REAL,
        entry_book_age_ms REAL,
        entry_reference_price REAL,
        entry_volatility REAL,
        entry_tenor_minutes INTEGER,
        entry_reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS cex_latency_paper_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        position_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        mark REAL,
        edge REAL,
        pnl REAL,
        reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS temporal_inventory_markets (
        market_id TEXT PRIMARY KEY,
        asset TEXT NOT NULL,
        tenor_minutes INTEGER NOT NULL,
        first_seen_ts TEXT NOT NULL,
        updated_ts TEXT NOT NULL,
        state TEXT NOT NULL,
        yes_shares REAL NOT NULL DEFAULT 0.0,
        no_shares REAL NOT NULL DEFAULT 0.0,
        yes_cost_usdc REAL NOT NULL DEFAULT 0.0,
        no_cost_usdc REAL NOT NULL DEFAULT 0.0,
        realized_pnl_usdc REAL NOT NULL DEFAULT 0.0,
        expired_inventory_cost_usdc REAL NOT NULL DEFAULT 0.0,
        locked_pair_shares REAL NOT NULL DEFAULT 0.0,
        locked_pair_cost REAL NOT NULL DEFAULT 0.0,
        locked_pair_pnl_usdc REAL NOT NULL DEFAULT 0.0,
        last_signal_side TEXT,
        last_signal_edge REAL,
        last_quote_id TEXT,
        mode TEXT NOT NULL DEFAULT 'temporal_inventory_maker_paper'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS temporal_inventory_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        market_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        state TEXT NOT NULL,
        side TEXT,
        price REAL,
        size REAL,
        notional_usdc REAL,
        pnl_usdc REAL,
        pair_cost REAL,
        reason TEXT,
        metadata TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS temporal_inventory_quotes (
        quote_id TEXT PRIMARY KEY,
        ts_created TEXT NOT NULL,
        ts_updated TEXT NOT NULL,
        market_id TEXT NOT NULL,
        side TEXT NOT NULL,
        price REAL NOT NULL,
        size REAL NOT NULL,
        notional_usdc REAL NOT NULL,
        status TEXT NOT NULL,
        edge REAL,
        fill_ts TEXT,
        fill_price REAL,
        fill_size REAL,
        cancel_reason TEXT,
        adverse_selection_loss_usdc REAL NOT NULL DEFAULT 0.0,
        reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS live_temporal_inventory_maker_orders (
        local_order_id TEXT PRIMARY KEY,
        ts_created TEXT NOT NULL,
        ts_updated TEXT NOT NULL,
        market_id TEXT NOT NULL,
        asset TEXT NOT NULL,
        side TEXT NOT NULL,
        token_id TEXT NOT NULL,
        price REAL NOT NULL,
        size REAL NOT NULL,
        notional_usdc REAL NOT NULL,
        mode TEXT NOT NULL,
        decision TEXT NOT NULL,
        status TEXT NOT NULL,
        edge REAL NOT NULL,
        reason TEXT NOT NULL,
        clob_order_id TEXT,
        order_payload TEXT,
        cancel_payload TEXT,
        error TEXT,
        heartbeat_ts TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS live_temporal_inventory_maker_heartbeats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        status TEXT NOT NULL,
        armed INTEGER NOT NULL,
        open_orders INTEGER NOT NULL,
        cancel_all_ok INTEGER NOT NULL,
        reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS late_resolution_capture_signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        market_id TEXT NOT NULL,
        asset TEXT NOT NULL,
        side TEXT NOT NULL,
        tenor_minutes INTEGER NOT NULL,
        signal_type TEXT NOT NULL,
        edge REAL NOT NULL,
        order_price REAL NOT NULL,
        fair_yes REAL NOT NULL,
        fair_no REAL NOT NULL,
        official_confidence REAL NOT NULL,
        boundary_distance_bps REAL NOT NULL,
        seconds_left REAL NOT NULL,
        eligible INTEGER NOT NULL,
        reason TEXT NOT NULL,
        blocked_reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS late_resolution_capture_positions (
        position_id TEXT PRIMARY KEY,
        market_id TEXT NOT NULL,
        asset TEXT NOT NULL,
        side TEXT NOT NULL,
        entry_ts TEXT NOT NULL,
        entry_price REAL NOT NULL,
        size REAL NOT NULL,
        notional_usdc REAL NOT NULL,
        status TEXT NOT NULL,
        entry_edge REAL,
        entry_official_confidence REAL,
        entry_boundary_distance_bps REAL,
        entry_seconds_left REAL,
        entry_reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS late_resolution_capture_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        position_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        mark REAL,
        pnl REAL,
        reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shadow_variant_signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        variant_id TEXT NOT NULL,
        market_id TEXT NOT NULL,
        asset TEXT,
        tenor_minutes INTEGER,
        signal_type TEXT NOT NULL,
        edge REAL NOT NULL,
        mode TEXT NOT NULL,
        fair_yes REAL,
        fair_no REAL,
        yes_bid REAL,
        yes_ask REAL,
        no_bid REAL,
        no_ask REAL,
        min_depth_usdc REAL,
        book_age_ms REAL,
        seconds_left REAL,
        reference_price REAL,
        volatility REAL,
        reason TEXT,
        eligible INTEGER NOT NULL,
        blocked_reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shadow_variant_signal_summary (
        variant_id TEXT PRIMARY KEY,
        signals INTEGER NOT NULL,
        eligible INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shadow_variant_reason_summary (
        variant_id TEXT NOT NULL,
        reason TEXT NOT NULL,
        count INTEGER NOT NULL,
        max_edge REAL,
        max_side_fair REAL,
        PRIMARY KEY (variant_id, reason)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shadow_variant_positions (
        position_id TEXT PRIMARY KEY,
        variant_id TEXT NOT NULL,
        market_id TEXT NOT NULL,
        asset TEXT NOT NULL,
        side TEXT NOT NULL,
        entry_ts TEXT NOT NULL,
        entry_price REAL NOT NULL,
        size REAL NOT NULL,
        mode TEXT NOT NULL,
        status TEXT NOT NULL,
        entry_signal_type TEXT,
        entry_edge REAL,
        entry_fair_yes REAL,
        entry_fair_no REAL,
        entry_seconds_left REAL,
        entry_min_depth_usdc REAL,
        entry_book_age_ms REAL,
        entry_reference_price REAL,
        entry_volatility REAL,
        entry_tenor_minutes INTEGER,
        entry_reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shadow_variant_position_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        variant_id TEXT NOT NULL,
        position_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        mark REAL,
        edge REAL,
        pnl REAL,
        reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS complete_set_arb_signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        market_id TEXT NOT NULL,
        asset TEXT NOT NULL,
        tenor_minutes INTEGER NOT NULL,
        yes_ask REAL NOT NULL,
        no_ask REAL NOT NULL,
        total_cost REAL NOT NULL,
        gross_edge REAL NOT NULL,
        net_edge REAL NOT NULL,
        executable_depth_usdc REAL NOT NULL,
        book_age_ms REAL,
        seconds_left REAL,
        eligible INTEGER NOT NULL,
        reason TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS complete_set_arb_positions (
        position_id TEXT PRIMARY KEY,
        market_id TEXT NOT NULL,
        asset TEXT NOT NULL,
        tenor_minutes INTEGER NOT NULL,
        entry_ts TEXT NOT NULL,
        yes_entry_price REAL NOT NULL,
        no_entry_price REAL NOT NULL,
        total_cost REAL NOT NULL,
        size REAL NOT NULL,
        notional_usdc REAL NOT NULL,
        gross_edge REAL NOT NULL,
        net_edge REAL NOT NULL,
        status TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS complete_set_arb_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        position_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        payout_price REAL,
        pnl REAL,
        reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS live_complete_set_arb_pilot_attempts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        market_id TEXT NOT NULL,
        asset TEXT NOT NULL,
        tenor_minutes INTEGER NOT NULL,
        mode TEXT NOT NULL,
        decision TEXT NOT NULL,
        reason TEXT NOT NULL,
        yes_token_id TEXT,
        no_token_id TEXT,
        yes_price REAL NOT NULL,
        no_price REAL NOT NULL,
        total_cost REAL NOT NULL,
        gross_edge REAL NOT NULL,
        net_edge REAL NOT NULL,
        adjusted_edge REAL NOT NULL,
        executable_depth_usdc REAL NOT NULL,
        effective_depth_usdc REAL NOT NULL,
        notional_usdc REAL NOT NULL,
        size REAL NOT NULL,
        expected_pnl_usdc REAL NOT NULL,
        realized_pnl_usdc REAL NOT NULL,
        yes_order_id TEXT,
        no_order_id TEXT,
        yes_order_payload TEXT,
        no_order_payload TEXT,
        error TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS live_complete_set_arb_pilot_resolutions (
        market_id TEXT PRIMARY KEY,
        resolved INTEGER NOT NULL,
        winner TEXT NOT NULL,
        question TEXT NOT NULL,
        error TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS polymarket_us_arb_ticks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        symbol TEXT NOT NULL,
        state TEXT,
        best_bid REAL,
        best_ask REAL,
        bid_qty REAL,
        ask_qty REAL,
        gross_edge REAL,
        executable_depth_usdc REAL,
        transact_time TEXT,
        source TEXT,
        error TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS kalshi_arb_ticks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        ticker TEXT NOT NULL,
        asset TEXT,
        title TEXT,
        status TEXT,
        yes_bid REAL,
        no_bid REAL,
        yes_bid_qty REAL,
        no_bid_qty REAL,
        yes_ask REAL,
        no_ask REAL,
        total_cost REAL,
        gross_edge REAL,
        executable_depth_usdc REAL,
        seconds_left REAL,
        close_time TEXT,
        source TEXT,
        error TEXT
    )
    """,
)


_SIGNAL_FEATURE_COLUMNS = {
    "asset": "TEXT",
    "side": "TEXT",
    "tenor_minutes": "INTEGER",
    "fair_yes": "REAL",
    "fair_no": "REAL",
    "yes_bid": "REAL",
    "yes_ask": "REAL",
    "no_bid": "REAL",
    "no_ask": "REAL",
    "min_depth_usdc": "REAL",
    "book_age_ms": "REAL",
    "seconds_left": "REAL",
    "reference_price": "REAL",
    "volatility": "REAL",
}


_POSITION_ENTRY_FEATURE_COLUMNS = {
    "entry_signal_type": "TEXT",
    "entry_edge": "REAL",
    "entry_fair_yes": "REAL",
    "entry_fair_no": "REAL",
    "entry_seconds_left": "REAL",
    "entry_min_depth_usdc": "REAL",
    "entry_book_age_ms": "REAL",
    "entry_reference_price": "REAL",
    "entry_volatility": "REAL",
    "entry_tenor_minutes": "INTEGER",
    "entry_reason": "TEXT",
}

_POLYMARKET_BOOK_COLUMNS = {
    "no_best_bid": "REAL",
    "no_best_ask": "REAL",
    "no_bid_depth": "REAL",
    "no_ask_depth": "REAL",
    "complete_set_cost": "REAL",
    "complete_set_edge": "REAL",
    "bid_vwap": "REAL",
    "ask_vwap": "REAL",
    "bid_fillable_usdc": "REAL",
    "ask_fillable_usdc": "REAL",
    "bid_fillable_shares": "REAL",
    "ask_fillable_shares": "REAL",
    "no_bid_vwap": "REAL",
    "no_ask_vwap": "REAL",
    "no_bid_fillable_usdc": "REAL",
    "no_ask_fillable_usdc": "REAL",
    "no_bid_fillable_shares": "REAL",
    "no_ask_fillable_shares": "REAL",
}


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {str(row["name"] or "") for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, definition in columns.items():
        if name in existing:
            continue
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")


def _backfill_signal_dimensions(conn: sqlite3.Connection, table: str) -> None:
    conn.execute(
        f"""
        UPDATE {table}
           SET asset = COALESCE(
                   NULLIF(asset, ''),
                   LOWER((
                       SELECT m.asset
                       FROM markets m
                       WHERE m.market_id = {table}.market_id
                       LIMIT 1
                   )),
                   LOWER((
                       SELECT fv.asset
                       FROM fair_values fv
                       WHERE fv.market_id = {table}.market_id
                         AND COALESCE(fv.asset, '') != ''
                       ORDER BY fv.ts DESC
                       LIMIT 1
                   ))
               ),
               tenor_minutes = COALESCE(
                   NULLIF(tenor_minutes, 0),
                   (
                       SELECT m.tenor_minutes
                       FROM markets m
                       WHERE m.market_id = {table}.market_id
                       LIMIT 1
                   )
               )
         WHERE COALESCE(asset, '') = ''
            OR COALESCE(tenor_minutes, 0) = 0
        """
    )


def _backfill_position_entry_features(conn: sqlite3.Connection, table: str, signal_table: str, signal_type: str) -> None:
    conn.execute(
        f"""
        UPDATE {table}
           SET entry_signal_type = COALESCE(
                   NULLIF(entry_signal_type, ''),
                   (
                       SELECT s.signal_type
                       FROM {signal_table} s
                       WHERE s.market_id = {table}.market_id
                         AND s.signal_type = ?
                         AND s.ts <= {table}.entry_ts
                       ORDER BY s.ts DESC
                       LIMIT 1
                   )
               ),
               entry_edge = COALESCE(
                   entry_edge,
                   (
                       SELECT s.edge
                       FROM {signal_table} s
                       WHERE s.market_id = {table}.market_id
                         AND s.signal_type = ?
                         AND s.ts <= {table}.entry_ts
                       ORDER BY s.ts DESC
                       LIMIT 1
                   )
               ),
               entry_fair_yes = COALESCE(
                   entry_fair_yes,
                   (
                       SELECT s.fair_yes
                       FROM {signal_table} s
                       WHERE s.market_id = {table}.market_id
                         AND s.signal_type = ?
                         AND s.ts <= {table}.entry_ts
                       ORDER BY s.ts DESC
                       LIMIT 1
                   )
               ),
               entry_fair_no = COALESCE(
                   entry_fair_no,
                   (
                       SELECT s.fair_no
                       FROM {signal_table} s
                       WHERE s.market_id = {table}.market_id
                         AND s.signal_type = ?
                         AND s.ts <= {table}.entry_ts
                       ORDER BY s.ts DESC
                       LIMIT 1
                   )
               ),
               entry_seconds_left = COALESCE(
                   entry_seconds_left,
                   (
                       SELECT s.seconds_left
                       FROM {signal_table} s
                       WHERE s.market_id = {table}.market_id
                         AND s.signal_type = ?
                         AND s.ts <= {table}.entry_ts
                       ORDER BY s.ts DESC
                       LIMIT 1
                   )
               ),
               entry_min_depth_usdc = COALESCE(
                   entry_min_depth_usdc,
                   (
                       SELECT s.min_depth_usdc
                       FROM {signal_table} s
                       WHERE s.market_id = {table}.market_id
                         AND s.signal_type = ?
                         AND s.ts <= {table}.entry_ts
                       ORDER BY s.ts DESC
                       LIMIT 1
                   )
               ),
               entry_book_age_ms = COALESCE(
                   entry_book_age_ms,
                   (
                       SELECT s.book_age_ms
                       FROM {signal_table} s
                       WHERE s.market_id = {table}.market_id
                         AND s.signal_type = ?
                         AND s.ts <= {table}.entry_ts
                       ORDER BY s.ts DESC
                       LIMIT 1
                   )
               ),
               entry_reference_price = COALESCE(
                   entry_reference_price,
                   (
                       SELECT s.reference_price
                       FROM {signal_table} s
                       WHERE s.market_id = {table}.market_id
                         AND s.signal_type = ?
                         AND s.ts <= {table}.entry_ts
                       ORDER BY s.ts DESC
                       LIMIT 1
                   )
               ),
               entry_volatility = COALESCE(
                   entry_volatility,
                   (
                       SELECT s.volatility
                       FROM {signal_table} s
                       WHERE s.market_id = {table}.market_id
                         AND s.signal_type = ?
                         AND s.ts <= {table}.entry_ts
                       ORDER BY s.ts DESC
                       LIMIT 1
                   )
               ),
               entry_tenor_minutes = COALESCE(
                   NULLIF(entry_tenor_minutes, 0),
                   (
                       SELECT s.tenor_minutes
                       FROM {signal_table} s
                       WHERE s.market_id = {table}.market_id
                         AND s.signal_type = ?
                         AND s.ts <= {table}.entry_ts
                       ORDER BY s.ts DESC
                       LIMIT 1
                   ),
                   (
                       SELECT m.tenor_minutes
                       FROM markets m
                       WHERE m.market_id = {table}.market_id
                       LIMIT 1
                   )
               ),
               entry_reason = COALESCE(
                   NULLIF(entry_reason, ''),
                   (
                       SELECT s.reason
                       FROM {signal_table} s
                       WHERE s.market_id = {table}.market_id
                         AND s.signal_type = ?
                         AND s.ts <= {table}.entry_ts
                       ORDER BY s.ts DESC
                       LIMIT 1
                   )
               )
         WHERE entry_signal_type IS NULL
            OR entry_edge IS NULL
            OR entry_seconds_left IS NULL
            OR COALESCE(entry_tenor_minutes, 0) = 0
        """,
        (signal_type,) * 11,
    )


@contextmanager
def connect_latency_bot_db(settings: LatencyBotSettings) -> Iterator[sqlite3.Connection]:
    settings.ensure_dirs()
    conn = sqlite3.connect(settings.db_path, timeout=30.0)
    try:
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.row_factory = sqlite3.Row
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _rebuild_equity_snapshots(conn: sqlite3.Connection, settings: LatencyBotSettings) -> None:
    conn.execute("DELETE FROM equity_snapshots")
    close_rows = conn.execute(
        """
        SELECT ts, pnl
        FROM position_events
        WHERE event_type = 'close'
        ORDER BY ts ASC, id ASC
        """
    ).fetchall()
    running_realized = 0.0
    for row in close_rows:
        running_realized = round(running_realized + float(row["pnl"] or 0.0), 6)
        conn.execute(
            """
            INSERT INTO equity_snapshots (
                ts, realized_pnl_usdc, unrealized_pnl_usdc, equity_usdc
            ) VALUES (?, ?, ?, ?)
            """,
            (
                str(row["ts"] or ""),
                running_realized,
                0.0,
                round(settings.bankroll_usdc + running_realized, 6),
            ),
        )


def _dedupe_duplicate_live_closes(conn: sqlite3.Connection, settings: LatencyBotSettings) -> int:
    duplicate_rows = conn.execute(
        """
        SELECT pe.id, pe.position_id, pe.ts
        FROM position_events pe
        WHERE pe.event_type = 'close'
          AND pe.id NOT IN (
              SELECT MIN(id)
              FROM position_events
              WHERE event_type = 'close'
              GROUP BY position_id
          )
        ORDER BY pe.id ASC
        """
    ).fetchall()
    for row in duplicate_rows:
        order_id = f"paper-exit-{row['position_id']}-{row['ts']}"
        fill_id = f"paper-exit-fill-{row['position_id']}-{row['ts']}"
        conn.execute("DELETE FROM fills WHERE fill_id = ? OR order_id = ?", (fill_id, order_id))
        conn.execute("DELETE FROM orders WHERE order_id = ?", (order_id,))
        conn.execute("DELETE FROM position_events WHERE id = ?", (row["id"],))
    if duplicate_rows:
        _rebuild_equity_snapshots(conn, settings)
    return len(duplicate_rows)


def _dedupe_duplicate_shadow_closes(conn: sqlite3.Connection) -> int:
    duplicate_rows = conn.execute(
        """
        SELECT spe.id
        FROM shadow_position_events spe
        WHERE spe.event_type = 'close'
          AND spe.id NOT IN (
              SELECT MIN(id)
              FROM shadow_position_events
              WHERE event_type = 'close'
              GROUP BY position_id
          )
        ORDER BY spe.id ASC
        """
    ).fetchall()
    for row in duplicate_rows:
        conn.execute("DELETE FROM shadow_position_events WHERE id = ?", (row["id"],))
    return len(duplicate_rows)


def _ensure_integrity_indexes(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_position_events_one_close
        ON position_events(position_id)
        WHERE event_type = 'close'
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_shadow_position_events_one_close
        ON shadow_position_events(position_id)
        WHERE event_type = 'close'
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_shadow_variant_position_events_one_close
        ON shadow_variant_position_events(position_id)
        WHERE event_type = 'close'
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_cex_latency_paper_events_one_close
        ON cex_latency_paper_events(position_id)
        WHERE event_type = 'close'
        """
    )


def _ensure_query_indexes(conn: sqlite3.Connection) -> None:
    statements = (
        "CREATE INDEX IF NOT EXISTS idx_binance_ticks_ts ON binance_ticks(ts)",
        "CREATE INDEX IF NOT EXISTS idx_polymarket_books_ts ON polymarket_books(ts)",
        "CREATE INDEX IF NOT EXISTS idx_polymarket_books_market_ts ON polymarket_books(market_id, ts)",
        "CREATE INDEX IF NOT EXISTS idx_fair_values_ts ON fair_values(ts)",
        "CREATE INDEX IF NOT EXISTS idx_fair_values_market_ts ON fair_values(market_id, ts)",
        "CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(ts)",
        "CREATE INDEX IF NOT EXISTS idx_signals_asset_tenor_ts ON signals(asset, tenor_minutes, ts)",
        "CREATE INDEX IF NOT EXISTS idx_signals_market_type_ts ON signals(market_id, signal_type, ts)",
        "CREATE INDEX IF NOT EXISTS idx_orders_ts_created ON orders(ts_created)",
        "CREATE INDEX IF NOT EXISTS idx_fills_ts ON fills(ts)",
        "CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status)",
        "CREATE INDEX IF NOT EXISTS idx_positions_mode_status ON positions(mode, status)",
        "CREATE INDEX IF NOT EXISTS idx_position_events_type_ts ON position_events(event_type, ts)",
        "CREATE INDEX IF NOT EXISTS idx_shadow_signals_ts ON shadow_signals(ts)",
        "CREATE INDEX IF NOT EXISTS idx_shadow_signals_asset_tenor_ts ON shadow_signals(asset, tenor_minutes, ts)",
        "CREATE INDEX IF NOT EXISTS idx_shadow_signals_market_type_ts ON shadow_signals(market_id, signal_type, ts)",
        "CREATE INDEX IF NOT EXISTS idx_shadow_positions_status ON shadow_positions(status)",
        "CREATE INDEX IF NOT EXISTS idx_shadow_position_events_type_ts ON shadow_position_events(event_type, ts)",
        "CREATE INDEX IF NOT EXISTS idx_cex_latency_paper_signals_ts ON cex_latency_paper_signals(ts)",
        "CREATE INDEX IF NOT EXISTS idx_cex_latency_paper_signals_market_ts ON cex_latency_paper_signals(market_id, ts)",
        "CREATE INDEX IF NOT EXISTS idx_cex_latency_paper_signals_reason_ts ON cex_latency_paper_signals(reason, ts)",
        "CREATE INDEX IF NOT EXISTS idx_cex_latency_paper_positions_status ON cex_latency_paper_positions(status)",
        "CREATE INDEX IF NOT EXISTS idx_cex_latency_paper_positions_market_ts ON cex_latency_paper_positions(market_id, entry_ts)",
        "CREATE INDEX IF NOT EXISTS idx_cex_latency_paper_events_type_ts ON cex_latency_paper_events(event_type, ts)",
        "CREATE INDEX IF NOT EXISTS idx_temporal_inventory_markets_state ON temporal_inventory_markets(state)",
        "CREATE INDEX IF NOT EXISTS idx_temporal_inventory_events_type_ts ON temporal_inventory_events(event_type, ts)",
        "CREATE INDEX IF NOT EXISTS idx_temporal_inventory_events_market_ts ON temporal_inventory_events(market_id, ts)",
        "CREATE INDEX IF NOT EXISTS idx_temporal_inventory_quotes_status ON temporal_inventory_quotes(status)",
        "CREATE INDEX IF NOT EXISTS idx_temporal_inventory_quotes_market_ts ON temporal_inventory_quotes(market_id, ts_created)",
        "CREATE INDEX IF NOT EXISTS idx_live_temporal_inventory_maker_orders_status ON live_temporal_inventory_maker_orders(status)",
        "CREATE INDEX IF NOT EXISTS idx_live_temporal_inventory_maker_orders_market_ts ON live_temporal_inventory_maker_orders(market_id, ts_created)",
        "CREATE INDEX IF NOT EXISTS idx_live_temporal_inventory_maker_heartbeats_ts ON live_temporal_inventory_maker_heartbeats(ts)",
        "CREATE INDEX IF NOT EXISTS idx_late_resolution_capture_signals_ts ON late_resolution_capture_signals(ts)",
        "CREATE INDEX IF NOT EXISTS idx_late_resolution_capture_signals_market_ts ON late_resolution_capture_signals(market_id, ts)",
        "CREATE INDEX IF NOT EXISTS idx_late_resolution_capture_positions_status ON late_resolution_capture_positions(status)",
        "CREATE INDEX IF NOT EXISTS idx_late_resolution_capture_events_type_ts ON late_resolution_capture_events(event_type, ts)",
        "CREATE INDEX IF NOT EXISTS idx_shadow_variant_signals_ts ON shadow_variant_signals(ts)",
        "CREATE INDEX IF NOT EXISTS idx_shadow_variant_signals_variant ON shadow_variant_signals(variant_id)",
        "CREATE INDEX IF NOT EXISTS idx_shadow_variant_signals_variant_reason ON shadow_variant_signals(variant_id, reason)",
        "CREATE INDEX IF NOT EXISTS idx_shadow_variant_positions_status ON shadow_variant_positions(status)",
        "CREATE INDEX IF NOT EXISTS idx_shadow_variant_positions_variant_status ON shadow_variant_positions(variant_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_shadow_variant_position_events_type_ts ON shadow_variant_position_events(event_type, ts)",
        "CREATE INDEX IF NOT EXISTS idx_complete_set_arb_signals_ts ON complete_set_arb_signals(ts)",
        "CREATE INDEX IF NOT EXISTS idx_complete_set_arb_signals_market_ts ON complete_set_arb_signals(market_id, ts)",
        "CREATE INDEX IF NOT EXISTS idx_complete_set_arb_positions_market_ts ON complete_set_arb_positions(market_id, entry_ts)",
        "CREATE INDEX IF NOT EXISTS idx_complete_set_arb_positions_status ON complete_set_arb_positions(status)",
        "CREATE INDEX IF NOT EXISTS idx_complete_set_arb_events_type_ts ON complete_set_arb_events(event_type, ts)",
        "CREATE INDEX IF NOT EXISTS idx_live_complete_set_arb_pilot_attempts_ts ON live_complete_set_arb_pilot_attempts(ts)",
        "CREATE INDEX IF NOT EXISTS idx_live_complete_set_arb_pilot_attempts_market_ts ON live_complete_set_arb_pilot_attempts(market_id, ts)",
        "CREATE INDEX IF NOT EXISTS idx_live_complete_set_arb_pilot_attempts_decision_ts ON live_complete_set_arb_pilot_attempts(decision, ts)",
        "CREATE INDEX IF NOT EXISTS idx_polymarket_us_arb_ticks_ts ON polymarket_us_arb_ticks(ts)",
        "CREATE INDEX IF NOT EXISTS idx_polymarket_us_arb_ticks_symbol_ts ON polymarket_us_arb_ticks(symbol, ts)",
        "CREATE INDEX IF NOT EXISTS idx_kalshi_arb_ticks_ts ON kalshi_arb_ticks(ts)",
        "CREATE INDEX IF NOT EXISTS idx_kalshi_arb_ticks_ticker_ts ON kalshi_arb_ticks(ticker, ts)",
    )
    for statement in statements:
        conn.execute(statement)


def _ensure_shadow_variant_signal_summaries(conn: sqlite3.Connection) -> None:
    summary_count = int(conn.execute("SELECT COUNT(*) AS count FROM shadow_variant_signal_summary").fetchone()["count"])
    if summary_count:
        return
    max_signal_id = int(conn.execute("SELECT COALESCE(MAX(id), 0) AS max_id FROM shadow_variant_signals").fetchone()["max_id"] or 0)
    if max_signal_id == 0 or max_signal_id > 1_000_000:
        return
    conn.execute(
        """
        INSERT INTO shadow_variant_signal_summary (variant_id, signals, eligible)
        SELECT variant_id,
               COUNT(*) AS signals,
               SUM(CASE WHEN eligible THEN 1 ELSE 0 END) AS eligible
        FROM shadow_variant_signals
        GROUP BY variant_id
        """
    )
    conn.execute(
        """
        INSERT INTO shadow_variant_reason_summary (variant_id, reason, count, max_edge, max_side_fair)
        SELECT
            variant_id,
            COALESCE(reason, '') AS reason,
            COUNT(*) AS count,
            MAX(edge) AS max_edge,
            MAX(
                CASE
                    WHEN UPPER(COALESCE(side, 'YES')) = 'NO' THEN COALESCE(fair_no, 0.0)
                    ELSE COALESCE(fair_yes, 0.0)
                END
            ) AS max_side_fair
        FROM shadow_variant_signals
        GROUP BY variant_id, COALESCE(reason, '')
        """
    )


def _should_run_legacy_backfills(conn: sqlite3.Connection, user_version: int) -> bool:
    if user_version < 1:
        return True
    page_count = int(conn.execute("PRAGMA page_count").fetchone()[0] or 0)
    page_size = int(conn.execute("PRAGMA page_size").fetchone()[0] or 4096)
    return page_count * page_size < 100 * 1024 * 1024


def init_latency_bot_db(settings: LatencyBotSettings) -> dict[str, Any]:
    with connect_latency_bot_db(settings) as conn:
        user_version = int(conn.execute("PRAGMA user_version").fetchone()[0] or 0)
        for statement in _SCHEMA:
            conn.execute(statement)
        _ensure_columns(conn, "signals", _SIGNAL_FEATURE_COLUMNS)
        _ensure_columns(conn, "shadow_signals", _SIGNAL_FEATURE_COLUMNS)
        _ensure_columns(conn, "shadow_variant_signals", _SIGNAL_FEATURE_COLUMNS)
        _ensure_columns(conn, "cex_latency_paper_signals", {"order_price": "REAL"})
        _ensure_columns(conn, "polymarket_books", _POLYMARKET_BOOK_COLUMNS)
        _ensure_columns(conn, "positions", _POSITION_ENTRY_FEATURE_COLUMNS)
        _ensure_columns(conn, "shadow_positions", _POSITION_ENTRY_FEATURE_COLUMNS)
        _ensure_columns(conn, "shadow_variant_positions", _POSITION_ENTRY_FEATURE_COLUMNS)
        if _should_run_legacy_backfills(conn, user_version):
            _backfill_signal_dimensions(conn, "signals")
            _backfill_signal_dimensions(conn, "shadow_signals")
            _backfill_signal_dimensions(conn, "shadow_variant_signals")
            _backfill_signal_dimensions(conn, "cex_latency_paper_signals")
            _backfill_position_entry_features(conn, "positions", "signals", "TAKE_YES")
            _backfill_position_entry_features(conn, "shadow_positions", "shadow_signals", "SHADOW_TAKE_NO")
            conn.execute("PRAGMA user_version = 1")
        deduped_live_closes = _dedupe_duplicate_live_closes(conn, settings)
        deduped_shadow_closes = _dedupe_duplicate_shadow_closes(conn)
        _ensure_integrity_indexes(conn)
        _ensure_query_indexes(conn)
        _ensure_shadow_variant_signal_summaries(conn)
        snapshot_count = int(conn.execute("SELECT COUNT(*) AS count FROM equity_snapshots").fetchone()["count"])
        if snapshot_count == 0:
            _rebuild_equity_snapshots(conn, settings)
        conn.commit()
    return {
        "db_path": str(settings.db_path),
        "deduped_live_closes": deduped_live_closes,
        "deduped_shadow_closes": deduped_shadow_closes,
        "tables_created": [
            "markets",
            "binance_ticks",
            "polymarket_books",
            "fair_values",
            "signals",
            "orders",
            "fills",
            "positions",
            "position_events",
            "risk_events",
            "missed_opportunities",
            "engine_cycles",
            "equity_snapshots",
            "shadow_signals",
            "shadow_positions",
            "shadow_position_events",
            "temporal_inventory_markets",
            "temporal_inventory_events",
            "temporal_inventory_quotes",
            "live_temporal_inventory_maker_orders",
            "live_temporal_inventory_maker_heartbeats",
            "late_resolution_capture_signals",
            "late_resolution_capture_positions",
            "late_resolution_capture_events",
        ],
    }


def record_engine_cycle(
    settings: LatencyBotSettings,
    *,
    ts: str,
    phase: str,
    markets_tracked: int,
    signals_seen: int,
    orders_open: int,
    positions_open: int,
    risk_state: str,
    notes: str = "",
) -> None:
    with connect_latency_bot_db(settings) as conn:
        conn.execute(
            """
            INSERT INTO engine_cycles (
                ts, phase, markets_tracked, signals_seen, orders_open, positions_open, risk_state, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (ts, phase, markets_tracked, signals_seen, orders_open, positions_open, risk_state, notes),
        )
        conn.commit()


def record_equity_snapshot(
    settings: LatencyBotSettings,
    *,
    ts: str,
    realized_pnl_usdc: float,
    unrealized_pnl_usdc: float,
    equity_usdc: float,
) -> None:
    with connect_latency_bot_db(settings) as conn:
        conn.execute(
            """
            INSERT INTO equity_snapshots (
                ts, realized_pnl_usdc, unrealized_pnl_usdc, equity_usdc
            ) VALUES (?, ?, ?, ?)
            """,
            (
                ts,
                float(realized_pnl_usdc),
                float(unrealized_pnl_usdc),
                float(equity_usdc),
            ),
        )
        conn.commit()


def replace_markets(settings: LatencyBotSettings, items: list[dict[str, Any]]) -> None:
    with connect_latency_bot_db(settings) as conn:
        conn.execute("DELETE FROM markets")
        for item in items:
            conn.execute(
                """
                INSERT INTO markets (
                    market_id, question, asset, tenor_minutes, expiry_ts,
                    yes_token_id, no_token_id, created_at, first_seen_at, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(item.get("market_id") or ""),
                    str(item.get("question") or ""),
                    str(item.get("asset") or ""),
                    int(item.get("tenor_minutes") or 0),
                    str(item.get("expiry_ts") or ""),
                    str(item.get("yes_token_id") or ""),
                    str(item.get("no_token_id") or ""),
                    str(item.get("created_at") or ""),
                    str(item.get("first_seen_at") or ""),
                    str(item.get("status") or ""),
                ),
            )
        conn.commit()


def replace_polymarket_books(settings: LatencyBotSettings, items: list[dict[str, Any]], *, ts: str) -> None:
    with connect_latency_bot_db(settings) as conn:
        for item in items:
            conn.execute(
                """
                INSERT INTO polymarket_books (
                    ts, market_id, side, best_bid, best_ask, bid_depth, ask_depth, spread, book_age_ms,
                    no_best_bid, no_best_ask, no_bid_depth, no_ask_depth, complete_set_cost, complete_set_edge,
                    bid_vwap, ask_vwap, bid_fillable_usdc, ask_fillable_usdc, bid_fillable_shares, ask_fillable_shares,
                    no_bid_vwap, no_ask_vwap, no_bid_fillable_usdc, no_ask_fillable_usdc, no_bid_fillable_shares, no_ask_fillable_shares
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    str(item.get("market_id") or ""),
                    "yes",
                    float(item.get("best_bid") or 0.0),
                    float(item.get("best_ask") or 0.0),
                    float(item.get("bids_depth_usdc") or 0.0),
                    float(item.get("asks_depth_usdc") or 0.0),
                    float(item.get("spread") or 0.0),
                    float(item.get("book_age_ms") or 0.0),
                    float(item.get("no_best_bid") or max(1.0 - float(item.get("best_ask") or 0.0), 0.0)),
                    float(item.get("no_best_ask") or max(1.0 - float(item.get("best_bid") or 0.0), 0.0)),
                    float(item.get("no_bids_depth_usdc") or float(item.get("asks_depth_usdc") or 0.0)),
                    float(item.get("no_asks_depth_usdc") or float(item.get("bids_depth_usdc") or 0.0)),
                    float(item.get("complete_set_cost") or (float(item.get("best_ask") or 0.0) + float(item.get("no_best_ask") or max(1.0 - float(item.get("best_bid") or 0.0), 0.0)))),
                    float(item.get("complete_set_edge") or (1.0 - float(item.get("best_ask") or 0.0) - float(item.get("no_best_ask") or max(1.0 - float(item.get("best_bid") or 0.0), 0.0)))),
                    float(item.get("bid_vwap") or item.get("best_bid") or 0.0),
                    float(item.get("ask_vwap") or item.get("best_ask") or 0.0),
                    float(item.get("bid_fillable_usdc") or item.get("bids_depth_usdc") or 0.0),
                    float(item.get("ask_fillable_usdc") or item.get("asks_depth_usdc") or 0.0),
                    float(item.get("bid_fillable_shares") or 0.0),
                    float(item.get("ask_fillable_shares") or 0.0),
                    float(item.get("no_bid_vwap") or item.get("no_best_bid") or max(1.0 - float(item.get("best_ask") or 0.0), 0.0)),
                    float(item.get("no_ask_vwap") or item.get("no_best_ask") or max(1.0 - float(item.get("best_bid") or 0.0), 0.0)),
                    float(item.get("no_bid_fillable_usdc") or item.get("no_bids_depth_usdc") or item.get("asks_depth_usdc") or 0.0),
                    float(item.get("no_ask_fillable_usdc") or item.get("no_asks_depth_usdc") or item.get("bids_depth_usdc") or 0.0),
                    float(item.get("no_bid_fillable_shares") or 0.0),
                    float(item.get("no_ask_fillable_shares") or 0.0),
                ),
            )
        conn.commit()


def replace_binance_ticks(settings: LatencyBotSettings, items: list[dict[str, Any]], *, ts: str) -> None:
    with connect_latency_bot_db(settings) as conn:
        for item in items:
            conn.execute(
                """
                INSERT INTO binance_ticks (
                    ts, asset, mid, bid, ask, last, source_latency_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    str(item.get("asset") or ""),
                    float(item.get("mid") or 0.0),
                    float(item.get("bid") or 0.0),
                    float(item.get("ask") or 0.0),
                    float(item.get("last") or 0.0),
                    float(item.get("source_latency_ms") or 0.0),
                ),
            )
        conn.commit()


def append_fair_values(settings: LatencyBotSettings, items: list[dict[str, Any]], *, ts: str) -> None:
    if not items:
        return
    with connect_latency_bot_db(settings) as conn:
        for item in items:
            conn.execute(
                """
                INSERT INTO fair_values (
                    ts, market_id, asset, fair_yes, fair_no, reference_price, volatility, time_to_expiry_sec
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    str(item.get("market_id") or ""),
                    str(item.get("asset") or ""),
                    float(item.get("fair_yes") or 0.0),
                    float(item.get("fair_no") or 0.0),
                    float(item.get("reference_price") or 0.0),
                    float(item.get("volatility") or 0.0),
                    float(item.get("time_to_expiry_sec") or 0.0),
                ),
            )
        conn.commit()


def append_signals(settings: LatencyBotSettings, items: list[dict[str, Any]], *, ts: str) -> None:
    if not items:
        return
    with connect_latency_bot_db(settings) as conn:
        for item in items:
            conn.execute(
                """
                INSERT INTO signals (
                    ts, market_id, asset, tenor_minutes, signal_type, edge, mode,
                    fair_yes, fair_no, yes_bid, yes_ask, no_bid, no_ask,
                    min_depth_usdc, book_age_ms, seconds_left, reference_price, volatility,
                    reason, eligible, blocked_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    str(item.get("market_id") or ""),
                    str(item.get("asset") or ""),
                    int(item.get("tenor_minutes") or 0),
                    str(item.get("signal_type") or "SKIP"),
                    float(item.get("edge") or 0.0),
                    str(item.get("mode") or "none"),
                    float(item.get("fair_yes") or 0.0),
                    float(item.get("fair_no") or 0.0),
                    float(item.get("yes_bid") or 0.0),
                    float(item.get("yes_ask") or 0.0),
                    float(item.get("no_bid") or 0.0),
                    float(item.get("no_ask") or 0.0),
                    float(item.get("min_depth_usdc") or 0.0),
                    float(item.get("book_age_ms") or 0.0),
                    float(item.get("seconds_left") or 0.0),
                    float(item.get("reference_price") or 0.0),
                    float(item.get("volatility") or 0.0),
                    str(item.get("reason") or ""),
                    1 if bool(item.get("eligible")) else 0,
                    str(item.get("blocked_reason") or ""),
                ),
            )
        conn.commit()


def append_shadow_signals(settings: LatencyBotSettings, items: list[dict[str, Any]], *, ts: str) -> None:
    if not items:
        return
    with connect_latency_bot_db(settings) as conn:
        for item in items:
            conn.execute(
                """
                INSERT INTO shadow_signals (
                    ts, market_id, asset, tenor_minutes, signal_type, edge, mode,
                    fair_yes, fair_no, yes_bid, yes_ask, no_bid, no_ask,
                    min_depth_usdc, book_age_ms, seconds_left, reference_price, volatility,
                    reason, eligible, blocked_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    str(item.get("market_id") or ""),
                    str(item.get("asset") or ""),
                    int(item.get("tenor_minutes") or 0),
                    str(item.get("signal_type") or "SKIP"),
                    float(item.get("edge") or 0.0),
                    str(item.get("mode") or "none"),
                    float(item.get("fair_yes") or 0.0),
                    float(item.get("fair_no") or 0.0),
                    float(item.get("yes_bid") or 0.0),
                    float(item.get("yes_ask") or 0.0),
                    float(item.get("no_bid") or 0.0),
                    float(item.get("no_ask") or 0.0),
                    float(item.get("min_depth_usdc") or 0.0),
                    float(item.get("book_age_ms") or 0.0),
                    float(item.get("seconds_left") or 0.0),
                    float(item.get("reference_price") or 0.0),
                    float(item.get("volatility") or 0.0),
                    str(item.get("reason") or ""),
                    1 if bool(item.get("eligible")) else 0,
                    str(item.get("blocked_reason") or ""),
                ),
            )
        conn.commit()


def append_cex_latency_paper_signals(settings: LatencyBotSettings, items: list[dict[str, Any]], *, ts: str) -> None:
    if not items:
        return
    with connect_latency_bot_db(settings) as conn:
        for item in items:
            conn.execute(
                """
                INSERT INTO cex_latency_paper_signals (
                    ts, market_id, asset, side, tenor_minutes, signal_type, edge, mode,
                    fair_yes, fair_no, yes_bid, yes_ask, no_bid, no_ask, order_price,
                    min_depth_usdc, book_age_ms, seconds_left, reference_price, volatility,
                    reason, eligible, blocked_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    str(item.get("market_id") or ""),
                    str(item.get("asset") or ""),
                    str(item.get("side") or ""),
                    int(item.get("tenor_minutes") or 0),
                    str(item.get("signal_type") or "CEX_LATENCY_SKIP"),
                    float(item.get("edge") or 0.0),
                    str(item.get("mode") or "cex_latency_paper"),
                    float(item.get("fair_yes") or 0.0),
                    float(item.get("fair_no") or 0.0),
                    float(item.get("yes_bid") or 0.0),
                    float(item.get("yes_ask") or 0.0),
                    float(item.get("no_bid") or 0.0),
                    float(item.get("no_ask") or 0.0),
                    float(item.get("order_price") or 0.0),
                    float(item.get("min_depth_usdc") or 0.0),
                    float(item.get("book_age_ms") or 0.0),
                    float(item.get("seconds_left") or 0.0),
                    float(item.get("reference_price") or 0.0),
                    float(item.get("volatility") or 0.0),
                    str(item.get("reason") or ""),
                    1 if bool(item.get("eligible")) else 0,
                    str(item.get("blocked_reason") or ""),
                ),
            )
        conn.commit()


def append_late_resolution_capture_signals(settings: LatencyBotSettings, items: list[dict[str, Any]], *, ts: str) -> None:
    if not items:
        return
    with connect_latency_bot_db(settings) as conn:
        for item in items:
            conn.execute(
                """
                INSERT INTO late_resolution_capture_signals (
                    ts, market_id, asset, side, tenor_minutes, signal_type, edge,
                    order_price, fair_yes, fair_no, official_confidence,
                    boundary_distance_bps, seconds_left, eligible, reason, blocked_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    str(item.get("market_id") or ""),
                    str(item.get("asset") or ""),
                    str(item.get("side") or ""),
                    int(item.get("tenor_minutes") or 0),
                    str(item.get("signal_type") or "LATE_RESOLUTION_SKIP"),
                    float(item.get("edge") or 0.0),
                    float(item.get("order_price") or 0.0),
                    float(item.get("fair_yes") or 0.0),
                    float(item.get("fair_no") or 0.0),
                    float(item.get("official_confidence") or 0.0),
                    float(item.get("boundary_distance_bps") or 0.0),
                    float(item.get("seconds_left") or 0.0),
                    1 if bool(item.get("eligible")) else 0,
                    str(item.get("reason") or ""),
                    str(item.get("blocked_reason") or ""),
                ),
            )
        conn.commit()


def append_shadow_variant_signals(settings: LatencyBotSettings, items: list[dict[str, Any]], *, ts: str) -> None:
    if not items:
        return
    with connect_latency_bot_db(settings) as conn:
        for item in items:
            variant_id = str(item.get("variant_id") or "")
            reason = str(item.get("reason") or "")
            edge = float(item.get("edge") or 0.0)
            side = str(item.get("side") or "")
            side_fair = float(item.get("fair_no") or 0.0) if side.upper() == "NO" else float(item.get("fair_yes") or 0.0)
            eligible = 1 if bool(item.get("eligible")) else 0
            conn.execute(
                """
                INSERT INTO shadow_variant_signals (
                    ts, variant_id, market_id, asset, side, tenor_minutes, signal_type, edge, mode,
                    fair_yes, fair_no, yes_bid, yes_ask, no_bid, no_ask,
                    min_depth_usdc, book_age_ms, seconds_left, reference_price, volatility,
                    reason, eligible, blocked_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    variant_id,
                    str(item.get("market_id") or ""),
                    str(item.get("asset") or ""),
                    side,
                    int(item.get("tenor_minutes") or 0),
                    str(item.get("signal_type") or "SHADOW_VARIANT_SKIP"),
                    edge,
                    str(item.get("mode") or "shadow_variant"),
                    float(item.get("fair_yes") or 0.0),
                    float(item.get("fair_no") or 0.0),
                    float(item.get("yes_bid") or 0.0),
                    float(item.get("yes_ask") or 0.0),
                    float(item.get("no_bid") or 0.0),
                    float(item.get("no_ask") or 0.0),
                    float(item.get("min_depth_usdc") or 0.0),
                    float(item.get("book_age_ms") or 0.0),
                    float(item.get("seconds_left") or 0.0),
                    float(item.get("reference_price") or 0.0),
                    float(item.get("volatility") or 0.0),
                    reason,
                    eligible,
                    str(item.get("blocked_reason") or ""),
                ),
            )
            conn.execute(
                """
                INSERT INTO shadow_variant_signal_summary (variant_id, signals, eligible)
                VALUES (?, 1, ?)
                ON CONFLICT(variant_id) DO UPDATE SET
                    signals = signals + 1,
                    eligible = eligible + excluded.eligible
                """,
                (variant_id, eligible),
            )
            conn.execute(
                """
                INSERT INTO shadow_variant_reason_summary (variant_id, reason, count, max_edge, max_side_fair)
                VALUES (?, ?, 1, ?, ?)
                ON CONFLICT(variant_id, reason) DO UPDATE SET
                    count = count + 1,
                    max_edge = MAX(COALESCE(max_edge, 0.0), excluded.max_edge),
                    max_side_fair = MAX(COALESCE(max_side_fair, 0.0), excluded.max_side_fair)
                """,
                (variant_id, reason, edge, side_fair),
            )
        conn.commit()


def append_complete_set_arb_signals(settings: LatencyBotSettings, items: list[dict[str, Any]], *, ts: str) -> None:
    if not items:
        return
    with connect_latency_bot_db(settings) as conn:
        for item in items:
            conn.execute(
                """
                INSERT INTO complete_set_arb_signals (
                    ts, market_id, asset, tenor_minutes, yes_ask, no_ask, total_cost,
                    gross_edge, net_edge, executable_depth_usdc, book_age_ms,
                    seconds_left, eligible, reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    str(item.get("market_id") or ""),
                    str(item.get("asset") or ""),
                    int(item.get("tenor_minutes") or 0),
                    float(item.get("yes_ask") or 0.0),
                    float(item.get("no_ask") or 0.0),
                    float(item.get("total_cost") or 0.0),
                    float(item.get("gross_edge") or 0.0),
                    float(item.get("net_edge") or 0.0),
                    float(item.get("executable_depth_usdc") or 0.0),
                    float(item.get("book_age_ms") or 0.0),
                    float(item.get("seconds_left") or 0.0),
                    1 if bool(item.get("eligible")) else 0,
                    str(item.get("reason") or ""),
                ),
            )
        conn.commit()


def append_polymarket_us_arb_ticks(settings: LatencyBotSettings, items: list[dict[str, Any]], *, ts: str | None = None) -> None:
    if not items:
        return
    with connect_latency_bot_db(settings) as conn:
        for item in items:
            conn.execute(
                """
                INSERT INTO polymarket_us_arb_ticks (
                    ts, symbol, state, best_bid, best_ask, bid_qty, ask_qty,
                    gross_edge, executable_depth_usdc, transact_time, source, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(item.get("ts") or ts or ""),
                    str(item.get("symbol") or ""),
                    str(item.get("state") or ""),
                    float(item.get("best_bid") or 0.0),
                    float(item.get("best_ask") or 0.0),
                    float(item.get("bid_qty") or 0.0),
                    float(item.get("ask_qty") or 0.0),
                    float(item.get("gross_edge") or 0.0),
                    float(item.get("executable_depth_usdc") or 0.0),
                    str(item.get("transact_time") or ""),
                    str(item.get("source") or ""),
                    str(item.get("error") or ""),
                ),
            )
        conn.commit()


def append_kalshi_arb_ticks(settings: LatencyBotSettings, items: list[dict[str, Any]], *, ts: str | None = None) -> None:
    if not items:
        return
    with connect_latency_bot_db(settings) as conn:
        for item in items:
            conn.execute(
                """
                INSERT INTO kalshi_arb_ticks (
                    ts, ticker, asset, title, status, yes_bid, no_bid, yes_bid_qty, no_bid_qty,
                    yes_ask, no_ask, total_cost, gross_edge, executable_depth_usdc,
                    seconds_left, close_time, source, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(item.get("ts") or ts or ""),
                    str(item.get("ticker") or ""),
                    str(item.get("asset") or ""),
                    str(item.get("title") or ""),
                    str(item.get("status") or ""),
                    float(item.get("yes_bid") or 0.0),
                    float(item.get("no_bid") or 0.0),
                    float(item.get("yes_bid_qty") or 0.0),
                    float(item.get("no_bid_qty") or 0.0),
                    float(item.get("yes_ask") or 0.0),
                    float(item.get("no_ask") or 0.0),
                    float(item.get("total_cost") or 0.0),
                    float(item.get("gross_edge") or 0.0),
                    float(item.get("executable_depth_usdc") or 0.0),
                    float(item.get("seconds_left") or 0.0),
                    str(item.get("close_time") or ""),
                    str(item.get("source") or ""),
                    str(item.get("error") or ""),
                ),
            )
        conn.commit()


def load_open_positions(settings: LatencyBotSettings) -> list[dict[str, Any]]:
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT
                position_id, market_id, asset, side, entry_ts, entry_price, size, mode, status,
                entry_signal_type, entry_edge, entry_fair_yes, entry_fair_no, entry_seconds_left,
                entry_min_depth_usdc, entry_book_age_ms, entry_reference_price, entry_volatility,
                entry_tenor_minutes, entry_reason
            FROM positions
            WHERE status = 'open'
            ORDER BY entry_ts ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def load_shadow_open_positions(settings: LatencyBotSettings) -> list[dict[str, Any]]:
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT
                position_id, market_id, asset, side, entry_ts, entry_price, size, mode, status,
                entry_signal_type, entry_edge, entry_fair_yes, entry_fair_no, entry_seconds_left,
                entry_min_depth_usdc, entry_book_age_ms, entry_reference_price, entry_volatility,
                entry_tenor_minutes, entry_reason
            FROM shadow_positions
            WHERE status = 'open'
            ORDER BY entry_ts ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def load_shadow_variant_open_positions(settings: LatencyBotSettings, *, variant_id: str | None = None) -> list[dict[str, Any]]:
    where = "WHERE status = 'open'"
    params: tuple[Any, ...] = ()
    if variant_id is not None:
        where += " AND variant_id = ?"
        params = (variant_id,)
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            f"""
            SELECT
                position_id, variant_id, market_id, asset, side, entry_ts, entry_price, size, mode, status,
                entry_signal_type, entry_edge, entry_fair_yes, entry_fair_no, entry_seconds_left,
                entry_min_depth_usdc, entry_book_age_ms, entry_reference_price, entry_volatility,
                entry_tenor_minutes, entry_reason
            FROM shadow_variant_positions
            {where}
            ORDER BY variant_id ASC, entry_ts ASC
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def load_recent_position_closes(settings: LatencyBotSettings, *, limit: int = 20) -> list[dict[str, Any]]:
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT pe.ts, pe.position_id, p.market_id, p.asset, p.side, p.entry_price, pe.mark, pe.pnl, pe.reason
            FROM position_events pe
            JOIN positions p ON p.position_id = pe.position_id
            WHERE pe.event_type = 'close'
            ORDER BY pe.ts DESC, pe.id DESC
            LIMIT ?
            """,
            (max(1, limit),),
        ).fetchall()
    return [dict(row) for row in rows]


def load_open_orders(settings: LatencyBotSettings) -> list[dict[str, Any]]:
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT order_id, ts_created, market_id, mode, side, price, size, status, reprices, cancel_reason
            FROM orders
            WHERE status = 'open'
            ORDER BY ts_created ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def latency_bot_capital_usage(settings: LatencyBotSettings) -> dict[str, Any]:
    with connect_latency_bot_db(settings) as conn:
        live_positions = conn.execute(
            """
            SELECT COUNT(*) AS count, COALESCE(SUM(entry_price * size), 0.0) AS capital
            FROM positions
            WHERE status = 'open'
            """
        ).fetchone()
        open_orders = conn.execute(
            """
            SELECT COUNT(*) AS count, COALESCE(SUM(price * size), 0.0) AS capital
            FROM orders
            WHERE status = 'open'
            """
        ).fetchone()
        complete_sets = conn.execute(
            """
            SELECT COUNT(*) AS count, COALESCE(SUM(notional_usdc), 0.0) AS capital
            FROM complete_set_arb_positions
            WHERE status = 'open'
            """
        ).fetchone()
    live_capital = float(live_positions["capital"] or 0.0) if live_positions else 0.0
    order_capital = float(open_orders["capital"] or 0.0) if open_orders else 0.0
    complete_set_capital = float(complete_sets["capital"] or 0.0) if complete_sets else 0.0
    total_capital = live_capital + order_capital + complete_set_capital
    bankroll = max(float(settings.bankroll_usdc), 1.0)
    max_complete_set_cycle_notional = float(settings.complete_set_arb_notional_usdc) * max(
        int(settings.complete_set_arb_max_sets_per_cycle), 0
    )
    return {
        "bankroll_usdc": float(settings.bankroll_usdc),
        "live_position_count": int(live_positions["count"] or 0) if live_positions else 0,
        "live_position_capital_usdc": round(live_capital, 6),
        "open_order_count": int(open_orders["count"] or 0) if open_orders else 0,
        "open_order_capital_usdc": round(order_capital, 6),
        "complete_set_open_count": int(complete_sets["count"] or 0) if complete_sets else 0,
        "complete_set_locked_capital_usdc": round(complete_set_capital, 6),
        "total_current_capital_usdc": round(total_capital, 6),
        "total_current_bankroll_fraction": round(total_capital / bankroll, 6),
        "directional_trade_notional_usdc": float(settings.paper_position_notional_usdc),
        "complete_set_trade_notional_usdc": float(settings.complete_set_arb_notional_usdc),
        "complete_set_max_sets_per_cycle": int(settings.complete_set_arb_max_sets_per_cycle),
        "complete_set_max_cycle_notional_usdc": round(max_complete_set_cycle_notional, 6),
        "complete_set_max_cycle_bankroll_fraction": round(max_complete_set_cycle_notional / bankroll, 6),
    }


def load_latest_polymarket_books(settings: LatencyBotSettings, market_ids: list[str]) -> dict[str, dict[str, Any]]:
    normalized = [str(item or "").strip() for item in market_ids if str(item or "").strip()]
    if not normalized:
        return {}
    placeholders = ",".join("?" for _ in normalized)
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            f"""
            SELECT
                pb.market_id, pb.ts, pb.best_bid, pb.best_ask, pb.bid_depth, pb.ask_depth, pb.spread, pb.book_age_ms,
                pb.no_best_bid, pb.no_best_ask, pb.no_bid_depth, pb.no_ask_depth,
                pb.complete_set_cost, pb.complete_set_edge,
                pb.bid_vwap, pb.ask_vwap, pb.bid_fillable_usdc, pb.ask_fillable_usdc, pb.bid_fillable_shares, pb.ask_fillable_shares,
                pb.no_bid_vwap, pb.no_ask_vwap, pb.no_bid_fillable_usdc, pb.no_ask_fillable_usdc, pb.no_bid_fillable_shares, pb.no_ask_fillable_shares
            FROM polymarket_books pb
            JOIN (
                SELECT market_id, MAX(ts) AS latest_ts
                FROM polymarket_books
                WHERE market_id IN ({placeholders})
                GROUP BY market_id
            ) latest
              ON latest.market_id = pb.market_id
             AND latest.latest_ts = pb.ts
            """,
            normalized,
        ).fetchall()
    return {str(row["market_id"]): dict(row) for row in rows}


def create_position(
    settings: LatencyBotSettings,
    *,
    ts: str,
    market_id: str,
    asset: str,
    side: str,
    entry_price: float,
    size: float,
    mode: str,
    signal: dict[str, Any] | None = None,
) -> dict[str, Any]:
    position_id = str(uuid.uuid4())
    order_id = f"paper-entry-{position_id}"
    fill_id = f"paper-fill-{position_id}"
    with connect_latency_bot_db(settings) as conn:
        conn.execute(
            """
            INSERT INTO orders (
                order_id, ts_created, market_id, mode, side, price, size, status, reprices, cancel_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (order_id, ts, market_id, mode, side, entry_price, size, "filled", 0, ""),
        )
        conn.execute(
            """
            INSERT INTO fills (
                fill_id, order_id, ts, market_id, side, price, size, fill_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (fill_id, order_id, ts, market_id, side, entry_price, size, "entry"),
        )
        conn.execute(
            """
            INSERT INTO positions (
                position_id, market_id, asset, side, entry_ts, entry_price, size, mode, status,
                entry_signal_type, entry_edge, entry_fair_yes, entry_fair_no, entry_seconds_left,
                entry_min_depth_usdc, entry_book_age_ms, entry_reference_price, entry_volatility,
                entry_tenor_minutes, entry_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                position_id,
                market_id,
                asset,
                side,
                ts,
                entry_price,
                size,
                mode,
                "open",
                str((signal or {}).get("signal_type") or ""),
                float((signal or {}).get("edge") or 0.0),
                float((signal or {}).get("fair_yes") or 0.0),
                float((signal or {}).get("fair_no") or 0.0),
                float((signal or {}).get("seconds_left") or 0.0),
                float((signal or {}).get("min_depth_usdc") or 0.0),
                float((signal or {}).get("book_age_ms") or 0.0),
                float((signal or {}).get("reference_price") or 0.0),
                float((signal or {}).get("volatility") or 0.0),
                int((signal or {}).get("tenor_minutes") or 0),
                str((signal or {}).get("reason") or ""),
            ),
        )
        conn.execute(
            """
            INSERT INTO position_events (
                ts, position_id, event_type, mark, edge, pnl, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (ts, position_id, "open", entry_price, None, 0.0, "signal_entry"),
        )
        conn.commit()
    return {
        "position_id": position_id,
        "market_id": market_id,
        "asset": asset,
        "side": side,
        "entry_price": entry_price,
        "size": size,
        "mode": mode,
        "entry_edge": float((signal or {}).get("edge") or 0.0),
        "entry_seconds_left": float((signal or {}).get("seconds_left") or 0.0),
    }


def create_order(
    settings: LatencyBotSettings,
    *,
    ts: str,
    market_id: str,
    side: str,
    price: float,
    size: float,
    mode: str,
    reprices: int = 0,
) -> dict[str, Any]:
    order_id = str(uuid.uuid4())
    with connect_latency_bot_db(settings) as conn:
        conn.execute(
            """
            INSERT INTO orders (
                order_id, ts_created, market_id, mode, side, price, size, status, reprices, cancel_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (order_id, ts, market_id, mode, side, price, size, "open", reprices, ""),
        )
        conn.commit()
    return {
        "order_id": order_id,
        "ts_created": ts,
        "market_id": market_id,
        "mode": mode,
        "side": side,
        "price": price,
        "size": size,
        "status": "open",
        "reprices": reprices,
        "cancel_reason": "",
    }


def create_shadow_position(
    settings: LatencyBotSettings,
    *,
    ts: str,
    market_id: str,
    asset: str,
    side: str,
    entry_price: float,
    size: float,
    mode: str,
    signal: dict[str, Any] | None = None,
) -> dict[str, Any]:
    position_id = str(uuid.uuid4())
    with connect_latency_bot_db(settings) as conn:
        conn.execute(
            """
            INSERT INTO shadow_positions (
                position_id, market_id, asset, side, entry_ts, entry_price, size, mode, status,
                entry_signal_type, entry_edge, entry_fair_yes, entry_fair_no, entry_seconds_left,
                entry_min_depth_usdc, entry_book_age_ms, entry_reference_price, entry_volatility,
                entry_tenor_minutes, entry_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                position_id,
                market_id,
                asset,
                side,
                ts,
                entry_price,
                size,
                mode,
                "open",
                str((signal or {}).get("signal_type") or ""),
                float((signal or {}).get("edge") or 0.0),
                float((signal or {}).get("fair_yes") or 0.0),
                float((signal or {}).get("fair_no") or 0.0),
                float((signal or {}).get("seconds_left") or 0.0),
                float((signal or {}).get("min_depth_usdc") or 0.0),
                float((signal or {}).get("book_age_ms") or 0.0),
                float((signal or {}).get("reference_price") or 0.0),
                float((signal or {}).get("volatility") or 0.0),
                int((signal or {}).get("tenor_minutes") or 0),
                str((signal or {}).get("reason") or ""),
            ),
        )
        conn.execute(
            """
            INSERT INTO shadow_position_events (
                ts, position_id, event_type, mark, edge, pnl, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (ts, position_id, "open", entry_price, None, 0.0, "shadow_signal_entry"),
        )
        conn.commit()
    return {
        "position_id": position_id,
        "market_id": market_id,
        "asset": asset,
        "side": side,
        "entry_price": entry_price,
        "size": size,
        "mode": mode,
        "entry_edge": float((signal or {}).get("edge") or 0.0),
        "entry_seconds_left": float((signal or {}).get("seconds_left") or 0.0),
    }


def create_shadow_variant_position(
    settings: LatencyBotSettings,
    *,
    ts: str,
    variant_id: str,
    market_id: str,
    asset: str,
    side: str,
    entry_price: float,
    size: float,
    mode: str,
    signal: dict[str, Any] | None = None,
) -> dict[str, Any]:
    position_id = str(uuid.uuid4())
    with connect_latency_bot_db(settings) as conn:
        conn.execute(
            """
            INSERT INTO shadow_variant_positions (
                position_id, variant_id, market_id, asset, side, entry_ts, entry_price, size, mode, status,
                entry_signal_type, entry_edge, entry_fair_yes, entry_fair_no, entry_seconds_left,
                entry_min_depth_usdc, entry_book_age_ms, entry_reference_price, entry_volatility,
                entry_tenor_minutes, entry_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                position_id,
                variant_id,
                market_id,
                asset,
                side,
                ts,
                entry_price,
                size,
                mode,
                "open",
                str((signal or {}).get("signal_type") or ""),
                float((signal or {}).get("edge") or 0.0),
                float((signal or {}).get("fair_yes") or 0.0),
                float((signal or {}).get("fair_no") or 0.0),
                float((signal or {}).get("seconds_left") or 0.0),
                float((signal or {}).get("min_depth_usdc") or 0.0),
                float((signal or {}).get("book_age_ms") or 0.0),
                float((signal or {}).get("reference_price") or 0.0),
                float((signal or {}).get("volatility") or 0.0),
                int((signal or {}).get("tenor_minutes") or 0),
                str((signal or {}).get("reason") or ""),
            ),
        )
        conn.execute(
            """
            INSERT INTO shadow_variant_position_events (
                ts, variant_id, position_id, event_type, mark, edge, pnl, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (ts, variant_id, position_id, "open", entry_price, None, 0.0, "shadow_variant_signal_entry"),
        )
        conn.commit()
    return {
        "position_id": position_id,
        "variant_id": variant_id,
        "market_id": market_id,
        "asset": asset,
        "side": side,
        "entry_price": entry_price,
        "size": size,
        "mode": mode,
        "entry_edge": float((signal or {}).get("edge") or 0.0),
        "entry_seconds_left": float((signal or {}).get("seconds_left") or 0.0),
    }


def load_cex_latency_paper_open_positions(
    settings: LatencyBotSettings,
    *,
    mode: str = "cex_latency_paper",
) -> list[dict[str, Any]]:
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM cex_latency_paper_positions
            WHERE status = 'open'
              AND mode = ?
            ORDER BY entry_ts ASC
            """,
            (mode,),
        ).fetchall()
    return [dict(row) for row in rows]


def create_cex_latency_paper_position(
    settings: LatencyBotSettings,
    *,
    ts: str,
    market_id: str,
    asset: str,
    side: str,
    entry_price: float,
    size: float,
    signal: dict[str, Any] | None = None,
    mode: str = "cex_latency_paper",
) -> dict[str, Any]:
    position_id = str(uuid.uuid4())
    notional_usdc = round(float(entry_price) * float(size), 6)
    with connect_latency_bot_db(settings) as conn:
        conn.execute(
            """
            INSERT INTO cex_latency_paper_positions (
                position_id, market_id, asset, side, entry_ts, entry_price, size,
                notional_usdc, mode, status, entry_signal_type, entry_edge,
                entry_fair_yes, entry_fair_no, entry_seconds_left, entry_min_depth_usdc,
                entry_book_age_ms, entry_reference_price, entry_volatility,
                entry_tenor_minutes, entry_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                position_id,
                market_id,
                asset,
                side,
                ts,
                float(entry_price),
                float(size),
                notional_usdc,
                mode,
                "open",
                str((signal or {}).get("signal_type") or ""),
                float((signal or {}).get("edge") or 0.0),
                float((signal or {}).get("fair_yes") or 0.0),
                float((signal or {}).get("fair_no") or 0.0),
                float((signal or {}).get("seconds_left") or 0.0),
                float((signal or {}).get("min_depth_usdc") or 0.0),
                float((signal or {}).get("book_age_ms") or 0.0),
                float((signal or {}).get("reference_price") or 0.0),
                float((signal or {}).get("volatility") or 0.0),
                int((signal or {}).get("tenor_minutes") or 0),
                str((signal or {}).get("reason") or ""),
            ),
        )
        conn.execute(
            """
            INSERT INTO cex_latency_paper_events (
                ts, position_id, event_type, mark, edge, pnl, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (ts, position_id, "open", float(entry_price), float((signal or {}).get("edge") or 0.0), 0.0, f"{mode}_entry"),
        )
        conn.commit()
    return {
        "position_id": position_id,
        "market_id": market_id,
        "asset": asset,
        "side": side,
        "entry_price": float(entry_price),
        "size": float(size),
        "notional_usdc": notional_usdc,
        "mode": mode,
        "entry_edge": float((signal or {}).get("edge") or 0.0),
    }


def close_cex_latency_paper_position(
    settings: LatencyBotSettings,
    *,
    position_id: str,
    ts: str,
    exit_price: float,
    pnl: float,
    reason: str,
    edge: float | None = None,
) -> None:
    with connect_latency_bot_db(settings) as conn:
        conn.execute(
            """
            UPDATE cex_latency_paper_positions
            SET status = 'closed'
            WHERE position_id = ? AND status = 'open'
            """,
            (position_id,),
        )
        conn.execute(
            """
            INSERT INTO cex_latency_paper_events (
                ts, position_id, event_type, mark, edge, pnl, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (ts, position_id, "close", float(exit_price), edge, float(pnl), reason),
        )
        conn.commit()


def load_late_resolution_capture_open_positions(settings: LatencyBotSettings) -> list[dict[str, Any]]:
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM late_resolution_capture_positions
            WHERE status = 'open'
            ORDER BY entry_ts ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def create_late_resolution_capture_position(
    settings: LatencyBotSettings,
    *,
    ts: str,
    market_id: str,
    asset: str,
    side: str,
    entry_price: float,
    size: float,
    signal: dict[str, Any] | None = None,
) -> dict[str, Any]:
    position_id = str(uuid.uuid4())
    notional_usdc = round(float(entry_price) * float(size), 8)
    with connect_latency_bot_db(settings) as conn:
        conn.execute(
            """
            INSERT INTO late_resolution_capture_positions (
                position_id, market_id, asset, side, entry_ts, entry_price, size,
                notional_usdc, status, entry_edge, entry_official_confidence,
                entry_boundary_distance_bps, entry_seconds_left, entry_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                position_id,
                market_id,
                asset,
                side,
                ts,
                float(entry_price),
                float(size),
                notional_usdc,
                "open",
                float((signal or {}).get("edge") or 0.0),
                float((signal or {}).get("official_confidence") or 0.0),
                float((signal or {}).get("boundary_distance_bps") or 0.0),
                float((signal or {}).get("seconds_left") or 0.0),
                str((signal or {}).get("reason") or ""),
            ),
        )
        conn.execute(
            """
            INSERT INTO late_resolution_capture_events (
                ts, position_id, event_type, mark, pnl, reason
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (ts, position_id, "open", float(entry_price), 0.0, "late_resolution_capture_entry"),
        )
        conn.commit()
    return {
        "position_id": position_id,
        "market_id": market_id,
        "asset": asset,
        "side": side,
        "entry_price": float(entry_price),
        "size": float(size),
        "notional_usdc": notional_usdc,
    }


def close_late_resolution_capture_position(
    settings: LatencyBotSettings,
    *,
    position_id: str,
    ts: str,
    mark: float,
    pnl: float,
    reason: str,
) -> None:
    with connect_latency_bot_db(settings) as conn:
        conn.execute(
            """
            UPDATE late_resolution_capture_positions
            SET status = 'closed'
            WHERE position_id = ?
              AND status = 'open'
            """,
            (position_id,),
        )
        conn.execute(
            """
            INSERT INTO late_resolution_capture_events (
                ts, position_id, event_type, mark, pnl, reason
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (ts, position_id, "close", float(mark), float(pnl), reason),
        )
        conn.commit()


def load_temporal_inventory_open_markets(settings: LatencyBotSettings) -> list[dict[str, Any]]:
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM temporal_inventory_markets
            WHERE state != 'CLOSED'
            ORDER BY updated_ts ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def load_temporal_inventory_open_quotes(settings: LatencyBotSettings) -> list[dict[str, Any]]:
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM temporal_inventory_quotes
            WHERE status = 'open'
            ORDER BY ts_created ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def upsert_temporal_inventory_market(
    settings: LatencyBotSettings,
    *,
    ts: str,
    market_id: str,
    asset: str,
    tenor_minutes: int,
    state: str,
    yes_shares: float = 0.0,
    no_shares: float = 0.0,
    yes_cost_usdc: float = 0.0,
    no_cost_usdc: float = 0.0,
    realized_pnl_usdc: float = 0.0,
    expired_inventory_cost_usdc: float = 0.0,
    locked_pair_shares: float = 0.0,
    locked_pair_cost: float = 0.0,
    locked_pair_pnl_usdc: float = 0.0,
    last_signal_side: str = "",
    last_signal_edge: float = 0.0,
    last_quote_id: str = "",
) -> dict[str, Any]:
    with connect_latency_bot_db(settings) as conn:
        conn.execute(
            """
            INSERT INTO temporal_inventory_markets (
                market_id, asset, tenor_minutes, first_seen_ts, updated_ts, state,
                yes_shares, no_shares, yes_cost_usdc, no_cost_usdc, realized_pnl_usdc,
                expired_inventory_cost_usdc, locked_pair_shares, locked_pair_cost,
                locked_pair_pnl_usdc, last_signal_side, last_signal_edge, last_quote_id, mode
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(market_id) DO UPDATE SET
                asset = excluded.asset,
                tenor_minutes = excluded.tenor_minutes,
                updated_ts = excluded.updated_ts,
                state = excluded.state,
                yes_shares = excluded.yes_shares,
                no_shares = excluded.no_shares,
                yes_cost_usdc = excluded.yes_cost_usdc,
                no_cost_usdc = excluded.no_cost_usdc,
                realized_pnl_usdc = excluded.realized_pnl_usdc,
                expired_inventory_cost_usdc = excluded.expired_inventory_cost_usdc,
                locked_pair_shares = excluded.locked_pair_shares,
                locked_pair_cost = excluded.locked_pair_cost,
                locked_pair_pnl_usdc = excluded.locked_pair_pnl_usdc,
                last_signal_side = excluded.last_signal_side,
                last_signal_edge = excluded.last_signal_edge,
                last_quote_id = excluded.last_quote_id
            """,
            (
                market_id,
                asset,
                int(tenor_minutes),
                ts,
                ts,
                state,
                round(float(yes_shares), 8),
                round(float(no_shares), 8),
                round(float(yes_cost_usdc), 8),
                round(float(no_cost_usdc), 8),
                round(float(realized_pnl_usdc), 8),
                round(float(expired_inventory_cost_usdc), 8),
                round(float(locked_pair_shares), 8),
                round(float(locked_pair_cost), 8),
                round(float(locked_pair_pnl_usdc), 8),
                str(last_signal_side or ""),
                round(float(last_signal_edge), 8),
                str(last_quote_id or ""),
                "temporal_inventory_maker_paper",
            ),
        )
        row = conn.execute("SELECT * FROM temporal_inventory_markets WHERE market_id = ?", (market_id,)).fetchone()
        conn.commit()
    return dict(row) if row is not None else {}


def record_temporal_inventory_event(
    settings: LatencyBotSettings,
    *,
    ts: str,
    market_id: str,
    event_type: str,
    state: str,
    side: str = "",
    price: float | None = None,
    size: float | None = None,
    notional_usdc: float | None = None,
    pnl_usdc: float | None = None,
    pair_cost: float | None = None,
    reason: str = "",
    metadata: dict[str, Any] | None = None,
) -> None:
    with connect_latency_bot_db(settings) as conn:
        conn.execute(
            """
            INSERT INTO temporal_inventory_events (
                ts, market_id, event_type, state, side, price, size, notional_usdc,
                pnl_usdc, pair_cost, reason, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                market_id,
                event_type,
                state,
                side,
                None if price is None else float(price),
                None if size is None else float(size),
                None if notional_usdc is None else float(notional_usdc),
                None if pnl_usdc is None else float(pnl_usdc),
                None if pair_cost is None else float(pair_cost),
                reason,
                json.dumps(metadata or {}, sort_keys=True),
            ),
        )
        conn.commit()


def create_temporal_inventory_quote(
    settings: LatencyBotSettings,
    *,
    ts: str,
    market_id: str,
    side: str,
    price: float,
    size: float,
    edge: float,
    reason: str,
) -> dict[str, Any]:
    quote_id = str(uuid.uuid4())
    notional_usdc = round(float(price) * float(size), 8)
    with connect_latency_bot_db(settings) as conn:
        conn.execute(
            """
            INSERT INTO temporal_inventory_quotes (
                quote_id, ts_created, ts_updated, market_id, side, price, size,
                notional_usdc, status, edge, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                quote_id,
                ts,
                ts,
                market_id,
                side,
                float(price),
                float(size),
                notional_usdc,
                "open",
                float(edge),
                reason,
            ),
        )
        row = conn.execute("SELECT * FROM temporal_inventory_quotes WHERE quote_id = ?", (quote_id,)).fetchone()
        conn.commit()
    return dict(row) if row is not None else {}


def close_temporal_inventory_quote(
    settings: LatencyBotSettings,
    *,
    quote_id: str,
    ts: str,
    status: str,
    fill_price: float | None = None,
    fill_size: float | None = None,
    cancel_reason: str = "",
    adverse_selection_loss_usdc: float = 0.0,
) -> None:
    with connect_latency_bot_db(settings) as conn:
        conn.execute(
            """
            UPDATE temporal_inventory_quotes
            SET status = ?,
                ts_updated = ?,
                fill_ts = CASE WHEN ? = 'filled' THEN ? ELSE fill_ts END,
                fill_price = ?,
                fill_size = ?,
                cancel_reason = ?,
                adverse_selection_loss_usdc = ?
            WHERE quote_id = ?
              AND status = 'open'
            """,
            (
                status,
                ts,
                status,
                ts,
                None if fill_price is None else float(fill_price),
                None if fill_size is None else float(fill_size),
                cancel_reason,
                float(adverse_selection_loss_usdc),
                quote_id,
            ),
        )
        conn.commit()


def _json_dump_payload(payload: Any) -> str:
    try:
        return json.dumps(payload, sort_keys=True)
    except TypeError:
        return json.dumps({"repr": repr(payload)}, sort_keys=True)


def record_live_temporal_inventory_maker_order(
    settings: LatencyBotSettings,
    *,
    ts: str,
    market_id: str,
    asset: str,
    side: str,
    token_id: str,
    price: float,
    size: float,
    mode: str,
    decision: str,
    status: str,
    edge: float,
    reason: str,
    clob_order_id: str = "",
    order_payload: Any = None,
    error: str = "",
) -> dict[str, Any]:
    local_order_id = str(uuid.uuid4())
    notional_usdc = round(float(price) * float(size), 8)
    with connect_latency_bot_db(settings) as conn:
        conn.execute(
            """
            INSERT INTO live_temporal_inventory_maker_orders (
                local_order_id, ts_created, ts_updated, market_id, asset, side, token_id,
                price, size, notional_usdc, mode, decision, status, edge, reason,
                clob_order_id, order_payload, cancel_payload, error, heartbeat_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                local_order_id,
                ts,
                ts,
                market_id,
                asset,
                side,
                token_id,
                float(price),
                float(size),
                notional_usdc,
                mode,
                decision,
                status,
                float(edge),
                reason,
                clob_order_id,
                _json_dump_payload(order_payload) if order_payload is not None else "",
                "",
                error,
                ts,
            ),
        )
        row = conn.execute(
            "SELECT * FROM live_temporal_inventory_maker_orders WHERE local_order_id = ?",
            (local_order_id,),
        ).fetchone()
        conn.commit()
    return dict(row) if row is not None else {}


def update_live_temporal_inventory_maker_order(
    settings: LatencyBotSettings,
    *,
    local_order_id: str,
    ts: str,
    status: str,
    decision: str | None = None,
    reason: str | None = None,
    clob_order_id: str | None = None,
    order_payload: Any = None,
    cancel_payload: Any = None,
    error: str | None = None,
) -> None:
    with connect_latency_bot_db(settings) as conn:
        existing = conn.execute(
            "SELECT * FROM live_temporal_inventory_maker_orders WHERE local_order_id = ?",
            (local_order_id,),
        ).fetchone()
        if existing is None:
            return
        conn.execute(
            """
            UPDATE live_temporal_inventory_maker_orders
            SET ts_updated = ?,
                status = ?,
                decision = ?,
                reason = ?,
                clob_order_id = ?,
                order_payload = ?,
                cancel_payload = ?,
                error = ?,
                heartbeat_ts = ?
            WHERE local_order_id = ?
            """,
            (
                ts,
                status,
                decision if decision is not None else str(existing["decision"] or ""),
                reason if reason is not None else str(existing["reason"] or ""),
                clob_order_id if clob_order_id is not None else str(existing["clob_order_id"] or ""),
                _json_dump_payload(order_payload) if order_payload is not None else str(existing["order_payload"] or ""),
                _json_dump_payload(cancel_payload) if cancel_payload is not None else str(existing["cancel_payload"] or ""),
                error if error is not None else str(existing["error"] or ""),
                ts,
                local_order_id,
            ),
        )
        conn.commit()


def load_live_temporal_inventory_maker_open_orders(settings: LatencyBotSettings) -> list[dict[str, Any]]:
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM live_temporal_inventory_maker_orders
            WHERE status = 'open'
            ORDER BY ts_created ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def record_live_temporal_inventory_maker_heartbeat(
    settings: LatencyBotSettings,
    *,
    ts: str,
    status: str,
    armed: bool,
    open_orders: int,
    cancel_all_ok: bool,
    reason: str,
) -> None:
    with connect_latency_bot_db(settings) as conn:
        conn.execute(
            """
            INSERT INTO live_temporal_inventory_maker_heartbeats (
                ts, status, armed, open_orders, cancel_all_ok, reason
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (ts, status, 1 if armed else 0, int(open_orders), 1 if cancel_all_ok else 0, reason),
        )
        conn.commit()


def cancel_order(
    settings: LatencyBotSettings,
    *,
    order_id: str,
    reason: str,
) -> None:
    with connect_latency_bot_db(settings) as conn:
        conn.execute(
            """
            UPDATE orders
            SET status = 'cancelled', cancel_reason = ?
            WHERE order_id = ? AND status = 'open'
            """,
            (reason, order_id),
        )
        conn.commit()


def fill_open_order_as_position(
    settings: LatencyBotSettings,
    *,
    order_id: str,
    ts: str,
    asset: str,
) -> dict[str, Any] | None:
    position_id = str(uuid.uuid4())
    fill_id = f"paper-fill-{position_id}"
    with connect_latency_bot_db(settings) as conn:
        row = conn.execute(
            """
            SELECT order_id, ts_created, market_id, mode, side, price, size, status, reprices
            FROM orders
            WHERE order_id = ? AND status = 'open'
            """,
            (order_id,),
        ).fetchone()
        if row is None:
            return None
        conn.execute(
            "UPDATE orders SET status = 'filled' WHERE order_id = ?",
            (order_id,),
        )
        conn.execute(
            """
            INSERT INTO fills (
                fill_id, order_id, ts, market_id, side, price, size, fill_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fill_id,
                row["order_id"],
                ts,
                row["market_id"],
                row["side"],
                row["price"],
                row["size"],
                "entry",
            ),
        )
        conn.execute(
            """
            INSERT INTO positions (
                position_id, market_id, asset, side, entry_ts, entry_price, size, mode, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                position_id,
                row["market_id"],
                asset,
                row["side"],
                ts,
                row["price"],
                row["size"],
                row["mode"],
                "open",
            ),
        )
        conn.execute(
            """
            INSERT INTO position_events (
                ts, position_id, event_type, mark, edge, pnl, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (ts, position_id, "open", row["price"], None, 0.0, "maker_fill"),
        )
        conn.commit()
    return {
        "position_id": position_id,
        "market_id": row["market_id"],
        "asset": asset,
        "side": row["side"],
        "entry_price": row["price"],
        "size": row["size"],
        "mode": row["mode"],
    }


def close_position(
    settings: LatencyBotSettings,
    *,
    position_id: str,
    ts: str,
    exit_price: float,
    pnl: float,
    reason: str,
) -> None:
    order_id = f"paper-exit-{position_id}-{ts}"
    fill_id = f"paper-exit-fill-{position_id}-{ts}"
    with connect_latency_bot_db(settings) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT market_id, side, size, mode
            FROM positions
            WHERE position_id = ? AND status = 'open'
            """,
            (position_id,),
        ).fetchone()
        if row is None:
            conn.rollback()
            return
        updated = conn.execute(
            "UPDATE positions SET status = 'closed' WHERE position_id = ? AND status = 'open'",
            (position_id,),
        )
        if updated.rowcount != 1:
            conn.rollback()
            return
        conn.execute(
            """
            INSERT INTO orders (
                order_id, ts_created, market_id, mode, side, price, size, status, reprices, cancel_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (order_id, ts, row["market_id"], row["mode"], row["side"], exit_price, row["size"], "filled", 0, ""),
        )
        conn.execute(
            """
            INSERT INTO fills (
                fill_id, order_id, ts, market_id, side, price, size, fill_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (fill_id, order_id, ts, row["market_id"], row["side"], exit_price, row["size"], "exit"),
        )
        conn.execute(
            """
            INSERT INTO position_events (
                ts, position_id, event_type, mark, edge, pnl, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (ts, position_id, "close", exit_price, None, pnl, reason),
        )
        conn.commit()


def close_shadow_position(
    settings: LatencyBotSettings,
    *,
    position_id: str,
    ts: str,
    exit_price: float,
    pnl: float,
    reason: str,
) -> None:
    with connect_latency_bot_db(settings) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT market_id
            FROM shadow_positions
            WHERE position_id = ? AND status = 'open'
            """,
            (position_id,),
        ).fetchone()
        if row is None:
            conn.rollback()
            return
        updated = conn.execute(
            "UPDATE shadow_positions SET status = 'closed' WHERE position_id = ? AND status = 'open'",
            (position_id,),
        )
        if updated.rowcount != 1:
            conn.rollback()
            return
        conn.execute(
            """
            INSERT INTO shadow_position_events (
                ts, position_id, event_type, mark, edge, pnl, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (ts, position_id, "close", exit_price, None, pnl, reason),
        )
        conn.commit()


def close_shadow_variant_position(
    settings: LatencyBotSettings,
    *,
    position_id: str,
    ts: str,
    exit_price: float,
    pnl: float,
    reason: str,
) -> None:
    with connect_latency_bot_db(settings) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT variant_id
            FROM shadow_variant_positions
            WHERE position_id = ? AND status = 'open'
            """,
            (position_id,),
        ).fetchone()
        if row is None:
            conn.rollback()
            return
        updated = conn.execute(
            "UPDATE shadow_variant_positions SET status = 'closed' WHERE position_id = ? AND status = 'open'",
            (position_id,),
        )
        if updated.rowcount != 1:
            conn.rollback()
            return
        conn.execute(
            """
            INSERT INTO shadow_variant_position_events (
                ts, variant_id, position_id, event_type, mark, edge, pnl, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (ts, row["variant_id"], position_id, "close", exit_price, None, pnl, reason),
        )
        conn.commit()


def create_complete_set_arb_position(
    settings: LatencyBotSettings,
    *,
    ts: str,
    market_id: str,
    asset: str,
    tenor_minutes: int,
    yes_entry_price: float,
    no_entry_price: float,
    size: float,
    notional_usdc: float,
    gross_edge: float,
    net_edge: float,
) -> dict[str, Any]:
    position_id = str(uuid.uuid4())
    total_cost = round(float(yes_entry_price) + float(no_entry_price), 6)
    with connect_latency_bot_db(settings) as conn:
        conn.execute(
            """
            INSERT INTO complete_set_arb_positions (
                position_id, market_id, asset, tenor_minutes, entry_ts,
                yes_entry_price, no_entry_price, total_cost, size, notional_usdc,
                gross_edge, net_edge, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                position_id,
                market_id,
                asset,
                int(tenor_minutes),
                ts,
                float(yes_entry_price),
                float(no_entry_price),
                total_cost,
                float(size),
                float(notional_usdc),
                float(gross_edge),
                float(net_edge),
                "open",
            ),
        )
        conn.execute(
            """
            INSERT INTO complete_set_arb_events (
                ts, position_id, event_type, payout_price, pnl, reason
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (ts, position_id, "open", total_cost, 0.0, "complete_set_pair_entry"),
        )
        conn.commit()
    return {
        "position_id": position_id,
        "market_id": market_id,
        "asset": asset,
        "tenor_minutes": int(tenor_minutes),
        "yes_entry_price": float(yes_entry_price),
        "no_entry_price": float(no_entry_price),
        "total_cost": total_cost,
        "size": float(size),
        "notional_usdc": float(notional_usdc),
        "gross_edge": float(gross_edge),
        "net_edge": float(net_edge),
    }


def close_complete_set_arb_position(
    settings: LatencyBotSettings,
    *,
    position_id: str,
    ts: str,
    payout_price: float,
    pnl: float,
    reason: str,
) -> None:
    with connect_latency_bot_db(settings) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT position_id
            FROM complete_set_arb_positions
            WHERE position_id = ? AND status = 'open'
            """,
            (position_id,),
        ).fetchone()
        if row is None:
            conn.rollback()
            return
        updated = conn.execute(
            "UPDATE complete_set_arb_positions SET status = 'closed' WHERE position_id = ? AND status = 'open'",
            (position_id,),
        )
        if updated.rowcount != 1:
            conn.rollback()
            return
        conn.execute(
            """
            INSERT INTO complete_set_arb_events (
                ts, position_id, event_type, payout_price, pnl, reason
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (ts, position_id, "close", float(payout_price), float(pnl), reason),
        )
        conn.commit()


def record_live_complete_set_arb_pilot_attempt(
    settings: LatencyBotSettings,
    *,
    ts: str,
    market_id: str,
    asset: str,
    tenor_minutes: int,
    mode: str,
    decision: str,
    reason: str,
    yes_token_id: str = "",
    no_token_id: str = "",
    yes_price: float = 0.0,
    no_price: float = 0.0,
    total_cost: float = 0.0,
    gross_edge: float = 0.0,
    net_edge: float = 0.0,
    adjusted_edge: float = 0.0,
    executable_depth_usdc: float = 0.0,
    effective_depth_usdc: float = 0.0,
    notional_usdc: float = 0.0,
    size: float = 0.0,
    expected_pnl_usdc: float = 0.0,
    realized_pnl_usdc: float = 0.0,
    yes_order_id: str = "",
    no_order_id: str = "",
    yes_order_payload: str = "",
    no_order_payload: str = "",
    error: str = "",
) -> dict[str, Any]:
    with connect_latency_bot_db(settings) as conn:
        cursor = conn.execute(
            """
            INSERT INTO live_complete_set_arb_pilot_attempts (
                ts, market_id, asset, tenor_minutes, mode, decision, reason,
                yes_token_id, no_token_id, yes_price, no_price, total_cost,
                gross_edge, net_edge, adjusted_edge, executable_depth_usdc,
                effective_depth_usdc, notional_usdc, size, expected_pnl_usdc,
                realized_pnl_usdc, yes_order_id, no_order_id, yes_order_payload,
                no_order_payload, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                market_id,
                asset,
                int(tenor_minutes),
                mode,
                decision,
                reason,
                yes_token_id,
                no_token_id,
                float(yes_price),
                float(no_price),
                float(total_cost),
                float(gross_edge),
                float(net_edge),
                float(adjusted_edge),
                float(executable_depth_usdc),
                float(effective_depth_usdc),
                float(notional_usdc),
                float(size),
                float(expected_pnl_usdc),
                float(realized_pnl_usdc),
                yes_order_id,
                no_order_id,
                yes_order_payload,
                no_order_payload,
                error,
            ),
        )
        row_id = int(cursor.lastrowid or 0)
        conn.commit()
    return {
        "id": row_id,
        "ts": ts,
        "market_id": market_id,
        "asset": asset,
        "tenor_minutes": int(tenor_minutes),
        "mode": mode,
        "decision": decision,
        "reason": reason,
        "yes_price": float(yes_price),
        "no_price": float(no_price),
        "total_cost": float(total_cost),
        "adjusted_edge": float(adjusted_edge),
        "notional_usdc": float(notional_usdc),
        "size": float(size),
        "expected_pnl_usdc": float(expected_pnl_usdc),
        "realized_pnl_usdc": float(realized_pnl_usdc),
        "error": error,
    }


def _live_pilot_extract_matched_legs(error: str) -> list[dict[str, Any]]:
    try:
        payload = json.loads(str(error or "{}"))
    except json.JSONDecodeError:
        return []

    def from_response(response: Any) -> list[dict[str, Any]]:
        if not isinstance(response, list):
            return []
        legs: list[dict[str, Any]] = []
        for index, item in enumerate(response):
            if not isinstance(item, dict):
                continue
            status = str(item.get("status") or "").lower()
            if status not in {"matched", "filled", "partially_matched"}:
                continue
            try:
                shares = float(item.get("takingAmount") or 0.0)
                spend = float(item.get("makingAmount") or 0.0)
            except (TypeError, ValueError):
                continue
            if shares <= 0.0 or spend <= 0.0:
                continue
            legs.append(
                {
                    "side": "yes" if index == 0 else "no",
                    "shares": shares,
                    "entry_spend_usdc": spend,
                    "token_id": str(item.get("asset_id") or ""),
                }
            )
        return legs

    if isinstance(payload, list):
        return from_response(payload)
    if not isinstance(payload, dict):
        return []

    matched_leg = payload.get("matched_leg")
    if isinstance(matched_leg, dict):
        try:
            shares = float(matched_leg.get("shares") or 0.0)
            spend = float(matched_leg.get("entry_spend_usdc") or 0.0)
        except (TypeError, ValueError):
            return []
        if shares > 0.0 and spend > 0.0:
            return [
                {
                    "side": str(matched_leg.get("side") or ""),
                    "shares": shares,
                    "entry_spend_usdc": spend,
                    "token_id": str(matched_leg.get("token_id") or ""),
                }
            ]
    paired_response_legs = from_response(payload.get("paired_response"))
    if paired_response_legs:
        return paired_response_legs
    return from_response(payload.get("response"))


def _live_pilot_extract_matched_leg(error: str) -> dict[str, Any] | None:
    legs = _live_pilot_extract_matched_legs(error)
    return legs[0] if legs else None


def _live_pilot_fetch_market_resolution(market_id: str) -> dict[str, Any]:
    if not market_id:
        return {"resolved": False, "winner": "", "question": "", "error": "missing market id"}
    request = urllib.request.Request(
        f"https://gamma-api.polymarket.com/markets/{market_id}",
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=3.0) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
        return {"resolved": False, "winner": "", "question": "", "error": str(exc)}

    outcomes_raw = payload.get("outcomes") or []
    prices_raw = payload.get("outcomePrices") or []
    try:
        outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else list(outcomes_raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        outcomes = []
    try:
        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else list(prices_raw)
        prices = [float(price) for price in prices]
    except (TypeError, ValueError, json.JSONDecodeError):
        prices = []

    winner = ""
    if outcomes and prices and len(outcomes) == len(prices) and max(prices) >= 0.999:
        winner = str(outcomes[prices.index(max(prices))]).lower()
    resolved = bool(winner) and (
        str(payload.get("umaResolutionStatus") or "").lower() == "resolved" or bool(payload.get("closed"))
    )
    return {
        "resolved": resolved,
        "winner": winner,
        "question": str(payload.get("question") or ""),
        "error": "",
    }


def _live_pilot_get_market_resolution(settings: LatencyBotSettings, market_id: str) -> dict[str, Any]:
    if not market_id:
        return {"resolved": False, "winner": "", "question": "", "error": "missing market id"}
    with connect_latency_bot_db(settings) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS live_complete_set_arb_pilot_resolutions (
                market_id TEXT PRIMARY KEY,
                resolved INTEGER NOT NULL,
                winner TEXT NOT NULL,
                question TEXT NOT NULL,
                error TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        row = conn.execute(
            """
            SELECT resolved, winner, question, error
            FROM live_complete_set_arb_pilot_resolutions
            WHERE market_id = ?
            """,
            (market_id,),
        ).fetchone()
        if row is not None and int(row["resolved"] or 0) == 1:
            return {
                "resolved": True,
                "winner": str(row["winner"] or ""),
                "question": str(row["question"] or ""),
                "error": str(row["error"] or ""),
            }

    resolution = _live_pilot_fetch_market_resolution(market_id)
    if bool(resolution.get("resolved")):
        with connect_latency_bot_db(settings) as conn:
            conn.execute(
                """
                INSERT INTO live_complete_set_arb_pilot_resolutions (
                    market_id, resolved, winner, question, error, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(market_id) DO UPDATE SET
                    resolved = excluded.resolved,
                    winner = excluded.winner,
                    question = excluded.question,
                    error = excluded.error,
                    updated_at = excluded.updated_at
                """,
                (
                    market_id,
                    1,
                    str(resolution.get("winner") or ""),
                    str(resolution.get("question") or ""),
                    str(resolution.get("error") or ""),
                    datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                ),
            )
            conn.commit()
    return resolution


def latency_bot_live_complete_set_arb_pilot_stats(settings: LatencyBotSettings) -> dict[str, Any]:
    cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat().replace("+00:00", "Z")
    cutoff_60m = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat().replace("+00:00", "Z")
    with connect_latency_bot_db(settings) as conn:
        all_rows = conn.execute(
            """
            SELECT *
            FROM live_complete_set_arb_pilot_attempts
            ORDER BY ts ASC, id ASC
            """
        ).fetchall()
        recent_rows = conn.execute(
            """
            SELECT *
            FROM live_complete_set_arb_pilot_attempts
            ORDER BY ts DESC, id DESC
            LIMIT 30
            """
        ).fetchall()
        reason_rows = conn.execute(
            """
            SELECT reason, decision, COUNT(*) AS count, MAX(adjusted_edge) AS max_adjusted_edge
            FROM live_complete_set_arb_pilot_attempts
            WHERE ts >= ?
            GROUP BY reason, decision
            ORDER BY count DESC, reason ASC, decision ASC
            LIMIT 40
            """,
            (cutoff_24h,),
        ).fetchall()
        recent_summary = conn.execute(
            """
            SELECT
                COUNT(*) AS attempts_60m,
                SUM(CASE WHEN decision IN ('DRY_RUN', 'SUBMITTED') THEN 1 ELSE 0 END) AS eligible_60m,
                MAX(adjusted_edge) AS best_adjusted_edge_60m
            FROM live_complete_set_arb_pilot_attempts
            WHERE ts >= ?
            """,
            (cutoff_60m,),
        ).fetchone()
    submitted_rows = [row for row in all_rows if str(row["decision"] or "") == "SUBMITTED"]
    dry_run_rows = [row for row in all_rows if str(row["decision"] or "") == "DRY_RUN"]
    failed_rows = [row for row in all_rows if str(row["decision"] or "") in {"FAILED", "ONE_LEG_FAILED", "RESCUED_ONE_LEG"}]
    blocked_rows = [row for row in all_rows if str(row["decision"] or "") == "BLOCKED"]
    one_leg_rows = [row for row in all_rows if str(row["decision"] or "") == "ONE_LEG_FAILED"]
    unresolved_one_leg_spend = 0.0
    unresolved_one_leg_markets: set[str] = set()
    one_leg_spend = 0.0
    one_leg_payout = 0.0
    one_leg_pnl = 0.0
    resolved_one_leg_count = 0
    matched_leg_count = 0
    resolved_matched_leg_count = 0
    matched_leg_spend = 0.0
    matched_leg_payout = 0.0
    matched_leg_pnl = 0.0
    unresolved_matched_leg_spend = 0.0
    unresolved_matched_leg_markets: set[str] = set()
    resolution_errors: set[str] = set()
    matched_leg_pnl_by_row_id: dict[int, float] = {}
    matched_leg_reconciliation: list[dict[str, Any]] = []
    resolution_cache: dict[str, dict[str, Any]] = {}
    exposure_rows = [
        row
        for row in all_rows
        if str(row["decision"] or "") in {"FAILED", "ONE_LEG_FAILED", "RESCUED_ONE_LEG", "SUBMITTED"}
    ]
    for row in exposure_rows:
        row_id = int(row["id"] or 0)
        market_id = str(row["market_id"] or "")
        decision = str(row["decision"] or "")
        matched_legs = _live_pilot_extract_matched_legs(str(row["error"] or ""))
        if not matched_legs:
            continue
        resolution = resolution_cache.get(market_id)
        if resolution is None:
            resolution = _live_pilot_get_market_resolution(settings, market_id)
            resolution_cache[market_id] = resolution
        winner = str(resolution.get("winner") or "")
        resolved = bool(resolution.get("resolved")) and winner in {"up", "down"}
        for matched_leg in matched_legs:
            spend = float(matched_leg.get("entry_spend_usdc") or 0.0)
            shares = float(matched_leg.get("shares") or 0.0)
            side = str(matched_leg.get("side") or "")
            side_price = (
                float(row["yes_price"] or 0.0)
                if side == "yes"
                else float(row["no_price"] or 0.0)
                if side == "no"
                else 0.0
            )
            if spend > 0.0 and side_price > 0.0:
                implied_shares = spend / side_price
                if implied_shares > 0.0 and shares > max(implied_shares * 1000.0, 1_000_000.0):
                    # Only normalize obvious raw integer token-unit artifacts. Current CLOB
                    # trade responses report actual share counts and must remain the source
                    # of truth even if the row's snapshot price is stale.
                    shares = implied_shares
            if spend <= 0.0 or shares <= 0.0:
                continue
            matched_leg_count += 1
            matched_leg_spend = round(matched_leg_spend + spend, 6)
            if decision == "ONE_LEG_FAILED":
                one_leg_spend = round(one_leg_spend + spend, 6)
            side_outcome = "up" if side == "yes" else "down" if side == "no" else ""
            payout = shares if resolved and side_outcome == winner else 0.0
            pnl = payout - spend if resolved else 0.0
            if resolved:
                resolved_matched_leg_count += 1
                matched_leg_payout = round(matched_leg_payout + payout, 6)
                matched_leg_pnl = round(matched_leg_pnl + pnl, 6)
                matched_leg_pnl_by_row_id[row_id] = round(matched_leg_pnl_by_row_id.get(row_id, 0.0) + pnl, 6)
                if decision == "ONE_LEG_FAILED":
                    resolved_one_leg_count += 1
                    one_leg_payout = round(one_leg_payout + payout, 6)
                    one_leg_pnl = round(one_leg_pnl + pnl, 6)
            else:
                unresolved_matched_leg_spend = round(unresolved_matched_leg_spend + spend, 6)
                unresolved_matched_leg_markets.add(market_id)
                if decision == "ONE_LEG_FAILED":
                    unresolved_one_leg_spend = round(unresolved_one_leg_spend + spend, 6)
                    unresolved_one_leg_markets.add(market_id)
                if resolution.get("error"):
                    resolution_errors.add(str(resolution.get("error")))
            matched_leg_reconciliation.append(
                {
                    "id": row_id,
                    "ts": str(row["ts"] or ""),
                    "market_id": market_id,
                    "asset": str(row["asset"] or ""),
                    "decision": decision,
                    "side": side,
                    "winner": winner if resolved else "",
                    "status": "won" if payout > 0.0 else "lost" if resolved else "unresolved",
                    "entry_spend_usdc": round(spend, 6),
                    "shares": round(shares, 6),
                    "payout_usdc": round(payout, 6),
                    "pnl_usdc": round(pnl, 6),
                    "question": str(resolution.get("question") or ""),
                    "resolution_error": str(resolution.get("error") or ""),
                }
            )
    running = 0.0
    peak = 0.0
    max_drawdown = 0.0
    curve: list[dict[str, Any]] = []
    realized_24h = 0.0
    expected_24h = 0.0
    markets: set[str] = set()
    for row in all_rows:
        row_id = int(row["id"] or 0)
        decision = str(row["decision"] or "")
        expected = float(row["expected_pnl_usdc"] or 0.0)
        realized = float(row["realized_pnl_usdc"] or 0.0)
        if row_id in matched_leg_pnl_by_row_id:
            pnl = float(matched_leg_pnl_by_row_id[row_id])
        else:
            pnl = realized if decision in {"FAILED", "ONE_LEG_FAILED", "RESCUED_ONE_LEG"} else expected if decision == "SUBMITTED" else 0.0
        if decision == "SUBMITTED":
            markets.add(str(row["market_id"] or ""))
        running = round(running + pnl, 6)
        peak = max(peak, running)
        max_drawdown = min(max_drawdown, running - peak)
        parsed_ts = _storage_parse_ts(str(row["ts"] or ""))
        if parsed_ts is not None and parsed_ts >= _storage_parse_ts(cutoff_24h):
            realized_24h = round(realized_24h + pnl, 6)
            if decision in {"DRY_RUN", "SUBMITTED"}:
                expected_24h = round(expected_24h + expected, 6)
        if decision in {"SUBMITTED", "FAILED", "ONE_LEG_FAILED", "RESCUED_ONE_LEG"}:
            curve.append(
                {
                    "ts": str(row["ts"] or ""),
                    "realized_pnl_usdc": running,
                    "unrealized_pnl_usdc": 0.0,
                    "equity_usdc": round(float(settings.live_complete_set_arb_pilot_capital_usdc) + running, 6),
                }
            )
    attempts_60m = int(recent_summary["attempts_60m"] or 0) if recent_summary else 0
    eligible_60m = int(recent_summary["eligible_60m"] or 0) if recent_summary else 0
    mode = str(settings.live_complete_set_arb_pilot_mode or "dry_run").lower()
    armed = (
        bool(settings.live_complete_set_arb_pilot_enabled)
        and mode == "live"
        and str(settings.live_complete_set_arb_pilot_confirm) == "LIVE_COMPLETE_SET_ARB_PILOT"
    )
    credentials_configured = all(
        (
            settings.live_complete_set_arb_pilot_private_key,
            settings.live_complete_set_arb_pilot_api_key,
            settings.live_complete_set_arb_pilot_api_secret,
            settings.live_complete_set_arb_pilot_api_passphrase,
            settings.live_complete_set_arb_pilot_funder_address,
        )
    )
    summary = {
        "mode": "Guarded live complete-set arb pilot",
        "accounting_basis": "local matched-leg reconstruction; not Polymarket wallet/account cash",
        "enabled": bool(settings.live_complete_set_arb_pilot_enabled),
        "pilot_mode": mode,
        "armed_for_live_orders": armed,
        "credentials_configured": bool(credentials_configured),
        "confirmation_required": "LIVE_COMPLETE_SET_ARB_PILOT",
        "simulated_or_live_capital_usdc": round(float(settings.live_complete_set_arb_pilot_capital_usdc), 6),
        "target_notional_usdc": round(float(settings.live_complete_set_arb_pilot_notional_usdc), 6),
        "submitted_sets": len(submitted_rows),
        "actual_locked_complete_sets": len(submitted_rows),
        "directional_exposure_warning": (
            "YES: PnL is from one-leg directional fills, not locked complete-set arbitrage"
            if len(submitted_rows) == 0 and matched_leg_count > 0
            else "no"
        ),
        "dry_run_candidates": len(dry_run_rows),
        "blocked_attempts": len(blocked_rows),
        "failed_attempts": len(failed_rows),
        "matched_live_legs": matched_leg_count,
        "resolved_matched_live_legs": resolved_matched_leg_count,
        "unresolved_matched_live_legs": max(matched_leg_count - resolved_matched_leg_count, 0),
        "matched_live_leg_spend_usdc": round(matched_leg_spend, 6),
        "resolved_matched_live_leg_payout_usdc": round(matched_leg_payout, 6),
        "resolved_matched_live_leg_pnl_usdc": round(matched_leg_pnl, 6),
        "unresolved_matched_live_leg_spend_usdc": round(unresolved_matched_leg_spend, 6),
        "unresolved_matched_live_leg_markets": len(unresolved_matched_leg_markets),
        "one_leg_failures": len(one_leg_rows),
        "resolved_one_leg_failures": resolved_one_leg_count,
        "unresolved_one_leg_failures": max(len(one_leg_rows) - resolved_one_leg_count, 0),
        "one_leg_spend_usdc": round(one_leg_spend, 6),
        "resolved_one_leg_payout_usdc": round(one_leg_payout, 6),
        "resolved_one_leg_pnl_usdc": round(one_leg_pnl, 6),
        "unresolved_one_leg_spend_usdc": round(unresolved_one_leg_spend, 6),
        "unresolved_one_leg_markets": len(unresolved_one_leg_markets),
        "matched_leg_resolution_source": "gamma-api.polymarket.com",
        "matched_leg_resolution_errors": len(resolution_errors),
        "one_leg_resolution_source": "gamma-api.polymarket.com",
        "one_leg_resolution_errors": len(resolution_errors),
        "unique_markets_submitted": len(markets),
        "expected_locked_pnl_usdc": round(sum(float(row["expected_pnl_usdc"] or 0.0) for row in submitted_rows), 6),
        "dry_run_candidate_pnl_usdc": round(sum(float(row["expected_pnl_usdc"] or 0.0) for row in dry_run_rows), 6),
        "net_pnl": round(running, 6),
        "realized_pnl_24h_usdc": round(realized_24h, 6),
        "expected_candidate_pnl_24h_usdc": round(expected_24h, 6),
        "projected_monthly_revenue_usdc": round(realized_24h * 30.0, 6),
        "projected_yearly_revenue_usdc": round(realized_24h * 365.0, 6),
        "max_drawdown": round(max_drawdown, 6),
        "attempts_60m": attempts_60m,
        "eligible_60m": eligible_60m,
        "eligible_rate_60m": round(eligible_60m / attempts_60m, 6) if attempts_60m else 0.0,
        "best_adjusted_edge_60m": round(float(recent_summary["best_adjusted_edge_60m"] or 0.0), 6) if recent_summary else 0.0,
        "min_edge_per_share": round(float(settings.live_complete_set_arb_pilot_min_edge_per_share), 6),
        "min_depth_usdc": round(float(settings.live_complete_set_arb_pilot_min_depth_usdc), 6),
        "min_seconds_left": int(settings.live_complete_set_arb_pilot_min_seconds_left),
        "min_leg_amount_usdc": round(float(settings.live_complete_set_arb_pilot_min_leg_amount_usdc), 6),
        "depth_haircut": round(float(settings.live_complete_set_arb_pilot_depth_haircut), 6),
        "extra_slippage_per_share": round(float(settings.live_complete_set_arb_pilot_extra_slippage_per_share), 6),
        "max_sets_per_cycle": int(settings.live_complete_set_arb_pilot_max_sets_per_cycle),
        "max_open_sets": int(settings.live_complete_set_arb_pilot_max_open_sets),
        "daily_loss_limit_usdc": round(float(settings.live_complete_set_arb_pilot_daily_loss_limit_usdc), 6),
        "allow_sequential_orders": bool(settings.live_complete_set_arb_pilot_allow_sequential_orders),
        "require_fok": bool(settings.live_complete_set_arb_pilot_require_fok),
        "rescue_enabled": bool(settings.live_complete_set_arb_pilot_enable_rescue),
        "signature_type": int(settings.live_complete_set_arb_pilot_signature_type),
        "host": str(settings.live_complete_set_arb_pilot_host),
        "same_market_cooldown_seconds": int(settings.live_complete_set_arb_pilot_same_market_cooldown_seconds),
    }
    return {
        "summary": summary,
        "recent_attempts": [dict(row) for row in recent_rows],
        "matched_leg_reconciliation": matched_leg_reconciliation[-80:],
        "one_leg_reconciliation": matched_leg_reconciliation[-40:],
        "reason_breakdown": [dict(row) for row in reason_rows],
        "equity_curve": curve[-200:],
    }


def latency_bot_polymarket_account_reconciliation(settings: LatencyBotSettings) -> dict[str, Any]:
    now_ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    pilot_capital = float(settings.live_complete_set_arb_pilot_capital_usdc)

    def empty_summary(status: str, error: str = "") -> dict[str, Any]:
        return {
            "mode": "Polymarket account reconciliation",
            "source": "official CLOB SDK account/trade endpoints",
            "status": status,
            "error": error,
            "last_checked_ts": now_ts,
            "funder_address": _mask_address(settings.live_complete_set_arb_pilot_funder_address),
            "pilot_starting_capital_usdc": round(pilot_capital, 6),
            "clob_cash_usdc": 0.0,
            "cash_minus_pilot_capital_usdc": 0.0,
            "clob_open_orders": 0,
            "clob_open_positions": 0,
            "clob_recent_trades": 0,
            "clob_recent_buy_spend_usdc": 0.0,
            "clob_recent_sell_proceeds_usdc": 0.0,
            "clob_recent_net_trade_cashflow_usdc": 0.0,
            "local_live_attempt_rows": 0,
            "local_live_maker_order_rows": 0,
            "local_order_ids_recorded": 0,
            "local_order_id_match_rate": 0.0,
            "clob_trades_matching_local_order_ids": 0,
            "clob_trades_not_in_local_order_ids": 0,
            "local_realized_pnl_usdc": 0.0,
            "local_vs_clob_cashflow_gap_usdc": 0.0,
            "note": "Polymarket account cash is authoritative; local matched-leg PnL is a reconstruction.",
        }

    def money_from_micro(raw: Any) -> float:
        try:
            return float(raw or 0.0) / 1_000_000.0
        except (TypeError, ValueError):
            return 0.0

    def trade_ts(raw: Any) -> str:
        try:
            return datetime.fromtimestamp(int(float(raw)), tz=timezone.utc).isoformat().replace("+00:00", "Z")
        except (TypeError, ValueError, OSError, OverflowError):
            return ""

    credentials_configured = all(
        (
            settings.live_complete_set_arb_pilot_private_key,
            settings.live_complete_set_arb_pilot_api_key,
            settings.live_complete_set_arb_pilot_api_secret,
            settings.live_complete_set_arb_pilot_api_passphrase,
            settings.live_complete_set_arb_pilot_funder_address,
        )
    )
    if not credentials_configured:
        return {"summary": empty_summary("missing_credentials"), "recent_trades": [], "unmatched_trades": [], "open_orders": [], "open_positions": []}

    try:
        from .execution.polymarket_live_client import PolymarketLiveCompleteSetClient

        client = PolymarketLiveCompleteSetClient(settings)
        balance = client.get_collateral_balance_allowance()
        open_orders = client.get_open_orders()
        positions = client.get_positions()
        trades = client.get_recent_trades()
    except Exception as exc:
        return {"summary": empty_summary("error", str(exc)), "recent_trades": [], "unmatched_trades": [], "open_orders": [], "open_positions": []}

    local_order_ids: set[str] = set()
    local_rows = 0
    local_maker_rows = 0
    local_realized_pnl = 0.0
    try:
        with connect_latency_bot_db(settings) as conn:
            rows = conn.execute(
                """
                SELECT yes_order_id, no_order_id, error, realized_pnl_usdc
                FROM live_complete_set_arb_pilot_attempts
                WHERE COALESCE(yes_order_id, '') <> '' OR COALESCE(no_order_id, '') <> ''
                   OR COALESCE(error, '') <> ''
                """
            ).fetchall()
            maker_rows = conn.execute(
                """
                SELECT clob_order_id, error
                FROM live_temporal_inventory_maker_orders
                WHERE COALESCE(clob_order_id, '') <> '' OR COALESCE(error, '') <> ''
                """
            ).fetchall()
        local_rows = len(rows)
        local_maker_rows = len(maker_rows)
        for row in rows:
            local_realized_pnl = round(local_realized_pnl + float(row["realized_pnl_usdc"] or 0.0), 6)
            for key in ("yes_order_id", "no_order_id"):
                order_id = str(row[key] or "").strip()
                if order_id:
                    local_order_ids.add(order_id)
            for order_id in _extract_clob_order_ids(str(row["error"] or "")):
                local_order_ids.add(order_id)
        for row in maker_rows:
            order_id = str(row["clob_order_id"] or "").strip()
            if order_id:
                local_order_ids.add(order_id)
            for order_id in _extract_clob_order_ids(str(row["error"] or "")):
                local_order_ids.add(order_id)
    except sqlite3.Error:
        local_rows = 0
        local_maker_rows = 0
        local_realized_pnl = 0.0
        local_order_ids = set()

    clob_cash = money_from_micro(balance.get("balance"))
    normalized_trades: list[dict[str, Any]] = []
    buy_spend = 0.0
    sell_proceeds = 0.0
    matched_local = 0
    matched_local_order_ids: set[str] = set()
    unmatched_trades: list[dict[str, Any]] = []
    for trade in trades:
        side = str(trade.get("side") or "").upper()
        try:
            size = float(trade.get("size") or 0.0)
            price = float(trade.get("price") or 0.0)
        except (TypeError, ValueError):
            size = 0.0
            price = 0.0
        cash_amount = round(size * price, 6)
        if side == "BUY":
            buy_spend = round(buy_spend + cash_amount, 6)
        elif side == "SELL":
            sell_proceeds = round(sell_proceeds + cash_amount, 6)
        order_id = str(trade.get("taker_order_id") or trade.get("order_id") or trade.get("id") or "").strip()
        is_local = bool(order_id and order_id in local_order_ids)
        if is_local:
            matched_local += 1
            matched_local_order_ids.add(order_id)
        normalized = {
            "ts": trade_ts(trade.get("match_time") or trade.get("last_update")),
            "market": str(trade.get("market") or ""),
            "outcome": str(trade.get("outcome") or ""),
            "side": side,
            "size": round(size, 6),
            "price": round(price, 6),
            "cash_amount_usdc": cash_amount,
            "status": str(trade.get("status") or ""),
            "order_id": order_id,
            "transaction_hash": str(trade.get("transaction_hash") or ""),
            "matched_local_order": is_local,
        }
        normalized_trades.append(normalized)
        if not is_local:
            unmatched_trades.append(normalized)

    summary = empty_summary("ok")
    summary.update(
        {
            "clob_cash_usdc": round(clob_cash, 6),
            "cash_minus_pilot_capital_usdc": round(clob_cash - pilot_capital, 6),
            "clob_open_orders": len(open_orders),
            "clob_open_positions": len(positions),
            "clob_recent_trades": len(normalized_trades),
            "clob_recent_buy_spend_usdc": round(buy_spend, 6),
            "clob_recent_sell_proceeds_usdc": round(sell_proceeds, 6),
            "clob_recent_net_trade_cashflow_usdc": round(sell_proceeds - buy_spend, 6),
            "local_live_attempt_rows": local_rows,
            "local_live_maker_order_rows": local_maker_rows,
            "local_order_ids_recorded": len(local_order_ids),
            "local_order_id_match_rate": round(len(matched_local_order_ids) / len(local_order_ids), 4) if local_order_ids else 0.0,
            "clob_trades_matching_local_order_ids": matched_local,
            "clob_trades_not_in_local_order_ids": max(len(normalized_trades) - matched_local, 0),
            "local_realized_pnl_usdc": round(local_realized_pnl, 6),
            "local_vs_clob_cashflow_gap_usdc": round((sell_proceeds - buy_spend) - local_realized_pnl, 6),
        }
    )
    return {
        "summary": summary,
        "recent_trades": normalized_trades[:40],
        "unmatched_trades": unmatched_trades[:20],
        "open_orders": open_orders[:20],
        "open_positions": positions[:20],
    }


def _mask_address(address: str) -> str:
    text = str(address or "").strip()
    if len(text) <= 12:
        return text
    return f"{text[:6]}...{text[-4:]}"


def _extract_clob_order_ids(raw: str) -> set[str]:
    try:
        payload = json.loads(str(raw or "{}"))
    except json.JSONDecodeError:
        return set()
    order_ids: set[str] = set()

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for key, nested in value.items():
                if str(key) in {"orderID", "orderId", "order_id"}:
                    order_id = str(nested or "").strip()
                    if order_id:
                        order_ids.add(order_id)
                else:
                    walk(nested)
        elif isinstance(value, list):
            for nested in value:
                walk(nested)

    walk(payload)
    return order_ids


def latest_rows(settings: LatencyBotSettings, table: str, *, limit: int = 20) -> list[dict[str, Any]]:
    allowed = {"signals", "fills", "positions", "fair_values", "orders"}
    if table not in allowed:
        return []
    order_column = "entry_ts" if table == "positions" else "ts_created" if table == "orders" else "ts"
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            f"SELECT * FROM {table} ORDER BY {order_column} DESC LIMIT ?",
            (max(1, limit),),
        ).fetchall()
    return [dict(row) for row in rows]


def latest_rows_since(
    settings: LatencyBotSettings,
    table: str,
    *,
    since_ts: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    allowed = {"signals", "fills", "positions", "fair_values", "orders"}
    if table not in allowed:
        return []
    order_column = "entry_ts" if table == "positions" else "ts_created" if table == "orders" else "ts"
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            f"SELECT * FROM {table} WHERE {order_column} >= ? ORDER BY {order_column} DESC LIMIT ?",
            (since_ts, max(1, limit)),
        ).fetchall()
    return [dict(row) for row in rows]


def latest_shadow_rows_since(
    settings: LatencyBotSettings,
    table: str,
    *,
    since_ts: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    allowed = {"shadow_signals", "shadow_positions", "shadow_position_events"}
    if table not in allowed:
        return []
    order_column = "entry_ts" if table == "shadow_positions" else "ts"
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            f"SELECT * FROM {table} WHERE {order_column} >= ? ORDER BY {order_column} DESC LIMIT ?",
            (since_ts, max(1, limit)),
        ).fetchall()
    return [dict(row) for row in rows]


def _price_band(price: float) -> str:
    if price < 0.15:
        return "<0.15"
    if price < 0.35:
        return "0.15-0.35"
    if price < 0.55:
        return "0.35-0.55"
    if price < 0.75:
        return "0.55-0.75"
    return ">=0.75"


def _edge_band(edge: float) -> str:
    if edge < 0.03:
        return "<0.03"
    if edge < 0.05:
        return "0.03-0.05"
    if edge < 0.08:
        return "0.05-0.08"
    return ">=0.08"


def _seconds_band(seconds_left: float) -> str:
    if seconds_left <= 180.0:
        return "<=180s"
    if seconds_left <= 420.0:
        return "181-420s"
    if seconds_left <= 900.0:
        return "421-900s"
    return ">900s"


def _sorted_band_rows(buckets: dict[str, dict[str, float]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for label, values in buckets.items():
        total = int(values["total"])
        eligible = int(values["eligible"])
        closed = int(values.get("closed", 0.0))
        rows.append(
            {
                "band": label,
                "total": total,
                "eligible": eligible,
                "eligible_rate": round((eligible / total), 4) if total else 0.0,
                "avg_edge": round((values["edge_sum"] / total), 6) if total else 0.0,
                "net_pnl": round(float(values.get("net_pnl", 0.0)), 6),
                "win_rate": round((values.get("wins", 0.0) / values.get("closed", 0.0)), 4) if values.get("closed", 0.0) else 0.0,
                "closed": closed,
            }
        )
    return rows


def latency_bot_signal_feature_stats(
    settings: LatencyBotSettings,
    *,
    shadow: bool = False,
    hours: int = 24,
    live_enabled_only: bool = False,
) -> dict[str, Any]:
    table = "shadow_signals" if shadow else "signals"
    since = (datetime.now(timezone.utc) - timedelta(hours=max(hours, 1))).isoformat().replace("+00:00", "Z")
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            f"""
            SELECT ts, market_id, asset, tenor_minutes, signal_type, edge, eligible, reason,
                   yes_ask, no_ask, seconds_left
            FROM {table}
            WHERE ts >= ?
              AND COALESCE(asset, '') != ''
              AND COALESCE(tenor_minutes, 0) > 0
            ORDER BY ts DESC
            LIMIT 5000
            """,
            (since,),
        ).fetchall()
    if live_enabled_only and not shadow:
        rows = [row for row in rows if _live_slice_enabled(settings, row)]
    slice_buckets: dict[str, dict[str, float]] = {}
    reason_buckets: dict[str, int] = {}
    price_buckets: dict[str, dict[str, float]] = {}
    edge_buckets: dict[str, dict[str, float]] = {}
    seconds_buckets: dict[str, dict[str, float]] = {}
    for row in rows:
        asset = str(row["asset"] or "").upper() or "?"
        tenor = int(row["tenor_minutes"] or 0)
        edge = float(row["edge"] or 0.0)
        eligible = 1 if bool(row["eligible"]) else 0
        reason = str(row["reason"] or "unknown")
        price = float(row["no_ask"] or 0.0) if shadow else float(row["yes_ask"] or 0.0)
        seconds_left = float(row["seconds_left"] or 0.0)
        reason_buckets[reason] = reason_buckets.get(reason, 0) + 1
        for bucket_map, key in (
            (slice_buckets, f"{asset} {tenor}m"),
            (price_buckets, _price_band(price)),
            (edge_buckets, _edge_band(edge)),
            (seconds_buckets, _seconds_band(seconds_left)),
        ):
            bucket = bucket_map.setdefault(key, {"total": 0.0, "eligible": 0.0, "edge_sum": 0.0})
            bucket["total"] += 1.0
            bucket["eligible"] += float(eligible)
            bucket["edge_sum"] += edge
    return {
        "total_signals": len(rows),
        "eligible_signals": sum(1 for row in rows if bool(row["eligible"])),
        "reason_breakdown": [
            {"reason": reason, "count": count}
            for reason, count in sorted(reason_buckets.items(), key=lambda item: (-item[1], item[0]))
        ],
        "slice_breakdown": _sorted_band_rows(slice_buckets),
        "price_band_breakdown": _sorted_band_rows(price_buckets),
        "edge_band_breakdown": _sorted_band_rows(edge_buckets),
        "seconds_band_breakdown": _sorted_band_rows(seconds_buckets),
    }


def latency_bot_opportunity_stats(
    settings: LatencyBotSettings,
    *,
    shadow: bool = False,
    minutes: int = 15,
    live_enabled_only: bool = False,
) -> dict[str, Any]:
    table = "shadow_signals" if shadow else "signals"
    since = (datetime.now(timezone.utc) - timedelta(minutes=max(minutes, 1))).isoformat().replace("+00:00", "Z")
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            f"""
            SELECT ts, market_id, asset, tenor_minutes, signal_type, edge, eligible, reason
            FROM {table}
            WHERE ts >= ?
              AND COALESCE(asset, '') != ''
              AND COALESCE(tenor_minutes, 0) > 0
            ORDER BY ts DESC
            LIMIT 2000
            """,
            (since,),
        ).fetchall()
        if shadow:
            last_activity_row = conn.execute(
                """
                SELECT MAX(ts) AS ts
                FROM shadow_position_events
                """
            ).fetchone()
            open_positions_row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM shadow_positions
                WHERE status = 'open'
                """
            ).fetchone()
        else:
            last_activity_row = conn.execute(
                """
                SELECT MAX(ts) AS ts
                FROM fills
                """
            ).fetchone()
            open_positions_row = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM positions
                WHERE status = 'open'
                """
            ).fetchone()
    if live_enabled_only and not shadow:
        rows = [row for row in rows if _live_slice_enabled(settings, row)]
    total = len(rows)
    eligible_rows = [row for row in rows if bool(row["eligible"])]
    reasons: dict[str, int] = {}
    top_market = ""
    top_edge = 0.0
    for row in rows:
        reason = str(row["reason"] or "unknown")
        reasons[reason] = reasons.get(reason, 0) + 1
        edge = float(row["edge"] or 0.0)
        if edge >= top_edge:
            top_edge = edge
            top_market = str(row["market_id"] or "")
    top_reason = max(reasons.items(), key=lambda item: (item[1], item[0]))[0] if reasons else "no recent signals"
    if total == 0:
        state = "No recent signals"
    elif int(open_positions_row["count"] or 0) > 0:
        state = "Active exposure"
    elif eligible_rows:
        state = "Eligible edge available"
    elif top_reason == "edge below thresholds":
        state = "Flat: no qualifying edge"
    elif top_reason == "insufficient visible depth":
        state = "Flat: depth blocked"
    elif top_reason == "too close to expiry":
        state = "Flat: too close to expiry"
    else:
        state = f"Flat: {top_reason}"
    return {
        "window_minutes": max(minutes, 1),
        "signals": total,
        "eligible": len(eligible_rows),
        "eligible_rate": round((len(eligible_rows) / total), 4) if total else 0.0,
        "top_edge": round(top_edge, 6),
        "top_market": top_market,
        "top_reason": top_reason,
        "state": state,
        "last_activity_ts": str(last_activity_row["ts"] or "-") if last_activity_row else "-",
        "open_positions": int(open_positions_row["count"] or 0) if open_positions_row else 0,
    }


def _live_slice_enabled(settings: LatencyBotSettings, row: Any) -> bool:
    asset = str(row["asset"] or "").lower()
    tenor = int(row["tenor_minutes"] or 0)
    yes_taker_enabled = (
        (tenor <= 5 or settings.allow_15m_taker)
        and (
            (asset == "btc" and settings.allow_btc_yes_taker)
            or (asset == "eth" and settings.allow_eth_yes_taker)
        )
    )
    no_taker_enabled = settings.allow_taker_no and (asset != "eth" or settings.allow_eth_no_taker)
    maker_enabled = settings.allow_maker_join or settings.allow_maker_improve
    return yes_taker_enabled or no_taker_enabled or maker_enabled


def latency_bot_threshold_relaxation_stats(
    settings: LatencyBotSettings,
    *,
    minutes: int = 60,
) -> dict[str, Any]:
    """Estimate how many currently-blocked BTC 5m YES taker signals each fair floor would admit."""
    since = (datetime.now(timezone.utc) - timedelta(minutes=max(minutes, 1))).isoformat().replace("+00:00", "Z")
    thresholds = [0.50, 0.52, 0.55, 0.58, settings.taker_min_fair_yes_5m]
    thresholds = sorted({round(float(value), 4) for value in thresholds})
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT ts, market_id, asset, tenor_minutes, edge, fair_yes, yes_ask,
                   min_depth_usdc, seconds_left, reason, blocked_reason
            FROM signals
            WHERE ts >= ?
              AND asset = 'btc'
              AND tenor_minutes <= 5
            ORDER BY ts DESC
            LIMIT 5000
            """,
            (since,),
        ).fetchall()
    operational_rows = [
        row
        for row in rows
        if float(row["edge"] or 0.0) >= settings.taker_min_edge_5m
        and settings.min_trade_price <= float(row["yes_ask"] or 0.0) <= settings.max_trade_price
        and float(row["seconds_left"] or 0.0) >= settings.min_taker_entry_seconds_left_5m
        and float(row["min_depth_usdc"] or 0.0) >= settings.min_book_depth_usdc
    ]
    threshold_rows: list[dict[str, Any]] = []
    for threshold in thresholds:
        admitted = [row for row in operational_rows if float(row["fair_yes"] or 0.0) >= threshold]
        markets = {str(row["market_id"] or "") for row in admitted}
        edge_sum = sum(float(row["edge"] or 0.0) for row in admitted)
        fair_sum = sum(float(row["fair_yes"] or 0.0) for row in admitted)
        price_sum = sum(float(row["yes_ask"] or 0.0) for row in admitted)
        threshold_rows.append(
            {
                "fair_yes_floor": threshold,
                "signals": len(admitted),
                "markets": len(markets),
                "avg_edge": round(edge_sum / len(admitted), 6) if admitted else 0.0,
                "avg_fair_yes": round(fair_sum / len(admitted), 6) if admitted else 0.0,
                "avg_entry_price": round(price_sum / len(admitted), 6) if admitted else 0.0,
            }
        )
    top_rows = sorted(operational_rows, key=lambda row: float(row["edge"] or 0.0), reverse=True)[:12]
    return {
        "window_minutes": max(minutes, 1),
        "operational_candidate_signals": len(operational_rows),
        "threshold_breakdown": threshold_rows,
        "top_blocked": [
            {
                "ts": str(row["ts"] or ""),
                "market_id": str(row["market_id"] or ""),
                "edge": round(float(row["edge"] or 0.0), 6),
                "fair_yes": round(float(row["fair_yes"] or 0.0), 6),
                "yes_ask": round(float(row["yes_ask"] or 0.0), 6),
                "seconds_left": round(float(row["seconds_left"] or 0.0), 3),
                "reason": str(row["reason"] or ""),
            }
            for row in top_rows
        ],
    }


def latency_bot_equity_curve(settings: LatencyBotSettings, *, limit: int = 200) -> list[dict[str, Any]]:
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT ts, realized_pnl_usdc, unrealized_pnl_usdc, equity_usdc
            FROM (
                SELECT ts, realized_pnl_usdc, unrealized_pnl_usdc, equity_usdc
                FROM equity_snapshots
                ORDER BY ts DESC
                LIMIT ?
            )
            ORDER BY ts ASC
            """,
            (max(1, limit),),
        ).fetchall()
    points: list[dict[str, Any]] = []
    for row in rows:
        points.append(
            {
                "ts": str(row["ts"] or ""),
                "realized_pnl_usdc": round(float(row["realized_pnl_usdc"] or 0.0), 6),
                "unrealized_pnl_usdc": round(float(row["unrealized_pnl_usdc"] or 0.0), 6),
                "equity_usdc": round(float(row["equity_usdc"] or settings.bankroll_usdc), 6),
            }
        )
    return points


def latency_bot_live_strategy_equity_curves(settings: LatencyBotSettings, *, limit: int = 500) -> list[dict[str, Any]]:
    promoted_modes = [
        f"promoted_variant:{variant_id}"
        for variant_id in dict.fromkeys(str(item).strip().lower() for item in settings.promoted_variant_ids if str(item).strip())
    ]
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT pe.ts, p.mode, p.market_id, pe.pnl
            FROM (
                SELECT pe_inner.id, pe_inner.ts, pe_inner.position_id, pe_inner.pnl
                FROM position_events pe_inner
                WHERE pe_inner.event_type = 'close'
                ORDER BY pe_inner.ts DESC, pe_inner.id DESC
                LIMIT ?
            ) pe
            JOIN positions p ON p.position_id = pe.position_id
            ORDER BY pe.ts ASC, pe.id ASC
            """,
            (max(1, limit),),
        ).fetchall()
    running_by_mode: dict[str, float] = {}
    points: list[dict[str, Any]] = []
    seen_modes: set[str] = set()
    for row in rows:
        mode = str(row["mode"] or "live")
        if mode.startswith("promoted_variant:") and mode not in promoted_modes:
            continue
        seen_modes.add(mode)
        running_by_mode[mode] = round(running_by_mode.get(mode, 0.0) + float(row["pnl"] or 0.0), 6)
        points.append(
            {
                "ts": str(row["ts"] or ""),
                "variant_id": mode,
                "market_id": str(row["market_id"] or ""),
                "realized_pnl_usdc": running_by_mode[mode],
                "unrealized_pnl_usdc": 0.0,
                "equity_usdc": round(settings.bankroll_usdc + running_by_mode[mode], 6),
            }
        )
    baseline_ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    for mode in promoted_modes:
        if mode in seen_modes:
            continue
        points.append(
            {
                "ts": baseline_ts,
                "variant_id": mode,
                "market_id": "",
                "realized_pnl_usdc": 0.0,
                "unrealized_pnl_usdc": 0.0,
                "equity_usdc": round(settings.bankroll_usdc, 6),
            }
        )
    return points


def latency_bot_shadow_equity_curve(settings: LatencyBotSettings, *, limit: int = 200) -> list[dict[str, Any]]:
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT ts, pnl, reason
            FROM (
                SELECT ts, pnl, reason
                FROM shadow_position_events
                WHERE event_type = 'close'
                ORDER BY ts DESC
                LIMIT ?
            )
            ORDER BY ts ASC
            """,
            (max(1, limit),),
        ).fetchall()
    running_realized = 0.0
    points: list[dict[str, Any]] = []
    for row in rows:
        running_realized = round(running_realized + float(row["pnl"] or 0.0), 6)
        points.append(
            {
                "ts": str(row["ts"] or ""),
                "realized_pnl_usdc": running_realized,
                "unrealized_pnl_usdc": 0.0,
                "equity_usdc": round(settings.bankroll_usdc + running_realized, 6),
                "reason": str(row["reason"] or ""),
            }
        )
    return points


def latency_bot_shadow_variant_performance_stats(settings: LatencyBotSettings) -> dict[str, Any]:
    with connect_latency_bot_db(settings) as conn:
        recent_signal_since = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat().replace("+00:00", "Z")
        summary_rows = conn.execute(
            """
            SELECT
                svp.variant_id,
                COUNT(*) AS closed,
                SUM(CASE WHEN svpe.pnl > 0 THEN 1 ELSE 0 END) AS wins,
                SUM(COALESCE(svpe.pnl, 0.0)) AS net_pnl,
                AVG(CASE WHEN svpe.pnl > 0 THEN svpe.pnl END) AS avg_win,
                AVG(CASE WHEN svpe.pnl < 0 THEN svpe.pnl END) AS avg_loss,
                AVG(COALESCE(svp.entry_edge, 0.0)) AS avg_edge,
                AVG(
                    CASE
                        WHEN UPPER(COALESCE(svp.side, 'YES')) = 'NO' THEN COALESCE(svp.entry_fair_no, 0.0)
                        ELSE COALESCE(svp.entry_fair_yes, 0.0)
                    END
                ) AS avg_fair_yes,
                AVG(COALESCE(svp.entry_price, 0.0)) AS avg_entry_price,
                COUNT(DISTINCT svp.market_id) AS unique_markets
            FROM shadow_variant_position_events svpe
            JOIN shadow_variant_positions svp ON svp.position_id = svpe.position_id
            WHERE svpe.event_type = 'close'
            GROUP BY svp.variant_id
            ORDER BY net_pnl DESC
            """
        ).fetchall()
        open_rows = conn.execute(
            """
            SELECT variant_id, COUNT(*) AS open_count
            FROM shadow_variant_positions
            WHERE status = 'open'
            GROUP BY variant_id
            """
        ).fetchall()
        signal_rows = conn.execute(
            """
            SELECT variant_id, signals, eligible
            FROM shadow_variant_signal_summary
            """
        ).fetchall()
        if not signal_rows and _should_run_legacy_backfills(conn, 1):
            signal_rows = conn.execute(
                """
                SELECT variant_id, COUNT(*) AS signals, SUM(CASE WHEN eligible THEN 1 ELSE 0 END) AS eligible
                FROM shadow_variant_signals
                WHERE ts >= ?
                GROUP BY variant_id
                """,
                (recent_signal_since,),
            ).fetchall()
        recent_rows = conn.execute(
            """
            SELECT svpe.ts, svp.variant_id, svpe.position_id, svp.market_id, svp.entry_price, svpe.mark, svpe.pnl, svpe.reason
            FROM shadow_variant_position_events svpe
            JOIN shadow_variant_positions svp ON svp.position_id = svpe.position_id
            WHERE svpe.event_type = 'close'
            ORDER BY svpe.ts DESC, svpe.id DESC
            LIMIT 20
            """
        ).fetchall()
        reason_rows = conn.execute(
            """
            SELECT
                variant_id,
                reason,
                count,
                max_edge,
                max_side_fair AS max_fair_yes
            FROM shadow_variant_reason_summary
            ORDER BY variant_id ASC, count DESC
            """
        ).fetchall()
        if not reason_rows and _should_run_legacy_backfills(conn, 1):
            reason_rows = conn.execute(
                """
                SELECT
                    variant_id,
                    reason,
                    COUNT(*) AS count,
                    MAX(edge) AS max_edge,
                    MAX(
                        CASE
                            WHEN UPPER(COALESCE(side, 'YES')) = 'NO' THEN COALESCE(fair_no, 0.0)
                            ELSE COALESCE(fair_yes, 0.0)
                        END
                    ) AS max_fair_yes
                FROM shadow_variant_signals
                WHERE ts >= ?
                GROUP BY variant_id, reason
                ORDER BY variant_id ASC, count DESC
                """,
                (recent_signal_since,),
            ).fetchall()
        curve_rows = conn.execute(
            """
            SELECT svpe.ts, svp.variant_id, svp.market_id, svpe.pnl
            FROM shadow_variant_position_events svpe
            JOIN shadow_variant_positions svp ON svp.position_id = svpe.position_id
            WHERE svpe.event_type = 'close'
            ORDER BY svpe.ts ASC, svpe.id ASC
            """
        ).fetchall()
        market_rows = conn.execute(
            """
            SELECT
                svp.market_id,
                COUNT(*) AS closed,
                COUNT(DISTINCT svp.variant_id) AS variants,
                SUM(COALESCE(svpe.pnl, 0.0)) AS net_pnl,
                MIN(COALESCE(svpe.pnl, 0.0)) AS worst_pnl,
                MAX(COALESCE(svpe.pnl, 0.0)) AS best_pnl,
                GROUP_CONCAT(DISTINCT svp.variant_id) AS variant_ids
            FROM shadow_variant_position_events svpe
            JOIN shadow_variant_positions svp ON svp.position_id = svpe.position_id
            WHERE svpe.event_type = 'close'
            GROUP BY svp.market_id
            ORDER BY ABS(SUM(COALESCE(svpe.pnl, 0.0))) DESC
            LIMIT 20
            """
        ).fetchall()
        calibration_rows = conn.execute(
            """
            SELECT
                svp.variant_id,
                CASE
                    WHEN CASE WHEN UPPER(COALESCE(svp.side, 'YES')) = 'NO' THEN COALESCE(svp.entry_fair_no, 0.0) ELSE COALESCE(svp.entry_fair_yes, 0.0) END < 0.48 THEN '<0.48'
                    WHEN CASE WHEN UPPER(COALESCE(svp.side, 'YES')) = 'NO' THEN COALESCE(svp.entry_fair_no, 0.0) ELSE COALESCE(svp.entry_fair_yes, 0.0) END < 0.50 THEN '0.48-0.50'
                    WHEN CASE WHEN UPPER(COALESCE(svp.side, 'YES')) = 'NO' THEN COALESCE(svp.entry_fair_no, 0.0) ELSE COALESCE(svp.entry_fair_yes, 0.0) END < 0.52 THEN '0.50-0.52'
                    WHEN CASE WHEN UPPER(COALESCE(svp.side, 'YES')) = 'NO' THEN COALESCE(svp.entry_fair_no, 0.0) ELSE COALESCE(svp.entry_fair_yes, 0.0) END < 0.55 THEN '0.52-0.55'
                    ELSE '>=0.55'
                END AS fair_band,
                COUNT(*) AS closed,
                SUM(CASE WHEN COALESCE(svpe.pnl, 0.0) > 0.0 THEN 1 ELSE 0 END) AS wins,
                AVG(CASE WHEN UPPER(COALESCE(svp.side, 'YES')) = 'NO' THEN COALESCE(svp.entry_fair_no, 0.0) ELSE COALESCE(svp.entry_fair_yes, 0.0) END) AS avg_fair_yes,
                AVG(COALESCE(svp.entry_edge, 0.0)) AS avg_edge,
                SUM(COALESCE(svpe.pnl, 0.0)) AS net_pnl,
                AVG(
                    (CASE WHEN UPPER(COALESCE(svp.side, 'YES')) = 'NO' THEN COALESCE(svp.entry_fair_no, 0.0) ELSE COALESCE(svp.entry_fair_yes, 0.0) END - CASE WHEN COALESCE(svpe.pnl, 0.0) > 0.0 THEN 1.0 ELSE 0.0 END)
                    * (CASE WHEN UPPER(COALESCE(svp.side, 'YES')) = 'NO' THEN COALESCE(svp.entry_fair_no, 0.0) ELSE COALESCE(svp.entry_fair_yes, 0.0) END - CASE WHEN COALESCE(svpe.pnl, 0.0) > 0.0 THEN 1.0 ELSE 0.0 END)
                ) AS brier_vs_win
            FROM shadow_variant_position_events svpe
            JOIN shadow_variant_positions svp ON svp.position_id = svpe.position_id
            WHERE svpe.event_type = 'close'
            GROUP BY svp.variant_id, fair_band
            ORDER BY svp.variant_id ASC, fair_band ASC
            """
        ).fetchall()
    open_by_variant = {str(row["variant_id"] or ""): int(row["open_count"] or 0) for row in open_rows}
    signals_by_variant = {
        str(row["variant_id"] or ""): {
            "signals": int(row["signals"] or 0),
            "eligible": int(row["eligible"] or 0),
        }
        for row in signal_rows
    }
    summaries: list[dict[str, Any]] = []
    curve_dicts = [dict(row) for row in curve_rows]
    variant_diagnostics = _shadow_variant_diagnostics(curve_dicts)
    for row in summary_rows:
        variant_id = str(row["variant_id"] or "")
        closed = int(row["closed"] or 0)
        wins = int(row["wins"] or 0)
        sig = signals_by_variant.get(variant_id, {"signals": 0, "eligible": 0})
        diagnostics = variant_diagnostics.get(variant_id, {})
        recent_10_pnl = float(diagnostics.get("recent_10_pnl", 0.0))
        max_drawdown = float(diagnostics.get("max_drawdown", 0.0))
        unique_markets = int(row["unique_markets"] or 0)
        win_rate = round(wins / closed, 4) if closed else 0.0
        promotion = _shadow_variant_promotion_status(
            closed=closed,
            unique_markets=unique_markets,
            win_rate=win_rate,
            net_pnl=float(row["net_pnl"] or 0.0),
            recent_10_pnl=recent_10_pnl,
            max_drawdown=max_drawdown,
        )
        summaries.append(
            {
                "variant_id": variant_id,
                "signals": sig["signals"],
                "eligible": sig["eligible"],
                "open": open_by_variant.get(variant_id, 0),
                "closed": closed,
                "unique_markets": unique_markets,
                "win_rate": win_rate,
                "net_pnl": round(float(row["net_pnl"] or 0.0), 6),
                "recent_10_pnl": round(recent_10_pnl, 6),
                "max_drawdown": round(max_drawdown, 6),
                "avg_win": round(float(row["avg_win"] or 0.0), 6),
                "avg_loss": round(float(row["avg_loss"] or 0.0), 6),
                "avg_edge": round(float(row["avg_edge"] or 0.0), 6),
                "avg_fair_yes": round(float(row["avg_fair_yes"] or 0.0), 6),
                "avg_entry_price": round(float(row["avg_entry_price"] or 0.0), 6),
                "promotion_status": promotion["status"],
                "promotion_reason": promotion["reason"],
            }
        )
    for variant_id, sig in signals_by_variant.items():
        if any(item["variant_id"] == variant_id for item in summaries):
            continue
        summaries.append(
            {
                "variant_id": variant_id,
                "signals": sig["signals"],
                "eligible": sig["eligible"],
                "open": open_by_variant.get(variant_id, 0),
                "closed": 0,
                "unique_markets": 0,
                "win_rate": 0.0,
                "net_pnl": 0.0,
                "recent_10_pnl": 0.0,
                "max_drawdown": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "avg_edge": 0.0,
                "avg_fair_yes": 0.0,
                "avg_entry_price": 0.0,
                "promotion_status": "collecting",
                "promotion_reason": "needs 30 closed trades",
            }
        )
    summaries.sort(key=lambda item: (float(item["net_pnl"]), int(item["closed"])), reverse=True)
    summary_by_variant = {str(item["variant_id"]): item for item in summaries}
    family_summaries = _shadow_variant_family_summaries(
        summaries,
        limit=max(int(settings.shadow_variant_dashboard_family_limit), 0),
    )
    watchlist: list[dict[str, Any]] = []
    for variant_id in settings.shadow_variant_watchlist:
        existing = summary_by_variant.get(str(variant_id))
        if existing is not None:
            watchlist.append(dict(existing))
            continue
        watchlist.append(
            {
                "variant_id": str(variant_id),
                "signals": 0,
                "eligible": 0,
                "open": 0,
                "closed": 0,
                "unique_markets": 0,
                "win_rate": 0.0,
                "net_pnl": 0.0,
                "recent_10_pnl": 0.0,
                "max_drawdown": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "avg_edge": 0.0,
                "avg_fair_yes": 0.0,
                "avg_entry_price": 0.0,
                "promotion_status": "watching",
                "promotion_reason": "configured watchlist; no observations yet",
            }
        )
    top_raw_pnl_count = max(int(settings.shadow_variant_top_raw_pnl_count), 0)
    top_raw_pnl = summaries[:top_raw_pnl_count]
    grid_limit = max(int(settings.shadow_variant_dashboard_grid_limit), 0)
    grid_summaries = summaries[:grid_limit] if grid_limit else []
    curve_variant_ids = {str(item["variant_id"]) for item in top_raw_pnl}
    curve_variant_ids.update(str(variant_id) for variant_id in settings.shadow_variant_watchlist)
    filtered_curve_dicts = [row for row in curve_dicts if str(row.get("variant_id") or "") in curve_variant_ids]
    reason_limit = max(int(settings.shadow_variant_dashboard_reason_limit), 0)
    return {
        "variants": grid_summaries,
        "variants_total": len(summaries),
        "top_raw_pnl": top_raw_pnl,
        "families": family_summaries,
        "watchlist": watchlist,
        "recent_closes": [dict(row) for row in recent_rows],
        "reason_breakdown": [dict(row) for row in reason_rows[:reason_limit]],
        "reason_breakdown_total": len(reason_rows),
        "market_breakdown": [dict(row) for row in market_rows],
        "calibration": [dict(row) for row in calibration_rows],
        "equity_curve": _shadow_variant_equity_curve(settings, filtered_curve_dicts),
    }


def _shadow_variant_diagnostics(rows: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    running_by_variant: dict[str, float] = {}
    peak_by_variant: dict[str, float] = {}
    max_drawdown_by_variant: dict[str, float] = {}
    pnl_by_variant: dict[str, list[float]] = {}
    for row in rows:
        variant_id = str(row.get("variant_id") or "")
        pnl = float(row.get("pnl") or 0.0)
        running_by_variant[variant_id] = running_by_variant.get(variant_id, 0.0) + pnl
        peak_by_variant[variant_id] = max(peak_by_variant.get(variant_id, 0.0), running_by_variant[variant_id])
        drawdown = running_by_variant[variant_id] - peak_by_variant[variant_id]
        max_drawdown_by_variant[variant_id] = min(max_drawdown_by_variant.get(variant_id, 0.0), drawdown)
        pnl_by_variant.setdefault(variant_id, []).append(pnl)
    diagnostics: dict[str, dict[str, float]] = {}
    for variant_id, pnls in pnl_by_variant.items():
        diagnostics[variant_id] = {
            "recent_10_pnl": round(sum(pnls[-10:]), 6),
            "max_drawdown": round(max_drawdown_by_variant.get(variant_id, 0.0), 6),
        }
    return diagnostics


def _shadow_variant_promotion_status(
    *,
    closed: int,
    unique_markets: int,
    win_rate: float,
    net_pnl: float,
    recent_10_pnl: float,
    max_drawdown: float,
) -> dict[str, str]:
    if closed < 30:
        return {"status": "collecting", "reason": f"needs {30 - closed} more closes"}
    if unique_markets < 20:
        return {"status": "blocked", "reason": f"needs {20 - unique_markets} more unique markets"}
    if net_pnl <= 0.0:
        return {"status": "blocked", "reason": "net PnL <= 0"}
    if recent_10_pnl <= 0.0:
        return {"status": "blocked", "reason": "recent 10 PnL <= 0"}
    if win_rate < 0.52:
        return {"status": "blocked", "reason": "win rate < 52%"}
    if max_drawdown < -150.0:
        return {"status": "blocked", "reason": "drawdown worse than -$150"}
    return {"status": "candidate", "reason": "passes shadow promotion gates"}


def _shadow_variant_family_id(variant_id: str) -> str:
    text = str(variant_id or "")
    if "_f" in text:
        return text.split("_f", 1)[0]
    return text


def _shadow_variant_family_summaries(summaries: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in summaries:
        grouped.setdefault(_shadow_variant_family_id(str(item.get("variant_id") or "")), []).append(item)
    rows: list[dict[str, Any]] = []
    for family, items in grouped.items():
        representative = max(items, key=lambda item: (float(item.get("net_pnl", 0.0)), int(item.get("closed", 0))))
        candidate_count = sum(1 for item in items if str(item.get("promotion_status") or "") == "candidate")
        positive_count = sum(1 for item in items if float(item.get("net_pnl", 0.0)) > 0.0)
        rows.append(
            {
                "family": family,
                "variants": len(items),
                "positive_variants": positive_count,
                "candidate_variants": candidate_count,
                "representative": str(representative.get("variant_id") or ""),
                "rep_closed": int(representative.get("closed", 0) or 0),
                "rep_unique_markets": int(representative.get("unique_markets", 0) or 0),
                "rep_win_rate": float(representative.get("win_rate", 0.0) or 0.0),
                "rep_net_pnl": float(representative.get("net_pnl", 0.0) or 0.0),
                "rep_recent_10_pnl": float(representative.get("recent_10_pnl", 0.0) or 0.0),
                "rep_max_drawdown": float(representative.get("max_drawdown", 0.0) or 0.0),
                "rep_promotion_status": str(representative.get("promotion_status") or ""),
                "rep_promotion_reason": str(representative.get("promotion_reason") or ""),
            }
        )
    rows.sort(
        key=lambda item: (
            int(item["candidate_variants"]),
            float(item["rep_net_pnl"]),
            int(item["rep_closed"]),
        ),
        reverse=True,
    )
    return rows[:limit]


def _shadow_variant_equity_curve(settings: LatencyBotSettings, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    running_by_variant: dict[str, float] = {}
    points: list[dict[str, Any]] = []
    for row in rows:
        variant_id = str(row.get("variant_id") or "")
        running_by_variant[variant_id] = round(running_by_variant.get(variant_id, 0.0) + float(row.get("pnl") or 0.0), 6)
        points.append(
            {
                "ts": str(row.get("ts") or ""),
                "variant_id": variant_id,
                "realized_pnl_usdc": running_by_variant[variant_id],
                "unrealized_pnl_usdc": 0.0,
                "equity_usdc": round(settings.bankroll_usdc + running_by_variant[variant_id], 6),
            }
        )
    return points


def latency_bot_performance_stats(settings: LatencyBotSettings) -> dict[str, Any]:
    with connect_latency_bot_db(settings) as conn:
        close_rows = conn.execute(
            """
            SELECT ts, position_id, mark, pnl, reason
            FROM position_events
            WHERE event_type = 'close'
            ORDER BY ts DESC
            LIMIT 50
            """
        ).fetchall()
        all_close_rows = conn.execute(
            """
            SELECT pnl, reason
            FROM position_events
            WHERE event_type = 'close'
            """
        ).fetchall()
        slice_rows = conn.execute(
            """
            SELECT
                p.entry_price,
                pe.pnl,
                pe.reason,
                p.entry_edge AS edge,
                p.entry_seconds_left AS seconds_left,
                p.asset,
                p.entry_tenor_minutes AS tenor_minutes
            FROM positions p
            JOIN position_events pe
              ON pe.position_id = p.position_id
             AND pe.event_type = 'close'
            """
        ).fetchall()
        duplicate_close_rows = conn.execute(
            """
            SELECT position_id, COUNT(*) AS close_count, MIN(ts) AS first_close_ts, MAX(ts) AS last_close_ts
            FROM position_events
            WHERE event_type = 'close'
            GROUP BY position_id
            HAVING COUNT(*) > 1
            ORDER BY close_count DESC, last_close_ts DESC
            LIMIT 20
            """
        ).fetchall()
    closes = [dict(row) for row in close_rows]
    pnls = [float(row["pnl"] or 0.0) for row in all_close_rows]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    reason_totals: dict[str, dict[str, float]] = {}
    for row in all_close_rows:
        reason = str(row["reason"] or "unknown")
        bucket = reason_totals.setdefault(reason, {"count": 0, "pnl": 0.0})
        bucket["count"] += 1
        bucket["pnl"] = round(bucket["pnl"] + float(row["pnl"] or 0.0), 6)
    breakdown = [
        {
            "reason": reason,
            "count": int(values["count"]),
            "pnl": round(float(values["pnl"]), 6),
        }
        for reason, values in sorted(reason_totals.items(), key=lambda item: (-(item[1]["count"]), item[0]))
    ]
    price_buckets: dict[str, dict[str, float]] = {}
    edge_buckets: dict[str, dict[str, float]] = {}
    seconds_buckets: dict[str, dict[str, float]] = {}
    slice_buckets: dict[str, dict[str, float]] = {}
    for row in slice_rows:
        pnl = float(row["pnl"] or 0.0)
        edge = float(row["edge"] or 0.0)
        seconds_left = float(row["seconds_left"] or 0.0)
        price = float(row["entry_price"] or 0.0)
        asset = str(row["asset"] or "btc").upper()
        tenor = int(row["tenor_minutes"] or 5)
        for bucket_map, key in (
            (price_buckets, _price_band(price)),
            (edge_buckets, _edge_band(edge)),
            (seconds_buckets, _seconds_band(seconds_left)),
            (slice_buckets, f"{asset} {tenor}m"),
        ):
            bucket = bucket_map.setdefault(key, {"total": 0.0, "eligible": 0.0, "edge_sum": 0.0, "net_pnl": 0.0, "wins": 0.0, "closed": 0.0})
            bucket["total"] += 1.0
            bucket["eligible"] += 1.0
            bucket["edge_sum"] += edge
            bucket["net_pnl"] += pnl
            bucket["closed"] += 1.0
            if pnl > 0:
                bucket["wins"] += 1.0
    closes_desc = closes
    closes_asc = list(reversed(closes_desc))
    current_streak_direction = "flat"
    current_streak_length = 0
    for row in closes_desc:
        pnl = float(row.get("pnl") or 0.0)
        direction = "win" if pnl > 0 else "loss" if pnl < 0 else "flat"
        if current_streak_length == 0:
            current_streak_direction = direction
            current_streak_length = 1
            continue
        if direction == current_streak_direction:
            current_streak_length += 1
        else:
            break
    def _window_stats(items: list[dict[str, Any]], limit: int) -> dict[str, Any]:
        window = items[:limit]
        pnls_window = [float(item.get("pnl") or 0.0) for item in window]
        return {
            "count": len(window),
            "net_pnl": round(sum(pnls_window), 6),
            "wins": sum(1 for pnl in pnls_window if pnl > 0),
            "losses": sum(1 for pnl in pnls_window if pnl < 0),
            "stop_losses": sum(1 for item in window if str(item.get("reason") or "") == "STOP_LOSS"),
        }
    recent_5 = _window_stats(closes_desc, 5)
    recent_10 = _window_stats(closes_desc, 10)
    duplicate_close_events = sum(max(int(row["close_count"] or 0) - 1, 0) for row in duplicate_close_rows)
    total_closed = len(pnls)
    return {
        "recent_closes": closes,
        "total_closed": total_closed,
        "win_rate": round((len(wins) / total_closed), 4) if total_closed else 0.0,
        "avg_win": round(sum(wins) / len(wins), 6) if wins else 0.0,
        "avg_loss": round(sum(losses) / len(losses), 6) if losses else 0.0,
        "net_pnl": round(sum(pnls), 6),
        "exit_reason_breakdown": breakdown,
        "slice_breakdown": _sorted_band_rows(slice_buckets),
        "price_band_breakdown": _sorted_band_rows(price_buckets),
        "edge_band_breakdown": _sorted_band_rows(edge_buckets),
        "seconds_band_breakdown": _sorted_band_rows(seconds_buckets),
        "current_streak_direction": current_streak_direction,
        "current_streak_length": current_streak_length,
        "recent_5": recent_5,
        "recent_10": recent_10,
        "duplicate_close_positions": len(duplicate_close_rows),
        "duplicate_close_events": duplicate_close_events,
        "duplicate_close_rows": [dict(row) for row in duplicate_close_rows],
    }


def latency_bot_portfolio_summary(
    settings: LatencyBotSettings,
    *,
    cache_items: list[dict[str, Any]] | None = None,
) -> dict[str, float]:
    with connect_latency_bot_db(settings) as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(pnl), 0.0) AS realized_pnl_usdc
            FROM position_events
            WHERE event_type = 'close'
            """
        ).fetchone()
        realized_last_24h_row = conn.execute(
            """
            SELECT COALESCE(SUM(pnl), 0.0) AS realized_pnl_24h_usdc
            FROM position_events
            WHERE event_type = 'close'
              AND ts >= ?
            """,
            ((datetime.now(timezone.utc) - timedelta(hours=24)).isoformat().replace("+00:00", "Z"),),
        ).fetchone()
    realized_pnl = round(float(row["realized_pnl_usdc"] or 0.0), 6) if row else 0.0
    realized_pnl_24h = round(float(realized_last_24h_row["realized_pnl_24h_usdc"] or 0.0), 6) if realized_last_24h_row else 0.0
    cache_by_market = {
        str(item.get("market_id") or ""): item
        for item in (cache_items or [])
        if isinstance(item, dict)
    }
    unrealized_pnl = 0.0
    for position in load_open_positions(settings):
        cache = cache_by_market.get(str(position.get("market_id") or ""))
        if cache is None:
            continue
        best_bid = float(cache.get("best_bid") or 0.0)
        best_ask = float(cache.get("best_ask") or 0.0)
        mark = best_bid if str(position.get("side") or "") == "YES" else max(1.0 - best_ask, 0.0)
        unrealized_pnl += (mark - float(position.get("entry_price") or 0.0)) * float(position.get("size") or 0.0)
    unrealized_pnl = round(unrealized_pnl, 6)
    return {
        "realized_pnl_usdc": realized_pnl,
        "realized_pnl_24h_usdc": realized_pnl_24h,
        "realized_usdc_per_day": realized_pnl_24h,
        "projected_monthly_revenue_usdc": round(realized_pnl_24h * 30.0, 6),
        "projected_yearly_revenue_usdc": round(realized_pnl_24h * 365.0, 6),
        "unrealized_pnl_usdc": unrealized_pnl,
        "equity_usdc": round(settings.bankroll_usdc + realized_pnl + unrealized_pnl, 6),
    }


def _storage_parse_ts(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def latency_bot_promoted_variant_performance_stats(
    settings: LatencyBotSettings,
    *,
    cache_items: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    prefix = "promoted_variant:"
    enabled_variant_ids = tuple(dict.fromkeys(str(item).strip().lower() for item in settings.promoted_variant_ids if str(item).strip()))
    variant_rows: dict[str, dict[str, Any]] = {
        variant_id: {
            "variant_id": variant_id,
            "open": 0,
            "closed": 0,
            "unique_markets": 0,
            "win_rate": 0.0,
            "net_pnl": 0.0,
            "realized_pnl_24h_usdc": 0.0,
            "realized_usdc_per_day": 0.0,
            "projected_monthly_revenue_usdc": 0.0,
            "projected_yearly_revenue_usdc": 0.0,
            "unrealized_pnl_usdc": 0.0,
            "recent_10_pnl": 0.0,
            "max_drawdown": 0.0,
        }
        for variant_id in enabled_variant_ids
    }
    with connect_latency_bot_db(settings) as conn:
        open_rows = conn.execute(
            """
            SELECT
                SUBSTR(mode, ?) AS variant_id,
                market_id,
                side,
                entry_price,
                size
            FROM positions
            WHERE status = 'open'
              AND mode LIKE ?
            ORDER BY entry_ts ASC
            """,
            (len(prefix) + 1, f"{prefix}%"),
        ).fetchall()
        close_rows = conn.execute(
            """
            SELECT
                pe.ts,
                p.position_id,
                SUBSTR(p.mode, ?) AS variant_id,
                p.market_id,
                p.side,
                p.entry_price,
                pe.mark,
                pe.pnl,
                pe.reason
            FROM position_events pe
            JOIN positions p ON p.position_id = pe.position_id
            WHERE pe.event_type = 'close'
              AND p.mode LIKE ?
            ORDER BY pe.ts ASC, pe.id ASC
            """,
            (len(prefix) + 1, f"{prefix}%"),
        ).fetchall()

    cache_by_market = {
        str(item.get("market_id") or ""): item
        for item in (cache_items or [])
        if isinstance(item, dict)
    }
    market_sets: dict[str, set[str]] = {variant_id: set() for variant_id in variant_rows}
    pnls_by_variant: dict[str, list[float]] = {variant_id: [] for variant_id in variant_rows}
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    running_by_variant: dict[str, float] = {}
    peak_by_variant: dict[str, float] = {}

    for row in open_rows:
        variant_id = str(row["variant_id"] or "")
        if variant_id not in variant_rows:
            continue
        item = variant_rows.setdefault(
            variant_id,
            {
                "variant_id": variant_id,
                "open": 0,
                "closed": 0,
                "unique_markets": 0,
                "win_rate": 0.0,
                "net_pnl": 0.0,
                "realized_pnl_24h_usdc": 0.0,
                "realized_usdc_per_day": 0.0,
                "projected_monthly_revenue_usdc": 0.0,
                "projected_yearly_revenue_usdc": 0.0,
                "unrealized_pnl_usdc": 0.0,
                "recent_10_pnl": 0.0,
                "max_drawdown": 0.0,
            },
        )
        item["open"] = int(item.get("open", 0) or 0) + 1
        cache = cache_by_market.get(str(row["market_id"] or ""))
        if cache is None:
            continue
        best_bid = float(cache.get("best_bid") or 0.0)
        best_ask = float(cache.get("best_ask") or 0.0)
        mark = best_bid if str(row["side"] or "") == "YES" else max(1.0 - best_ask, 0.0)
        item["unrealized_pnl_usdc"] = round(
            float(item.get("unrealized_pnl_usdc", 0.0) or 0.0)
            + (mark - float(row["entry_price"] or 0.0)) * float(row["size"] or 0.0),
            6,
        )

    for row in close_rows:
        variant_id = str(row["variant_id"] or "")
        if variant_id not in variant_rows:
            continue
        item = variant_rows.setdefault(
            variant_id,
            {
                "variant_id": variant_id,
                "open": 0,
                "closed": 0,
                "unique_markets": 0,
                "win_rate": 0.0,
                "net_pnl": 0.0,
                "realized_pnl_24h_usdc": 0.0,
                "realized_usdc_per_day": 0.0,
                "projected_monthly_revenue_usdc": 0.0,
                "projected_yearly_revenue_usdc": 0.0,
                "unrealized_pnl_usdc": 0.0,
                "recent_10_pnl": 0.0,
                "max_drawdown": 0.0,
            },
        )
        pnl = float(row["pnl"] or 0.0)
        item["closed"] = int(item.get("closed", 0) or 0) + 1
        item["net_pnl"] = round(float(item.get("net_pnl", 0.0) or 0.0) + pnl, 6)
        market_sets.setdefault(variant_id, set()).add(str(row["market_id"] or ""))
        pnls_by_variant.setdefault(variant_id, []).append(pnl)
        parsed_ts = _storage_parse_ts(str(row["ts"] or ""))
        if parsed_ts is not None and parsed_ts >= cutoff:
            item["realized_pnl_24h_usdc"] = round(float(item.get("realized_pnl_24h_usdc", 0.0) or 0.0) + pnl, 6)
        running_by_variant[variant_id] = round(running_by_variant.get(variant_id, 0.0) + pnl, 6)
        peak_by_variant[variant_id] = max(peak_by_variant.get(variant_id, 0.0), running_by_variant[variant_id])
        item["max_drawdown"] = min(float(item.get("max_drawdown", 0.0) or 0.0), running_by_variant[variant_id] - peak_by_variant[variant_id])

    for variant_id, item in variant_rows.items():
        pnls = pnls_by_variant.get(variant_id, [])
        closed = int(item.get("closed", 0) or 0)
        wins = sum(1 for pnl in pnls if pnl > 0.0)
        item["unique_markets"] = len(market_sets.get(variant_id, set()))
        item["win_rate"] = round(wins / closed, 4) if closed else 0.0
        item["recent_10_pnl"] = round(sum(pnls[-10:]), 6)
        item["realized_usdc_per_day"] = round(float(item.get("realized_pnl_24h_usdc", 0.0) or 0.0), 6)
        item["projected_monthly_revenue_usdc"] = round(float(item["realized_usdc_per_day"]) * 30.0, 6)
        item["projected_yearly_revenue_usdc"] = round(float(item["realized_usdc_per_day"]) * 365.0, 6)
        item["max_drawdown"] = round(float(item.get("max_drawdown", 0.0) or 0.0), 6)

    variants = sorted(variant_rows.values(), key=lambda item: (int(item.get("closed", 0)), float(item.get("net_pnl", 0.0))), reverse=True)
    summary = {
        "enabled_variant_ids": list(enabled_variant_ids),
        "open": sum(int(item.get("open", 0) or 0) for item in variants),
        "closed": sum(int(item.get("closed", 0) or 0) for item in variants),
        "net_pnl": round(sum(float(item.get("net_pnl", 0.0) or 0.0) for item in variants), 6),
        "realized_pnl_24h_usdc": round(sum(float(item.get("realized_pnl_24h_usdc", 0.0) or 0.0) for item in variants), 6),
        "unrealized_pnl_usdc": round(sum(float(item.get("unrealized_pnl_usdc", 0.0) or 0.0) for item in variants), 6),
    }
    summary["realized_usdc_per_day"] = summary["realized_pnl_24h_usdc"]
    summary["projected_monthly_revenue_usdc"] = round(float(summary["realized_usdc_per_day"]) * 30.0, 6)
    summary["projected_yearly_revenue_usdc"] = round(float(summary["realized_usdc_per_day"]) * 365.0, 6)
    return {
        "summary": summary,
        "variants": variants,
    }


def latency_bot_complete_set_arb_stats(settings: LatencyBotSettings) -> dict[str, Any]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    cutoff_ts = cutoff.isoformat().replace("+00:00", "Z")
    with connect_latency_bot_db(settings) as conn:
        signal_rows = conn.execute(
            """
            SELECT ts, market_id, asset, tenor_minutes, yes_ask, no_ask, total_cost,
                   gross_edge, net_edge, executable_depth_usdc, book_age_ms, seconds_left,
                   eligible, reason
            FROM complete_set_arb_signals
            ORDER BY ts DESC, id DESC
            LIMIT 50
            """
        ).fetchall()
        signal_summary_row = conn.execute(
            """
            SELECT
                COUNT(*) AS signals,
                SUM(CASE WHEN eligible THEN 1 ELSE 0 END) AS eligible,
                MAX(net_edge) AS best_net_edge
            FROM complete_set_arb_signals
            WHERE ts >= ?
            """,
            ((datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat().replace("+00:00", "Z"),),
        ).fetchone()
        reason_rows = conn.execute(
            """
            SELECT reason, COUNT(*) AS count, MAX(net_edge) AS max_net_edge
            FROM complete_set_arb_signals
            WHERE ts >= ?
            GROUP BY reason
            ORDER BY count DESC, reason ASC
            """,
            ((datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat().replace("+00:00", "Z"),),
        ).fetchall()
        open_row = conn.execute(
            """
            SELECT COUNT(*) AS open
            FROM complete_set_arb_positions
            WHERE status = 'open'
            """
        ).fetchone()
        close_rows = conn.execute(
            """
            SELECT
                cse.ts,
                csp.position_id,
                csp.market_id,
                csp.asset,
                csp.tenor_minutes,
                csp.yes_entry_price,
                csp.no_entry_price,
                csp.total_cost,
                csp.size,
                csp.net_edge,
                cse.pnl,
                cse.reason
            FROM complete_set_arb_events cse
            JOIN complete_set_arb_positions csp ON csp.position_id = cse.position_id
            WHERE cse.event_type = 'close'
            ORDER BY cse.ts ASC, cse.id ASC
            """
        ).fetchall()
        recent_rows = conn.execute(
            """
            SELECT
                cse.ts,
                csp.position_id,
                csp.market_id,
                csp.asset,
                csp.tenor_minutes,
                csp.yes_entry_price,
                csp.no_entry_price,
                csp.total_cost,
                csp.size,
                csp.net_edge,
                cse.pnl,
                cse.reason
            FROM complete_set_arb_events cse
            JOIN complete_set_arb_positions csp ON csp.position_id = cse.position_id
            WHERE cse.event_type = 'close'
            ORDER BY cse.ts DESC, cse.id DESC
            LIMIT 20
            """
        ).fetchall()

    pnls = [float(row["pnl"] or 0.0) for row in close_rows]
    realized_24h = 0.0
    running = 0.0
    peak = 0.0
    max_drawdown = 0.0
    curve: list[dict[str, Any]] = []
    markets: set[str] = set()
    for row in close_rows:
        pnl = float(row["pnl"] or 0.0)
        running = round(running + pnl, 6)
        peak = max(peak, running)
        max_drawdown = min(max_drawdown, running - peak)
        markets.add(str(row["market_id"] or ""))
        parsed_ts = _storage_parse_ts(str(row["ts"] or ""))
        if parsed_ts is not None and parsed_ts >= cutoff:
            realized_24h = round(realized_24h + pnl, 6)
        curve.append(
            {
                "ts": str(row["ts"] or ""),
                "realized_pnl_usdc": running,
                "unrealized_pnl_usdc": 0.0,
                "equity_usdc": round(settings.bankroll_usdc + running, 6),
            }
        )
    signals = int(signal_summary_row["signals"] or 0) if signal_summary_row else 0
    eligible = int(signal_summary_row["eligible"] or 0) if signal_summary_row else 0
    summary = {
        "enabled": bool(settings.complete_set_arb_enabled),
        "open": int(open_row["open"] or 0) if open_row else 0,
        "closed": len(pnls),
        "unique_markets": len(markets),
        "win_rate": round(sum(1 for pnl in pnls if pnl > 0.0) / len(pnls), 4) if pnls else 0.0,
        "net_pnl": round(sum(pnls), 6),
        "realized_pnl_24h_usdc": round(realized_24h, 6),
        "realized_usdc_per_day": round(realized_24h, 6),
        "projected_monthly_revenue_usdc": round(realized_24h * 30.0, 6),
        "projected_yearly_revenue_usdc": round(realized_24h * 365.0, 6),
        "avg_pnl": round(sum(pnls) / len(pnls), 6) if pnls else 0.0,
        "max_drawdown": round(max_drawdown, 6),
        "signals_60m": signals,
        "eligible_60m": eligible,
        "eligible_rate_60m": round(eligible / signals, 4) if signals else 0.0,
        "best_net_edge_60m": round(float(signal_summary_row["best_net_edge"] or 0.0), 6) if signal_summary_row else 0.0,
        "min_profit_per_share": settings.complete_set_arb_min_profit_per_share,
        "notional_usdc": settings.complete_set_arb_notional_usdc,
        "execution_policy": settings.complete_set_arb_execution_policy,
        "simulated_atomicity": "paired FOK batch; count trade only if both legs can fill",
    }
    return {
        "summary": summary,
        "recent_signals": [dict(row) for row in signal_rows],
        "reason_breakdown": [dict(row) for row in reason_rows],
        "recent_closes": [dict(row) for row in recent_rows],
        "equity_curve": curve[-200:],
    }


def latency_bot_cex_latency_paper_stats(settings: LatencyBotSettings) -> dict[str, Any]:
    cutoff_24h = datetime.now(timezone.utc) - timedelta(hours=24)
    cutoff_24h_ts = cutoff_24h.isoformat().replace("+00:00", "Z")
    cutoff_60m_ts = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat().replace("+00:00", "Z")
    with connect_latency_bot_db(settings) as conn:
        signal_rows = conn.execute(
            """
            SELECT ts, market_id, asset, side, tenor_minutes, signal_type, edge, fair_yes,
                   fair_no, yes_ask, no_ask, order_price, min_depth_usdc, book_age_ms,
                   seconds_left, eligible, reason
            FROM cex_latency_paper_signals
            ORDER BY ts DESC, id DESC
            LIMIT 50
            """
        ).fetchall()
        signal_summary_row = conn.execute(
            """
            SELECT
                COUNT(*) AS signals,
                SUM(CASE WHEN eligible THEN 1 ELSE 0 END) AS eligible,
                MAX(edge) AS best_edge
            FROM cex_latency_paper_signals
            WHERE ts >= ?
            """,
            (cutoff_60m_ts,),
        ).fetchone()
        reason_rows = conn.execute(
            """
            SELECT reason, COUNT(*) AS count, MAX(edge) AS max_edge
            FROM cex_latency_paper_signals
            WHERE ts >= ?
            GROUP BY reason
            ORDER BY count DESC, reason ASC
            LIMIT 30
            """,
            (cutoff_60m_ts,),
        ).fetchall()
        threshold_candidate_rows = conn.execute(
            """
            SELECT ts, market_id, asset, side, edge, order_price, min_depth_usdc, seconds_left
            FROM cex_latency_paper_signals
            WHERE ts >= ?
              AND edge > 0
              AND (eligible = 1 OR blocked_reason IN ('edge', ''))
            ORDER BY ts DESC, id DESC
            """,
            (cutoff_24h_ts,),
        ).fetchall()
        open_rows = conn.execute(
            """
            SELECT position_id, market_id, asset, side, entry_ts, entry_price, size,
                   notional_usdc, entry_edge, entry_seconds_left, entry_reason
            FROM cex_latency_paper_positions
            WHERE status = 'open'
            ORDER BY entry_ts ASC
            """
        ).fetchall()
        close_rows = conn.execute(
            """
            SELECT
                e.ts,
                p.position_id,
                p.market_id,
                p.asset,
                p.side,
                p.entry_price,
                e.mark AS exit_price,
                p.size,
                p.notional_usdc,
                p.entry_edge,
                e.edge AS exit_edge,
                e.pnl,
                e.reason
            FROM cex_latency_paper_events e
            JOIN cex_latency_paper_positions p ON p.position_id = e.position_id
            WHERE e.event_type = 'close'
            ORDER BY e.ts ASC, e.id ASC
            """
        ).fetchall()
        recent_close_rows = conn.execute(
            """
            SELECT
                e.ts,
                p.position_id,
                p.market_id,
                p.asset,
                p.side,
                p.entry_price,
                e.mark AS exit_price,
                p.size,
                p.notional_usdc,
                p.entry_edge,
                e.edge AS exit_edge,
                e.pnl,
                e.reason
            FROM cex_latency_paper_events e
            JOIN cex_latency_paper_positions p ON p.position_id = e.position_id
            WHERE e.event_type = 'close'
            ORDER BY e.ts DESC, e.id DESC
            LIMIT 30
            """
        ).fetchall()
        closed_base = """
            FROM cex_latency_paper_events e
            JOIN cex_latency_paper_positions p ON p.position_id = e.position_id
            WHERE e.event_type = 'close'
        """
        pnl_by_asset_side_rows = conn.execute(
            f"""
            SELECT
                p.asset,
                p.side,
                COUNT(*) AS closed,
                SUM(CASE WHEN e.pnl > 0 THEN 1 ELSE 0 END) AS wins,
                SUM(e.pnl) AS net_pnl,
                AVG(e.pnl) AS avg_pnl,
                AVG(p.entry_edge) AS avg_edge,
                AVG(p.entry_price) AS avg_entry
            {closed_base}
            GROUP BY p.asset, p.side
            ORDER BY net_pnl ASC
            """
        ).fetchall()
        pnl_by_exit_reason_rows = conn.execute(
            f"""
            SELECT
                e.reason,
                COUNT(*) AS closed,
                SUM(CASE WHEN e.pnl > 0 THEN 1 ELSE 0 END) AS wins,
                SUM(e.pnl) AS net_pnl,
                AVG(e.pnl) AS avg_pnl
            {closed_base}
            GROUP BY e.reason
            ORDER BY net_pnl ASC
            """
        ).fetchall()
        pnl_by_price_band_rows = conn.execute(
            f"""
            SELECT
                CASE
                    WHEN p.entry_price < 0.25 THEN '<0.25'
                    WHEN p.entry_price < 0.35 THEN '0.25-0.35'
                    WHEN p.entry_price < 0.45 THEN '0.35-0.45'
                    WHEN p.entry_price < 0.55 THEN '0.45-0.55'
                    WHEN p.entry_price < 0.70 THEN '0.55-0.70'
                    ELSE '>=0.70'
                END AS band,
                COUNT(*) AS closed,
                SUM(CASE WHEN e.pnl > 0 THEN 1 ELSE 0 END) AS wins,
                SUM(e.pnl) AS net_pnl,
                AVG(e.pnl) AS avg_pnl,
                AVG(p.entry_edge) AS avg_edge
            {closed_base}
            GROUP BY band
            ORDER BY MIN(p.entry_price) ASC
            """
        ).fetchall()
        pnl_by_edge_band_rows = conn.execute(
            f"""
            SELECT
                CASE
                    WHEN p.entry_edge < 0.005 THEN '<0.005'
                    WHEN p.entry_edge < 0.01 THEN '0.005-0.01'
                    WHEN p.entry_edge < 0.02 THEN '0.01-0.02'
                    WHEN p.entry_edge < 0.05 THEN '0.02-0.05'
                    WHEN p.entry_edge < 0.09 THEN '0.05-0.09'
                    ELSE '>=0.09'
                END AS band,
                COUNT(*) AS closed,
                SUM(CASE WHEN e.pnl > 0 THEN 1 ELSE 0 END) AS wins,
                SUM(e.pnl) AS net_pnl,
                AVG(e.pnl) AS avg_pnl,
                AVG(p.entry_price) AS avg_entry
            {closed_base}
            GROUP BY band
            ORDER BY MIN(p.entry_edge) ASC
            """
        ).fetchall()

    pnls = [float(row["pnl"] or 0.0) for row in close_rows]
    running = 0.0
    peak = 0.0
    max_drawdown = 0.0
    realized_24h = 0.0
    curve: list[dict[str, Any]] = []
    markets: set[str] = set()
    for row in close_rows:
        pnl = float(row["pnl"] or 0.0)
        running = round(running + pnl, 6)
        peak = max(peak, running)
        max_drawdown = min(max_drawdown, running - peak)
        markets.add(str(row["market_id"] or ""))
        parsed_ts = _storage_parse_ts(str(row["ts"] or ""))
        if parsed_ts is not None and parsed_ts >= cutoff_24h:
            realized_24h = round(realized_24h + pnl, 6)
        curve.append(
            {
                "ts": str(row["ts"] or ""),
                "realized_pnl_usdc": running,
                "unrealized_pnl_usdc": 0.0,
                "equity_usdc": round(float(settings.cex_latency_paper_capital_usdc) + running, 6),
            }
        )

    signals = int(signal_summary_row["signals"] or 0) if signal_summary_row else 0
    eligible = int(signal_summary_row["eligible"] or 0) if signal_summary_row else 0
    open_capital = round(sum(float(row["notional_usdc"] or 0.0) for row in open_rows), 6)
    closed = len(pnls)
    wins = sum(1 for pnl in pnls if pnl > 0.0)
    capital = max(float(settings.cex_latency_paper_capital_usdc), 0.0)
    edge_floors = [0.005, 0.01, 0.02, 0.03, 0.05, 0.07, 0.09, 0.12, 0.15]
    target_notional = max(float(settings.cex_latency_paper_notional_usdc), 0.0)
    threshold_sensitivity: list[dict[str, Any]] = []
    threshold_candidates = [dict(row) for row in threshold_candidate_rows]
    for floor in edge_floors:
        raw_matches = [row for row in threshold_candidates if float(row.get("edge") or 0.0) >= floor]
        best_by_market: dict[str, dict[str, Any]] = {}
        for row in raw_matches:
            market_key = str(row.get("market_id") or "")
            current = best_by_market.get(market_key)
            if current is None or float(row.get("edge") or 0.0) > float(current.get("edge") or 0.0):
                best_by_market[market_key] = row
        unique_matches = list(best_by_market.values())
        indicative_edge = 0.0
        for row in unique_matches:
            price = float(row.get("order_price") or 0.0)
            edge = float(row.get("edge") or 0.0)
            if price > 0.0 and target_notional > 0.0:
                indicative_edge += (target_notional / price) * edge
        threshold_sensitivity.append(
            {
                "edge_floor": round(floor, 6),
                "raw_ticks": len(raw_matches),
                "unique_markets": len(unique_matches),
                "best_edge": round(max((float(row.get("edge") or 0.0) for row in raw_matches), default=0.0), 6),
                "avg_best_edge": round(
                    sum(float(row.get("edge") or 0.0) for row in unique_matches) / len(unique_matches),
                    6,
                )
                if unique_matches
                else 0.0,
                "avg_entry": round(
                    sum(float(row.get("order_price") or 0.0) for row in unique_matches) / len(unique_matches),
                    6,
                )
                if unique_matches
                else 0.0,
                "indicative_edge_usdc": round(indicative_edge, 6),
            }
        )
    summary = {
        "mode": "Polymarket API CEX-latency directional paper bot",
        "execution_pricing": "CLOB book-level VWAP at target notional when available",
        "data_sources": "Gamma market discovery, Polymarket CLOB books, Binance REST bookTicker",
        "enabled": bool(settings.cex_latency_paper_enabled),
        "starting_capital_usdc": round(capital, 6),
        "equity_usdc": round(capital + sum(pnls), 6),
        "target_notional_usdc": round(float(settings.cex_latency_paper_notional_usdc), 6),
        "open": len(open_rows),
        "closed": closed,
        "unique_markets": len(markets),
        "win_rate": round(wins / closed, 4) if closed else 0.0,
        "wins": wins,
        "losses": closed - wins,
        "net_pnl": round(sum(pnls), 6),
        "realized_pnl_24h_usdc": round(realized_24h, 6),
        "projected_monthly_revenue_usdc": round(realized_24h * 30.0, 6),
        "projected_yearly_revenue_usdc": round(realized_24h * 365.0, 6),
        "avg_pnl": round(sum(pnls) / closed, 6) if closed else 0.0,
        "max_drawdown": round(max_drawdown, 6),
        "current_capital_in_use_usdc": open_capital,
        "current_capital_fraction": round(open_capital / capital, 6) if capital else 0.0,
        "signals_60m": signals,
        "eligible_60m": eligible,
        "eligible_rate_60m": round(eligible / signals, 4) if signals else 0.0,
        "best_edge_60m": round(float(signal_summary_row["best_edge"] or 0.0), 6) if signal_summary_row else 0.0,
        "min_edge_per_share": round(float(settings.cex_latency_paper_min_edge_per_share), 6),
        "min_depth_usdc": round(float(settings.cex_latency_paper_min_depth_usdc), 6),
        "max_open_positions": int(settings.cex_latency_paper_max_open_positions),
        "assets": list(settings.cex_latency_paper_assets),
        "model": settings.cex_latency_paper_model,
        "take_profit_fraction": round(float(settings.cex_latency_paper_take_profit_fraction), 6),
        "stop_loss_fraction": round(float(settings.cex_latency_paper_stop_loss_fraction), 6),
        "exit_edge_floor": round(float(settings.cex_latency_paper_exit_edge_floor), 6),
        "force_exit_seconds": int(settings.cex_latency_paper_force_exit_seconds),
    }
    return {
        "summary": summary,
        "recent_signals": [dict(row) for row in signal_rows],
        "reason_breakdown": [dict(row) for row in reason_rows],
        "open_positions": [dict(row) for row in open_rows],
        "recent_closes": [dict(row) for row in recent_close_rows],
        "equity_curve": curve[-200:],
        "threshold_sensitivity": threshold_sensitivity,
        "pnl_by_asset_side": [dict(row) for row in pnl_by_asset_side_rows],
        "pnl_by_exit_reason": [dict(row) for row in pnl_by_exit_reason_rows],
        "pnl_by_price_band": [dict(row) for row in pnl_by_price_band_rows],
        "pnl_by_edge_band": [dict(row) for row in pnl_by_edge_band_rows],
    }


def latency_bot_wallet_teacher_sniper_stats(settings: LatencyBotSettings) -> dict[str, Any]:
    mode = "wallet_teacher_sniper"
    cutoff_24h = datetime.now(timezone.utc) - timedelta(hours=24)
    cutoff_24h_ts = cutoff_24h.isoformat().replace("+00:00", "Z")
    cutoff_60m_ts = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat().replace("+00:00", "Z")
    capital = max(float(settings.wallet_teacher_sniper_capital_usdc), 0.0)
    with connect_latency_bot_db(settings) as conn:
        signal_rows = conn.execute(
            """
            SELECT ts, market_id, asset, side, tenor_minutes, signal_type, edge, fair_yes,
                   fair_no, yes_ask, no_ask, order_price, min_depth_usdc, book_age_ms,
                   seconds_left, eligible, reason
            FROM cex_latency_paper_signals
            WHERE mode = ?
            ORDER BY ts DESC, id DESC
            LIMIT 50
            """,
            (mode,),
        ).fetchall()
        signal_summary_row = conn.execute(
            """
            SELECT
                COUNT(*) AS signals,
                SUM(CASE WHEN eligible THEN 1 ELSE 0 END) AS eligible,
                MAX(edge) AS best_edge
            FROM cex_latency_paper_signals
            WHERE mode = ?
              AND ts >= ?
            """,
            (mode, cutoff_60m_ts),
        ).fetchone()
        reason_rows = conn.execute(
            """
            SELECT reason, COUNT(*) AS count, MAX(edge) AS max_edge
            FROM cex_latency_paper_signals
            WHERE mode = ?
              AND ts >= ?
            GROUP BY reason
            ORDER BY count DESC, reason ASC
            LIMIT 30
            """,
            (mode, cutoff_60m_ts),
        ).fetchall()
        open_rows = conn.execute(
            """
            SELECT position_id, market_id, asset, side, entry_ts, entry_price, size,
                   notional_usdc, entry_edge, entry_seconds_left, entry_reason
            FROM cex_latency_paper_positions
            WHERE status = 'open'
              AND mode = ?
            ORDER BY entry_ts ASC
            """,
            (mode,),
        ).fetchall()
        close_rows = conn.execute(
            """
            SELECT
                e.ts,
                p.position_id,
                p.market_id,
                p.asset,
                p.side,
                p.entry_price,
                e.mark AS exit_price,
                p.size,
                p.notional_usdc,
                p.entry_edge,
                e.edge AS exit_edge,
                e.pnl,
                e.reason
            FROM cex_latency_paper_events e
            JOIN cex_latency_paper_positions p ON p.position_id = e.position_id
            WHERE e.event_type = 'close'
              AND p.mode = ?
            ORDER BY e.ts ASC, e.id ASC
            """,
            (mode,),
        ).fetchall()
        recent_close_rows = conn.execute(
            """
            SELECT
                e.ts,
                p.position_id,
                p.market_id,
                p.asset,
                p.side,
                p.entry_price,
                e.mark AS exit_price,
                p.size,
                p.notional_usdc,
                p.entry_edge,
                e.edge AS exit_edge,
                e.pnl,
                e.reason
            FROM cex_latency_paper_events e
            JOIN cex_latency_paper_positions p ON p.position_id = e.position_id
            WHERE e.event_type = 'close'
              AND p.mode = ?
            ORDER BY e.ts DESC, e.id DESC
            LIMIT 30
            """,
            (mode,),
        ).fetchall()

    pnls = [float(row["pnl"] or 0.0) for row in close_rows]
    running = 0.0
    peak = 0.0
    max_drawdown = 0.0
    realized_24h = 0.0
    curve: list[dict[str, Any]] = []
    markets: set[str] = set()
    for row in close_rows:
        pnl = float(row["pnl"] or 0.0)
        running = round(running + pnl, 6)
        peak = max(peak, running)
        max_drawdown = min(max_drawdown, running - peak)
        markets.add(str(row["market_id"] or ""))
        parsed_ts = _storage_parse_ts(str(row["ts"] or ""))
        if parsed_ts is not None and parsed_ts >= cutoff_24h:
            realized_24h = round(realized_24h + pnl, 6)
        curve.append(
            {
                "ts": str(row["ts"] or ""),
                "realized_pnl_usdc": running,
                "unrealized_pnl_usdc": 0.0,
                "equity_usdc": round(capital + running, 6),
            }
        )

    signals = int(signal_summary_row["signals"] or 0) if signal_summary_row else 0
    eligible = int(signal_summary_row["eligible"] or 0) if signal_summary_row else 0
    open_capital = round(sum(float(row["notional_usdc"] or 0.0) for row in open_rows), 6)
    closed = len(pnls)
    wins = sum(1 for pnl in pnls if pnl > 0.0)
    summary = {
        "mode": "Target-wallet teacher 5m sniper paper bot",
        "execution_pricing": "CLOB book-level VWAP at target notional when available",
        "data_sources": "Public wallet trades, Gamma market discovery, Polymarket CLOB books",
        "enabled": bool(settings.wallet_teacher_sniper_enabled),
        "target_wallet": str(settings.wallet_teacher_sniper_wallet or ""),
        "starting_capital_usdc": round(capital, 6),
        "equity_usdc": round(capital + sum(pnls), 6),
        "target_notional_usdc": round(float(settings.wallet_teacher_sniper_notional_usdc), 6),
        "open": len(open_rows),
        "closed": closed,
        "unique_markets": len(markets),
        "win_rate": round(wins / closed, 4) if closed else 0.0,
        "wins": wins,
        "losses": closed - wins,
        "net_pnl": round(sum(pnls), 6),
        "realized_pnl_24h_usdc": round(realized_24h, 6),
        "projected_monthly_revenue_usdc": round(realized_24h * 30.0, 6),
        "projected_yearly_revenue_usdc": round(realized_24h * 365.0, 6),
        "avg_pnl": round(sum(pnls) / closed, 6) if closed else 0.0,
        "max_drawdown": round(max_drawdown, 6),
        "current_capital_in_use_usdc": open_capital,
        "current_capital_fraction": round(open_capital / capital, 6) if capital else 0.0,
        "signals_60m": signals,
        "eligible_60m": eligible,
        "eligible_rate_60m": round(eligible / signals, 4) if signals else 0.0,
        "best_edge_60m": round(float(signal_summary_row["best_edge"] or 0.0), 6) if signal_summary_row else 0.0,
        "teacher_trade_lookback_seconds": int(settings.wallet_teacher_sniper_trade_lookback_seconds),
        "min_teacher_notional_usdc": round(float(settings.wallet_teacher_sniper_min_teacher_notional_usdc), 6),
        "min_depth_usdc": round(float(settings.wallet_teacher_sniper_min_depth_usdc), 6),
        "max_open_positions": int(settings.wallet_teacher_sniper_max_open_positions),
        "assets": list(settings.wallet_teacher_sniper_assets),
        "take_profit_fraction": round(float(settings.wallet_teacher_sniper_take_profit_fraction), 6),
        "stop_loss_fraction": round(float(settings.wallet_teacher_sniper_stop_loss_fraction), 6),
        "exit_edge_floor": round(float(settings.wallet_teacher_sniper_exit_edge_floor), 6),
        "force_exit_seconds": int(settings.wallet_teacher_sniper_force_exit_seconds),
    }
    return {
        "summary": summary,
        "recent_signals": [dict(row) for row in signal_rows],
        "reason_breakdown": [dict(row) for row in reason_rows],
        "open_positions": [dict(row) for row in open_rows],
        "recent_closes": [dict(row) for row in recent_close_rows],
        "equity_curve": curve[-200:],
    }


def latency_bot_realistic_complete_set_arb_sim(settings: LatencyBotSettings) -> dict[str, Any]:
    lookback_hours = max(int(settings.realistic_complete_set_arb_lookback_hours), 1)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    cutoff_ts = cutoff.isoformat().replace("+00:00", "Z")
    recent_cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    recent_signal_cutoff = datetime.now(timezone.utc) - timedelta(minutes=60)
    capital_usdc = max(float(settings.realistic_complete_set_arb_capital_usdc), 0.0)
    target_notional = max(float(settings.complete_set_arb_notional_usdc), 0.0)
    min_notional = max(float(settings.complete_set_arb_min_depth_usdc), 0.0)
    min_edge = float(settings.complete_set_arb_min_profit_per_share)
    depth_haircut = min(max(float(settings.realistic_complete_set_arb_depth_haircut), 0.0), 1.0)
    latency_seconds = max(float(settings.realistic_complete_set_arb_latency_ms), 0.0) / 1000.0
    latency_decay = latency_seconds * max(float(settings.realistic_complete_set_arb_latency_edge_decay_per_second), 0.0)
    extra_slippage = max(float(settings.realistic_complete_set_arb_extra_slippage_per_share), 0.0)
    partial_fill_fraction = min(max(float(settings.realistic_complete_set_arb_partial_fill_fraction), 0.0), 1.0)
    failed_leg_loss_fraction = max(float(settings.realistic_complete_set_arb_failed_leg_loss_fraction), 0.0)
    operational_failure_rate = min(max(float(settings.realistic_complete_set_arb_operational_failure_rate), 0.0), 1.0)
    redeem_lag_seconds = max(int(settings.realistic_complete_set_arb_redeem_lag_seconds), 0)
    cooldown_seconds = max(int(settings.complete_set_arb_same_market_cooldown_seconds), 0)

    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT ts, market_id, asset, tenor_minutes, yes_ask, no_ask, total_cost,
                   gross_edge, net_edge, executable_depth_usdc, book_age_ms, seconds_left,
                   eligible, reason
            FROM complete_set_arb_signals
            WHERE ts >= ?
            ORDER BY ts ASC, id ASC
            """,
            (cutoff_ts,),
        ).fetchall()

    locked: list[tuple[datetime, float, str, str]] = []
    last_market_entry: dict[str, datetime] = {}
    reasons: dict[str, dict[str, Any]] = {}
    recent_events: list[dict[str, Any]] = []
    curve: list[dict[str, Any]] = []
    locked_sets = 0
    paired_failures = 0
    capital_blocked = 0
    depth_latency_blocked = 0
    original_skips = 0
    signals_60m = 0
    eligible_60m = 0
    best_adjusted_edge_60m = 0.0
    current_locked_capital = 0.0
    peak_locked_capital = 0.0
    running_pnl = 0.0
    realized_24h = 0.0
    peak_pnl = 0.0
    max_drawdown = 0.0
    unique_markets: set[str] = set()
    first_tick_ts = ""
    last_tick_ts = ""
    operational_expected_loss_total = 0.0

    def add_reason(reason: str, *, edge: float = 0.0) -> None:
        item = reasons.setdefault(reason, {"reason": reason, "count": 0, "max_adjusted_edge": 0.0})
        item["count"] = int(item["count"]) + 1
        item["max_adjusted_edge"] = max(float(item["max_adjusted_edge"]), float(edge))

    def add_event(row: sqlite3.Row, decision: str, reason: str, *, adjusted_edge: float, notional: float, pnl: float, locked_capital: float) -> None:
        recent_events.append(
            {
                "ts": str(row["ts"] or ""),
                "market_id": str(row["market_id"] or ""),
                "asset": str(row["asset"] or ""),
                "decision": decision,
                "reason": reason,
                "net_edge": float(row["net_edge"] or 0.0),
                "adjusted_edge": round(adjusted_edge, 6),
                "depth": float(row["executable_depth_usdc"] or 0.0),
                "effective_depth": round(float(row["executable_depth_usdc"] or 0.0) * depth_haircut, 6),
                "notional_usdc": round(notional, 6),
                "pnl": round(pnl, 6),
                "locked_capital_usdc": round(locked_capital, 6),
            }
        )
        if len(recent_events) > 60:
            del recent_events[: len(recent_events) - 60]

    def release_capital(ts: datetime) -> None:
        nonlocal current_locked_capital
        if not locked:
            return
        kept: list[tuple[datetime, float]] = []
        released = 0.0
        for release_ts, amount in locked:
            if release_ts <= ts:
                released += amount
            else:
                kept.append((release_ts, amount))
        if released:
            current_locked_capital = max(0.0, current_locked_capital - released)
        locked[:] = kept

    for row in rows:
        parsed_ts = _storage_parse_ts(str(row["ts"] or ""))
        if parsed_ts is None:
            continue
        if not first_tick_ts:
            first_tick_ts = str(row["ts"] or "")
        last_tick_ts = str(row["ts"] or "")
        release_capital(parsed_ts)
        if parsed_ts >= recent_signal_cutoff:
            signals_60m += 1

        net_edge = float(row["net_edge"] or 0.0)
        book_age_ms = max(float(row["book_age_ms"] or 0.0), 0.0)
        stale_penalty = max(book_age_ms - float(settings.complete_set_arb_max_book_age_ms), 0.0) / 1000.0 * 0.001
        adjusted_edge = net_edge - extra_slippage - latency_decay - stale_penalty
        if parsed_ts >= recent_signal_cutoff:
            best_adjusted_edge_60m = max(best_adjusted_edge_60m, adjusted_edge)

        if not settings.realistic_complete_set_arb_enabled:
            reason = "realistic arb simulation disabled"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "SKIP", reason, adjusted_edge=adjusted_edge, notional=0.0, pnl=0.0, locked_capital=current_locked_capital)
            continue

        if not bool(row["eligible"]):
            original_skips += 1
            reason = f"original skip: {str(row['reason'] or 'unknown')}"
            add_reason(reason, edge=adjusted_edge)
            continue

        if parsed_ts >= recent_signal_cutoff:
            eligible_60m += 1

        market_id = str(row["market_id"] or "")
        last_entry = last_market_entry.get(market_id)
        if last_entry is not None and cooldown_seconds > 0 and (parsed_ts - last_entry).total_seconds() < cooldown_seconds:
            reason = "same-market cooldown"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "SKIP", reason, adjusted_edge=adjusted_edge, notional=0.0, pnl=0.0, locked_capital=current_locked_capital)
            continue

        effective_depth = float(row["executable_depth_usdc"] or 0.0) * depth_haircut
        if target_notional <= 0.0:
            reason = "target notional is zero"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "SKIP", reason, adjusted_edge=adjusted_edge, notional=0.0, pnl=0.0, locked_capital=current_locked_capital)
            continue
        if effective_depth < min_notional:
            depth_latency_blocked += 1
            reason = "effective paired depth below minimum after haircut"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "DEPTH_BLOCK", reason, adjusted_edge=adjusted_edge, notional=min_notional, pnl=0.0, locked_capital=current_locked_capital)
            continue

        available_capital = capital_usdc - current_locked_capital
        if available_capital < min_notional:
            capital_blocked += 1
            reason = "capital locked until expiry/redeem"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "CAPITAL_BLOCK", reason, adjusted_edge=adjusted_edge, notional=min_notional, pnl=0.0, locked_capital=current_locked_capital)
            continue

        if adjusted_edge < min_edge:
            depth_latency_blocked += 1
            reason = "latency/slippage adjusted edge below threshold"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "LATENCY_SKIP", reason, adjusted_edge=adjusted_edge, notional=target_notional, pnl=0.0, locked_capital=current_locked_capital)
            continue

        notional = min(target_notional, effective_depth, available_capital)
        total_cost = max(float(row["total_cost"] or 0.0), 0.0001)
        shares = notional / total_cost
        gross_pnl = adjusted_edge * shares
        operational_expected_loss = notional * operational_failure_rate * failed_leg_loss_fraction
        operational_expected_loss_total = round(operational_expected_loss_total + operational_expected_loss, 6)
        pnl = round(gross_pnl - operational_expected_loss, 6)
        running_pnl = round(running_pnl + pnl, 6)
        peak_pnl = max(peak_pnl, running_pnl)
        max_drawdown = min(max_drawdown, running_pnl - peak_pnl)
        if parsed_ts >= recent_cutoff:
            realized_24h = round(realized_24h + pnl, 6)
        seconds_left = max(float(row["seconds_left"] or 0.0), 0.0)
        release_ts = parsed_ts + timedelta(seconds=seconds_left + redeem_lag_seconds)
        locked.append((release_ts, notional))
        current_locked_capital = round(current_locked_capital + notional, 6)
        peak_locked_capital = max(peak_locked_capital, current_locked_capital)
        last_market_entry[market_id] = parsed_ts
        unique_markets.add(market_id)
        locked_sets += 1
        reason = "locked set after realistic haircuts"
        add_reason(reason, edge=adjusted_edge)
        add_event(row, "LOCKED_SET", reason, adjusted_edge=adjusted_edge, notional=notional, pnl=pnl, locked_capital=current_locked_capital)
        curve.append(
            {
                "ts": str(row["ts"] or ""),
                "realized_pnl_usdc": running_pnl,
                "unrealized_pnl_usdc": 0.0,
                "equity_usdc": round(capital_usdc + running_pnl, 6),
            }
        )

    now = datetime.now(timezone.utc)
    release_capital(now)
    summary = {
        "mode": "Tick-replay realistic complete-set arb simulation",
        "enabled": bool(settings.realistic_complete_set_arb_enabled),
        "tick_poll_source": "complete_set_arb_signals table; one row per market per engine cycle",
        "lookback_hours": lookback_hours,
        "ticks_replayed": len(rows),
        "first_tick_ts": first_tick_ts,
        "last_tick_ts": last_tick_ts,
        "simulated_capital_usdc": round(capital_usdc, 6),
        "target_notional_usdc": round(target_notional, 6),
        "min_notional_usdc": round(min_notional, 6),
        "current_locked_capital_usdc": round(current_locked_capital, 6),
        "peak_locked_capital_usdc": round(peak_locked_capital, 6),
        "peak_capital_fraction": round(peak_locked_capital / capital_usdc, 6) if capital_usdc else 0.0,
        "locked_sets": locked_sets,
        "unique_markets": len(unique_markets),
        "paired_fill_failures": paired_failures,
        "expected_operational_loss_usdc": round(operational_expected_loss_total, 6),
        "capital_blocked": capital_blocked,
        "depth_latency_blocked": depth_latency_blocked,
        "original_skips": original_skips,
        "net_pnl": round(running_pnl, 6),
        "realized_pnl_24h_usdc": round(realized_24h, 6),
        "projected_monthly_revenue_usdc": round(realized_24h * 30.0, 6),
        "projected_yearly_revenue_usdc": round(realized_24h * 365.0, 6),
        "avg_pnl_per_locked_set": round(running_pnl / locked_sets, 6) if locked_sets else 0.0,
        "max_drawdown": round(max_drawdown, 6),
        "signals_60m": signals_60m,
        "eligible_60m": eligible_60m,
        "eligible_rate_60m": round(eligible_60m / signals_60m, 6) if signals_60m else 0.0,
        "best_adjusted_edge_60m": round(best_adjusted_edge_60m, 6),
        "min_edge_per_share": min_edge,
        "latency_ms": round(float(settings.realistic_complete_set_arb_latency_ms), 6),
        "depth_haircut": round(depth_haircut, 6),
        "extra_slippage_per_share": round(extra_slippage, 6),
        "latency_decay_per_share": round(latency_decay, 6),
        "partial_fill_fraction": round(partial_fill_fraction, 6),
        "failed_leg_loss_fraction": round(failed_leg_loss_fraction, 6),
        "operational_failure_rate": round(operational_failure_rate, 6),
        "redeem_lag_seconds": redeem_lag_seconds,
    }
    reason_rows = sorted(reasons.values(), key=lambda item: (-int(item["count"]), str(item["reason"])))[:40]
    return {
        "summary": summary,
        "recent_events": list(reversed(recent_events[-40:])),
        "reason_breakdown": reason_rows,
        "equity_curve": curve[-200:],
    }


def latency_bot_btc_fair_value_paper_stats(settings: LatencyBotSettings) -> dict[str, Any]:
    mode = "btc_fair_value_paper"
    cutoff_24h = datetime.now(timezone.utc) - timedelta(hours=24)
    cutoff_24h_ts = cutoff_24h.isoformat().replace("+00:00", "Z")
    cutoff_60m_ts = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat().replace("+00:00", "Z")
    capital = max(float(settings.btc_fair_value_paper_capital_usdc), 0.0)
    with connect_latency_bot_db(settings) as conn:
        signal_rows = conn.execute(
            """
            SELECT ts, market_id, asset, side, tenor_minutes, signal_type, edge, fair_yes,
                   fair_no, yes_ask, no_ask, order_price, min_depth_usdc, book_age_ms,
                   seconds_left, eligible, reason
            FROM cex_latency_paper_signals
            WHERE mode = ?
            ORDER BY ts DESC, id DESC
            LIMIT 50
            """,
            (mode,),
        ).fetchall()
        signal_summary_row = conn.execute(
            """
            SELECT
                COUNT(*) AS signals,
                SUM(CASE WHEN eligible THEN 1 ELSE 0 END) AS eligible,
                MAX(edge) AS best_edge
            FROM cex_latency_paper_signals
            WHERE mode = ? AND ts >= ?
            """,
            (mode, cutoff_60m_ts),
        ).fetchone()
        reason_rows = conn.execute(
            """
            SELECT reason, COUNT(*) AS count, MAX(edge) AS max_edge
            FROM cex_latency_paper_signals
            WHERE mode = ? AND ts >= ?
            GROUP BY reason
            ORDER BY count DESC, reason ASC
            LIMIT 30
            """,
            (mode, cutoff_60m_ts),
        ).fetchall()
        open_rows = conn.execute(
            """
            SELECT position_id, market_id, asset, side, entry_ts, entry_price, size,
                   notional_usdc, entry_edge, entry_seconds_left, entry_reason
            FROM cex_latency_paper_positions
            WHERE status = 'open' AND mode = ?
            ORDER BY entry_ts ASC
            """,
            (mode,),
        ).fetchall()
        close_rows = conn.execute(
            """
            SELECT
                e.ts,
                p.position_id,
                p.market_id,
                p.asset,
                p.side,
                p.entry_price,
                e.mark AS exit_price,
                p.size,
                p.notional_usdc,
                p.entry_edge,
                e.edge AS exit_edge,
                e.pnl,
                e.reason
            FROM cex_latency_paper_events e
            JOIN cex_latency_paper_positions p ON p.position_id = e.position_id
            WHERE e.event_type = 'close' AND p.mode = ?
            ORDER BY e.ts ASC, e.id ASC
            """,
            (mode,),
        ).fetchall()
        recent_close_rows = conn.execute(
            """
            SELECT
                e.ts,
                p.position_id,
                p.market_id,
                p.asset,
                p.side,
                p.entry_price,
                e.mark AS exit_price,
                p.size,
                p.notional_usdc,
                p.entry_edge,
                e.edge AS exit_edge,
                e.pnl,
                e.reason
            FROM cex_latency_paper_events e
            JOIN cex_latency_paper_positions p ON p.position_id = e.position_id
            WHERE e.event_type = 'close' AND p.mode = ?
            ORDER BY e.ts DESC, e.id DESC
            LIMIT 30
            """,
            (mode,),
        ).fetchall()

    pnls = [float(row["pnl"] or 0.0) for row in close_rows]
    running = 0.0
    peak = 0.0
    max_drawdown = 0.0
    realized_24h = 0.0
    curve: list[dict[str, Any]] = []
    markets: set[str] = set()
    for row in close_rows:
        pnl = float(row["pnl"] or 0.0)
        running = round(running + pnl, 6)
        peak = max(peak, running)
        max_drawdown = min(max_drawdown, running - peak)
        markets.add(str(row["market_id"] or ""))
        parsed_ts = _storage_parse_ts(str(row["ts"] or ""))
        if parsed_ts is not None and parsed_ts >= cutoff_24h:
            realized_24h = round(realized_24h + pnl, 6)
        curve.append(
            {
                "ts": str(row["ts"] or ""),
                "realized_pnl_usdc": running,
                "unrealized_pnl_usdc": 0.0,
                "equity_usdc": round(capital + running, 6),
            }
        )

    signals = int(signal_summary_row["signals"] or 0) if signal_summary_row else 0
    eligible = int(signal_summary_row["eligible"] or 0) if signal_summary_row else 0
    open_capital = round(sum(float(row["notional_usdc"] or 0.0) for row in open_rows), 6)
    closed = len(pnls)
    wins = sum(1 for pnl in pnls if pnl > 0.0)
    summary = {
        "mode": "BTC fair-value directional paper bot",
        "execution_pricing": "single-leg CLOB book-level VWAP; no YES+NO atomic assumption",
        "data_sources": "Polymarket CLOB books, Binance-derived fair values, order-book microprice/imbalance",
        "enabled": bool(settings.btc_fair_value_paper_enabled),
        "starting_capital_usdc": round(capital, 6),
        "equity_usdc": round(capital + sum(pnls), 6),
        "target_notional_usdc": round(float(settings.btc_fair_value_paper_notional_usdc), 6),
        "open": len(open_rows),
        "closed": closed,
        "unique_markets": len(markets),
        "win_rate": round(wins / closed, 4) if closed else 0.0,
        "wins": wins,
        "losses": closed - wins,
        "net_pnl": round(sum(pnls), 6),
        "realized_pnl_24h_usdc": round(realized_24h, 6),
        "projected_monthly_revenue_usdc": round(realized_24h * 30.0, 6),
        "projected_yearly_revenue_usdc": round(realized_24h * 365.0, 6),
        "avg_pnl": round(sum(pnls) / closed, 6) if closed else 0.0,
        "max_drawdown": round(max_drawdown, 6),
        "current_capital_in_use_usdc": open_capital,
        "current_capital_fraction": round(open_capital / capital, 6) if capital else 0.0,
        "signals_60m": signals,
        "eligible_60m": eligible,
        "eligible_rate_60m": round(eligible / signals, 4) if signals else 0.0,
        "best_edge_60m": round(float(signal_summary_row["best_edge"] or 0.0), 6) if signal_summary_row else 0.0,
        "min_edge_per_share": round(float(settings.btc_fair_value_paper_min_edge_per_share), 6),
        "min_depth_usdc": round(float(settings.btc_fair_value_paper_min_depth_usdc), 6),
        "max_open_positions": int(settings.btc_fair_value_paper_max_open_positions),
        "assets": list(settings.btc_fair_value_paper_assets),
        "market_weight": round(float(settings.btc_fair_value_paper_market_weight), 6),
        "microprice_weight": round(float(settings.btc_fair_value_paper_microprice_weight), 6),
        "binance_weight": round(float(settings.btc_fair_value_paper_binance_weight), 6),
        "min_model_confidence": round(float(settings.btc_fair_value_paper_min_model_confidence), 6),
        "take_profit_fraction": round(float(settings.btc_fair_value_paper_take_profit_fraction), 6),
        "stop_loss_fraction": round(float(settings.btc_fair_value_paper_stop_loss_fraction), 6),
        "exit_edge_floor": round(float(settings.btc_fair_value_paper_exit_edge_floor), 6),
        "force_exit_seconds": int(settings.btc_fair_value_paper_force_exit_seconds),
    }
    return {
        "summary": summary,
        "recent_signals": [dict(row) for row in signal_rows],
        "reason_breakdown": [dict(row) for row in reason_rows],
        "open_positions": [dict(row) for row in open_rows],
        "recent_closes": [dict(row) for row in recent_close_rows],
        "equity_curve": curve[-200:],
    }


def latency_bot_temporal_inventory_maker_paper_stats(settings: LatencyBotSettings) -> dict[str, Any]:
    cutoff_24h = datetime.now(timezone.utc) - timedelta(hours=24)
    cutoff_24h_ts = cutoff_24h.isoformat().replace("+00:00", "Z")
    cutoff_60m_ts = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat().replace("+00:00", "Z")
    capital = max(float(settings.temporal_inventory_maker_paper_capital_usdc), 0.0)
    with connect_latency_bot_db(settings) as conn:
        market_rows = conn.execute(
            """
            SELECT *
            FROM temporal_inventory_markets
            ORDER BY updated_ts DESC
            """
        ).fetchall()
        event_rows = conn.execute(
            """
            SELECT ts, market_id, event_type, state, side, price, size, notional_usdc,
                   pnl_usdc, pair_cost, reason
            FROM temporal_inventory_events
            ORDER BY ts DESC, id DESC
            LIMIT 80
            """
        ).fetchall()
        quote_rows = conn.execute(
            """
            SELECT quote_id, ts_created, ts_updated, market_id, side, price, size,
                   notional_usdc, status, edge, fill_price, fill_size, cancel_reason,
                   adverse_selection_loss_usdc, reason
            FROM temporal_inventory_quotes
            ORDER BY ts_created DESC
            LIMIT 80
            """
        ).fetchall()
        quote_summary = conn.execute(
            """
            SELECT
                COUNT(*) AS quotes,
                SUM(CASE WHEN status = 'filled' THEN 1 ELSE 0 END) AS filled,
                SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
                SUM(adverse_selection_loss_usdc) AS adverse_selection_loss
            FROM temporal_inventory_quotes
            """
        ).fetchone()
        quote_summary_60m = conn.execute(
            """
            SELECT
                COUNT(*) AS quotes,
                SUM(CASE WHEN status = 'filled' THEN 1 ELSE 0 END) AS filled
            FROM temporal_inventory_quotes
            WHERE ts_created >= ?
            """,
            (cutoff_60m_ts,),
        ).fetchone()
        events_60m = conn.execute(
            """
            SELECT COUNT(*) AS events
            FROM temporal_inventory_events
            WHERE ts >= ?
            """,
            (cutoff_60m_ts,),
        ).fetchone()
        realized_24h_row = conn.execute(
            """
            SELECT SUM(COALESCE(pnl_usdc, 0.0)) AS pnl
            FROM temporal_inventory_events
            WHERE ts >= ?
              AND event_type IN ('SELL', 'EXPIRE', 'RESOLVE')
            """,
            (cutoff_24h_ts,),
        ).fetchone()

    market_dicts = [dict(row) for row in market_rows]
    market_ids = [str(row.get("market_id") or "") for row in market_dicts]
    latest_books = load_latest_polymarket_books(settings, market_ids)

    realized_pnl = round(sum(float(row.get("realized_pnl_usdc") or 0.0) for row in market_dicts), 6)
    locked_pair_shares = round(sum(float(row.get("locked_pair_shares") or 0.0) for row in market_dicts), 6)
    locked_pair_pnl = round(sum(float(row.get("locked_pair_pnl_usdc") or 0.0) for row in market_dicts), 6)
    locked_pair_cost_sum = 0.0
    open_exposure = 0.0
    unpaired_exposure = 0.0
    unpaired_marked_pnl = 0.0
    expired_inventory_cost = 0.0
    active_rows: list[dict[str, Any]] = []
    for row in market_dicts:
        state = str(row.get("state") or "")
        yes_shares = float(row.get("yes_shares") or 0.0)
        no_shares = float(row.get("no_shares") or 0.0)
        yes_cost = float(row.get("yes_cost_usdc") or 0.0)
        no_cost = float(row.get("no_cost_usdc") or 0.0)
        avg_yes = yes_cost / yes_shares if yes_shares > 0.0 else 0.0
        avg_no = no_cost / no_shares if no_shares > 0.0 else 0.0
        paired = min(yes_shares, no_shares)
        if paired > 0.0:
            locked_pair_cost_sum += paired * (avg_yes + avg_no)
        if state != "CLOSED":
            active_rows.append(row)
            open_exposure += yes_cost + no_cost
            book = latest_books.get(str(row.get("market_id") or ""), {})
            yes_bid = float(book.get("best_bid") or 0.0)
            no_bid = float(book.get("no_best_bid") or 0.0)
            if no_bid <= 0.0:
                best_ask = float(book.get("best_ask") or 0.0)
                no_bid = max(1.0 - best_ask, 0.0) if best_ask > 0.0 else 0.0
            if yes_shares > no_shares:
                excess = yes_shares - no_shares
                excess_cost = excess * avg_yes
                unpaired_exposure += excess_cost
                unpaired_marked_pnl += excess * yes_bid - excess_cost
            elif no_shares > yes_shares:
                excess = no_shares - yes_shares
                excess_cost = excess * avg_no
                unpaired_exposure += excess_cost
                unpaired_marked_pnl += excess * no_bid - excess_cost
        expired_inventory_cost += float(row.get("expired_inventory_cost_usdc") or 0.0)

    event_dicts = [dict(row) for row in reversed(event_rows)]
    running = 0.0
    peak = 0.0
    max_drawdown = 0.0
    curve: list[dict[str, Any]] = []
    for row in event_dicts:
        event_type = str(row.get("event_type") or "")
        pnl = float(row.get("pnl_usdc") or 0.0)
        if event_type in {"SELL", "EXPIRE", "RESOLVE", "LOCKED_PAIR"}:
            running = round(running + pnl, 6)
            peak = max(peak, running)
            max_drawdown = min(max_drawdown, running - peak)
            curve.append(
                {
                    "ts": str(row.get("ts") or ""),
                    "realized_pnl_usdc": running,
                    "unrealized_pnl_usdc": 0.0,
                    "equity_usdc": round(capital + running, 6),
                }
            )

    quote_count = int(quote_summary["quotes"] or 0) if quote_summary else 0
    quote_filled = int(quote_summary["filled"] or 0) if quote_summary else 0
    closed_rows = [row for row in market_dicts if str(row.get("state") or "") == "CLOSED"]
    wins = sum(1 for row in closed_rows if float(row.get("realized_pnl_usdc") or 0.0) > 0.0)
    realized_24h = float(realized_24h_row["pnl"] or 0.0) if realized_24h_row else 0.0
    marked_net_pnl = round(realized_pnl + locked_pair_pnl + unpaired_marked_pnl, 6)
    summary = {
        "mode": "temporal_inventory_maker_paper",
        "enabled": bool(settings.temporal_inventory_maker_paper_enabled),
        "starting_capital_usdc": round(capital, 6),
        "equity_usdc": round(capital + marked_net_pnl, 6),
        "realized_pnl_usdc": round(realized_pnl, 6),
        "marked_pnl_usdc": round(marked_net_pnl, 6),
        "unpaired_marked_pnl_usdc": round(unpaired_marked_pnl, 6),
        "locked_pair_pnl_usdc": round(locked_pair_pnl, 6),
        "open_exposure_usdc": round(open_exposure, 6),
        "unpaired_exposure_usdc": round(unpaired_exposure, 6),
        "locked_pair_shares": round(locked_pair_shares, 6),
        "average_pair_cost": round(locked_pair_cost_sum / locked_pair_shares, 6) if locked_pair_shares else 0.0,
        "expired_inventory_cost_usdc": round(expired_inventory_cost, 6),
        "quote_count": quote_count,
        "quote_filled": quote_filled,
        "quote_cancelled": int(quote_summary["cancelled"] or 0) if quote_summary else 0,
        "quote_fill_rate": round(quote_filled / quote_count, 4) if quote_count else 0.0,
        "adverse_selection_loss_usdc": round(float(quote_summary["adverse_selection_loss"] or 0.0), 6) if quote_summary else 0.0,
        "quotes_60m": int(quote_summary_60m["quotes"] or 0) if quote_summary_60m else 0,
        "quote_fills_60m": int(quote_summary_60m["filled"] or 0) if quote_summary_60m else 0,
        "events_60m": int(events_60m["events"] or 0) if events_60m else 0,
        "open_markets": len(active_rows),
        "closed_markets": len(closed_rows),
        "win_rate": round(wins / len(closed_rows), 4) if closed_rows else 0.0,
        "max_drawdown": round(max_drawdown, 6),
        "realized_pnl_24h_usdc": round(realized_24h, 6),
        "projected_monthly_revenue_usdc": round(realized_24h * 30.0, 6),
        "projected_yearly_revenue_usdc": round(realized_24h * 365.0, 6),
        "base_order_usdc": round(float(settings.temporal_inventory_maker_paper_base_order_usdc), 6),
        "max_market_exposure_usdc": round(float(settings.temporal_inventory_maker_paper_max_market_exposure_usdc), 6),
        "max_total_exposure_usdc": round(float(settings.temporal_inventory_maker_paper_max_total_exposure_usdc), 6),
        "min_net_edge": round(float(settings.temporal_inventory_maker_paper_min_net_edge), 6),
        "max_pair_cost": round(float(settings.temporal_inventory_maker_paper_max_pair_cost), 6),
        "quote_ttl_seconds": int(settings.temporal_inventory_maker_paper_quote_ttl_seconds),
        "force_exit_seconds": int(settings.temporal_inventory_maker_paper_force_exit_seconds),
        "daily_loss_limit_usdc": round(float(settings.temporal_inventory_maker_paper_daily_loss_limit_usdc), 6),
    }
    return {
        "summary": summary,
        "markets": market_dicts[:50],
        "recent_events": [dict(row) for row in event_rows],
        "recent_quotes": [dict(row) for row in quote_rows],
        "equity_curve": curve[-200:],
    }


def latency_bot_live_temporal_inventory_maker_stats(settings: LatencyBotSettings) -> dict[str, Any]:
    cutoff_24h_ts = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat().replace("+00:00", "Z")
    with connect_latency_bot_db(settings) as conn:
        order_rows = conn.execute(
            """
            SELECT *
            FROM live_temporal_inventory_maker_orders
            ORDER BY ts_created DESC
            LIMIT 80
            """
        ).fetchall()
        open_rows = conn.execute(
            """
            SELECT *
            FROM live_temporal_inventory_maker_orders
            WHERE status = 'open'
            ORDER BY ts_created ASC
            """
        ).fetchall()
        heartbeat_row = conn.execute(
            """
            SELECT *
            FROM live_temporal_inventory_maker_heartbeats
            ORDER BY ts DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        summary_row = conn.execute(
            """
            SELECT
                COUNT(*) AS orders,
                SUM(CASE WHEN decision = 'SUBMITTED' THEN 1 ELSE 0 END) AS submitted,
                SUM(CASE WHEN decision = 'DRY_RUN' THEN 1 ELSE 0 END) AS dry_run,
                SUM(CASE WHEN decision = 'BLOCKED' THEN 1 ELSE 0 END) AS blocked,
                SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed
            FROM live_temporal_inventory_maker_orders
            WHERE ts_created >= ?
            """,
            (cutoff_24h_ts,),
        ).fetchone()
    open_notional = round(sum(float(row["notional_usdc"] or 0.0) for row in open_rows), 6)
    heartbeat = dict(heartbeat_row) if heartbeat_row is not None else {}
    armed = (
        bool(settings.live_temporal_inventory_maker_enabled)
        and str(settings.live_temporal_inventory_maker_mode or "").lower() == "live"
        and str(settings.live_temporal_inventory_maker_confirm or "") == "LIVE_TEMPORAL_INVENTORY_MAKER"
    )
    summary = {
        "mode": "Guarded live temporal inventory maker",
        "enabled": bool(settings.live_temporal_inventory_maker_enabled),
        "pilot_mode": str(settings.live_temporal_inventory_maker_mode or "dry_run"),
        "armed_for_live_orders": armed,
        "confirmation_required": "LIVE_TEMPORAL_INVENTORY_MAKER",
        "capital_usdc": round(float(settings.live_temporal_inventory_maker_capital_usdc), 6),
        "base_order_usdc": round(float(settings.live_temporal_inventory_maker_base_order_usdc), 6),
        "max_open_orders": int(settings.live_temporal_inventory_maker_max_open_orders),
        "max_orders_per_cycle": int(settings.live_temporal_inventory_maker_max_orders_per_cycle),
        "min_edge": round(float(settings.live_temporal_inventory_maker_min_edge), 6),
        "min_seconds_left": int(settings.live_temporal_inventory_maker_min_seconds_left),
        "max_order_age_seconds": int(settings.live_temporal_inventory_maker_max_order_age_seconds),
        "heartbeat_timeout_seconds": int(settings.live_temporal_inventory_maker_heartbeat_timeout_seconds),
        "daily_loss_limit_usdc": round(float(settings.live_temporal_inventory_maker_daily_loss_limit_usdc), 6),
        "open_orders": len(open_rows),
        "open_notional_usdc": open_notional,
        "submitted_24h": int(summary_row["submitted"] or 0) if summary_row else 0,
        "dry_run_24h": int(summary_row["dry_run"] or 0) if summary_row else 0,
        "blocked_24h": int(summary_row["blocked"] or 0) if summary_row else 0,
        "cancelled_24h": int(summary_row["cancelled"] or 0) if summary_row else 0,
        "failed_24h": int(summary_row["failed"] or 0) if summary_row else 0,
        "last_heartbeat_ts": str(heartbeat.get("ts") or ""),
        "last_heartbeat_status": str(heartbeat.get("status") or ""),
        "last_heartbeat_reason": str(heartbeat.get("reason") or ""),
        "last_heartbeat_cancel_all_ok": bool(heartbeat.get("cancel_all_ok")),
    }
    return {
        "summary": summary,
        "recent_orders": [dict(row) for row in order_rows],
        "open_orders": [dict(row) for row in open_rows],
    }


def latency_bot_late_resolution_capture_paper_stats(settings: LatencyBotSettings) -> dict[str, Any]:
    cutoff_24h = datetime.now(timezone.utc) - timedelta(hours=24)
    cutoff_24h_ts = cutoff_24h.isoformat().replace("+00:00", "Z")
    cutoff_60m_ts = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat().replace("+00:00", "Z")
    capital = max(float(settings.late_resolution_capture_paper_capital_usdc), 0.0)
    with connect_latency_bot_db(settings) as conn:
        signal_rows = conn.execute(
            """
            SELECT *
            FROM late_resolution_capture_signals
            ORDER BY ts DESC, id DESC
            LIMIT 60
            """
        ).fetchall()
        signal_summary = conn.execute(
            """
            SELECT COUNT(*) AS signals,
                   SUM(CASE WHEN eligible THEN 1 ELSE 0 END) AS eligible,
                   MAX(edge) AS best_edge
            FROM late_resolution_capture_signals
            WHERE ts >= ?
            """,
            (cutoff_60m_ts,),
        ).fetchone()
        reason_rows = conn.execute(
            """
            SELECT reason, COUNT(*) AS count, MAX(edge) AS max_edge
            FROM late_resolution_capture_signals
            WHERE ts >= ?
            GROUP BY reason
            ORDER BY count DESC, reason ASC
            LIMIT 30
            """,
            (cutoff_60m_ts,),
        ).fetchall()
        open_rows = conn.execute(
            """
            SELECT *
            FROM late_resolution_capture_positions
            WHERE status = 'open'
            ORDER BY entry_ts ASC
            """
        ).fetchall()
        close_rows = conn.execute(
            """
            SELECT e.ts, p.position_id, p.market_id, p.asset, p.side, p.entry_price,
                   e.mark AS exit_price, p.size, p.notional_usdc, p.entry_edge,
                   p.entry_official_confidence, p.entry_boundary_distance_bps,
                   e.pnl, e.reason
            FROM late_resolution_capture_events e
            JOIN late_resolution_capture_positions p ON p.position_id = e.position_id
            WHERE e.event_type = 'close'
            ORDER BY e.ts ASC, e.id ASC
            """
        ).fetchall()
        recent_close_rows = conn.execute(
            """
            SELECT e.ts, p.position_id, p.market_id, p.asset, p.side, p.entry_price,
                   e.mark AS exit_price, p.size, p.notional_usdc, p.entry_edge,
                   p.entry_official_confidence, p.entry_boundary_distance_bps,
                   e.pnl, e.reason
            FROM late_resolution_capture_events e
            JOIN late_resolution_capture_positions p ON p.position_id = e.position_id
            WHERE e.event_type = 'close'
            ORDER BY e.ts DESC, e.id DESC
            LIMIT 30
            """
        ).fetchall()

    pnls = [float(row["pnl"] or 0.0) for row in close_rows]
    running = 0.0
    peak = 0.0
    max_drawdown = 0.0
    realized_24h = 0.0
    curve: list[dict[str, Any]] = []
    for row in close_rows:
        pnl = float(row["pnl"] or 0.0)
        running = round(running + pnl, 6)
        peak = max(peak, running)
        max_drawdown = min(max_drawdown, running - peak)
        parsed_ts = _storage_parse_ts(str(row["ts"] or ""))
        if parsed_ts is not None and parsed_ts >= cutoff_24h:
            realized_24h = round(realized_24h + pnl, 6)
        curve.append(
            {
                "ts": str(row["ts"] or ""),
                "realized_pnl_usdc": running,
                "unrealized_pnl_usdc": 0.0,
                "equity_usdc": round(capital + running, 6),
            }
        )
    closed = len(pnls)
    wins = sum(1 for pnl in pnls if pnl > 0.0)
    signals = int(signal_summary["signals"] or 0) if signal_summary else 0
    eligible = int(signal_summary["eligible"] or 0) if signal_summary else 0
    open_capital = round(sum(float(row["notional_usdc"] or 0.0) for row in open_rows), 6)
    summary = {
        "mode": "late_resolution_capture_paper",
        "enabled": bool(settings.late_resolution_capture_paper_enabled),
        "starting_capital_usdc": round(capital, 6),
        "equity_usdc": round(capital + sum(pnls), 6),
        "net_pnl": round(sum(pnls), 6),
        "realized_pnl_24h_usdc": round(realized_24h, 6),
        "projected_monthly_revenue_usdc": round(realized_24h * 30.0, 6),
        "projected_yearly_revenue_usdc": round(realized_24h * 365.0, 6),
        "open": len(open_rows),
        "closed": closed,
        "win_rate": round(wins / closed, 4) if closed else 0.0,
        "avg_pnl": round(sum(pnls) / closed, 6) if closed else 0.0,
        "max_drawdown": round(max_drawdown, 6),
        "current_capital_in_use_usdc": open_capital,
        "signals_60m": signals,
        "eligible_60m": eligible,
        "eligible_rate_60m": round(eligible / signals, 4) if signals else 0.0,
        "best_edge_60m": round(float(signal_summary["best_edge"] or 0.0), 6) if signal_summary else 0.0,
        "target_notional_usdc": round(float(settings.late_resolution_capture_paper_notional_usdc), 6),
        "max_market_exposure_usdc": round(float(settings.late_resolution_capture_paper_max_market_exposure_usdc), 6),
        "max_total_exposure_usdc": round(float(settings.late_resolution_capture_paper_max_total_exposure_usdc), 6),
        "min_seconds_left": int(settings.late_resolution_capture_paper_min_seconds_left),
        "max_seconds_left": int(settings.late_resolution_capture_paper_max_seconds_left),
        "min_official_confidence": round(float(settings.late_resolution_capture_paper_min_official_confidence), 6),
        "min_boundary_distance_bps": round(float(settings.late_resolution_capture_paper_min_boundary_distance_bps), 6),
        "min_edge": round(float(settings.late_resolution_capture_paper_min_edge), 6),
        "daily_loss_limit_usdc": round(float(settings.late_resolution_capture_paper_daily_loss_limit_usdc), 6),
    }
    return {
        "summary": summary,
        "recent_signals": [dict(row) for row in signal_rows],
        "reason_breakdown": [dict(row) for row in reason_rows],
        "open_positions": [dict(row) for row in open_rows],
        "recent_closes": [dict(row) for row in recent_close_rows],
        "equity_curve": curve[-200:],
    }


def latency_bot_preowned_inventory_arb_sim(settings: LatencyBotSettings) -> dict[str, Any]:
    lookback_hours = max(int(settings.realistic_complete_set_arb_lookback_hours), 1)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    cutoff_ts = cutoff.isoformat().replace("+00:00", "Z")
    recent_cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    recent_signal_cutoff = datetime.now(timezone.utc) - timedelta(minutes=60)
    capital_usdc = max(float(settings.preowned_inventory_arb_capital_usdc), 0.0)
    target_notional = max(float(settings.preowned_inventory_arb_notional_usdc), 0.0)
    min_edge = float(settings.preowned_inventory_arb_min_edge_per_share)
    min_seconds_left = max(float(settings.preowned_inventory_arb_min_seconds_left), 0.0)
    min_depth = max(float(settings.preowned_inventory_arb_min_depth_usdc), 0.0)
    depth_haircut = min(max(float(settings.live_complete_set_arb_pilot_depth_haircut), 0.0), 1.0)
    extra_slippage = max(float(settings.live_complete_set_arb_pilot_extra_slippage_per_share), 0.0)
    cooldown_seconds = max(int(settings.preowned_inventory_arb_same_market_cooldown_seconds), 0)
    redeem_lag_seconds = max(int(settings.realistic_complete_set_arb_redeem_lag_seconds), 0)
    seed_side_notional = max(float(settings.preowned_inventory_arb_seed_side_notional_usdc), 0.0)
    seed_max_cost = max(float(settings.preowned_inventory_arb_seed_max_complete_set_cost), 0.0)
    seed_min_seconds_left = max(float(settings.preowned_inventory_arb_seed_min_seconds_left), 0.0)
    seed_max_seconds_left = max(float(settings.preowned_inventory_arb_seed_max_seconds_left), seed_min_seconds_left)
    seed_min_depth = max(float(settings.preowned_inventory_arb_seed_min_depth_usdc), 0.0)
    max_open_seeded_markets = max(int(settings.preowned_inventory_arb_max_open_seeded_markets), 0)
    per_asset_time_bucket_cap = max(int(settings.preowned_inventory_arb_per_asset_time_bucket_cap), 0)
    time_bucket_seconds = max(int(settings.preowned_inventory_arb_time_bucket_seconds), 1)
    seed_require_original_eligible = bool(settings.preowned_inventory_arb_seed_require_original_eligible)

    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT ts, market_id, asset, tenor_minutes, yes_ask, no_ask, total_cost,
                   gross_edge, net_edge, executable_depth_usdc, book_age_ms, seconds_left,
                   eligible, reason
            FROM complete_set_arb_signals
            WHERE ts >= ?
            ORDER BY ts ASC, id ASC
            """,
            (cutoff_ts,),
        ).fetchall()

    inventory: dict[str, dict[str, Any]] = {}
    locked: list[tuple[datetime, float, str, str, tuple[str, int] | None]] = []
    last_market_entry: dict[str, datetime] = {}
    seeded_bucket_counts: dict[tuple[str, int], int] = {}
    reasons: dict[str, dict[str, Any]] = {}
    recent_events: list[dict[str, Any]] = []
    curve: list[dict[str, Any]] = []
    seeded_markets: set[str] = set()
    traded_markets: set[str] = set()
    seed_spend = 0.0
    consumed_inventory_cost = 0.0
    buy_leg_spend = 0.0
    expired_inventory_cost = 0.0
    expired_inventory_guaranteed_payout = 0.0
    expired_inventory_conservative_pnl = 0.0
    current_locked_capital = 0.0
    peak_locked_capital = 0.0
    running_pnl = 0.0
    completed_set_pnl = 0.0
    realized_24h = 0.0
    peak_pnl = 0.0
    max_drawdown = 0.0
    inventory_sets = 0
    inventory_blocked = 0
    capital_blocked = 0
    depth_blocked = 0
    original_skips = 0
    signals_60m = 0
    eligible_60m = 0
    best_inventory_edge_60m = 0.0
    first_tick_ts = ""
    last_tick_ts = ""

    def add_reason(reason: str, *, edge: float = 0.0) -> None:
        item = reasons.setdefault(reason, {"reason": reason, "count": 0, "max_inventory_edge": 0.0})
        item["count"] = int(item["count"]) + 1
        item["max_inventory_edge"] = max(float(item["max_inventory_edge"]), float(edge))

    def add_event(
        row: sqlite3.Row,
        decision: str,
        reason: str,
        *,
        side_bought: str = "",
        inventory_side: str = "",
        inventory_edge: float = 0.0,
        shares: float = 0.0,
        buy_spend: float = 0.0,
        inventory_cost: float = 0.0,
        pnl: float = 0.0,
    ) -> None:
        recent_events.append(
            {
                "ts": str(row["ts"] or ""),
                "market_id": str(row["market_id"] or ""),
                "asset": str(row["asset"] or ""),
                "decision": decision,
                "reason": reason,
                "side_bought": side_bought,
                "inventory_side": inventory_side,
                "yes_ask": float(row["yes_ask"] or 0.0),
                "no_ask": float(row["no_ask"] or 0.0),
                "total_cost": float(row["total_cost"] or 0.0),
                "net_edge": float(row["net_edge"] or 0.0),
                "inventory_edge": round(inventory_edge, 6),
                "shares": round(shares, 6),
                "buy_spend_usdc": round(buy_spend, 6),
                "inventory_cost_usdc": round(inventory_cost, 6),
                "pnl": round(pnl, 6),
                "locked_capital_usdc": round(current_locked_capital, 6),
            }
        )
        if len(recent_events) > 80:
            del recent_events[: len(recent_events) - 80]

    def conservative_inventory_mark(inv: dict[str, Any]) -> tuple[float, float, float]:
        """Return (cost, guaranteed payout, conservative PnL) for unpaired inventory."""
        yes_shares = max(float(inv.get("yes_shares") or 0.0), 0.0)
        no_shares = max(float(inv.get("no_shares") or 0.0), 0.0)
        yes_cost = max(float(inv.get("yes_cost") or 0.0), 0.0)
        no_cost = max(float(inv.get("no_cost") or 0.0), 0.0)
        total_cost = round(yes_cost + no_cost, 6)
        if total_cost <= 0.0:
            return 0.0, 0.0, 0.0
        # Only overlapping YES+NO shares are guaranteed redeemable as complete sets.
        # Any one-sided excess is marked at zero unless it has already paired.
        guaranteed_payout = round(min(yes_shares, no_shares), 6)
        conservative_pnl = round(guaranteed_payout - total_cost, 6)
        return total_cost, guaranteed_payout, conservative_pnl

    def release_capital(ts: datetime) -> None:
        nonlocal current_locked_capital, expired_inventory_cost, expired_inventory_guaranteed_payout
        nonlocal expired_inventory_conservative_pnl, running_pnl, realized_24h, peak_pnl, max_drawdown
        kept: list[tuple[datetime, float, str, str, tuple[str, int] | None]] = []
        released = 0.0
        for release_ts, amount, market_id, kind, seed_bucket in locked:
            if release_ts <= ts:
                released += amount
                if kind == "seed_inventory":
                    if seed_bucket is not None:
                        seeded_bucket_counts[seed_bucket] = max(0, int(seeded_bucket_counts.get(seed_bucket, 0)) - 1)
                    inv = inventory.get(market_id)
                    if inv:
                        leftover_cost, guaranteed_payout, conservative_pnl = conservative_inventory_mark(inv)
                        if leftover_cost > 0.0:
                            expired_inventory_cost = round(expired_inventory_cost + leftover_cost, 6)
                            expired_inventory_guaranteed_payout = round(
                                expired_inventory_guaranteed_payout + guaranteed_payout,
                                6,
                            )
                            expired_inventory_conservative_pnl = round(
                                expired_inventory_conservative_pnl + conservative_pnl,
                                6,
                            )
                            running_pnl = round(running_pnl + conservative_pnl, 6)
                            peak_pnl = max(peak_pnl, running_pnl)
                            max_drawdown = min(max_drawdown, running_pnl - peak_pnl)
                            if release_ts >= recent_cutoff:
                                realized_24h = round(realized_24h + conservative_pnl, 6)
                            curve.append(
                                {
                                    "ts": release_ts.isoformat().replace("+00:00", "Z"),
                                    "realized_pnl_usdc": running_pnl,
                                    "unrealized_pnl_usdc": 0.0,
                                    "equity_usdc": round(capital_usdc + running_pnl, 6),
                                }
                            )
                        inv["yes_shares"] = 0.0
                        inv["no_shares"] = 0.0
                        inv["yes_cost"] = 0.0
                        inv["no_cost"] = 0.0
            else:
                kept.append((release_ts, amount, market_id, kind, seed_bucket))
        if released:
            current_locked_capital = max(0.0, current_locked_capital - released)
        locked[:] = kept

    def market_inventory(market_id: str) -> dict[str, Any]:
        return inventory.setdefault(
            market_id,
            {
                "yes_shares": 0.0,
                "no_shares": 0.0,
                "yes_cost": 0.0,
                "no_cost": 0.0,
                "seeded": False,
            },
        )

    for row in rows:
        parsed_ts = _storage_parse_ts(str(row["ts"] or ""))
        if parsed_ts is None:
            continue
        if not first_tick_ts:
            first_tick_ts = str(row["ts"] or "")
        last_tick_ts = str(row["ts"] or "")
        release_capital(parsed_ts)
        if parsed_ts >= recent_signal_cutoff:
            signals_60m += 1

        market_id = str(row["market_id"] or "")
        yes_ask = max(float(row["yes_ask"] or 0.0), 0.0)
        no_ask = max(float(row["no_ask"] or 0.0), 0.0)
        total_cost = max(float(row["total_cost"] or 0.0), 0.0001)
        seconds_left = max(float(row["seconds_left"] or 0.0), 0.0)
        effective_depth = max(float(row["executable_depth_usdc"] or 0.0), 0.0) * depth_haircut
        inv = market_inventory(market_id)

        asset = str(row["asset"] or "")
        seed_bucket = (asset, int(parsed_ts.timestamp() // time_bucket_seconds))
        active_seed_count = sum(
            1
            for _, _, _, kind, _ in locked
            if kind == "seed_inventory"
        )
        can_seed = (
            not inv["seeded"]
            and yes_ask > 0.0
            and no_ask > 0.0
            and total_cost <= seed_max_cost
            and seed_min_seconds_left <= seconds_left <= seed_max_seconds_left
            and effective_depth >= seed_min_depth
            and (not seed_require_original_eligible or bool(row["eligible"]))
            and (max_open_seeded_markets <= 0 or active_seed_count < max_open_seeded_markets)
            and (
                per_asset_time_bucket_cap <= 0
                or int(seeded_bucket_counts.get(seed_bucket, 0)) < per_asset_time_bucket_cap
            )
        )
        if can_seed:
            seed_shares = min(seed_side_notional / yes_ask, seed_side_notional / no_ask) if yes_ask > 0.0 and no_ask > 0.0 else 0.0
            yes_seed_cost = seed_shares * yes_ask
            no_seed_cost = seed_shares * no_ask
            seed_cost = round(yes_seed_cost + no_seed_cost, 6)
            available_capital = capital_usdc - current_locked_capital
            if seed_cost > 0.0 and available_capital >= seed_cost:
                inv["yes_shares"] = round(float(inv["yes_shares"]) + seed_shares, 6)
                inv["no_shares"] = round(float(inv["no_shares"]) + seed_shares, 6)
                inv["yes_cost"] = round(float(inv["yes_cost"]) + yes_seed_cost, 6)
                inv["no_cost"] = round(float(inv["no_cost"]) + no_seed_cost, 6)
                inv["seeded"] = True
                seeded_markets.add(market_id)
                seed_spend = round(seed_spend + seed_cost, 6)
                current_locked_capital = round(current_locked_capital + seed_cost, 6)
                peak_locked_capital = max(peak_locked_capital, current_locked_capital)
                release_ts = parsed_ts + timedelta(seconds=seconds_left + redeem_lag_seconds)
                locked.append((release_ts, seed_cost, market_id, "seed_inventory", seed_bucket))
                seeded_bucket_counts[seed_bucket] = int(seeded_bucket_counts.get(seed_bucket, 0)) + 1
                add_reason("seeded both-side inventory buffer", edge=0.0)

        net_edge = float(row["net_edge"] or 0.0)
        if parsed_ts >= recent_signal_cutoff:
            best_inventory_edge_60m = max(best_inventory_edge_60m, net_edge)

        if not bool(row["eligible"]):
            original_skips += 1
            add_reason(f"original skip: {str(row['reason'] or 'unknown')}", edge=net_edge)
            continue
        if parsed_ts >= recent_signal_cutoff:
            eligible_60m += 1
        if seconds_left < min_seconds_left:
            reason = "below live min seconds left"
            add_reason(reason, edge=net_edge)
            add_event(row, "TIME_BLOCK", reason, inventory_edge=net_edge)
            continue
        if effective_depth < min_depth:
            depth_blocked += 1
            reason = "effective one-leg depth below minimum"
            add_reason(reason, edge=net_edge)
            add_event(row, "DEPTH_BLOCK", reason, inventory_edge=net_edge)
            continue
        last_entry = last_market_entry.get(market_id)
        if last_entry is not None and cooldown_seconds > 0 and (parsed_ts - last_entry).total_seconds() < cooldown_seconds:
            reason = "same-market cooldown"
            add_reason(reason, edge=net_edge)
            add_event(row, "SKIP", reason, inventory_edge=net_edge)
            continue

        candidates: list[dict[str, Any]] = []
        yes_inv_shares = float(inv["yes_shares"] or 0.0)
        no_inv_shares = float(inv["no_shares"] or 0.0)
        if no_inv_shares > 0.0 and yes_ask > 0.0:
            no_avg_cost = float(inv["no_cost"] or 0.0) / no_inv_shares
            edge = 1.0 - yes_ask - no_avg_cost - extra_slippage
            candidates.append(
                {
                    "buy_side": "yes",
                    "inventory_side": "no",
                    "buy_price": yes_ask,
                    "inventory_avg_cost": no_avg_cost,
                    "inventory_shares": no_inv_shares,
                    "edge": edge,
                }
            )
        if yes_inv_shares > 0.0 and no_ask > 0.0:
            yes_avg_cost = float(inv["yes_cost"] or 0.0) / yes_inv_shares
            edge = 1.0 - no_ask - yes_avg_cost - extra_slippage
            candidates.append(
                {
                    "buy_side": "no",
                    "inventory_side": "yes",
                    "buy_price": no_ask,
                    "inventory_avg_cost": yes_avg_cost,
                    "inventory_shares": yes_inv_shares,
                    "edge": edge,
                }
            )
        candidate = max(candidates, key=lambda item: float(item["edge"]), default=None)
        if candidate is None:
            inventory_blocked += 1
            reason = "no opposite inventory available"
            add_reason(reason, edge=net_edge)
            add_event(row, "INVENTORY_BLOCK", reason, inventory_edge=net_edge)
            continue
        inventory_edge = float(candidate["edge"])
        if inventory_edge < min_edge:
            inventory_blocked += 1
            reason = "inventory-cost-adjusted edge below threshold"
            add_reason(reason, edge=inventory_edge)
            add_event(row, "INVENTORY_EDGE_SKIP", reason, side_bought=str(candidate["buy_side"]), inventory_side=str(candidate["inventory_side"]), inventory_edge=inventory_edge)
            continue

        available_capital = capital_usdc - current_locked_capital
        if available_capital < min_depth:
            capital_blocked += 1
            reason = "capital locked in inventory/sets"
            add_reason(reason, edge=inventory_edge)
            add_event(row, "CAPITAL_BLOCK", reason, inventory_edge=inventory_edge)
            continue

        buy_price = float(candidate["buy_price"])
        max_shares_by_notional = target_notional / buy_price if buy_price > 0.0 else 0.0
        max_shares_by_depth = effective_depth / buy_price if buy_price > 0.0 else 0.0
        max_shares_by_capital = available_capital / buy_price if buy_price > 0.0 else 0.0
        shares = max(0.0, min(float(candidate["inventory_shares"]), max_shares_by_notional, max_shares_by_depth, max_shares_by_capital))
        buy_spend = shares * buy_price
        if buy_spend < min_depth:
            depth_blocked += 1
            reason = "available inventory notional below minimum"
            add_reason(reason, edge=inventory_edge)
            add_event(row, "INVENTORY_SIZE_BLOCK", reason, side_bought=str(candidate["buy_side"]), inventory_side=str(candidate["inventory_side"]), inventory_edge=inventory_edge, shares=shares, buy_spend=buy_spend)
            continue

        inv_side = str(candidate["inventory_side"])
        inv_avg_cost = float(candidate["inventory_avg_cost"])
        inv_cost = shares * inv_avg_cost
        if inv_side == "yes":
            inv["yes_shares"] = round(float(inv["yes_shares"]) - shares, 6)
            inv["yes_cost"] = round(float(inv["yes_cost"]) - inv_cost, 6)
        else:
            inv["no_shares"] = round(float(inv["no_shares"]) - shares, 6)
            inv["no_cost"] = round(float(inv["no_cost"]) - inv_cost, 6)

        pnl = round(shares - buy_spend - inv_cost, 6)
        completed_set_pnl = round(completed_set_pnl + pnl, 6)
        running_pnl = round(running_pnl + pnl, 6)
        peak_pnl = max(peak_pnl, running_pnl)
        max_drawdown = min(max_drawdown, running_pnl - peak_pnl)
        if parsed_ts >= recent_cutoff:
            realized_24h = round(realized_24h + pnl, 6)
        buy_leg_spend = round(buy_leg_spend + buy_spend, 6)
        consumed_inventory_cost = round(consumed_inventory_cost + inv_cost, 6)
        current_locked_capital = round(current_locked_capital + buy_spend, 6)
        peak_locked_capital = max(peak_locked_capital, current_locked_capital)
        release_ts = parsed_ts + timedelta(seconds=seconds_left + redeem_lag_seconds)
        locked.append((release_ts, buy_spend, market_id, "buy_leg", None))
        last_market_entry[market_id] = parsed_ts
        traded_markets.add(market_id)
        inventory_sets += 1
        reason = "one live leg paired with pre-owned inventory"
        add_reason(reason, edge=inventory_edge)
        add_event(
            row,
            "INVENTORY_SET_LOCKED",
            reason,
            side_bought=str(candidate["buy_side"]),
            inventory_side=inv_side,
            inventory_edge=inventory_edge,
            shares=shares,
            buy_spend=buy_spend,
            inventory_cost=inv_cost,
            pnl=pnl,
        )
        curve.append(
            {
                "ts": str(row["ts"] or ""),
                "realized_pnl_usdc": running_pnl,
                "unrealized_pnl_usdc": 0.0,
                "equity_usdc": round(capital_usdc + running_pnl, 6),
            }
        )

    now = datetime.now(timezone.utc)
    release_capital(now)
    unused_inventory_cost = round(
        sum(float(item.get("yes_cost") or 0.0) + float(item.get("no_cost") or 0.0) for item in inventory.values()),
        6,
    )
    unused_inventory_guaranteed_payout = 0.0
    unused_inventory_conservative_pnl = 0.0
    for item in inventory.values():
        _, guaranteed_payout, conservative_pnl = conservative_inventory_mark(item)
        unused_inventory_guaranteed_payout = round(unused_inventory_guaranteed_payout + guaranteed_payout, 6)
        unused_inventory_conservative_pnl = round(unused_inventory_conservative_pnl + conservative_pnl, 6)
    unused_inventory_shares = round(
        sum(float(item.get("yes_shares") or 0.0) + float(item.get("no_shares") or 0.0) for item in inventory.values()),
        6,
    )
    marked_net_pnl = round(running_pnl + unused_inventory_conservative_pnl, 6)
    avg_pnl_per_set = round(completed_set_pnl / inventory_sets, 6) if inventory_sets else 0.0
    avg_net_pnl_per_set = round(running_pnl / inventory_sets, 6) if inventory_sets else 0.0
    seed_conversion_rate = round(inventory_sets / len(seeded_markets), 6) if seeded_markets else 0.0
    profit_explanation = (
        f"{inventory_sets} completed inventory-paired sets produced {completed_set_pnl:.2f} gross PnL; "
        f"expired inventory marked at {expired_inventory_conservative_pnl:.2f} conservative PnL; "
        f"wallet-adjusted net is {running_pnl:.2f} before marking current open inventory."
    )
    summary = {
        "mode": "Research-only pre-owned inventory complete-set arb simulation",
        "enabled": True,
        "tick_poll_source": "complete_set_arb_signals table; one row per market per engine cycle",
        "lookback_hours": lookback_hours,
        "ticks_replayed": len(rows),
        "first_tick_ts": first_tick_ts,
        "last_tick_ts": last_tick_ts,
        "simulated_capital_usdc": round(capital_usdc, 6),
        "target_notional_usdc": round(target_notional, 6),
        "seed_side_notional_usdc": round(seed_side_notional, 6),
        "seed_max_complete_set_cost": round(seed_max_cost, 6),
        "seed_min_seconds_left": int(seed_min_seconds_left),
        "seed_max_seconds_left": int(seed_max_seconds_left),
        "seed_min_depth_usdc": round(seed_min_depth, 6),
        "seed_require_original_eligible": seed_require_original_eligible,
        "max_open_seeded_markets": max_open_seeded_markets,
        "per_asset_time_bucket_cap": per_asset_time_bucket_cap,
        "time_bucket_seconds": time_bucket_seconds,
        "same_market_cooldown_seconds": cooldown_seconds,
        "seeded_markets": len(seeded_markets),
        "seed_to_set_conversion_rate": seed_conversion_rate,
        "profit_explanation": profit_explanation,
        "seed_spend_usdc": round(seed_spend, 6),
        "expired_inventory_cost_usdc": round(expired_inventory_cost, 6),
        "expired_inventory_guaranteed_payout_usdc": round(expired_inventory_guaranteed_payout, 6),
        "expired_inventory_conservative_pnl_usdc": round(expired_inventory_conservative_pnl, 6),
        "unused_inventory_cost_usdc": unused_inventory_cost,
        "unused_inventory_guaranteed_payout_usdc": unused_inventory_guaranteed_payout,
        "unused_inventory_conservative_pnl_usdc": unused_inventory_conservative_pnl,
        "unused_inventory_shares": unused_inventory_shares,
        "consumed_inventory_cost_usdc": round(consumed_inventory_cost, 6),
        "live_buy_leg_spend_usdc": round(buy_leg_spend, 6),
        "current_locked_capital_usdc": round(current_locked_capital, 6),
        "peak_locked_capital_usdc": round(peak_locked_capital, 6),
        "peak_capital_fraction": round(peak_locked_capital / capital_usdc, 6) if capital_usdc else 0.0,
        "inventory_sets": inventory_sets,
        "unique_markets": len(traded_markets),
        "inventory_blocked": inventory_blocked,
        "capital_blocked": capital_blocked,
        "depth_blocked": depth_blocked,
        "original_skips": original_skips,
        "completed_set_gross_pnl_usdc": round(completed_set_pnl, 6),
        "net_pnl": round(running_pnl, 6),
        "marked_net_pnl": marked_net_pnl,
        "realized_pnl_24h_usdc": round(realized_24h, 6),
        "projected_monthly_revenue_usdc": round(realized_24h * 30.0, 6),
        "projected_yearly_revenue_usdc": round(realized_24h * 365.0, 6),
        "avg_pnl_per_inventory_set": avg_pnl_per_set,
        "avg_net_pnl_per_inventory_set": avg_net_pnl_per_set,
        "max_drawdown": round(max_drawdown, 6),
        "signals_60m": signals_60m,
        "eligible_60m": eligible_60m,
        "eligible_rate_60m": round(eligible_60m / signals_60m, 6) if signals_60m else 0.0,
        "best_inventory_edge_60m": round(best_inventory_edge_60m, 6),
        "min_edge_per_share": round(min_edge, 6),
        "min_seconds_left": int(min_seconds_left),
        "min_depth_usdc": round(min_depth, 6),
        "depth_haircut": round(depth_haircut, 6),
        "extra_slippage_per_share": round(extra_slippage, 6),
        "redeem_lag_seconds": redeem_lag_seconds,
    }
    reason_rows = sorted(reasons.values(), key=lambda item: (-int(item["count"]), str(item["reason"])))[:40]
    return {
        "summary": summary,
        "recent_events": list(reversed(recent_events[-40:])),
        "reason_breakdown": reason_rows,
        "equity_curve": curve[-200:],
    }


def latency_bot_polymarket_us_arb_sim(settings: LatencyBotSettings) -> dict[str, Any]:
    lookback_hours = max(int(settings.polymarket_us_arb_lookback_hours), 1)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    cutoff_ts = cutoff.isoformat().replace("+00:00", "Z")
    recent_cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    recent_signal_cutoff = datetime.now(timezone.utc) - timedelta(minutes=60)
    capital_usdc = max(float(settings.polymarket_us_arb_capital_usdc), 0.0)
    target_notional = max(float(settings.polymarket_us_arb_notional_usdc), 0.0)
    min_edge = float(settings.polymarket_us_arb_min_edge_per_share)
    min_depth = max(float(settings.polymarket_us_arb_min_depth_usdc), 0.0)
    depth_haircut = min(max(float(settings.polymarket_us_arb_depth_haircut), 0.0), 1.0)
    latency_seconds = max(float(settings.polymarket_us_arb_latency_ms), 0.0) / 1000.0
    latency_decay = latency_seconds * max(float(settings.polymarket_us_arb_latency_edge_decay_per_second), 0.0)
    extra_slippage = max(float(settings.polymarket_us_arb_extra_slippage_per_share), 0.0)
    fee_per_share = max(float(settings.polymarket_us_arb_fee_per_share), 0.0)
    operational_failure_rate = min(max(float(settings.polymarket_us_arb_operational_failure_rate), 0.0), 1.0)
    failed_leg_loss_fraction = max(float(settings.polymarket_us_arb_failed_leg_loss_fraction), 0.0)
    cooldown_seconds = max(int(settings.polymarket_us_arb_same_symbol_cooldown_seconds), 0)
    symbols = tuple(str(item).strip() for item in settings.polymarket_us_arb_symbols if str(item).strip())

    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT ts, symbol, state, best_bid, best_ask, bid_qty, ask_qty,
                   gross_edge, executable_depth_usdc, transact_time, source, error
            FROM polymarket_us_arb_ticks
            WHERE ts >= ?
            ORDER BY ts ASC, id ASC
            """,
            (cutoff_ts,),
        ).fetchall()

    reasons: dict[str, dict[str, Any]] = {}
    recent_events: list[dict[str, Any]] = []
    curve: list[dict[str, Any]] = []
    last_symbol_trade: dict[str, datetime] = {}
    simulated_trades = 0
    unique_symbols: set[str] = set()
    ticks_60m = 0
    eligible_60m = 0
    best_adjusted_edge_60m = 0.0
    capital_blocked = 0
    depth_latency_blocked = 0
    source_errors = 0
    running_pnl = 0.0
    realized_24h = 0.0
    peak_pnl = 0.0
    max_drawdown = 0.0
    peak_in_flight_capital = 0.0
    operational_expected_loss_total = 0.0
    first_tick_ts = ""
    last_tick_ts = ""

    def add_reason(reason: str, *, edge: float = 0.0) -> None:
        item = reasons.setdefault(reason, {"reason": reason, "count": 0, "max_adjusted_edge": 0.0})
        item["count"] = int(item["count"]) + 1
        item["max_adjusted_edge"] = max(float(item["max_adjusted_edge"]), float(edge))

    def add_event(row: sqlite3.Row, decision: str, reason: str, *, adjusted_edge: float, notional: float, shares: float, pnl: float) -> None:
        recent_events.append(
            {
                "ts": str(row["ts"] or ""),
                "symbol": str(row["symbol"] or ""),
                "decision": decision,
                "reason": reason,
                "best_bid": float(row["best_bid"] or 0.0),
                "best_ask": float(row["best_ask"] or 0.0),
                "gross_edge": float(row["gross_edge"] or 0.0),
                "adjusted_edge": round(adjusted_edge, 6),
                "depth": float(row["executable_depth_usdc"] or 0.0),
                "effective_depth": round(float(row["executable_depth_usdc"] or 0.0) * depth_haircut, 6),
                "notional_usdc": round(notional, 6),
                "shares": round(shares, 6),
                "pnl": round(pnl, 6),
            }
        )
        if len(recent_events) > 80:
            del recent_events[: len(recent_events) - 80]

    for row in rows:
        parsed_ts = _storage_parse_ts(str(row["ts"] or ""))
        if parsed_ts is None:
            continue
        if not first_tick_ts:
            first_tick_ts = str(row["ts"] or "")
        last_tick_ts = str(row["ts"] or "")
        if parsed_ts >= recent_signal_cutoff:
            ticks_60m += 1

        gross_edge = float(row["gross_edge"] or 0.0)
        adjusted_edge = gross_edge - extra_slippage - latency_decay - fee_per_share
        if parsed_ts >= recent_signal_cutoff:
            best_adjusted_edge_60m = max(best_adjusted_edge_60m, adjusted_edge)

        symbol = str(row["symbol"] or "")
        error = str(row["error"] or "").strip()
        if error:
            source_errors += 1
            reason = "orderbook fetch error"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "ERROR", reason, adjusted_edge=adjusted_edge, notional=0.0, shares=0.0, pnl=0.0)
            continue

        if not settings.polymarket_us_arb_enabled:
            reason = "polymarket us arb simulation disabled"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "SKIP", reason, adjusted_edge=adjusted_edge, notional=0.0, shares=0.0, pnl=0.0)
            continue

        if not symbols:
            reason = "no Polymarket US symbols configured"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "SKIP", reason, adjusted_edge=adjusted_edge, notional=0.0, shares=0.0, pnl=0.0)
            continue

        best_bid = float(row["best_bid"] or 0.0)
        best_ask = float(row["best_ask"] or 0.0)
        if best_bid <= 0.0 or best_ask <= 0.0:
            reason = "missing top-of-book"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "SKIP", reason, adjusted_edge=adjusted_edge, notional=0.0, shares=0.0, pnl=0.0)
            continue

        state = str(row["state"] or "").upper()
        if state and state not in {"OPEN", "INSTRUMENT_STATE_OPEN", "MARKET_STATE_OPEN"}:
            reason = "instrument not open"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "SKIP", reason, adjusted_edge=adjusted_edge, notional=0.0, shares=0.0, pnl=0.0)
            continue

        last_trade = last_symbol_trade.get(symbol)
        if last_trade is not None and cooldown_seconds > 0 and (parsed_ts - last_trade).total_seconds() < cooldown_seconds:
            reason = "same-symbol cooldown"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "SKIP", reason, adjusted_edge=adjusted_edge, notional=0.0, shares=0.0, pnl=0.0)
            continue

        effective_depth = float(row["executable_depth_usdc"] or 0.0) * depth_haircut
        if effective_depth < min_depth:
            depth_latency_blocked += 1
            reason = "effective crossed depth below minimum"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "DEPTH_BLOCK", reason, adjusted_edge=adjusted_edge, notional=0.0, shares=0.0, pnl=0.0)
            continue
        if adjusted_edge < min_edge:
            depth_latency_blocked += 1
            reason = "adjusted crossed edge below threshold"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "EDGE_SKIP", reason, adjusted_edge=adjusted_edge, notional=0.0, shares=0.0, pnl=0.0)
            continue
        if target_notional <= 0.0:
            reason = "target notional is zero"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "SKIP", reason, adjusted_edge=adjusted_edge, notional=0.0, shares=0.0, pnl=0.0)
            continue

        trade_notional = min(target_notional, effective_depth, capital_usdc)
        if trade_notional <= 0.0:
            capital_blocked += 1
            reason = "simulated capital unavailable"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "CAPITAL_BLOCK", reason, adjusted_edge=adjusted_edge, notional=0.0, shares=0.0, pnl=0.0)
            continue

        if parsed_ts >= recent_signal_cutoff:
            eligible_60m += 1

        shares = trade_notional / best_ask
        gross_pnl = adjusted_edge * shares
        operational_expected_loss = trade_notional * operational_failure_rate * failed_leg_loss_fraction
        operational_expected_loss_total = round(operational_expected_loss_total + operational_expected_loss, 6)
        pnl = round(gross_pnl - operational_expected_loss, 6)
        running_pnl = round(running_pnl + pnl, 6)
        peak_pnl = max(peak_pnl, running_pnl)
        max_drawdown = min(max_drawdown, running_pnl - peak_pnl)
        if parsed_ts >= recent_cutoff:
            realized_24h = round(realized_24h + pnl, 6)
        peak_in_flight_capital = max(peak_in_flight_capital, trade_notional)
        last_symbol_trade[symbol] = parsed_ts
        unique_symbols.add(symbol)
        simulated_trades += 1
        reason = "simulated crossed-book FOK buy/sell"
        add_reason(reason, edge=adjusted_edge)
        add_event(row, "SIM_TRADE", reason, adjusted_edge=adjusted_edge, notional=trade_notional, shares=shares, pnl=pnl)
        curve.append(
            {
                "ts": str(row["ts"] or ""),
                "realized_pnl_usdc": running_pnl,
                "unrealized_pnl_usdc": 0.0,
                "equity_usdc": round(capital_usdc + running_pnl, 6),
            }
        )

    summary = {
        "mode": "Research-only Polymarket US crossed-book arb simulation",
        "enabled": bool(settings.polymarket_us_arb_enabled),
        "data_source": "polymarket_us_arb_ticks table; populated by daemon-latency-bot-us-arb-probe",
        "symbols_configured": len(symbols),
        "symbols": ", ".join(symbols),
        "lookback_hours": lookback_hours,
        "ticks_replayed": len(rows),
        "first_tick_ts": first_tick_ts,
        "last_tick_ts": last_tick_ts,
        "source_errors": source_errors,
        "simulated_capital_usdc": round(capital_usdc, 6),
        "target_notional_usdc": round(target_notional, 6),
        "peak_in_flight_capital_usdc": round(peak_in_flight_capital, 6),
        "peak_capital_fraction": round(peak_in_flight_capital / capital_usdc, 6) if capital_usdc else 0.0,
        "simulated_trades": simulated_trades,
        "unique_symbols_traded": len(unique_symbols),
        "expected_operational_loss_usdc": round(operational_expected_loss_total, 6),
        "capital_blocked": capital_blocked,
        "depth_latency_blocked": depth_latency_blocked,
        "net_pnl": round(running_pnl, 6),
        "realized_pnl_24h_usdc": round(realized_24h, 6),
        "projected_monthly_revenue_usdc": round(realized_24h * 30.0, 6),
        "projected_yearly_revenue_usdc": round(realized_24h * 365.0, 6),
        "avg_pnl_per_trade": round(running_pnl / simulated_trades, 6) if simulated_trades else 0.0,
        "max_drawdown": round(max_drawdown, 6),
        "ticks_60m": ticks_60m,
        "eligible_60m": eligible_60m,
        "eligible_rate_60m": round(eligible_60m / ticks_60m, 6) if ticks_60m else 0.0,
        "best_adjusted_edge_60m": round(best_adjusted_edge_60m, 6),
        "min_edge_per_share": min_edge,
        "min_depth_usdc": round(min_depth, 6),
        "latency_ms": round(float(settings.polymarket_us_arb_latency_ms), 6),
        "depth_haircut": round(depth_haircut, 6),
        "extra_slippage_per_share": round(extra_slippage, 6),
        "latency_decay_per_share": round(latency_decay, 6),
        "fee_per_share": round(fee_per_share, 6),
        "operational_failure_rate": round(operational_failure_rate, 6),
        "failed_leg_loss_fraction": round(failed_leg_loss_fraction, 6),
        "same_symbol_cooldown_seconds": cooldown_seconds,
    }
    reason_rows = sorted(reasons.values(), key=lambda item: (-int(item["count"]), str(item["reason"])))[:40]
    return {
        "summary": summary,
        "recent_events": list(reversed(recent_events[-40:])),
        "reason_breakdown": reason_rows,
        "equity_curve": curve[-200:],
    }


def latency_bot_kalshi_arb_sim(settings: LatencyBotSettings) -> dict[str, Any]:
    lookback_hours = max(int(settings.kalshi_arb_lookback_hours), 1)
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=lookback_hours)
    cutoff_ts = cutoff.isoformat().replace("+00:00", "Z")
    recent_cutoff = now - timedelta(hours=24)
    recent_signal_cutoff = now - timedelta(minutes=60)
    capital_usdc = max(float(settings.kalshi_arb_capital_usdc), 0.0)
    target_notional = max(float(settings.kalshi_arb_notional_usdc), 0.0)
    min_edge = float(settings.kalshi_arb_min_edge_per_share)
    min_depth = max(float(settings.kalshi_arb_min_depth_usdc), 0.0)
    depth_haircut = min(max(float(settings.kalshi_arb_depth_haircut), 0.0), 1.0)
    latency_seconds = max(float(settings.kalshi_arb_latency_ms), 0.0) / 1000.0
    latency_decay = latency_seconds * max(float(settings.kalshi_arb_latency_edge_decay_per_second), 0.0)
    extra_slippage = max(float(settings.kalshi_arb_extra_slippage_per_share), 0.0)
    fee_per_share = max(float(settings.kalshi_arb_fee_per_share), 0.0)
    operational_failure_rate = min(max(float(settings.kalshi_arb_operational_failure_rate), 0.0), 1.0)
    failed_leg_loss_fraction = max(float(settings.kalshi_arb_failed_leg_loss_fraction), 0.0)
    cooldown_seconds = max(int(settings.kalshi_arb_same_ticker_cooldown_seconds), 0)
    settlement_lag_seconds = max(int(settings.kalshi_arb_settlement_lag_seconds), 0)

    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT ts, ticker, asset, title, status, yes_bid, no_bid, yes_bid_qty, no_bid_qty,
                   yes_ask, no_ask, total_cost, gross_edge, executable_depth_usdc,
                   seconds_left, close_time, source, error
            FROM kalshi_arb_ticks
            WHERE ts >= ?
            ORDER BY ts ASC, id ASC
            """,
            (cutoff_ts,),
        ).fetchall()

    reasons: dict[str, dict[str, Any]] = {}
    recent_events: list[dict[str, Any]] = []
    curve: list[dict[str, Any]] = []
    last_ticker_trade: dict[str, datetime] = {}
    locked_releases: list[tuple[datetime, float]] = []
    simulated_sets = 0
    unique_tickers: set[str] = set()
    ticks_60m = 0
    eligible_60m = 0
    best_adjusted_edge_60m = 0.0
    capital_blocked = 0
    depth_latency_blocked = 0
    source_errors = 0
    running_pnl = 0.0
    realized_24h = 0.0
    peak_pnl = 0.0
    max_drawdown = 0.0
    current_locked_capital = 0.0
    peak_locked_capital = 0.0
    operational_expected_loss_total = 0.0
    first_tick_ts = ""
    last_tick_ts = ""

    def add_reason(reason: str, *, edge: float = 0.0) -> None:
        item = reasons.setdefault(reason, {"reason": reason, "count": 0, "max_adjusted_edge": 0.0})
        item["count"] = int(item["count"]) + 1
        item["max_adjusted_edge"] = max(float(item["max_adjusted_edge"]), float(edge))

    def add_event(
        row: sqlite3.Row,
        decision: str,
        reason: str,
        *,
        adjusted_edge: float,
        notional: float,
        shares: float,
        pnl: float,
    ) -> None:
        recent_events.append(
            {
                "ts": str(row["ts"] or ""),
                "ticker": str(row["ticker"] or ""),
                "asset": str(row["asset"] or ""),
                "decision": decision,
                "reason": reason,
                "yes_ask": float(row["yes_ask"] or 0.0),
                "no_ask": float(row["no_ask"] or 0.0),
                "total_cost": float(row["total_cost"] or 0.0),
                "gross_edge": float(row["gross_edge"] or 0.0),
                "adjusted_edge": round(adjusted_edge, 6),
                "depth": float(row["executable_depth_usdc"] or 0.0),
                "effective_depth": round(float(row["executable_depth_usdc"] or 0.0) * depth_haircut, 6),
                "seconds_left": round(float(row["seconds_left"] or 0.0), 3),
                "notional_usdc": round(notional, 6),
                "shares": round(shares, 6),
                "pnl": round(pnl, 6),
            }
        )
        if len(recent_events) > 80:
            del recent_events[: len(recent_events) - 80]

    for row in rows:
        parsed_ts = _storage_parse_ts(str(row["ts"] or ""))
        if parsed_ts is None:
            continue
        if not first_tick_ts:
            first_tick_ts = str(row["ts"] or "")
        last_tick_ts = str(row["ts"] or "")
        if parsed_ts >= recent_signal_cutoff:
            ticks_60m += 1

        still_locked: list[tuple[datetime, float]] = []
        current_locked_capital = 0.0
        for release_ts, amount in locked_releases:
            if release_ts > parsed_ts:
                still_locked.append((release_ts, amount))
                current_locked_capital += amount
        locked_releases = still_locked

        gross_edge = float(row["gross_edge"] or 0.0)
        adjusted_edge = gross_edge - extra_slippage - latency_decay - fee_per_share
        if parsed_ts >= recent_signal_cutoff:
            best_adjusted_edge_60m = max(best_adjusted_edge_60m, adjusted_edge)

        error = str(row["error"] or "").strip()
        if error:
            source_errors += 1
            reason = "orderbook fetch error"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "ERROR", reason, adjusted_edge=adjusted_edge, notional=0.0, shares=0.0, pnl=0.0)
            continue

        if not settings.kalshi_arb_enabled:
            reason = "kalshi arb simulation disabled"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "SKIP", reason, adjusted_edge=adjusted_edge, notional=0.0, shares=0.0, pnl=0.0)
            continue

        ticker = str(row["ticker"] or "")
        if not ticker:
            reason = "missing ticker"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "SKIP", reason, adjusted_edge=adjusted_edge, notional=0.0, shares=0.0, pnl=0.0)
            continue

        status = str(row["status"] or "").strip().lower()
        if status in {"closed", "settled", "finalized", "expired"}:
            reason = "market not open"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "SKIP", reason, adjusted_edge=adjusted_edge, notional=0.0, shares=0.0, pnl=0.0)
            continue

        total_cost = float(row["total_cost"] or 0.0)
        yes_ask = float(row["yes_ask"] or 0.0)
        no_ask = float(row["no_ask"] or 0.0)
        if total_cost <= 0.0 or yes_ask <= 0.0 or no_ask <= 0.0:
            reason = "missing complete-set quote"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "SKIP", reason, adjusted_edge=adjusted_edge, notional=0.0, shares=0.0, pnl=0.0)
            continue

        last_trade = last_ticker_trade.get(ticker)
        if last_trade is not None and cooldown_seconds > 0 and (parsed_ts - last_trade).total_seconds() < cooldown_seconds:
            reason = "same-ticker cooldown"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "SKIP", reason, adjusted_edge=adjusted_edge, notional=0.0, shares=0.0, pnl=0.0)
            continue

        effective_depth = float(row["executable_depth_usdc"] or 0.0) * depth_haircut
        if effective_depth < min_depth:
            depth_latency_blocked += 1
            reason = "effective complete-set depth below minimum"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "DEPTH_BLOCK", reason, adjusted_edge=adjusted_edge, notional=0.0, shares=0.0, pnl=0.0)
            continue
        if adjusted_edge < min_edge:
            depth_latency_blocked += 1
            reason = "adjusted complete-set edge below threshold"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "EDGE_SKIP", reason, adjusted_edge=adjusted_edge, notional=0.0, shares=0.0, pnl=0.0)
            continue
        if target_notional <= 0.0:
            reason = "target notional is zero"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "SKIP", reason, adjusted_edge=adjusted_edge, notional=0.0, shares=0.0, pnl=0.0)
            continue

        available_capital = max(capital_usdc - current_locked_capital, 0.0)
        trade_notional = min(target_notional, effective_depth, available_capital)
        if trade_notional <= 0.0:
            capital_blocked += 1
            reason = "simulated capital locked until settlement"
            add_reason(reason, edge=adjusted_edge)
            add_event(row, "CAPITAL_BLOCK", reason, adjusted_edge=adjusted_edge, notional=0.0, shares=0.0, pnl=0.0)
            continue

        if parsed_ts >= recent_signal_cutoff:
            eligible_60m += 1

        shares = trade_notional / total_cost
        gross_pnl = adjusted_edge * shares
        operational_expected_loss = trade_notional * operational_failure_rate * failed_leg_loss_fraction
        operational_expected_loss_total = round(operational_expected_loss_total + operational_expected_loss, 6)
        pnl = round(gross_pnl - operational_expected_loss, 6)
        running_pnl = round(running_pnl + pnl, 6)
        peak_pnl = max(peak_pnl, running_pnl)
        max_drawdown = min(max_drawdown, running_pnl - peak_pnl)
        if parsed_ts >= recent_cutoff:
            realized_24h = round(realized_24h + pnl, 6)
        seconds_left = max(float(row["seconds_left"] or 0.0), 0.0)
        release_ts = parsed_ts + timedelta(seconds=seconds_left + settlement_lag_seconds)
        locked_releases.append((release_ts, trade_notional))
        current_locked_capital = round(current_locked_capital + trade_notional, 6)
        peak_locked_capital = max(peak_locked_capital, current_locked_capital)
        last_ticker_trade[ticker] = parsed_ts
        unique_tickers.add(ticker)
        simulated_sets += 1
        reason = "simulated Kalshi complete-set buy"
        add_reason(reason, edge=adjusted_edge)
        add_event(row, "SIM_SET", reason, adjusted_edge=adjusted_edge, notional=trade_notional, shares=shares, pnl=pnl)
        curve.append(
            {
                "ts": str(row["ts"] or ""),
                "realized_pnl_usdc": running_pnl,
                "unrealized_pnl_usdc": 0.0,
                "equity_usdc": round(capital_usdc + running_pnl, 6),
            }
        )

    current_locked_capital = round(
        sum(amount for release_ts, amount in locked_releases if release_ts > now),
        6,
    )
    summary = {
        "mode": "Research-only Kalshi complete-set arb simulation",
        "enabled": bool(settings.kalshi_arb_enabled),
        "data_source": "kalshi_arb_ticks table; populated by daemon-latency-bot-kalshi-arb-probe",
        "auto_discover": bool(settings.kalshi_arb_auto_discover),
        "tickers_configured": len(tuple(item for item in settings.kalshi_arb_tickers if str(item).strip())),
        "assets": ", ".join(str(item) for item in settings.kalshi_arb_assets),
        "lookback_hours": lookback_hours,
        "ticks_replayed": len(rows),
        "first_tick_ts": first_tick_ts,
        "last_tick_ts": last_tick_ts,
        "source_errors": source_errors,
        "simulated_capital_usdc": round(capital_usdc, 6),
        "target_notional_usdc": round(target_notional, 6),
        "current_locked_capital_usdc": current_locked_capital,
        "peak_locked_capital_usdc": round(peak_locked_capital, 6),
        "peak_capital_fraction": round(peak_locked_capital / capital_usdc, 6) if capital_usdc else 0.0,
        "simulated_sets": simulated_sets,
        "unique_tickers_traded": len(unique_tickers),
        "expected_operational_loss_usdc": round(operational_expected_loss_total, 6),
        "capital_blocked": capital_blocked,
        "depth_latency_blocked": depth_latency_blocked,
        "net_pnl": round(running_pnl, 6),
        "realized_pnl_24h_usdc": round(realized_24h, 6),
        "projected_monthly_revenue_usdc": round(realized_24h * 30.0, 6),
        "projected_yearly_revenue_usdc": round(realized_24h * 365.0, 6),
        "avg_pnl_per_set": round(running_pnl / simulated_sets, 6) if simulated_sets else 0.0,
        "max_drawdown": round(max_drawdown, 6),
        "ticks_60m": ticks_60m,
        "eligible_60m": eligible_60m,
        "eligible_rate_60m": round(eligible_60m / ticks_60m, 6) if ticks_60m else 0.0,
        "best_adjusted_edge_60m": round(best_adjusted_edge_60m, 6),
        "min_edge_per_share": round(min_edge, 6),
        "min_depth_usdc": round(min_depth, 6),
        "latency_ms": round(float(settings.kalshi_arb_latency_ms), 6),
        "depth_haircut": round(depth_haircut, 6),
        "extra_slippage_per_share": round(extra_slippage, 6),
        "latency_decay_per_share": round(latency_decay, 6),
        "fee_per_share": round(fee_per_share, 6),
        "operational_failure_rate": round(operational_failure_rate, 6),
        "failed_leg_loss_fraction": round(failed_leg_loss_fraction, 6),
        "same_ticker_cooldown_seconds": cooldown_seconds,
        "settlement_lag_seconds": settlement_lag_seconds,
    }
    reason_rows = sorted(reasons.values(), key=lambda item: (-int(item["count"]), str(item["reason"])))[:40]
    return {
        "summary": summary,
        "recent_events": list(reversed(recent_events[-40:])),
        "reason_breakdown": reason_rows,
        "equity_curve": curve[-200:],
    }


def latency_bot_shadow_portfolio_summary(
    settings: LatencyBotSettings,
    *,
    cache_items: list[dict[str, Any]] | None = None,
) -> dict[str, float]:
    with connect_latency_bot_db(settings) as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(pnl), 0.0) AS realized_pnl_usdc
            FROM shadow_position_events
            WHERE event_type = 'close'
            """
        ).fetchone()
        realized_last_24h_row = conn.execute(
            """
            SELECT COALESCE(SUM(pnl), 0.0) AS realized_pnl_24h_usdc
            FROM shadow_position_events
            WHERE event_type = 'close'
              AND ts >= ?
            """,
            ((datetime.now(timezone.utc) - timedelta(hours=24)).isoformat().replace("+00:00", "Z"),),
        ).fetchone()
    realized_pnl = round(float(row["realized_pnl_usdc"] or 0.0), 6) if row else 0.0
    realized_pnl_24h = round(float(realized_last_24h_row["realized_pnl_24h_usdc"] or 0.0), 6) if realized_last_24h_row else 0.0
    cache_by_market = {
        str(item.get("market_id") or ""): item
        for item in (cache_items or [])
        if isinstance(item, dict)
    }
    unrealized_pnl = 0.0
    for position in load_shadow_open_positions(settings):
        cache = cache_by_market.get(str(position.get("market_id") or ""))
        if cache is None:
            continue
        best_ask = float(cache.get("best_ask") or 0.0)
        mark = max(1.0 - best_ask, 0.0)
        unrealized_pnl += (mark - float(position.get("entry_price") or 0.0)) * float(position.get("size") or 0.0)
    unrealized_pnl = round(unrealized_pnl, 6)
    return {
        "realized_pnl_usdc": realized_pnl,
        "realized_pnl_24h_usdc": realized_pnl_24h,
        "unrealized_pnl_usdc": unrealized_pnl,
        "equity_usdc": round(settings.bankroll_usdc + realized_pnl + unrealized_pnl, 6),
    }


def latency_bot_shadow_performance_stats(settings: LatencyBotSettings) -> dict[str, Any]:
    with connect_latency_bot_db(settings) as conn:
        close_rows = conn.execute(
            """
            SELECT ts, position_id, mark, pnl, reason
            FROM shadow_position_events
            WHERE event_type = 'close'
            ORDER BY ts DESC
            LIMIT 50
            """
        ).fetchall()
        all_close_rows = conn.execute(
            """
            SELECT pnl, reason
            FROM shadow_position_events
            WHERE event_type = 'close'
            """
        ).fetchall()
        slice_rows = conn.execute(
            """
            SELECT
                sp.entry_price,
                spe.pnl,
                sp.entry_edge AS edge,
                sp.entry_seconds_left AS seconds_left,
                sp.asset,
                sp.entry_tenor_minutes AS tenor_minutes
            FROM shadow_positions sp
            JOIN shadow_position_events spe
              ON spe.position_id = sp.position_id
             AND spe.event_type = 'close'
            """
        ).fetchall()
    closes = [dict(row) for row in close_rows]
    pnls = [float(row["pnl"] or 0.0) for row in all_close_rows]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    reason_totals: dict[str, dict[str, float]] = {}
    for row in all_close_rows:
        reason = str(row["reason"] or "unknown")
        bucket = reason_totals.setdefault(reason, {"count": 0, "pnl": 0.0})
        bucket["count"] += 1
        bucket["pnl"] = round(bucket["pnl"] + float(row["pnl"] or 0.0), 6)
    breakdown = [
        {
            "reason": reason,
            "count": int(values["count"]),
            "pnl": round(float(values["pnl"]), 6),
        }
        for reason, values in sorted(reason_totals.items(), key=lambda item: (-(item[1]["count"]), item[0]))
    ]
    price_buckets: dict[str, dict[str, float]] = {}
    edge_buckets: dict[str, dict[str, float]] = {}
    seconds_buckets: dict[str, dict[str, float]] = {}
    slice_buckets: dict[str, dict[str, float]] = {}
    for row in slice_rows:
        pnl = float(row["pnl"] or 0.0)
        edge = float(row["edge"] or 0.0)
        seconds_left = float(row["seconds_left"] or 0.0)
        price = float(row["entry_price"] or 0.0)
        asset = str(row["asset"] or "btc").upper()
        tenor = int(row["tenor_minutes"] or 5)
        for bucket_map, key in (
            (price_buckets, _price_band(price)),
            (edge_buckets, _edge_band(edge)),
            (seconds_buckets, _seconds_band(seconds_left)),
            (slice_buckets, f"{asset} {tenor}m"),
        ):
            bucket = bucket_map.setdefault(key, {"total": 0.0, "eligible": 0.0, "edge_sum": 0.0, "net_pnl": 0.0, "wins": 0.0, "closed": 0.0})
            bucket["total"] += 1.0
            bucket["eligible"] += 1.0
            bucket["edge_sum"] += edge
            bucket["net_pnl"] += pnl
            bucket["closed"] += 1.0
            if pnl > 0:
                bucket["wins"] += 1.0
    total_closed = len(pnls)
    return {
        "recent_closes": closes,
        "total_closed": total_closed,
        "win_rate": round((len(wins) / total_closed), 4) if total_closed else 0.0,
        "avg_win": round(sum(wins) / len(wins), 6) if wins else 0.0,
        "avg_loss": round(sum(losses) / len(losses), 6) if losses else 0.0,
        "net_pnl": round(sum(pnls), 6),
        "exit_reason_breakdown": breakdown,
        "slice_breakdown": _sorted_band_rows(slice_buckets),
        "price_band_breakdown": _sorted_band_rows(price_buckets),
        "edge_band_breakdown": _sorted_band_rows(edge_buckets),
        "seconds_band_breakdown": _sorted_band_rows(seconds_buckets),
    }


def summarize_latency_bot_db(settings: LatencyBotSettings, minutes: int = 60) -> dict[str, Any]:
    if not settings.db_path.exists():
        return {
            "db_present": False,
            "db_path": str(settings.db_path),
            "lookback_minutes": minutes,
        }
    since = (datetime.now(timezone.utc) - timedelta(minutes=max(minutes, 1))).isoformat().replace("+00:00", "Z")
    with connect_latency_bot_db(settings) as conn:
        def count_recent(table: str, ts_column: str = "ts") -> int:
            row = conn.execute(
                f"SELECT COUNT(*) AS count FROM {table} WHERE {ts_column} >= ?",
                (since,),
            ).fetchone()
            return int(row["count"]) if row else 0

        complete_set_closes_row = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM complete_set_arb_events
            WHERE ts >= ? AND event_type = 'close'
            """,
            (since,),
        ).fetchone()
        totals = {
            "markets_total": int(conn.execute("SELECT COUNT(*) AS count FROM markets").fetchone()["count"]),
            "orders_total": int(conn.execute("SELECT COUNT(*) AS count FROM orders").fetchone()["count"]),
            "positions_total": int(conn.execute("SELECT COUNT(*) AS count FROM positions").fetchone()["count"]),
            "engine_cycles_total": int(conn.execute("SELECT COUNT(*) AS count FROM engine_cycles").fetchone()["count"]),
            "equity_snapshots_total": int(conn.execute("SELECT COUNT(*) AS count FROM equity_snapshots").fetchone()["count"]),
            "shadow_signals_total": int(conn.execute("SELECT COUNT(*) AS count FROM shadow_signals").fetchone()["count"]),
            "shadow_positions_total": int(conn.execute("SELECT COUNT(*) AS count FROM shadow_positions").fetchone()["count"]),
            "shadow_variant_signals_total": int(conn.execute("SELECT COUNT(*) AS count FROM shadow_variant_signals").fetchone()["count"]),
            "shadow_variant_positions_total": int(conn.execute("SELECT COUNT(*) AS count FROM shadow_variant_positions").fetchone()["count"]),
            "cex_latency_paper_signals_total": int(conn.execute("SELECT COUNT(*) AS count FROM cex_latency_paper_signals").fetchone()["count"]),
            "cex_latency_paper_positions_total": int(conn.execute("SELECT COUNT(*) AS count FROM cex_latency_paper_positions").fetchone()["count"]),
            "temporal_inventory_markets_total": int(conn.execute("SELECT COUNT(*) AS count FROM temporal_inventory_markets").fetchone()["count"]),
            "temporal_inventory_events_total": int(conn.execute("SELECT COUNT(*) AS count FROM temporal_inventory_events").fetchone()["count"]),
            "temporal_inventory_quotes_total": int(conn.execute("SELECT COUNT(*) AS count FROM temporal_inventory_quotes").fetchone()["count"]),
            "live_temporal_inventory_maker_orders_total": int(conn.execute("SELECT COUNT(*) AS count FROM live_temporal_inventory_maker_orders").fetchone()["count"]),
            "late_resolution_capture_signals_total": int(conn.execute("SELECT COUNT(*) AS count FROM late_resolution_capture_signals").fetchone()["count"]),
            "late_resolution_capture_positions_total": int(conn.execute("SELECT COUNT(*) AS count FROM late_resolution_capture_positions").fetchone()["count"]),
            "complete_set_arb_signals_total": int(conn.execute("SELECT COUNT(*) AS count FROM complete_set_arb_signals").fetchone()["count"]),
            "complete_set_arb_positions_total": int(conn.execute("SELECT COUNT(*) AS count FROM complete_set_arb_positions").fetchone()["count"]),
            "live_complete_set_arb_pilot_attempts_total": int(conn.execute("SELECT COUNT(*) AS count FROM live_complete_set_arb_pilot_attempts").fetchone()["count"]),
            "polymarket_us_arb_ticks_total": int(conn.execute("SELECT COUNT(*) AS count FROM polymarket_us_arb_ticks").fetchone()["count"]),
            "kalshi_arb_ticks_total": int(conn.execute("SELECT COUNT(*) AS count FROM kalshi_arb_ticks").fetchone()["count"]),
        }
        latest_cycle = conn.execute(
            """
            SELECT ts, phase, markets_tracked, signals_seen, orders_open, positions_open, risk_state, notes
            FROM engine_cycles
            ORDER BY ts DESC
            LIMIT 1
            """
        ).fetchone()
        recent_counts = {
            "binance_ticks": count_recent("binance_ticks"),
            "polymarket_books": count_recent("polymarket_books"),
            "fair_values": count_recent("fair_values"),
            "signals": count_recent("signals"),
            "shadow_signals": count_recent("shadow_signals"),
            "shadow_variant_signals": count_recent("shadow_variant_signals"),
            "cex_latency_paper_signals": count_recent("cex_latency_paper_signals"),
            "temporal_inventory_events": count_recent("temporal_inventory_events"),
            "temporal_inventory_quotes": count_recent("temporal_inventory_quotes", "ts_created"),
            "live_temporal_inventory_maker_orders": count_recent("live_temporal_inventory_maker_orders", "ts_created"),
            "late_resolution_capture_signals": count_recent("late_resolution_capture_signals"),
            "complete_set_arb_signals": count_recent("complete_set_arb_signals"),
            "complete_set_arb_closes": int(complete_set_closes_row["count"]) if complete_set_closes_row else 0,
            "live_complete_set_arb_pilot_attempts": count_recent("live_complete_set_arb_pilot_attempts"),
            "polymarket_us_arb_ticks": count_recent("polymarket_us_arb_ticks"),
            "kalshi_arb_ticks": count_recent("kalshi_arb_ticks"),
            "fills": count_recent("fills"),
            "missed_opportunities": count_recent("missed_opportunities"),
            "engine_cycles": count_recent("engine_cycles"),
        }
        latest_cycle_payload = dict(latest_cycle) if latest_cycle else {}
    return {
        "db_present": True,
        "db_path": str(settings.db_path),
        "lookback_minutes": minutes,
        "recent_counts": recent_counts,
        "totals": totals,
        "latest_cycle": latest_cycle_payload,
    }
