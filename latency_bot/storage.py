from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

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


def connect_latency_bot_db(settings: LatencyBotSettings) -> sqlite3.Connection:
    settings.ensure_dirs()
    conn = sqlite3.connect(settings.db_path, timeout=30.0)
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


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
        _ensure_columns(conn, "polymarket_books", _POLYMARKET_BOOK_COLUMNS)
        _ensure_columns(conn, "positions", _POSITION_ENTRY_FEATURE_COLUMNS)
        _ensure_columns(conn, "shadow_positions", _POSITION_ENTRY_FEATURE_COLUMNS)
        _ensure_columns(conn, "shadow_variant_positions", _POSITION_ENTRY_FEATURE_COLUMNS)
        if _should_run_legacy_backfills(conn, user_version):
            _backfill_signal_dimensions(conn, "signals")
            _backfill_signal_dimensions(conn, "shadow_signals")
            _backfill_signal_dimensions(conn, "shadow_variant_signals")
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
                    no_best_bid, no_best_ask, no_bid_depth, no_ask_depth, complete_set_cost, complete_set_edge
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                pb.complete_set_cost, pb.complete_set_edge
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

    locked: list[tuple[datetime, float]] = []
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
            "complete_set_arb_signals_total": int(conn.execute("SELECT COUNT(*) AS count FROM complete_set_arb_signals").fetchone()["count"]),
            "complete_set_arb_positions_total": int(conn.execute("SELECT COUNT(*) AS count FROM complete_set_arb_positions").fetchone()["count"]),
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
            "complete_set_arb_signals": count_recent("complete_set_arb_signals"),
            "complete_set_arb_closes": int(complete_set_closes_row["count"]) if complete_set_closes_row else 0,
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
