from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import signal
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterator
from uuid import uuid4
from zoneinfo import ZoneInfo

from bot.accounting import effective_position_shares, position_mark, realized_pnl_from_trades, side_contract_price
from bot.config import Settings
from bot.dashboard import serve_dashboard
from bot.models import MarketCandidate, Position, Thesis, Vote, utc_now_iso

try:
    from websockets.sync.client import connect as websocket_connect
except Exception:  # noqa: BLE001
    websocket_connect = None


class GeoblockedError(RuntimeError):
    pass


class CycleTimeoutError(RuntimeError):
    pass


_EASTERN_TZ = ZoneInfo("America/New_York")
_CRYPTO_ASSET_MARKERS: dict[str, tuple[str, ...]] = {
    "btc": ("bitcoin", "btc"),
    "eth": ("ethereum", "eth"),
    "sol": ("solana", "sol"),
    "xrp": ("xrp", "ripple"),
    "doge": ("dogecoin", "doge"),
    "ada": ("cardano", "ada"),
    "avax": ("avalanche", "avax"),
    "link": ("chainlink", "link"),
    "ltc": ("litecoin", "ltc"),
    "bnb": ("binance", "bnb"),
    "sui": ("sui",),
    "hype": ("hyperliquid", "hype"),
    "megaeth": ("megaeth",),
    "opensea": ("opensea",),
}
_INTRADAY_MARKET_DETAIL_CACHE: dict[str, dict[str, Any] | None] = {}
_UPDOWN_SEARCH_QUERIES: tuple[str, ...] = (
    "Bitcoin Up or Down",
    "Ethereum Up or Down",
    "Solana Up or Down",
    "XRP Up or Down",
    "Dogecoin Up or Down",
    "BNB Up or Down",
    "Hyperliquid Up or Down",
)


@contextmanager
def _daemon_cycle_timeout(seconds: int, label: str):
    timeout_seconds = max(int(seconds), 0)
    if timeout_seconds <= 0 or not hasattr(signal, "SIGALRM"):
        yield
        return

    def _handle_timeout(signum: int, frame: Any) -> None:  # noqa: ARG001
        raise CycleTimeoutError(f"{label} timed out after {timeout_seconds}s")

    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.getitimer(signal.ITIMER_REAL)
    signal.signal(signal.SIGALRM, _handle_timeout)
    signal.setitimer(signal.ITIMER_REAL, float(timeout_seconds))
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer[0] > 0:
            signal.setitimer(signal.ITIMER_REAL, previous_timer[0], previous_timer[1])


def _parse_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


def _json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")


def _json_load(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    raw = path.read_text(encoding="utf-8")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        if "Extra data" not in str(exc):
            return default
    decoder = json.JSONDecoder()
    index = 0
    payloads: list[Any] = []
    length = len(raw)
    while index < length:
        while index < length and raw[index].isspace():
            index += 1
        if index >= length:
            break
        try:
            payload, next_index = decoder.raw_decode(raw, index)
        except json.JSONDecodeError:
            break
        payloads.append(payload)
        index = next_index
    return payloads[-1] if payloads else default


def _open_intraday_audit_connection(settings: Settings) -> sqlite3.Connection:
    path = settings.intraday_registry_audit_sqlite_path
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ws_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recorded_at TEXT NOT NULL,
            event_type TEXT NOT NULL,
            market_id TEXT,
            asset_id TEXT,
            question TEXT,
            slug TEXT,
            hours_to_resolution REAL,
            best_bid REAL,
            best_ask REAL,
            midpoint REAL,
            bids_depth_usdc REAL,
            asks_depth_usdc REAL,
            min_depth_usdc REAL,
            raw_event_json TEXT,
            market_json TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ws_events_recorded_at ON ws_events(recorded_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ws_events_market_id ON ws_events(market_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ws_events_asset_id ON ws_events(asset_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ws_events_event_type ON ws_events(event_type)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS book_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recorded_at TEXT NOT NULL,
            market_id TEXT,
            token_id TEXT,
            question TEXT,
            slug TEXT,
            asset TEXT,
            is_updown INTEGER NOT NULL,
            hours_to_resolution REAL,
            midpoint REAL,
            best_bid REAL,
            best_ask REAL,
            spread REAL,
            bids_depth_usdc REAL,
            asks_depth_usdc REAL,
            min_depth_usdc REAL,
            source TEXT NOT NULL,
            snapshot_json TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_book_snapshots_recorded_at ON book_snapshots(recorded_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_book_snapshots_market_id ON book_snapshots(market_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_book_snapshots_is_updown ON book_snapshots(is_updown)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS watchlist_transitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recorded_at TEXT NOT NULL,
            market_id TEXT,
            token_id TEXT,
            question TEXT,
            slug TEXT,
            asset TEXT,
            transition_stage TEXT NOT NULL,
            hours_to_resolution REAL,
            midpoint REAL,
            best_bid REAL,
            best_ask REAL,
            spread REAL,
            bids_depth_usdc REAL,
            asks_depth_usdc REAL,
            min_depth_usdc REAL,
            source TEXT NOT NULL,
            transition_json TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_watchlist_transitions_recorded_at ON watchlist_transitions(recorded_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_watchlist_transitions_stage ON watchlist_transitions(transition_stage)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_watchlist_transitions_market_id ON watchlist_transitions(market_id)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS arb_opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recorded_at TEXT NOT NULL,
            market_id TEXT,
            token_id TEXT,
            question TEXT,
            slug TEXT,
            asset TEXT,
            hours_to_resolution REAL,
            midpoint REAL,
            best_bid REAL,
            best_ask REAL,
            spread REAL,
            min_depth_usdc REAL,
            filled_shares REAL,
            yes_contract_price REAL,
            no_contract_price REAL,
            combined_contract_price REAL,
            gross_edge_per_share REAL,
            net_edge_per_share REAL,
            source TEXT NOT NULL,
            opportunity_json TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_arb_opps_recorded_at ON arb_opportunities(recorded_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_arb_opps_market_id ON arb_opportunities(market_id)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS imminent_box_candidates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recorded_at TEXT NOT NULL,
            market_id TEXT,
            token_id TEXT,
            question TEXT,
            slug TEXT,
            asset TEXT,
            hours_to_resolution REAL,
            midpoint REAL,
            best_bid REAL,
            best_ask REAL,
            spread REAL,
            min_depth_usdc REAL,
            meets_min_depth INTEGER,
            filled_shares REAL,
            yes_contract_price REAL,
            no_contract_price REAL,
            combined_contract_price REAL,
            gross_edge_per_share REAL,
            net_edge_per_share REAL,
            source TEXT NOT NULL,
            candidate_json TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_imminent_box_candidates_recorded_at ON imminent_box_candidates(recorded_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_imminent_box_candidates_market_id ON imminent_box_candidates(market_id)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS maker_box_simulations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recorded_at TEXT NOT NULL,
            market_id TEXT,
            token_id TEXT,
            question TEXT,
            slug TEXT,
            asset TEXT,
            hours_to_resolution REAL,
            midpoint REAL,
            best_bid REAL,
            best_ask REAL,
            spread REAL,
            min_depth_usdc REAL,
            filled_shares REAL,
            yes_contract_price REAL,
            no_contract_price REAL,
            combined_contract_price REAL,
            gross_edge_per_share REAL,
            net_edge_per_share REAL,
            source TEXT NOT NULL,
            simulation_json TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_maker_box_sims_recorded_at ON maker_box_simulations(recorded_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_maker_box_sims_market_id ON maker_box_simulations(market_id)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS aggressive_box_simulations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recorded_at TEXT NOT NULL,
            market_id TEXT,
            token_id TEXT,
            question TEXT,
            slug TEXT,
            asset TEXT,
            hours_to_resolution REAL,
            midpoint REAL,
            best_bid REAL,
            best_ask REAL,
            spread REAL,
            min_depth_usdc REAL,
            filled_shares REAL,
            yes_contract_price REAL,
            no_contract_price REAL,
            combined_contract_price REAL,
            gross_edge_per_share REAL,
            expected_gross_edge_per_share REAL,
            net_edge_per_share REAL,
            expected_net_edge_per_share REAL,
            fill_probability REAL,
            maker_fee_per_share REAL,
            estimated_slippage_per_share REAL,
            price_concession_ticks INTEGER,
            tick_size REAL,
            source TEXT NOT NULL,
            simulation_json TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_aggressive_box_sims_recorded_at ON aggressive_box_simulations(recorded_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_aggressive_box_sims_market_id ON aggressive_box_simulations(market_id)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cycle_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recorded_at TEXT NOT NULL,
            runner TEXT NOT NULL,
            execution_state TEXT,
            market_count INTEGER,
            threshold_live_markets INTEGER,
            imminent_box_arb_entries INTEGER,
            positive_gross_edge_count INTEGER,
            positive_net_edge_count INTEGER,
            executed_pairs INTEGER,
            websocket_connected INTEGER,
            websocket_messages INTEGER,
            raw_json TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cycle_stats_recorded_at ON cycle_stats(recorded_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cycle_stats_runner ON cycle_stats(runner)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS discovery_comparator_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            recorded_at TEXT NOT NULL,
            source TEXT NOT NULL,
            total_markets INTEGER NOT NULL,
            live_upcoming_count INTEGER NOT NULL,
            near_term_count INTEGER NOT NULL,
            imminent_count INTEGER NOT NULL,
            closed_count INTEGER NOT NULL,
            inactive_count INTEGER NOT NULL,
            accepting_orders_count INTEGER NOT NULL,
            min_hours_to_resolution REAL,
            max_hours_to_resolution REAL,
            error TEXT,
            sample_questions_json TEXT,
            raw_json TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_discovery_runs_batch_id ON discovery_comparator_runs(batch_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_discovery_runs_recorded_at ON discovery_comparator_runs(recorded_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_discovery_runs_source ON discovery_comparator_runs(source)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS discovery_comparator_markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            recorded_at TEXT NOT NULL,
            source TEXT NOT NULL,
            market_id TEXT,
            question TEXT,
            slug TEXT,
            asset TEXT,
            hours_to_resolution REAL,
            is_live_upcoming INTEGER NOT NULL,
            is_near_term INTEGER NOT NULL,
            is_imminent INTEGER NOT NULL,
            active INTEGER NOT NULL,
            closed INTEGER NOT NULL,
            archived INTEGER NOT NULL,
            accepting_orders INTEGER NOT NULL,
            market_json TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_discovery_markets_batch_id ON discovery_comparator_markets(batch_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_discovery_markets_source ON discovery_comparator_markets(source)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_discovery_markets_market_id ON discovery_comparator_markets(market_id)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_lifecycle (
            market_id TEXT PRIMARY KEY,
            question TEXT,
            slug TEXT,
            asset TEXT,
            is_updown INTEGER NOT NULL,
            first_seen_at TEXT,
            last_seen_at TEXT,
            first_seen_source TEXT,
            last_source TEXT,
            first_hours_to_resolution REAL,
            min_hours_to_resolution REAL,
            first_book_at TEXT,
            first_book_hours_to_resolution REAL,
            first_sub_60m_at TEXT,
            first_sub_60m_hours_to_resolution REAL,
            first_sub_15m_at TEXT,
            first_sub_15m_hours_to_resolution REAL,
            first_threshold_transition_at TEXT,
            first_imminent_transition_at TEXT,
            latest_midpoint REAL,
            latest_spread REAL,
            latest_min_depth_usdc REAL,
            max_min_depth_usdc REAL,
            best_combined_contract_price REAL,
            best_gross_edge_per_share REAL,
            best_net_edge_per_share REAL,
            best_box_seen_at TEXT,
            lifecycle_json TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_market_lifecycle_is_updown ON market_lifecycle(is_updown)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_market_lifecycle_last_seen_at ON market_lifecycle(last_seen_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_market_lifecycle_first_sub_60m_at ON market_lifecycle(first_sub_60m_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_market_lifecycle_first_sub_15m_at ON market_lifecycle(first_sub_15m_at)")
    return conn


@contextmanager
def _intraday_audit_connect(settings: Settings) -> Iterator[sqlite3.Connection]:
    conn = _open_intraday_audit_connection(settings)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _record_intraday_audit_ws_events(settings: Settings, entries: list[dict[str, Any]]) -> None:
    if not entries:
        return
    with _intraday_audit_connect(settings) as conn:
        conn.executemany(
            """
            INSERT INTO ws_events (
                recorded_at, event_type, market_id, asset_id, question, slug,
                hours_to_resolution, best_bid, best_ask, midpoint,
                bids_depth_usdc, asks_depth_usdc, min_depth_usdc,
                raw_event_json, market_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    str(entry.get("recorded_at") or utc_now_iso()),
                    str(entry.get("event_type") or ""),
                    str(entry.get("market_id") or ""),
                    str(entry.get("asset_id") or ""),
                    str(entry.get("question") or ""),
                    str(entry.get("slug") or ""),
                    _as_float(entry.get("hours_to_resolution"), 0.0),
                    _as_float(entry.get("best_bid"), 0.0),
                    _as_float(entry.get("best_ask"), 0.0),
                    _as_float(entry.get("midpoint"), 0.0),
                    _as_float(entry.get("bids_depth_usdc"), 0.0),
                    _as_float(entry.get("asks_depth_usdc"), 0.0),
                    _as_float(entry.get("min_depth_usdc"), 0.0),
                    json.dumps(entry.get("raw_event", {}), sort_keys=True),
                    json.dumps(entry.get("market", {}), sort_keys=True),
                )
                for entry in entries
            ],
        )
    _record_intraday_audit_market_lifecycle(settings, entries, source="ws_event")


def _record_intraday_audit_book_snapshots(settings: Settings, entries: list[dict[str, Any]], *, source: str) -> None:
    if not entries:
        return
    with _intraday_audit_connect(settings) as conn:
        conn.executemany(
            """
            INSERT INTO book_snapshots (
                recorded_at, market_id, token_id, question, slug, asset, is_updown,
                hours_to_resolution, midpoint, best_bid, best_ask, spread,
                bids_depth_usdc, asks_depth_usdc, min_depth_usdc, source, snapshot_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    str(entry.get("seen_at") or utc_now_iso()),
                    str(entry.get("market_id") or entry.get("id") or ""),
                    str(entry.get("token_id") or ""),
                    str(entry.get("question") or ""),
                    str(entry.get("slug") or ""),
                    str(entry.get("asset") or ""),
                    1 if bool(entry.get("is_updown")) else 0,
                    _as_float(entry.get("hours_to_resolution"), 0.0),
                    _as_float(entry.get("midpoint"), 0.0),
                    _as_float(entry.get("best_bid"), 0.0),
                    _as_float(entry.get("best_ask"), 0.0),
                    _as_float(entry.get("spread"), 0.0),
                    _as_float(entry.get("bids_depth_usdc"), 0.0),
                    _as_float(entry.get("asks_depth_usdc"), 0.0),
                    _as_float(entry.get("min_depth_usdc"), 0.0),
                    source,
                    json.dumps(entry, sort_keys=True),
                )
                for entry in entries
            ],
        )
    _record_intraday_audit_market_lifecycle(settings, entries, source=source)


def _record_intraday_audit_watchlist_transitions(
    settings: Settings,
    entries: list[dict[str, Any]],
    *,
    source: str,
) -> None:
    if not entries:
        return
    with _intraday_audit_connect(settings) as conn:
        conn.executemany(
            """
            INSERT INTO watchlist_transitions (
                recorded_at, market_id, token_id, question, slug, asset, transition_stage,
                hours_to_resolution, midpoint, best_bid, best_ask, spread,
                bids_depth_usdc, asks_depth_usdc, min_depth_usdc, source, transition_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    str(entry.get("seen_at") or utc_now_iso()),
                    str(entry.get("market_id") or entry.get("id") or ""),
                    str(entry.get("token_id") or ""),
                    str(entry.get("question") or ""),
                    str(entry.get("slug") or ""),
                    str(entry.get("asset") or ""),
                    str(entry.get("transition_stage") or ""),
                    _as_float(entry.get("hours_to_resolution"), 0.0),
                    _as_float(entry.get("midpoint"), 0.0),
                    _as_float(entry.get("best_bid"), 0.0),
                    _as_float(entry.get("best_ask"), 0.0),
                    _as_float(entry.get("spread"), 0.0),
                    _as_float(entry.get("bids_depth_usdc"), 0.0),
                    _as_float(entry.get("asks_depth_usdc"), 0.0),
                    _as_float(entry.get("min_depth_usdc"), 0.0),
                    source,
                    json.dumps(entry, sort_keys=True),
                )
                for entry in entries
            ],
        )
    _record_intraday_audit_market_lifecycle(settings, entries, source=source)


def _record_intraday_audit_market_lifecycle(settings: Settings, entries: list[dict[str, Any]], *, source: str) -> None:
    if not entries:
        return

    def _earlier(a: str | None, b: str | None) -> str | None:
        if not a:
            return b
        if not b:
            return a
        return a if a <= b else b

    def _later(a: str | None, b: str | None) -> str | None:
        if not a:
            return b
        if not b:
            return a
        return a if a >= b else b

    def _text(value: Any) -> str:
        return str(value or "").strip()

    def _f(value: Any) -> float | None:
        return None if value in (None, "") else _as_float(value)

    with _intraday_audit_connect(settings) as conn:
        for entry in entries:
            market_id = _text(entry.get("market_id") or entry.get("id"))
            if not market_id:
                continue
            question = _text(entry.get("question"))
            slug = _text(entry.get("slug"))
            asset = _text(entry.get("asset"))
            is_updown = bool(
                entry.get("is_updown")
                if "is_updown" in entry
                else _is_five_minute_updown_market(question, slug)
            )
            recorded_at = _text(entry.get("recorded_at") or entry.get("seen_at") or utc_now_iso())
            hours_to_resolution = _f(entry.get("hours_to_resolution"))
            midpoint = _f(entry.get("midpoint"))
            spread = _f(entry.get("spread"))
            min_depth = _f(entry.get("min_depth_usdc"))
            transition_stage = _text(entry.get("transition_stage"))
            combined_contract_price = _f(entry.get("combined_contract_price"))
            gross_edge = _f(entry.get("gross_edge_per_share"))
            net_edge = _f(entry.get("net_edge_per_share"))
            is_book_source = source.startswith("official_clob_book")

            existing = conn.execute("SELECT * FROM market_lifecycle WHERE market_id = ?", (market_id,)).fetchone()
            if existing is None:
                row: dict[str, Any] = {
                    "market_id": market_id,
                    "question": question,
                    "slug": slug,
                    "asset": asset,
                    "is_updown": 1 if is_updown else 0,
                    "first_seen_at": recorded_at,
                    "last_seen_at": recorded_at,
                    "first_seen_source": source,
                    "last_source": source,
                    "first_hours_to_resolution": hours_to_resolution,
                    "min_hours_to_resolution": hours_to_resolution,
                    "first_book_at": recorded_at if is_book_source else None,
                    "first_book_hours_to_resolution": hours_to_resolution if is_book_source else None,
                    "first_sub_60m_at": recorded_at if hours_to_resolution is not None and 0.0 < hours_to_resolution <= 1.0 else None,
                    "first_sub_60m_hours_to_resolution": hours_to_resolution if hours_to_resolution is not None and 0.0 < hours_to_resolution <= 1.0 else None,
                    "first_sub_15m_at": recorded_at if hours_to_resolution is not None and 0.0 < hours_to_resolution <= 0.25 else None,
                    "first_sub_15m_hours_to_resolution": hours_to_resolution if hours_to_resolution is not None and 0.0 < hours_to_resolution <= 0.25 else None,
                    "first_threshold_transition_at": recorded_at if transition_stage == "threshold" else None,
                    "first_imminent_transition_at": recorded_at if transition_stage == "imminent_updown" else None,
                    "latest_midpoint": midpoint,
                    "latest_spread": spread,
                    "latest_min_depth_usdc": min_depth,
                    "max_min_depth_usdc": min_depth,
                    "best_combined_contract_price": combined_contract_price,
                    "best_gross_edge_per_share": gross_edge,
                    "best_net_edge_per_share": net_edge,
                    "best_box_seen_at": recorded_at if any(v is not None for v in (combined_contract_price, gross_edge, net_edge)) else None,
                    "lifecycle_json": json.dumps(entry, sort_keys=True),
                }
            else:
                row = dict(existing)
                if question:
                    row["question"] = question
                if slug:
                    row["slug"] = slug
                if asset:
                    row["asset"] = asset
                row["is_updown"] = 1 if (is_updown or bool(row.get("is_updown"))) else 0
                row["first_seen_at"] = _earlier(row.get("first_seen_at"), recorded_at)
                row["last_seen_at"] = _later(row.get("last_seen_at"), recorded_at)
                if not row.get("first_seen_source"):
                    row["first_seen_source"] = source
                row["last_source"] = source
                if row.get("first_seen_at") == recorded_at and hours_to_resolution is not None:
                    row["first_hours_to_resolution"] = hours_to_resolution
                if hours_to_resolution is not None:
                    current_min = _f(row.get("min_hours_to_resolution"))
                    row["min_hours_to_resolution"] = hours_to_resolution if current_min is None else min(current_min, hours_to_resolution)
                if is_book_source and not row.get("first_book_at"):
                    row["first_book_at"] = recorded_at
                    row["first_book_hours_to_resolution"] = hours_to_resolution
                if hours_to_resolution is not None and 0.0 < hours_to_resolution <= 1.0 and not row.get("first_sub_60m_at"):
                    row["first_sub_60m_at"] = recorded_at
                    row["first_sub_60m_hours_to_resolution"] = hours_to_resolution
                if hours_to_resolution is not None and 0.0 < hours_to_resolution <= 0.25 and not row.get("first_sub_15m_at"):
                    row["first_sub_15m_at"] = recorded_at
                    row["first_sub_15m_hours_to_resolution"] = hours_to_resolution
                if transition_stage == "threshold" and not row.get("first_threshold_transition_at"):
                    row["first_threshold_transition_at"] = recorded_at
                if transition_stage == "imminent_updown" and not row.get("first_imminent_transition_at"):
                    row["first_imminent_transition_at"] = recorded_at
                if row.get("last_seen_at") == recorded_at:
                    if midpoint is not None:
                        row["latest_midpoint"] = midpoint
                    if spread is not None:
                        row["latest_spread"] = spread
                    if min_depth is not None:
                        row["latest_min_depth_usdc"] = min_depth
                    row["lifecycle_json"] = json.dumps(entry, sort_keys=True)
                current_max_depth = _f(row.get("max_min_depth_usdc"))
                if min_depth is not None:
                    row["max_min_depth_usdc"] = min_depth if current_max_depth is None else max(current_max_depth, min_depth)
                best_updated = False
                current_combined = _f(row.get("best_combined_contract_price"))
                if combined_contract_price is not None and (current_combined is None or combined_contract_price < current_combined):
                    row["best_combined_contract_price"] = combined_contract_price
                    best_updated = True
                current_gross = _f(row.get("best_gross_edge_per_share"))
                if gross_edge is not None and (current_gross is None or gross_edge > current_gross):
                    row["best_gross_edge_per_share"] = gross_edge
                    best_updated = True
                current_net = _f(row.get("best_net_edge_per_share"))
                if net_edge is not None and (current_net is None or net_edge > current_net):
                    row["best_net_edge_per_share"] = net_edge
                    best_updated = True
                if best_updated:
                    row["best_box_seen_at"] = recorded_at

            conn.execute(
                """
                INSERT INTO market_lifecycle (
                    market_id, question, slug, asset, is_updown, first_seen_at, last_seen_at,
                    first_seen_source, last_source, first_hours_to_resolution, min_hours_to_resolution,
                    first_book_at, first_book_hours_to_resolution, first_sub_60m_at, first_sub_60m_hours_to_resolution,
                    first_sub_15m_at, first_sub_15m_hours_to_resolution, first_threshold_transition_at,
                    first_imminent_transition_at, latest_midpoint, latest_spread, latest_min_depth_usdc,
                    max_min_depth_usdc, best_combined_contract_price, best_gross_edge_per_share,
                    best_net_edge_per_share, best_box_seen_at, lifecycle_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(market_id) DO UPDATE SET
                    question=excluded.question,
                    slug=excluded.slug,
                    asset=excluded.asset,
                    is_updown=excluded.is_updown,
                    first_seen_at=excluded.first_seen_at,
                    last_seen_at=excluded.last_seen_at,
                    first_seen_source=excluded.first_seen_source,
                    last_source=excluded.last_source,
                    first_hours_to_resolution=excluded.first_hours_to_resolution,
                    min_hours_to_resolution=excluded.min_hours_to_resolution,
                    first_book_at=excluded.first_book_at,
                    first_book_hours_to_resolution=excluded.first_book_hours_to_resolution,
                    first_sub_60m_at=excluded.first_sub_60m_at,
                    first_sub_60m_hours_to_resolution=excluded.first_sub_60m_hours_to_resolution,
                    first_sub_15m_at=excluded.first_sub_15m_at,
                    first_sub_15m_hours_to_resolution=excluded.first_sub_15m_hours_to_resolution,
                    first_threshold_transition_at=excluded.first_threshold_transition_at,
                    first_imminent_transition_at=excluded.first_imminent_transition_at,
                    latest_midpoint=excluded.latest_midpoint,
                    latest_spread=excluded.latest_spread,
                    latest_min_depth_usdc=excluded.latest_min_depth_usdc,
                    max_min_depth_usdc=excluded.max_min_depth_usdc,
                    best_combined_contract_price=excluded.best_combined_contract_price,
                    best_gross_edge_per_share=excluded.best_gross_edge_per_share,
                    best_net_edge_per_share=excluded.best_net_edge_per_share,
                    best_box_seen_at=excluded.best_box_seen_at,
                    lifecycle_json=excluded.lifecycle_json
                """,
                (
                    row["market_id"],
                    row["question"],
                    row["slug"],
                    row["asset"],
                    row["is_updown"],
                    row["first_seen_at"],
                    row["last_seen_at"],
                    row["first_seen_source"],
                    row["last_source"],
                    row["first_hours_to_resolution"],
                    row["min_hours_to_resolution"],
                    row["first_book_at"],
                    row["first_book_hours_to_resolution"],
                    row["first_sub_60m_at"],
                    row["first_sub_60m_hours_to_resolution"],
                    row["first_sub_15m_at"],
                    row["first_sub_15m_hours_to_resolution"],
                    row["first_threshold_transition_at"],
                    row["first_imminent_transition_at"],
                    row["latest_midpoint"],
                    row["latest_spread"],
                    row["latest_min_depth_usdc"],
                    row["max_min_depth_usdc"],
                    row["best_combined_contract_price"],
                    row["best_gross_edge_per_share"],
                    row["best_net_edge_per_share"],
                    row["best_box_seen_at"],
                    row["lifecycle_json"],
                ),
            )


def _record_intraday_audit_arb_opportunities(settings: Settings, entries: list[dict[str, Any]], *, source: str) -> None:
    if not entries:
        return
    with _intraday_audit_connect(settings) as conn:
        conn.executemany(
            """
            INSERT INTO arb_opportunities (
                recorded_at, market_id, token_id, question, slug, asset,
                hours_to_resolution, midpoint, best_bid, best_ask, spread, min_depth_usdc,
                filled_shares, yes_contract_price, no_contract_price, combined_contract_price,
                gross_edge_per_share, net_edge_per_share, source, opportunity_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    str(entry.get("seen_at") or utc_now_iso()),
                    str(entry.get("market_id") or entry.get("id") or ""),
                    str(entry.get("token_id") or ""),
                    str(entry.get("question") or ""),
                    str(entry.get("slug") or ""),
                    str(entry.get("asset") or ""),
                    _as_float(entry.get("hours_to_resolution"), 0.0),
                    _as_float(entry.get("midpoint"), 0.0),
                    _as_float(entry.get("best_bid"), 0.0),
                    _as_float(entry.get("best_ask"), 0.0),
                    _as_float(entry.get("spread"), 0.0),
                    _as_float(entry.get("min_depth_usdc"), 0.0),
                    _as_float(entry.get("filled_shares"), 0.0),
                    _as_float(entry.get("yes_contract_price"), 0.0),
                    _as_float(entry.get("no_contract_price"), 0.0),
                    _as_float(entry.get("combined_contract_price"), 0.0),
                    _as_float(entry.get("gross_edge_per_share"), 0.0),
                    _as_float(entry.get("net_edge_per_share"), 0.0),
                    source,
                    json.dumps(entry, sort_keys=True),
                )
                for entry in entries
            ],
        )
    _record_intraday_audit_market_lifecycle(settings, entries, source=source)


def _record_intraday_audit_imminent_candidates(settings: Settings, entries: list[dict[str, Any]], *, source: str) -> None:
    if not entries:
        return
    with _intraday_audit_connect(settings) as conn:
        conn.executemany(
            """
            INSERT INTO imminent_box_candidates (
                recorded_at, market_id, token_id, question, slug, asset,
                hours_to_resolution, midpoint, best_bid, best_ask, spread, min_depth_usdc,
                meets_min_depth, filled_shares, yes_contract_price, no_contract_price,
                combined_contract_price, gross_edge_per_share, net_edge_per_share, source, candidate_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    str(entry.get("seen_at") or utc_now_iso()),
                    str(entry.get("market_id") or entry.get("id") or ""),
                    str(entry.get("token_id") or ""),
                    str(entry.get("question") or ""),
                    str(entry.get("slug") or ""),
                    str(entry.get("asset") or ""),
                    _as_float(entry.get("hours_to_resolution"), 0.0),
                    _as_float(entry.get("midpoint"), 0.0),
                    _as_float(entry.get("best_bid"), 0.0),
                    _as_float(entry.get("best_ask"), 0.0),
                    _as_float(entry.get("spread"), 0.0),
                    _as_float(entry.get("min_depth_usdc"), 0.0),
                    1 if bool(entry.get("meets_min_depth")) else 0,
                    _as_float(entry.get("filled_shares"), 0.0),
                    _as_float(entry.get("yes_contract_price"), 0.0),
                    _as_float(entry.get("no_contract_price"), 0.0),
                    _as_float(entry.get("combined_contract_price"), 0.0),
                    _as_float(entry.get("gross_edge_per_share"), 0.0),
                    _as_float(entry.get("net_edge_per_share"), 0.0),
                    source,
                    json.dumps(entry, sort_keys=True),
                )
                for entry in entries
            ],
        )


def _record_intraday_audit_maker_box_simulations(settings: Settings, entries: list[dict[str, Any]], *, source: str) -> None:
    if not entries:
        return
    with _intraday_audit_connect(settings) as conn:
        conn.executemany(
            """
            INSERT INTO maker_box_simulations (
                recorded_at, market_id, token_id, question, slug, asset,
                hours_to_resolution, midpoint, best_bid, best_ask, spread,
                min_depth_usdc, filled_shares, yes_contract_price, no_contract_price,
                combined_contract_price, gross_edge_per_share, net_edge_per_share, source, simulation_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    str(entry.get("seen_at") or utc_now_iso()),
                    str(entry.get("market_id", "")),
                    str(entry.get("token_id", "")),
                    str(entry.get("question", "")),
                    str(entry.get("slug", "")),
                    str(entry.get("asset", "")),
                    _as_float(entry.get("hours_to_resolution"), 0.0),
                    _as_float(entry.get("midpoint"), 0.0),
                    _as_float(entry.get("best_bid"), 0.0),
                    _as_float(entry.get("best_ask"), 0.0),
                    _as_float(entry.get("spread"), 0.0),
                    _as_float(entry.get("min_depth_usdc"), 0.0),
                    _as_float(entry.get("maker_filled_shares"), 0.0),
                    _as_float(entry.get("maker_yes_contract_price"), 0.0),
                    _as_float(entry.get("maker_no_contract_price"), 0.0),
                    _as_float(entry.get("maker_combined_contract_price"), 0.0),
                    _as_float(entry.get("maker_gross_edge_per_share"), 0.0),
                    _as_float(entry.get("maker_net_edge_per_share"), 0.0),
                    source,
                    json.dumps(entry, sort_keys=True),
                )
                for entry in entries
            ],
        )
    _record_intraday_audit_market_lifecycle(settings, entries, source=source)


def _record_intraday_audit_aggressive_box_simulations(settings: Settings, entries: list[dict[str, Any]], *, source: str) -> None:
    if not entries:
        return
    with _intraday_audit_connect(settings) as conn:
        conn.executemany(
            """
            INSERT INTO aggressive_box_simulations (
                recorded_at, market_id, token_id, question, slug, asset,
                hours_to_resolution, midpoint, best_bid, best_ask, spread,
                min_depth_usdc, filled_shares, yes_contract_price, no_contract_price,
                combined_contract_price, gross_edge_per_share, expected_gross_edge_per_share,
                net_edge_per_share, expected_net_edge_per_share, fill_probability,
                maker_fee_per_share, estimated_slippage_per_share, price_concession_ticks,
                tick_size, source, simulation_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    str(entry.get("seen_at") or utc_now_iso()),
                    str(entry.get("market_id", "")),
                    str(entry.get("token_id", "")),
                    str(entry.get("question", "")),
                    str(entry.get("slug", "")),
                    str(entry.get("asset", "")),
                    _as_float(entry.get("hours_to_resolution"), 0.0),
                    _as_float(entry.get("midpoint"), 0.0),
                    _as_float(entry.get("best_bid"), 0.0),
                    _as_float(entry.get("best_ask"), 0.0),
                    _as_float(entry.get("spread"), 0.0),
                    _as_float(entry.get("min_depth_usdc"), 0.0),
                    _as_float(entry.get("aggressive_filled_shares"), 0.0),
                    _as_float(entry.get("aggressive_yes_contract_price"), 0.0),
                    _as_float(entry.get("aggressive_no_contract_price"), 0.0),
                    _as_float(entry.get("aggressive_combined_contract_price"), 0.0),
                    _as_float(entry.get("aggressive_gross_edge_per_share"), 0.0),
                    _as_float(entry.get("aggressive_expected_gross_edge_per_share"), 0.0),
                    _as_float(entry.get("aggressive_net_edge_per_share"), 0.0),
                    _as_float(entry.get("aggressive_expected_net_edge_per_share"), 0.0),
                    _as_float(entry.get("aggressive_fill_probability"), 0.0),
                    _as_float(entry.get("aggressive_maker_fee_per_share"), 0.0),
                    _as_float(entry.get("aggressive_estimated_slippage_per_share"), 0.0),
                    int(entry.get("aggressive_price_concession_ticks", 0) or 0),
                    _as_float(entry.get("aggressive_tick_size"), 0.0),
                    source,
                    json.dumps(entry, sort_keys=True),
                )
                for entry in entries
            ],
        )


def _record_intraday_audit_cycle(settings: Settings, *, runner: str, payload: dict[str, Any]) -> None:
    with _intraday_audit_connect(settings) as conn:
        conn.execute(
            """
            INSERT INTO cycle_stats (
                recorded_at, runner, execution_state, market_count, threshold_live_markets,
                imminent_box_arb_entries, positive_gross_edge_count, positive_net_edge_count,
                executed_pairs, websocket_connected, websocket_messages, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                utc_now_iso(),
                runner,
                str(payload.get("execution_state") or payload.get("box_execution_state") or ""),
                int(payload.get("market_count", 0) or 0),
                int(payload.get("threshold_live_markets", 0) or 0),
                int(payload.get("imminent_box_arb_entries", 0) or 0),
                int(payload.get("positive_gross_edge_count", 0) or 0),
                int(payload.get("positive_net_edge_count", 0) or 0),
                int(payload.get("executed_pairs", 0) or 0),
                1 if bool(payload.get("websocket_connected")) else 0,
                int(payload.get("websocket_messages", 0) or 0),
                json.dumps(payload, sort_keys=True),
            ),
        )


def _discovery_comparator_market_row(settings: Settings, market: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(market, dict):
        return None
    question = _extract_question(market)
    slug = _extract_slug(market)
    if not _intraday_market_looks_updown(question, slug):
        return None
    asset = _short_crypto_market_asset(question, slug)
    if asset is None:
        return None
    market_id = _extract_market_id(market) or slug or question
    hours = _extract_short_market_hours_to_resolution(market)
    active = bool(_first(market, "active", default=True))
    closed = bool(_first(market, "closed", default=False))
    archived = bool(_first(market, "archived", default=False))
    accepting_orders = bool(_first(market, "acceptingOrders", default=True))
    is_live_upcoming = active and not closed and not archived and accepting_orders and hours > 0.0
    near_term_hours = max(int(settings.intraday_registry_watchlist_max_minutes_to_resolution), 1) / 60.0
    imminent_hours = max(int(settings.intraday_registry_imminent_max_minutes_to_resolution), 1) / 60.0
    return {
        "market_id": str(market_id),
        "question": question,
        "slug": slug,
        "asset": asset,
        "hours_to_resolution": round(hours, 6),
        "is_live_upcoming": bool(is_live_upcoming),
        "is_near_term": bool(is_live_upcoming and 0.0 < hours <= near_term_hours),
        "is_imminent": bool(is_live_upcoming and 0.0 < hours <= imminent_hours),
        "active": active,
        "closed": closed,
        "archived": archived,
        "accepting_orders": accepting_orders,
        "market_json": json.dumps(market, sort_keys=True),
    }


def _summarize_discovery_comparator_rows(rows: list[dict[str, Any]], *, source: str, error: str | None = None) -> dict[str, Any]:
    hours = [float(row["hours_to_resolution"]) for row in rows if row.get("hours_to_resolution") not in (None, "")]
    ordered = sorted(
        rows,
        key=lambda row: (float(row.get("hours_to_resolution", 10**9) or 10**9), str(row.get("question", ""))),
    )
    return {
        "source": source,
        "error": error,
        "total_markets": len(rows),
        "live_upcoming_count": sum(1 for row in rows if row.get("is_live_upcoming")),
        "near_term_count": sum(1 for row in rows if row.get("is_near_term")),
        "imminent_count": sum(1 for row in rows if row.get("is_imminent")),
        "closed_count": sum(1 for row in rows if row.get("closed")),
        "inactive_count": sum(1 for row in rows if not row.get("active")),
        "accepting_orders_count": sum(1 for row in rows if row.get("accepting_orders")),
        "min_hours_to_resolution": min(hours) if hours else None,
        "max_hours_to_resolution": max(hours) if hours else None,
        "sample_questions": [str(row.get("question", "")) for row in ordered[:5]],
        "markets": rows,
    }


def _record_intraday_discovery_comparator_batch(settings: Settings, *, batch_id: str, snapshots: list[dict[str, Any]]) -> None:
    if not snapshots:
        return
    recorded_at = utc_now_iso()
    with _intraday_audit_connect(settings) as conn:
        conn.executemany(
            """
            INSERT INTO discovery_comparator_runs (
                batch_id, recorded_at, source, total_markets, live_upcoming_count, near_term_count,
                imminent_count, closed_count, inactive_count, accepting_orders_count,
                min_hours_to_resolution, max_hours_to_resolution, error, sample_questions_json, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    batch_id,
                    recorded_at,
                    str(snapshot.get("source") or ""),
                    int(snapshot.get("total_markets", 0) or 0),
                    int(snapshot.get("live_upcoming_count", 0) or 0),
                    int(snapshot.get("near_term_count", 0) or 0),
                    int(snapshot.get("imminent_count", 0) or 0),
                    int(snapshot.get("closed_count", 0) or 0),
                    int(snapshot.get("inactive_count", 0) or 0),
                    int(snapshot.get("accepting_orders_count", 0) or 0),
                    snapshot.get("min_hours_to_resolution"),
                    snapshot.get("max_hours_to_resolution"),
                    str(snapshot.get("error") or "") or None,
                    json.dumps(snapshot.get("sample_questions", []), sort_keys=True),
                    json.dumps({k: v for k, v in snapshot.items() if k != "markets"}, sort_keys=True),
                )
                for snapshot in snapshots
            ],
        )
        conn.executemany(
            """
            INSERT INTO discovery_comparator_markets (
                batch_id, recorded_at, source, market_id, question, slug, asset,
                hours_to_resolution, is_live_upcoming, is_near_term, is_imminent,
                active, closed, archived, accepting_orders, market_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    batch_id,
                    recorded_at,
                    str(snapshot.get("source") or ""),
                    str(row.get("market_id") or ""),
                    str(row.get("question") or ""),
                    str(row.get("slug") or ""),
                    str(row.get("asset") or ""),
                    _as_float(row.get("hours_to_resolution"), 0.0),
                    1 if bool(row.get("is_live_upcoming")) else 0,
                    1 if bool(row.get("is_near_term")) else 0,
                    1 if bool(row.get("is_imminent")) else 0,
                    1 if bool(row.get("active")) else 0,
                    1 if bool(row.get("closed")) else 0,
                    1 if bool(row.get("archived")) else 0,
                    1 if bool(row.get("accepting_orders")) else 0,
                    str(row.get("market_json") or "{}"),
                )
                for snapshot in snapshots
                for row in snapshot.get("markets", [])
            ],
        )


def compare_intraday_discovery_sources(settings: Settings, cli: PolymarketCLI) -> dict[str, Any]:
    limit = min(_subscription_seed_fetch_limit(settings), 200)
    batch_id = utc_now_iso()
    snapshots: list[dict[str, Any]] = []

    try:
        payload = _gamma_search_updown_market_payload(limit, settings)
        rows = [
            row
            for market in payload.get("matches", [])
            for row in [_discovery_comparator_market_row(settings, market)]
            if row is not None
        ]
        summary = _summarize_discovery_comparator_rows(rows, source="gamma_search_updown")
        summary["search_match_count"] = int(payload.get("match_count", 0) or 0)
        summary["search_upcoming_count"] = int(payload.get("upcoming_count", 0) or 0)
        snapshots.append(summary)
    except Exception as exc:  # noqa: BLE001
        snapshots.append(_summarize_discovery_comparator_rows([], source="gamma_search_updown", error=str(exc)))

    try:
        tag_refs = _gamma_updown_tag_candidates(limit, settings)
        seen_market_ids: set[str] = set()
        rows: list[dict[str, Any]] = []
        for tag_ref in tag_refs:
            for market in _fetch_gamma_events_by_tag_ref(tag_ref, settings, limit=limit):
                row = _discovery_comparator_market_row(settings, market)
                if row is None:
                    continue
                market_id = str(row.get("market_id") or "")
                if market_id and market_id in seen_market_ids:
                    continue
                if market_id:
                    seen_market_ids.add(market_id)
                rows.append(row)
        summary = _summarize_discovery_comparator_rows(rows, source="gamma_events_by_tag")
        summary["tag_candidate_count"] = len(tag_refs)
        summary["tag_candidates"] = [dict(ref) for ref in tag_refs[:8]]
        snapshots.append(summary)
    except Exception as exc:  # noqa: BLE001
        snapshots.append(_summarize_discovery_comparator_rows([], source="gamma_events_by_tag", error=str(exc)))

    try:
        rows = [
            row
            for market in _fetch_gamma_intraday_markets_page(min(limit, 500), 0)
            for row in [_discovery_comparator_market_row(settings, market)]
            if row is not None
        ]
        snapshots.append(_summarize_discovery_comparator_rows(rows, source="gamma_active_intraday"))
    except Exception as exc:  # noqa: BLE001
        snapshots.append(_summarize_discovery_comparator_rows([], source="gamma_active_intraday", error=str(exc)))

    try:
        rows = [
            row
            for market in cli.list_markets(limit)
            for row in [_discovery_comparator_market_row(settings, market)]
            if row is not None
        ]
        snapshots.append(_summarize_discovery_comparator_rows(rows, source="cli_markets"))
    except Exception as exc:  # noqa: BLE001
        snapshots.append(_summarize_discovery_comparator_rows([], source="cli_markets", error=str(exc)))

    _record_intraday_discovery_comparator_batch(settings, batch_id=batch_id, snapshots=snapshots)

    overlaps: dict[str, int] = {"live_overlap_count": 0, "near_term_overlap_count": 0, "imminent_overlap_count": 0}
    market_sources: dict[str, set[str]] = {}
    near_term_sources: dict[str, set[str]] = {}
    imminent_sources: dict[str, set[str]] = {}
    for snapshot in snapshots:
        source = str(snapshot.get("source") or "")
        for row in snapshot.get("markets", []):
            market_id = str(row.get("market_id") or "")
            if not market_id:
                continue
            if row.get("is_live_upcoming"):
                market_sources.setdefault(market_id, set()).add(source)
            if row.get("is_near_term"):
                near_term_sources.setdefault(market_id, set()).add(source)
            if row.get("is_imminent"):
                imminent_sources.setdefault(market_id, set()).add(source)
    overlaps["live_overlap_count"] = sum(1 for sources in market_sources.values() if len(sources) > 1)
    overlaps["near_term_overlap_count"] = sum(1 for sources in near_term_sources.values() if len(sources) > 1)
    overlaps["imminent_overlap_count"] = sum(1 for sources in imminent_sources.values() if len(sources) > 1)

    return {
        "batch_id": batch_id,
        "recorded_at": utc_now_iso(),
        "limit": limit,
        "sources": [{k: v for k, v in snapshot.items() if k != "markets"} for snapshot in snapshots],
        **overlaps,
        "db_path": str(settings.intraday_registry_audit_sqlite_path),
    }


def summarize_intraday_audit(settings: Settings, lookback_minutes: int = 60) -> dict[str, Any]:
    lookback = max(int(lookback_minutes), 1)
    since = (datetime.now(timezone.utc) - timedelta(minutes=lookback)).isoformat().replace("+00:00", "Z")
    with _intraday_audit_connect(settings) as conn:
        ws_total = conn.execute("SELECT COUNT(*) FROM ws_events WHERE recorded_at >= ?", (since,)).fetchone()[0]
        books_total = conn.execute("SELECT COUNT(*) FROM book_snapshots WHERE recorded_at >= ?", (since,)).fetchone()[0]
        arb_total = conn.execute("SELECT COUNT(*) FROM arb_opportunities WHERE recorded_at >= ?", (since,)).fetchone()[0]
        raw_imminent_total = conn.execute(
            "SELECT COUNT(*), COALESCE(MAX(recorded_at), '') FROM imminent_box_candidates WHERE recorded_at >= ?",
            (since,),
        ).fetchone()
        maker_total = conn.execute(
            "SELECT COUNT(*), COALESCE(MAX(recorded_at), '') FROM maker_box_simulations WHERE recorded_at >= ?",
            (since,),
        ).fetchone()
        aggressive_total = conn.execute(
            "SELECT COUNT(*), COALESCE(MAX(recorded_at), '') FROM aggressive_box_simulations WHERE recorded_at >= ?",
            (since,),
        ).fetchone()
        watchlist_imminent_total = conn.execute(
            """
            SELECT COUNT(*), COALESCE(MAX(recorded_at), '')
            FROM watchlist_transitions
            WHERE recorded_at >= ? AND transition_stage = 'imminent_updown'
            """,
            (since,),
        ).fetchone()
        positive_gross = conn.execute(
            "SELECT COUNT(*) FROM arb_opportunities WHERE recorded_at >= ? AND gross_edge_per_share > 0",
            (since,),
        ).fetchone()[0]
        positive_net = conn.execute(
            "SELECT COUNT(*) FROM arb_opportunities WHERE recorded_at >= ? AND net_edge_per_share > 0",
            (since,),
        ).fetchone()[0]
        maker_positive_gross = conn.execute(
            """
            SELECT COUNT(*)
            FROM maker_box_simulations
            WHERE recorded_at >= ?
              AND filled_shares > 0
              AND combined_contract_price > 0
              AND gross_edge_per_share > 0
            """,
            (since,),
        ).fetchone()[0]
        maker_positive_net = conn.execute(
            """
            SELECT COUNT(*)
            FROM maker_box_simulations
            WHERE recorded_at >= ?
              AND filled_shares > 0
              AND combined_contract_price > 0
              AND net_edge_per_share > 0
            """,
            (since,),
        ).fetchone()[0]
        aggressive_positive_gross = conn.execute(
            """
            SELECT COUNT(*)
            FROM aggressive_box_simulations
            WHERE recorded_at >= ?
              AND filled_shares > 0
              AND combined_contract_price > 0
              AND gross_edge_per_share > 0
            """,
            (since,),
        ).fetchone()[0]
        aggressive_positive_net = conn.execute(
            """
            SELECT COUNT(*)
            FROM aggressive_box_simulations
            WHERE recorded_at >= ?
              AND filled_shares > 0
              AND combined_contract_price > 0
              AND net_edge_per_share > 0
            """,
            (since,),
        ).fetchone()[0]
        aggressive_positive_expected_net = conn.execute(
            """
            SELECT COUNT(*)
            FROM aggressive_box_simulations
            WHERE recorded_at >= ?
              AND filled_shares > 0
              AND combined_contract_price > 0
              AND expected_net_edge_per_share > 0
            """,
            (since,),
        ).fetchone()[0]
        max_edges = conn.execute(
            """
            SELECT
                COALESCE(MAX(gross_edge_per_share), 0),
                COALESCE(MAX(net_edge_per_share), 0)
            FROM arb_opportunities
            WHERE recorded_at >= ?
            """,
            (since,),
        ).fetchone()
        maker_max_edges = conn.execute(
            """
            SELECT
                COALESCE(MAX(gross_edge_per_share), 0),
                COALESCE(MAX(net_edge_per_share), 0)
            FROM maker_box_simulations
            WHERE recorded_at >= ?
              AND filled_shares > 0
              AND combined_contract_price > 0
            """,
            (since,),
        ).fetchone()
        aggressive_max_edges = conn.execute(
            """
            SELECT
                COALESCE(MAX(gross_edge_per_share), 0),
                COALESCE(MAX(net_edge_per_share), 0),
                COALESCE(MAX(expected_net_edge_per_share), 0)
            FROM aggressive_box_simulations
            WHERE recorded_at >= ?
              AND filled_shares > 0
              AND combined_contract_price > 0
            """,
            (since,),
        ).fetchone()
        cycles = conn.execute(
            """
            SELECT COUNT(*), COALESCE(MAX(recorded_at), '')
            FROM cycle_stats
            WHERE recorded_at >= ? AND runner = 'intraday_registry'
            """,
            (since,),
        ).fetchone()
    return {
        "lookback_minutes": lookback,
        "since": since,
        "ws_event_count": int(ws_total or 0),
        "book_snapshot_count": int(books_total or 0),
        "arb_opportunity_count": int(arb_total or 0),
        "raw_imminent_candidate_count": int((raw_imminent_total or (0, ""))[0] or 0),
        "maker_box_simulation_count": int((maker_total or (0, ""))[0] or 0),
        "aggressive_box_simulation_count": int((aggressive_total or (0, ""))[0] or 0),
        "watchlist_imminent_candidate_count": int((watchlist_imminent_total or (0, ""))[0] or 0),
        "positive_gross_edge_count": int(positive_gross or 0),
        "positive_net_edge_count": int(positive_net or 0),
        "maker_positive_gross_edge_count": int(maker_positive_gross or 0),
        "maker_positive_net_edge_count": int(maker_positive_net or 0),
        "aggressive_positive_gross_edge_count": int(aggressive_positive_gross or 0),
        "aggressive_positive_net_edge_count": int(aggressive_positive_net or 0),
        "aggressive_positive_expected_net_edge_count": int(aggressive_positive_expected_net or 0),
        "max_gross_edge_per_share": round(_as_float(max_edges[0] if max_edges else 0.0), 6),
        "max_net_edge_per_share": round(_as_float(max_edges[1] if max_edges else 0.0), 6),
        "maker_max_gross_edge_per_share": round(_as_float(maker_max_edges[0] if maker_max_edges else 0.0), 6),
        "maker_max_net_edge_per_share": round(_as_float(maker_max_edges[1] if maker_max_edges else 0.0), 6),
        "aggressive_max_gross_edge_per_share": round(_as_float(aggressive_max_edges[0] if aggressive_max_edges else 0.0), 6),
        "aggressive_max_net_edge_per_share": round(_as_float(aggressive_max_edges[1] if aggressive_max_edges else 0.0), 6),
        "aggressive_max_expected_net_edge_per_share": round(_as_float(aggressive_max_edges[2] if aggressive_max_edges else 0.0), 6),
        "registry_cycle_count": int(cycles[0] or 0),
        "last_registry_cycle_at": str(cycles[1] or "") or None,
        "latest_raw_imminent_at": str((raw_imminent_total or (0, ""))[1] or "") or None,
        "latest_maker_simulation_at": str((maker_total or (0, ""))[1] or "") or None,
        "latest_aggressive_simulation_at": str((aggressive_total or (0, ""))[1] or "") or None,
        "latest_watchlist_imminent_at": str((watchlist_imminent_total or (0, ""))[1] or "") or None,
        "db_path": str(settings.intraday_registry_audit_sqlite_path),
    }


def summarize_intraday_lifecycle(settings: Settings, limit: int = 10) -> dict[str, Any]:
    topn = max(int(limit), 1)
    with _intraday_audit_connect(settings) as conn:
        totals = conn.execute(
            """
            SELECT
                COUNT(*) AS total_markets,
                SUM(CASE WHEN is_updown = 1 THEN 1 ELSE 0 END) AS updown_markets,
                SUM(CASE WHEN is_updown = 0 THEN 1 ELSE 0 END) AS threshold_markets,
                SUM(CASE WHEN is_updown = 1 AND first_sub_60m_at IS NOT NULL THEN 1 ELSE 0 END) AS updown_sub_60m_markets,
                SUM(CASE WHEN is_updown = 1 AND first_sub_15m_at IS NOT NULL THEN 1 ELSE 0 END) AS updown_sub_15m_markets,
                SUM(CASE WHEN is_updown = 1 AND first_imminent_transition_at IS NOT NULL THEN 1 ELSE 0 END) AS updown_imminent_transition_markets,
                SUM(CASE WHEN is_updown = 0 AND first_sub_60m_at IS NOT NULL THEN 1 ELSE 0 END) AS threshold_sub_60m_markets
            FROM market_lifecycle
            """
        ).fetchone()
        edge = conn.execute(
            """
            SELECT
                COALESCE(MIN(best_combined_contract_price), 0) AS best_updown_combined_contract_price,
                COALESCE(MAX(best_gross_edge_per_share), 0) AS best_updown_gross_edge_per_share,
                COALESCE(MAX(best_net_edge_per_share), 0) AS best_updown_net_edge_per_share,
                COALESCE(MAX(last_seen_at), '') AS latest_updown_seen_at
            FROM market_lifecycle
            WHERE is_updown = 1
            """
        ).fetchone()
        recent_updown = conn.execute(
            """
            SELECT
                question,
                asset,
                min_hours_to_resolution,
                first_sub_60m_at,
                first_sub_15m_at,
                first_imminent_transition_at,
                best_combined_contract_price,
                best_gross_edge_per_share,
                best_net_edge_per_share,
                last_seen_at
            FROM market_lifecycle
            WHERE is_updown = 1
            ORDER BY last_seen_at DESC
            LIMIT ?
            """,
            (topn,),
        ).fetchall()
    return {
        "total_markets": int(totals["total_markets"] or 0),
        "updown_markets": int(totals["updown_markets"] or 0),
        "threshold_markets": int(totals["threshold_markets"] or 0),
        "updown_sub_60m_markets": int(totals["updown_sub_60m_markets"] or 0),
        "updown_sub_15m_markets": int(totals["updown_sub_15m_markets"] or 0),
        "updown_imminent_transition_markets": int(totals["updown_imminent_transition_markets"] or 0),
        "threshold_sub_60m_markets": int(totals["threshold_sub_60m_markets"] or 0),
        "best_updown_combined_contract_price": round(_as_float(edge["best_updown_combined_contract_price"], 0.0), 6),
        "best_updown_gross_edge_per_share": round(_as_float(edge["best_updown_gross_edge_per_share"], 0.0), 6),
        "best_updown_net_edge_per_share": round(_as_float(edge["best_updown_net_edge_per_share"], 0.0), 6),
        "latest_updown_seen_at": str(edge["latest_updown_seen_at"] or "") or None,
        "recent_updown_markets": [dict(row) for row in recent_updown],
        "db_path": str(settings.intraday_registry_audit_sqlite_path),
    }


def summarize_intraday_discovery_comparator(settings: Settings, limit: int = 3) -> dict[str, Any]:
    topn = max(int(limit), 1)
    with _intraday_audit_connect(settings) as conn:
        latest = conn.execute(
            "SELECT batch_id, recorded_at FROM discovery_comparator_runs ORDER BY recorded_at DESC LIMIT 1"
        ).fetchone()
        if latest is None:
            return {
                "batch_id": None,
                "recorded_at": None,
                "sources": [],
                "live_overlap_count": 0,
                "near_term_overlap_count": 0,
                "imminent_overlap_count": 0,
                "recent_near_term_markets": [],
                "db_path": str(settings.intraday_registry_audit_sqlite_path),
            }
        batch_id = str(latest["batch_id"])
        recorded_at = str(latest["recorded_at"])
        source_rows = conn.execute(
            """
            SELECT source, total_markets, live_upcoming_count, near_term_count, imminent_count,
                   closed_count, inactive_count, accepting_orders_count, min_hours_to_resolution,
                   max_hours_to_resolution, error, sample_questions_json
            FROM discovery_comparator_runs
            WHERE batch_id = ?
            ORDER BY source ASC
            """,
            (batch_id,),
        ).fetchall()
        live_overlap = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT market_id
                FROM discovery_comparator_markets
                WHERE batch_id = ? AND is_live_upcoming = 1
                GROUP BY market_id
                HAVING COUNT(DISTINCT source) > 1
            )
            """,
            (batch_id,),
        ).fetchone()[0]
        near_overlap = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT market_id
                FROM discovery_comparator_markets
                WHERE batch_id = ? AND is_near_term = 1
                GROUP BY market_id
                HAVING COUNT(DISTINCT source) > 1
            )
            """,
            (batch_id,),
        ).fetchone()[0]
        imminent_overlap = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT market_id
                FROM discovery_comparator_markets
                WHERE batch_id = ? AND is_imminent = 1
                GROUP BY market_id
                HAVING COUNT(DISTINCT source) > 1
            )
            """,
            (batch_id,),
        ).fetchone()[0]
        near_rows = conn.execute(
            """
            SELECT source, question, asset, hours_to_resolution, is_live_upcoming, is_near_term, is_imminent
            FROM discovery_comparator_markets
            WHERE batch_id = ? AND is_live_upcoming = 1
            ORDER BY is_imminent DESC, is_near_term DESC, hours_to_resolution ASC, source ASC
            LIMIT ?
            """,
            (batch_id, topn),
        ).fetchall()
    return {
        "batch_id": batch_id,
        "recorded_at": recorded_at,
        "sources": [
            {
                "source": str(row["source"]),
                "total_markets": int(row["total_markets"] or 0),
                "live_upcoming_count": int(row["live_upcoming_count"] or 0),
                "near_term_count": int(row["near_term_count"] or 0),
                "imminent_count": int(row["imminent_count"] or 0),
                "closed_count": int(row["closed_count"] or 0),
                "inactive_count": int(row["inactive_count"] or 0),
                "accepting_orders_count": int(row["accepting_orders_count"] or 0),
                "min_hours_to_resolution": _as_float(row["min_hours_to_resolution"], 0.0) if row["min_hours_to_resolution"] is not None else None,
                "max_hours_to_resolution": _as_float(row["max_hours_to_resolution"], 0.0) if row["max_hours_to_resolution"] is not None else None,
                "error": str(row["error"] or "") or None,
                "sample_questions": json.loads(str(row["sample_questions_json"] or "[]")),
            }
            for row in source_rows
        ],
        "live_overlap_count": int(live_overlap or 0),
        "near_term_overlap_count": int(near_overlap or 0),
        "imminent_overlap_count": int(imminent_overlap or 0),
        "recent_near_term_markets": [dict(row) for row in near_rows],
        "db_path": str(settings.intraday_registry_audit_sqlite_path),
    }


def backfill_intraday_lifecycle(settings: Settings) -> dict[str, Any]:
    counts = {
        "ws_events": 0,
        "book_snapshots": 0,
        "watchlist_transitions": 0,
        "imminent_box_candidates": 0,
        "arb_opportunities": 0,
    }
    with _intraday_audit_connect(settings) as conn:
        ws_rows = [dict(row) for row in conn.execute(
            """
            SELECT recorded_at, market_id, question, slug, hours_to_resolution, best_bid, best_ask,
                   midpoint, bids_depth_usdc, asks_depth_usdc, min_depth_usdc
            FROM ws_events
            ORDER BY recorded_at ASC
            """
        ).fetchall()]
        book_rows = [dict(row) for row in conn.execute(
            """
            SELECT recorded_at AS seen_at, market_id, token_id, question, slug, asset, is_updown,
                   hours_to_resolution, midpoint, best_bid, best_ask, spread,
                   bids_depth_usdc, asks_depth_usdc, min_depth_usdc
            FROM book_snapshots
            ORDER BY recorded_at ASC
            """
        ).fetchall()]
        transition_rows = [dict(row) for row in conn.execute(
            """
            SELECT recorded_at AS seen_at, market_id, token_id, question, slug, asset, transition_stage,
                   hours_to_resolution, midpoint, best_bid, best_ask, spread,
                   bids_depth_usdc, asks_depth_usdc, min_depth_usdc
            FROM watchlist_transitions
            ORDER BY recorded_at ASC
            """
        ).fetchall()]
        imminent_rows = [dict(row) for row in conn.execute(
            """
            SELECT recorded_at AS seen_at, market_id, token_id, question, slug, asset,
                   hours_to_resolution, midpoint, best_bid, best_ask, spread, min_depth_usdc,
                   meets_min_depth, filled_shares, yes_contract_price, no_contract_price,
                   combined_contract_price, gross_edge_per_share, net_edge_per_share
            FROM imminent_box_candidates
            ORDER BY recorded_at ASC
            """
        ).fetchall()]
        arb_rows = [dict(row) for row in conn.execute(
            """
            SELECT recorded_at AS seen_at, market_id, token_id, question, slug, asset,
                   hours_to_resolution, midpoint, best_bid, best_ask, spread, min_depth_usdc,
                   filled_shares, yes_contract_price, no_contract_price, combined_contract_price,
                   gross_edge_per_share, net_edge_per_share
            FROM arb_opportunities
            ORDER BY recorded_at ASC
            """
        ).fetchall()]
    _record_intraday_audit_market_lifecycle(settings, ws_rows, source="ws_event_backfill")
    _record_intraday_audit_market_lifecycle(settings, book_rows, source="official_clob_book")
    _record_intraday_audit_market_lifecycle(settings, transition_rows, source="registry_watchlist_transition")
    _record_intraday_audit_market_lifecycle(settings, imminent_rows, source="official_clob_book_raw_imminent")
    _record_intraday_audit_market_lifecycle(settings, arb_rows, source="official_clob_book")
    counts["ws_events"] = len(ws_rows)
    counts["book_snapshots"] = len(book_rows)
    counts["watchlist_transitions"] = len(transition_rows)
    counts["imminent_box_candidates"] = len(imminent_rows)
    counts["arb_opportunities"] = len(arb_rows)
    return counts


def _first(mapping: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in mapping and mapping[key] not in (None, ""):
            return mapping[key]
    return default


def _as_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_market_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def _normalize_iso(raw: str | None) -> datetime | None:
    if not raw:
        return None
    text = raw.strip()
    if len(text) == 10 and text.count("-") == 2:
        text = text + "T00:00:00+00:00"
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _text_contains_marker(normalized_text: str, slug_text: str, marker: str) -> bool:
    token = marker.strip().lower()
    if not token:
        return False
    if re.search(r"(?<![a-z0-9])" + re.escape(token) + r"(?![a-z0-9])", normalized_text):
        return True
    if re.search(r"(?<![a-z0-9])" + re.escape(token) + r"(?![a-z0-9])", slug_text):
        return True
    return False


def _age_minutes(raw: str | None) -> float:
    timestamp = _normalize_iso(raw)
    if timestamp is None:
        return 0.0
    return max((datetime.now(timezone.utc) - timestamp).total_seconds() / 60.0, 0.0)


def _thesis_cache_entry_is_fresh(entry: dict[str, Any], ttl_seconds: int) -> bool:
    if ttl_seconds <= 0:
        return False
    generated_at = _normalize_iso(str(entry.get("generated_at", "")))
    if generated_at is None:
        return False
    age_seconds = max((datetime.now(timezone.utc) - generated_at).total_seconds(), 0.0)
    return age_seconds <= ttl_seconds


def _extract_yes_token_id(market: dict[str, Any]) -> str | None:
    direct = _first(market, "token_id", "tokenID", "clobTokenId")
    if direct:
        return str(direct)

    token_ids = _first(market, "clobTokenIds", "tokenIds", "token_ids")
    if isinstance(token_ids, list) and token_ids:
        return str(token_ids[0])
    if isinstance(token_ids, str):
        try:
            parsed = json.loads(token_ids)
            if parsed:
                return str(parsed[0])
        except json.JSONDecodeError:
            parts = [part.strip() for part in token_ids.split(",") if part.strip()]
            if parts:
                return parts[0]

    tokens = _first(market, "tokens", "outcomes", default=[])
    if isinstance(tokens, list):
        for token in tokens:
            if not isinstance(token, dict):
                continue
            outcome = str(_first(token, "outcome", "name", default="")).lower()
            token_id = _first(token, "token_id", "tokenID", "id")
            if token_id and outcome in {"yes", ""}:
                return str(token_id)
        if tokens and isinstance(tokens[0], dict):
            token_id = _first(tokens[0], "token_id", "tokenID", "id")
            if token_id:
                return str(token_id)
    return None


def _extract_market_token_ids(market: dict[str, Any]) -> list[str]:
    token_ids: list[str] = []
    direct = _first(market, "clobTokenIds", "clob_token_ids", "assets_ids", "tokenIds", "token_ids")
    if isinstance(direct, list):
        token_ids.extend(str(item) for item in direct if item not in (None, ""))
    elif isinstance(direct, str):
        try:
            parsed = json.loads(direct)
            if isinstance(parsed, list):
                token_ids.extend(str(item) for item in parsed if item not in (None, ""))
        except json.JSONDecodeError:
            token_ids.extend(part.strip() for part in direct.split(",") if part.strip())

    tokens = _first(market, "tokens", "outcomes", default=[])
    if isinstance(tokens, list):
        for token in tokens:
            if not isinstance(token, dict):
                continue
            token_id = _first(token, "token_id", "tokenID", "id", default="")
            if token_id not in (None, ""):
                token_ids.append(str(token_id))

    yes_token = _extract_yes_token_id(market)
    if yes_token:
        token_ids.append(str(yes_token))
    asset_id = _first(market, "asset_id", default="")
    if asset_id not in (None, ""):
        token_ids.append(str(asset_id))
    seen: set[str] = set()
    ordered: list[str] = []
    for token_id in token_ids:
        if not token_id or token_id in seen:
            continue
        seen.add(token_id)
        ordered.append(token_id)
    return ordered


def _extract_question(market: dict[str, Any]) -> str:
    return str(_first(market, "question", "title", "name", default="")).strip()


def _extract_slug(market: dict[str, Any]) -> str:
    return str(_first(market, "slug", "market_slug", default=_extract_question(market))).strip()


def _extract_market_id(market: dict[str, Any]) -> str:
    return str(_first(market, "id", "market_id", "conditionId", "condition_id", default=_extract_slug(market)))


def _extract_volume(market: dict[str, Any]) -> float:
    return _as_float(_first(market, "volume", "volumeNum", "volume_num", "liquidity", "liquidityNum"), 0.0)


def _normalize_category(raw: str | None) -> str:
    if not raw:
        return "unknown"
    value = raw.strip().lower()
    if not value or len(value) > 32 or "-" in value:
        return "unknown"
    aliases = {
        "politics": "politics",
        "elections": "politics",
        "government": "politics",
        "crypto": "crypto",
        "cryptocurrency": "crypto",
        "blockchain": "crypto",
        "bitcoin": "crypto",
        "ethereum": "crypto",
        "macro": "macro",
        "economy": "macro",
        "economic": "macro",
        "finance": "macro",
        "business": "macro",
        "stocks": "macro",
        "fed": "macro",
        "rates": "macro",
        "sports": "sports",
        "nba": "sports",
        "nfl": "sports",
        "mlb": "sports",
        "soccer": "sports",
        "tennis": "sports",
        "technology": "tech",
        "tech": "tech",
        "ai": "tech",
        "science": "science",
        "entertainment": "culture",
        "culture": "culture",
        "celebrity": "culture",
        "pop culture": "culture",
        "world": "world",
    }
    if value in aliases:
        return aliases[value]
    return value.replace("/", " ").replace("_", " ").split()[0]


def _categorize_text(question: str, slug: str = "") -> str:
    haystack = f"{question} {slug}".lower()
    keyword_groups = {
        "crypto": ("bitcoin", "ethereum", "eth", "btc", "token", "airdrop", "solana", "crypto", "fdv", "launch"),
        "politics": ("president", "senate", "house", "election", "governor", "democrats", "republicans", "nomination", "trump", "biden", "minister", "prime minister", "government", "court", "sentence"),
        "macro": ("fed", "inflation", "cpi", "gdp", "treasury", "interest rate", "recession", "tariff", "oil", "gold", "stocks"),
        "sports": ("nba", "nfl", "mlb", "nhl", "championship", "final", "goal", "touchdown", "world cup"),
        "tech": ("openai", "chatgpt", "ai", "tesla", "apple", "google", "meta"),
        "culture": ("movie", "oscar", "grammy", "celebrity", "festival"),
    }

    def matches(keyword: str) -> bool:
        pattern = r"(?<![a-z0-9])" + re.escape(keyword.lower()) + r"(?![a-z0-9])"
        return re.search(pattern, haystack) is not None

    for category, keywords in keyword_groups.items():
        if any(matches(keyword) for keyword in keywords):
            return category
    return "unknown"


def _extract_market_category(market: dict[str, Any]) -> str:
    direct_candidates = [
        _first(market, "category", "subcategory", default=""),
    ]
    events = _first(market, "events", default=[])
    if isinstance(events, list):
        for event in events:
            if not isinstance(event, dict):
                continue
            direct_candidates.extend(
                [
                    _first(event, "category", "subcategory", default=""),
                ]
            )
    tags = _first(market, "tags", default=[])
    if isinstance(tags, list):
        for tag in tags:
            if isinstance(tag, dict):
                direct_candidates.extend(
                    [
                        _first(tag, "name", "slug", default=""),
                    ]
                )
            elif isinstance(tag, str):
                direct_candidates.append(tag)
    for candidate in direct_candidates:
        normalized = _normalize_category(str(candidate)) if candidate else ""
        if normalized and normalized != "unknown":
            return normalized
    return _categorize_text(_extract_question(market), _extract_slug(market))


def _active_rotation_category(settings: Settings, now: datetime | None = None) -> str | None:
    if not settings.category_rotation:
        return None
    current = now or datetime.now(timezone.utc)
    rotation_days = max(settings.category_rotation_days, 1)
    bucket = current.toordinal() // rotation_days
    return settings.category_rotation[bucket % len(settings.category_rotation)]


def _market_allowed_by_category(settings: Settings, category: str, active_rotation: str | None) -> tuple[bool, str]:
    normalized = _normalize_category(category)
    if settings.category_exclude and normalized in settings.category_exclude:
        return False, f"excluded category {normalized}"
    if settings.category_include and normalized not in settings.category_include:
        return False, "outside included categories"
    if active_rotation and normalized not in {"unknown", active_rotation}:
        return False, f"rotation focus {active_rotation}"
    return True, "allowed"


def _extract_hours_to_resolution(market: dict[str, Any]) -> float:
    now = datetime.now(timezone.utc)
    candidates = []
    for key in (
        "gameStartTime",
        "eventStartTime",
        "startTime",
        "endDate",
        "end_date",
        "endDateIso",
        "closeTime",
        "closedTime",
        "end_time",
    ):
        value = _first(market, key, default="")
        parsed = _normalize_iso(value)
        if parsed is not None:
            candidates.append(parsed)

    future_candidates = [candidate for candidate in candidates if candidate > now]
    if future_candidates:
        delta = min(future_candidates) - now
        return max(delta.total_seconds() / 3600.0, 0.0)

    if bool(_first(market, "active", default=False)) and not bool(_first(market, "closed", default=False)):
        # Some active market payloads currently expose stale endDate values; use a neutral horizon.
        return 24.0
    return 0.0


def _parse_clock_time(raw: str) -> datetime | None:
    text = raw.strip().upper().replace(" ", "")
    for fmt in ("%I:%M%p", "%I%p"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _extract_intraday_question_window(question: str, now: datetime | None = None) -> tuple[datetime, datetime] | None:
    match = re.search(
        r"([A-Za-z]+)\s+(\d{1,2})(?:,\s*(\d{4}))?,\s*(\d{1,2}(?::\d{2})?\s*[AP]M)(?:\s*-\s*(\d{1,2}(?::\d{2})?\s*[AP]M))?",
        question,
        re.IGNORECASE,
    )
    if not match:
        return None

    month_name = match.group(1).strip()
    day = int(match.group(2))
    explicit_year = match.group(3)
    start_raw = match.group(4)
    end_raw = match.group(5) or start_raw

    month = None
    for fmt in ("%B", "%b"):
        try:
            month = datetime.strptime(month_name, fmt).month
            break
        except ValueError:
            continue
    if month is None:
        return None

    start_clock = _parse_clock_time(start_raw)
    end_clock = _parse_clock_time(end_raw)
    if start_clock is None or end_clock is None:
        return None

    current = (now or datetime.now(timezone.utc)).astimezone(_EASTERN_TZ)
    year = int(explicit_year) if explicit_year else current.year
    start_local = datetime(
        year,
        month,
        day,
        start_clock.hour,
        start_clock.minute,
        tzinfo=_EASTERN_TZ,
    )
    end_local = datetime(
        year,
        month,
        day,
        end_clock.hour,
        end_clock.minute,
        tzinfo=_EASTERN_TZ,
    )
    if end_local <= start_local:
        end_local += timedelta(minutes=5 if end_local == start_local else 24 * 60)
    if explicit_year is None and start_local - current > timedelta(days=3):
        start_local = start_local.replace(year=start_local.year - 1)
        end_local = end_local.replace(year=end_local.year - 1)
    if explicit_year is None and current - start_local > timedelta(days=362):
        start_local = start_local.replace(year=start_local.year + 1)
        end_local = end_local.replace(year=end_local.year + 1)
    if end_local == start_local:
        end_local += timedelta(minutes=5)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def _short_crypto_updown_asset(question: str, slug: str) -> str | None:
    normalized = _normalize_market_text(question)
    slug_text = str(slug or "").strip().lower()
    if "up or down" not in normalized and "up-or-down" not in slug_text and "updown" not in slug_text:
        return None
    for asset, markers in _CRYPTO_ASSET_MARKERS.items():
        if any(_text_contains_marker(normalized, slug_text, marker) for marker in markers):
            return asset
    return None


def _short_crypto_market_asset(question: str, slug: str) -> str | None:
    normalized = _normalize_market_text(question)
    slug_text = str(slug or "").strip().lower()
    for asset, markers in _CRYPTO_ASSET_MARKERS.items():
        if any(_text_contains_marker(normalized, slug_text, marker) for marker in markers):
            return asset
    return None


def _is_five_minute_updown_market(question: str, slug: str) -> bool:
    normalized = _normalize_market_text(question)
    slug_text = str(slug or "").strip().lower()
    if "up or down" not in normalized and "up-or-down" not in slug_text and "updown" not in slug_text:
        return False
    if "5 minute" in normalized or "5-minute" in question.lower() or "5m" in slug_text:
        return True
    window = _extract_intraday_question_window(question)
    if window is None:
        return True
    start, end = window
    return int((end - start).total_seconds() / 60.0) <= 15


def _extract_short_market_hours_to_resolution(market: dict[str, Any]) -> float:
    now = datetime.now(timezone.utc)
    question = _extract_question(market)
    window = _extract_intraday_question_window(question, now=now)
    if window is not None:
        _, end_utc = window
        return max((end_utc - now).total_seconds() / 3600.0, 0.0)
    return _extract_hours_to_resolution(market)


def _fetch_gamma_active_markets_page(limit: int, offset: int) -> list[dict[str, Any]]:
    params = urllib.parse.urlencode(
        {
            "limit": max(1, min(limit, 500)),
            "offset": max(offset, 0),
            "active": "true",
            "closed": "false",
            "archived": "false",
        }
    )
    request = urllib.request.Request(
        f"https://gamma-api.polymarket.com/markets?{params}",
        headers={"User-Agent": "polymarket-bot-scaffold/0.1", "Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("markets", "data", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return []


def _fetch_gamma_active_event_markets_page(limit: int, offset: int) -> list[dict[str, Any]]:
    params = urllib.parse.urlencode(
        {
            "limit": max(1, min(limit, 500)),
            "offset": max(offset, 0),
            "active": "true",
            "closed": "false",
            "archived": "false",
        }
    )
    request = urllib.request.Request(
        f"https://gamma-api.polymarket.com/events?{params}",
        headers={"User-Agent": "polymarket-bot-scaffold/0.1", "Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if isinstance(payload, list):
        events = payload
    elif isinstance(payload, dict):
        events = []
        for key in ("events", "data", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                events = value
                break
    else:
        events = []

    markets: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        for market in event.get("markets", []) or event.get("eventMarkets", []) or []:
            if not isinstance(market, dict):
                continue
            merged = dict(market)
            if "category" not in merged and event.get("category") not in (None, ""):
                merged["category"] = event.get("category")
            if not isinstance(merged.get("events"), list) or not merged.get("events"):
                merged["events"] = [event]
            markets.append(merged)
    return markets


def _gamma_market_payloads(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        candidates: list[dict[str, Any]] = []
        if payload.get("question") or payload.get("title") or payload.get("slug"):
            candidates.append(payload)
        for key in ("markets", "data", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                candidates.extend(item for item in value if isinstance(item, dict))
        return candidates
    return []


def _select_matching_gamma_market(
    markets: list[dict[str, Any]],
    *,
    market_id: str,
    slug: str,
) -> dict[str, Any] | None:
    if not markets:
        return None
    if market_id:
        for market in markets:
            if _extract_market_id(market) == market_id:
                return market
    if slug:
        for market in markets:
            if _extract_slug(market) == slug:
                return market
    return markets[0] if markets else None


def _fetch_gamma_market_detail(*, market_id: str = "", slug: str = "") -> dict[str, Any] | None:
    headers = {"User-Agent": "polymarket-bot-scaffold/0.1", "Accept": "application/json"}
    candidate_urls: list[str] = []
    if market_id:
        quoted_id = urllib.parse.quote(market_id, safe="")
        candidate_urls.append(f"https://gamma-api.polymarket.com/markets/{quoted_id}")
        candidate_urls.append(f"https://gamma-api.polymarket.com/markets?id={quoted_id}&limit=10")
        candidate_urls.append(f"https://gamma-api.polymarket.com/markets?market_id={quoted_id}&limit=10")
    if slug:
        quoted_slug = urllib.parse.quote(slug, safe="")
        candidate_urls.append(f"https://gamma-api.polymarket.com/markets?slug={quoted_slug}&limit=10")
    seen_urls: set[str] = set()
    for url in candidate_urls:
        if url in seen_urls:
            continue
        seen_urls.add(url)
        try:
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=15) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception:  # noqa: BLE001
            continue
        markets = _gamma_market_payloads(payload)
        matched = _select_matching_gamma_market(markets, market_id=market_id, slug=slug)
        if matched is not None:
            return matched
    return None


def _fetch_gamma_intraday_markets_page(limit: int, offset: int) -> list[dict[str, Any]]:
    try:
        event_markets = _fetch_gamma_active_event_markets_page(limit, offset)
        if event_markets:
            return event_markets
    except Exception:
        pass
    return _fetch_gamma_active_markets_page(limit, offset)


def _gamma_search_updown_market_payload(limit: int, settings: Settings | None = None) -> dict[str, Any]:
    per_query_limit = max(5, min(max(int(limit), 1), 50))
    headers = {"User-Agent": "polymarket-bot-scaffold/0.1", "Accept": "application/json"}
    markets_by_id: dict[str, dict[str, Any]] = {}
    upcoming_by_id: dict[str, dict[str, Any]] = {}
    timeout_seconds = 15
    max_queries = len(_UPDOWN_SEARCH_QUERIES)
    if settings is not None:
        timeout_seconds = max(int(settings.intraday_registry_gamma_search_timeout_seconds), 1)
        max_queries = max(1, min(int(settings.intraday_registry_gamma_refresh_max_queries), len(_UPDOWN_SEARCH_QUERIES)))
    for query in _UPDOWN_SEARCH_QUERIES[:max_queries]:
        params = urllib.parse.urlencode(
            {
                "q": query,
                "limit_per_type": per_query_limit,
                "search_tags": "false",
                "search_profiles": "false",
                "cache": "false",
            }
        )
        request = urllib.request.Request(
            f"https://gamma-api.polymarket.com/public-search?{params}",
            headers=headers,
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception:
            continue
        events = payload.get("events", []) if isinstance(payload, dict) else []
        for event in events:
            if not isinstance(event, dict):
                continue
            event_category = event.get("category")
            for market in event.get("markets", []) or []:
                if not isinstance(market, dict):
                    continue
                question = _extract_question(market) or _extract_question(event)
                slug = _extract_slug(market) or _extract_slug(event)
                if not _intraday_market_looks_updown(question, slug):
                    continue
                merged = dict(market)
                if "category" not in merged and event_category not in (None, ""):
                    merged["category"] = event_category
                if not isinstance(merged.get("events"), list) or not merged.get("events"):
                    merged["events"] = [event]
                market_id = _extract_market_id(merged)
                if market_id:
                    markets_by_id[market_id] = merged
                    if (
                        bool(_first(merged, "active", default=True))
                        and not bool(_first(merged, "closed", default=False))
                        and not bool(_first(merged, "archived", default=False))
                        and bool(_first(merged, "acceptingOrders", default=True))
                    ):
                        hours_left = _extract_short_market_hours_to_resolution(merged)
                        if hours_left > 0.0:
                            upcoming_by_id[market_id] = merged
    upcoming = sorted(
        upcoming_by_id.values(),
        key=lambda market: (
            _extract_short_market_hours_to_resolution(market),
            -_extract_volume(market),
            _extract_question(market),
        ),
    )
    return {
        "matches": list(markets_by_id.values()),
        "upcoming": upcoming,
        "match_count": len(markets_by_id),
        "upcoming_count": len(upcoming),
    }


def _fetch_gamma_search_updown_markets(limit: int, settings: Settings | None = None) -> list[dict[str, Any]]:
    return list(_gamma_search_updown_market_payload(limit, settings).get("upcoming", []))


def _extract_market_tag_refs(market: dict[str, Any]) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def append_tag(tag_id: Any, tag_slug: Any, tag_label: Any) -> None:
        slug = str(tag_slug or "").strip().lower()
        label = str(tag_label or "").strip().lower()
        ref_id = str(tag_id or "").strip()
        key = (ref_id, slug or label)
        if key[1] == "" or key in seen:
            return
        seen.add(key)
        refs.append({"id": ref_id, "slug": slug, "label": label})

    for container in (market,):
        tags = _first(container, "tags", default=[])
        if isinstance(tags, list):
            for tag in tags:
                if isinstance(tag, dict):
                    append_tag(_first(tag, "id", default=""), _first(tag, "slug", default=""), _first(tag, "label", "name", default=""))
                elif isinstance(tag, str):
                    append_tag("", tag, tag)
        events = _first(container, "events", default=[])
        if isinstance(events, list):
            for event in events:
                if not isinstance(event, dict):
                    continue
                event_tags = _first(event, "tags", default=[])
                if isinstance(event_tags, list):
                    for tag in event_tags:
                        if isinstance(tag, dict):
                            append_tag(_first(tag, "id", default=""), _first(tag, "slug", default=""), _first(tag, "label", "name", default=""))
                        elif isinstance(tag, str):
                            append_tag("", tag, tag)
    return refs


def _gamma_updown_tag_candidates(limit: int, settings: Settings | None = None) -> list[dict[str, str]]:
    payload = _gamma_search_updown_market_payload(limit, settings)
    counts: dict[tuple[str, str], dict[str, Any]] = {}
    for market in payload.get("matches", []):
        if not isinstance(market, dict):
            continue
        question = _extract_question(market)
        slug = _extract_slug(market)
        asset = _short_crypto_market_asset(question, slug)
        for ref in _extract_market_tag_refs(market):
            ref_slug = str(ref.get("slug") or "").strip().lower()
            ref_label = str(ref.get("label") or "").strip().lower()
            if not ref_slug and not ref_label:
                continue
            text = f"{ref_slug} {ref_label}"
            score = 0
            if "crypto" in text:
                score += 2
            if asset and asset in text:
                score += 3
            if "up" in text and "down" in text:
                score += 2
            if "5m" in text or "5-minute" in text or "5 minute" in text:
                score += 2
            if score <= 0:
                continue
            key = (str(ref.get("id") or ""), ref_slug or ref_label)
            item = counts.setdefault(
                key,
                {
                    "id": str(ref.get("id") or ""),
                    "slug": ref_slug,
                    "label": ref_label,
                    "score": 0,
                    "count": 0,
                },
            )
            item["score"] += score
            item["count"] += 1
    ordered = sorted(
        counts.values(),
        key=lambda item: (-int(item.get("score", 0)), -int(item.get("count", 0)), str(item.get("slug") or item.get("label") or "")),
    )
    max_tags = 6
    if settings is not None:
        max_tags = max(1, min(int(settings.intraday_registry_gamma_refresh_max_queries), 12))
    return [{"id": str(item.get("id") or ""), "slug": str(item.get("slug") or ""), "label": str(item.get("label") or "")} for item in ordered[:max_tags]]


def _fetch_gamma_events_by_tag_ref(tag_ref: dict[str, str], settings: Settings, *, limit: int) -> list[dict[str, Any]]:
    params: dict[str, Any] = {
        "limit": max(1, min(limit, 200)),
        "offset": 0,
        "active": "true",
        "closed": "false",
        "archived": "false",
        "related_tags": "true",
        "order": "endDate",
        "ascending": "true",
    }
    now = datetime.now(timezone.utc)
    end_max = now + timedelta(minutes=max(int(settings.intraday_registry_watchlist_max_minutes_to_resolution), 1))
    params["end_date_min"] = now.isoformat().replace("+00:00", "Z")
    params["end_date_max"] = end_max.isoformat().replace("+00:00", "Z")
    tag_id = str(tag_ref.get("id") or "").strip()
    tag_slug = str(tag_ref.get("slug") or "").strip()
    if tag_id:
        params["tag_id"] = tag_id
    elif tag_slug:
        params["tag_slug"] = tag_slug
    else:
        return []
    request = urllib.request.Request(
        f"https://gamma-api.polymarket.com/events?{urllib.parse.urlencode(params)}",
        headers={"User-Agent": "polymarket-bot-scaffold/0.1", "Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=max(int(settings.intraday_registry_gamma_search_timeout_seconds), 1)) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if isinstance(payload, list):
        events = payload
    elif isinstance(payload, dict):
        events = []
        for key in ("events", "data", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                events = value
                break
    else:
        events = []
    markets: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        for market in event.get("markets", []) or []:
            if not isinstance(market, dict):
                continue
            merged = dict(market)
            if not isinstance(merged.get("events"), list) or not merged.get("events"):
                merged["events"] = [event]
            markets.append(merged)
    return markets


def _gamma_tag_updown_market_payload(limit: int, settings: Settings) -> dict[str, Any]:
    tag_refs = _gamma_updown_tag_candidates(limit, settings)
    watchlist_window_hours = max(int(settings.intraday_registry_watchlist_max_minutes_to_resolution), 1) / 60.0
    markets_by_id: dict[str, dict[str, Any]] = {}
    fetched_count = 0
    for tag_ref in tag_refs:
        for market in _fetch_gamma_events_by_tag_ref(tag_ref, settings, limit=limit):
            if not isinstance(market, dict):
                continue
            fetched_count += 1
            question = _extract_question(market)
            slug = _extract_slug(market)
            if not _intraday_market_looks_updown(question, slug):
                continue
            if not bool(_first(market, "active", default=True)):
                continue
            if bool(_first(market, "closed", default=False)):
                continue
            if bool(_first(market, "archived", default=False)):
                continue
            if not bool(_first(market, "acceptingOrders", default=True)):
                continue
            hours_left = _extract_short_market_hours_to_resolution(market)
            if not (0.0 < hours_left <= watchlist_window_hours):
                continue
            market_id = _extract_market_id(market) or slug or question
            if not market_id:
                continue
            existing = markets_by_id.get(market_id)
            if existing is None or _extract_short_market_hours_to_resolution(existing) > hours_left:
                markets_by_id[market_id] = market
    upcoming = sorted(
        markets_by_id.values(),
        key=lambda market: (
            _extract_short_market_hours_to_resolution(market),
            -_extract_volume(market),
            _extract_question(market),
        ),
    )
    return {
        "markets": upcoming,
        "tag_candidate_count": len(tag_refs),
        "tag_candidates": [dict(ref) for ref in tag_refs[:8]],
        "fetched_count": fetched_count,
    }


def _intraday_registry_payload(settings: Settings) -> dict[str, Any]:
    payload = _json_load(settings.intraday_registry_path, {})
    return payload if isinstance(payload, dict) else {}


def _intraday_registry_is_fresh(settings: Settings, payload: dict[str, Any]) -> bool:
    updated_at = _normalize_iso(str(payload.get("updated_at", "")))
    if updated_at is None:
        return False
    age_seconds = max((datetime.now(timezone.utc) - updated_at).total_seconds(), 0.0)
    return age_seconds <= max(settings.intraday_registry_stale_after_seconds, 1)


def _intraday_registry_market_record(market: dict[str, Any], *, source: str, seen_at: str | None = None) -> dict[str, Any] | None:
    if not isinstance(market, dict):
        return None
    question = _extract_question(market)
    slug = _extract_slug(market)
    asset = _short_crypto_market_asset(question, slug)
    if asset is None:
        return None
    token_ids = _extract_market_token_ids(market)
    if not token_ids:
        return None
    market_id = _extract_market_id(market)
    if not market_id:
        return None
    category = _extract_market_category(market)
    hours_to_resolution = _extract_short_market_hours_to_resolution(market)
    record = dict(market)
    record["id"] = market_id
    record["question"] = question
    record["slug"] = slug
    record["category"] = category
    record["active"] = bool(_first(market, "active", default=True))
    record["closed"] = bool(_first(market, "closed", default=False))
    record["archived"] = bool(_first(market, "archived", default=False))
    record["acceptingOrders"] = bool(_first(market, "acceptingOrders", default=True))
    record["clobTokenIds"] = token_ids
    record["token_id"] = token_ids[0]
    record["registry_asset"] = asset
    record["registry_sources"] = sorted({source, *(record.get("registry_sources", []) or [])})
    record["registry_seen_at"] = seen_at or utc_now_iso()
    record["registry_hours_to_resolution"] = round(hours_to_resolution, 6)
    record["registry_updown"] = _is_five_minute_updown_market(question, slug)
    return record


def _intraday_registry_rejections_payload(settings: Settings) -> dict[str, Any]:
    payload = _json_load(settings.intraday_registry_rejections_path, {})
    return payload if isinstance(payload, dict) else {}


def _intraday_registry_raw_updown_payload(settings: Settings) -> dict[str, Any]:
    payload = _json_load(settings.intraday_registry_raw_updown_path, {})
    return payload if isinstance(payload, dict) else {}


def _intraday_registry_book_tape_payload(settings: Settings) -> dict[str, Any]:
    payload = _json_load(settings.intraday_registry_book_tape_path, {})
    return payload if isinstance(payload, dict) else {}


def _bootstrap_intraday_registry_from_local_cache(settings: Settings) -> dict[str, Any]:
    candidate_entries: list[dict[str, Any]] = []
    for payload_fn in (
        _intraday_registry_book_tape_payload,
        _intraday_registry_raw_updown_payload,
        _intraday_registry_rejections_payload,
    ):
        payload = payload_fn(settings)
        entries = payload.get("entries", []) if isinstance(payload, dict) else []
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if isinstance(entry, dict):
                candidate_entries.append(entry)
    eligible: list[dict[str, Any]] = []
    seen_market_ids: set[str] = set()
    for market in reversed(candidate_entries):
        merged = _merge_intraday_raw_entry_from_rejections(settings, market)
        record = _intraday_registry_market_record(merged, source="local-cache")
        if record is None:
            continue
        market_id = record["id"]
        if market_id in seen_market_ids:
            continue
        if not _registry_market_matches(
            record,
            max_minutes_to_resolution=settings.intraday_registry_max_minutes_to_resolution,
            require_updown=False,
        ):
            continue
        seen_market_ids.add(market_id)
        eligible.append(record)
    return _write_intraday_registry(
        settings,
        eligible,
        metadata={"source": "local-cache-bootstrap", "eligible_markets": len(eligible)},
    )


def _resolve_intraday_market_tokens(market: dict[str, Any]) -> dict[str, Any] | None:
    if _extract_market_token_ids(market):
        return dict(market)

    market_id = _extract_market_id(market)
    slug = _extract_slug(market)
    cache_keys = [key for key in (f"id:{market_id}" if market_id else "", f"slug:{slug}" if slug else "") if key]
    for cache_key in cache_keys:
        if cache_key in _INTRADAY_MARKET_DETAIL_CACHE:
            cached = _INTRADAY_MARKET_DETAIL_CACHE[cache_key]
            return dict(cached) if isinstance(cached, dict) else None

    resolved = _fetch_gamma_market_detail(market_id=market_id, slug=slug)
    merged: dict[str, Any] | None = None
    if isinstance(resolved, dict) and _extract_market_token_ids(resolved):
        merged = dict(market)
        for key, value in resolved.items():
            if value in (None, "", [], {}):
                continue
            merged[key] = value
        if "clobTokenIds" not in merged:
            merged["clobTokenIds"] = _extract_market_token_ids(resolved)

    for cache_key in cache_keys:
        _INTRADAY_MARKET_DETAIL_CACHE[cache_key] = dict(merged) if isinstance(merged, dict) else None
    return merged


def _intraday_market_looks_relevant(question: str, slug: str) -> bool:
    normalized = _normalize_market_text(question)
    slug_text = str(slug or "").strip().lower()
    if "up or down" in normalized or "up-or-down" in slug_text or "updown" in slug_text:
        return True
    for markers in _CRYPTO_ASSET_MARKERS.values():
        if any(_text_contains_marker(normalized, slug_text, marker) for marker in markers):
            return True
    return False


def _intraday_market_looks_updown(question: str, slug: str) -> bool:
    normalized = _normalize_market_text(question)
    slug_text = str(slug or "").strip().lower()
    return "up or down" in normalized or "up-or-down" in slug_text or "updown" in slug_text


def _intraday_registry_rejection_reason(settings: Settings, market: dict[str, Any], *, require_updown: bool = False) -> tuple[str | None, dict[str, Any]]:
    question = _extract_question(market)
    slug = _extract_slug(market)
    asset = _short_crypto_market_asset(question, slug)
    details = {
        "question": question,
        "slug": slug,
        "asset_guess": asset,
        "market_id": _extract_market_id(market),
        "token_ids": _extract_market_token_ids(market),
        "active": bool(_first(market, "active", default=True)),
        "closed": bool(_first(market, "closed", default=False)),
        "archived": bool(_first(market, "archived", default=False)),
        "accepting_orders": bool(_first(market, "acceptingOrders", default=True)),
        "looks_updown": _intraday_market_looks_updown(question, slug),
    }
    if asset is None:
        if details["looks_updown"]:
            return "updown_unrecognized_asset", details
        return ("unrecognized_crypto_asset" if _intraday_market_looks_relevant(question, slug) else None), details
    if not details["token_ids"]:
        return "missing_token_ids", details
    if not details["market_id"]:
        return "missing_market_id", details
    if not bool(details["active"]):
        return "inactive", details
    if bool(details["closed"]):
        return "closed", details
    if bool(details["archived"]):
        return "archived", details
    if not bool(details["accepting_orders"]):
        return "not_accepting_orders", details
    hours_left = _extract_short_market_hours_to_resolution(market)
    details["hours_to_resolution"] = round(hours_left, 6)
    if hours_left <= 0.0:
        return "expired_or_zero_hours", details
    if hours_left > (settings.intraday_registry_max_minutes_to_resolution / 60.0):
        return "outside_max_minutes_window", details
    if require_updown and not _is_five_minute_updown_market(question, slug):
        return "not_updown_family", details
    return None, details


def _append_intraday_registry_rejection(settings: Settings, market: dict[str, Any], *, source: str, reason: str, details: dict[str, Any] | None = None) -> None:
    payload = _intraday_registry_rejections_payload(settings)
    entries = payload.get("entries", [])
    if not isinstance(entries, list):
        entries = []
    entry = {
        "seen_at": utc_now_iso(),
        "source": source,
        "reason": reason,
        "question": _extract_question(market),
        "slug": _extract_slug(market),
    }
    if details:
        entry.update(details)
    entries.append(entry)
    limit = max(settings.intraday_registry_rejections_limit, 1)
    payload = {
        "updated_at": utc_now_iso(),
        "entries": entries[-limit:],
        "count": len(entries[-limit:]),
    }
    _json_dump(settings.intraday_registry_rejections_path, payload)


def _append_intraday_registry_raw_updown(
    settings: Settings,
    market: dict[str, Any],
    *,
    source: str,
    event_type: str = "",
) -> None:
    enriched = _resolve_intraday_market_tokens(market) or dict(market)
    question = _extract_question(enriched)
    slug = _extract_slug(enriched)
    if not _intraday_market_looks_updown(question, slug):
        return
    payload = _intraday_registry_raw_updown_payload(settings)
    entries = payload.get("entries", [])
    if not isinstance(entries, list):
        entries = []
    token_ids = _extract_market_token_ids(enriched)
    hours_left = _extract_short_market_hours_to_resolution(enriched)
    entries.append(
        {
            "seen_at": utc_now_iso(),
            "source": source,
            "event_type": event_type,
            "question": question,
            "slug": slug,
            "market_id": _extract_market_id(enriched),
            "asset_guess": _short_crypto_market_asset(question, slug),
            "token_ids": token_ids,
            "clobTokenIds": token_ids,
            "active": bool(_first(enriched, "active", default=True)),
            "closed": bool(_first(enriched, "closed", default=False)),
            "archived": bool(_first(enriched, "archived", default=False)),
            "accepting_orders": bool(_first(enriched, "acceptingOrders", default=True)),
            "acceptingOrders": bool(_first(enriched, "acceptingOrders", default=True)),
            "hours_to_resolution": round(hours_left, 6),
        }
    )
    limit = max(settings.intraday_registry_raw_updown_limit, 1)
    _json_dump(
        settings.intraday_registry_raw_updown_path,
        {
            "updated_at": utc_now_iso(),
            "entries": entries[-limit:],
            "count": len(entries[-limit:]),
        },
    )
    _record_intraday_audit_market_lifecycle(
        settings,
        [
            {
                "seen_at": utc_now_iso(),
                "market_id": _extract_market_id(enriched),
                "token_id": token_ids[0] if token_ids else "",
                "question": question,
                "slug": slug,
                "asset": _short_crypto_market_asset(question, slug),
                "hours_to_resolution": round(hours_left, 6),
                "is_updown": True,
                "source": source,
                "event_type": event_type,
            }
        ],
        source=f"{source}_discovery",
    )
    if 0.0 < hours_left <= (max(settings.intraday_registry_imminent_max_minutes_to_resolution, 1) / 60.0):
        imminent_payload = _json_load(settings.intraday_registry_imminent_updown_path, {"entries": []})
        imminent_entries = imminent_payload.get("entries", []) if isinstance(imminent_payload, dict) else []
        if not isinstance(imminent_entries, list):
            imminent_entries = []
        imminent_entries.append(entries[-1])
        imminent_limit = max(settings.intraday_registry_imminent_updown_limit, 1)
        _json_dump(
            settings.intraday_registry_imminent_updown_path,
            {
                "updated_at": utc_now_iso(),
                "entries": imminent_entries[-imminent_limit:],
                "count": len(imminent_entries[-imminent_limit:]),
            },
        )


def _registry_market_matches(
    record: dict[str, Any],
    *,
    min_minutes_to_resolution: int = 0,
    max_minutes_to_resolution: int,
    require_updown: bool,
) -> bool:
    if not _market_is_tradeable(record):
        return False
    hours_left = _extract_short_market_hours_to_resolution(record)
    if hours_left <= 0.0:
        return False
    if min_minutes_to_resolution > 0 and hours_left < (min_minutes_to_resolution / 60.0):
        return False
    if hours_left > (max_minutes_to_resolution / 60.0):
        return False
    question = _extract_question(record)
    slug = _extract_slug(record)
    if _short_crypto_market_asset(question, slug) is None:
        return False
    if require_updown and not _is_five_minute_updown_market(question, slug):
        return False
    return True


def _registry_market_persistable(settings: Settings, record: dict[str, Any]) -> bool:
    if _registry_market_matches(
        record,
        max_minutes_to_resolution=settings.intraday_registry_max_minutes_to_resolution,
        require_updown=False,
    ):
        return True
    if not bool(record.get("registry_watch_only")):
        return False
    hours_left = _extract_short_market_hours_to_resolution(record)
    watch_window_hours = (max(settings.intraday_registry_max_minutes_to_resolution, 24 * 60) / 60.0) + 6.0
    return 0.0 < hours_left <= watch_window_hours and _short_crypto_market_asset(
        _extract_question(record),
        _extract_slug(record),
    ) is not None


def _load_intraday_registry_markets(
    settings: Settings,
    *,
    min_minutes_to_resolution: int = 0,
    max_minutes_to_resolution: int,
    require_updown: bool,
) -> list[dict[str, Any]]:
    if not settings.intraday_registry_enabled:
        return []
    payload = _intraday_registry_payload(settings)
    if not payload or not _intraday_registry_is_fresh(settings, payload):
        return []
    markets = payload.get("markets", [])
    if not isinstance(markets, list):
        return []
    eligible = [
        market
        for market in markets
        if isinstance(market, dict)
        and _registry_market_matches(
            market,
            min_minutes_to_resolution=min_minutes_to_resolution,
            max_minutes_to_resolution=max_minutes_to_resolution,
            require_updown=require_updown,
        )
    ]
    eligible.sort(
        key=lambda market: (
            _extract_short_market_hours_to_resolution(market),
            -_extract_volume(market),
            _extract_question(market),
        )
    )
    return eligible


def _load_intraday_raw_updown_markets(
    settings: Settings,
    *,
    min_minutes_to_resolution: int = 0,
    max_minutes_to_resolution: int,
    require_updown: bool,
) -> list[dict[str, Any]]:
    payload = _intraday_registry_raw_updown_payload(settings)
    entries = payload.get("entries", []) if isinstance(payload, dict) else []
    if not isinstance(entries, list):
        return []
    eligible: list[dict[str, Any]] = []
    seen_market_ids: set[str] = set()
    for entry in reversed(entries):
        if not isinstance(entry, dict):
            continue
        market = _merge_intraday_raw_entry_from_rejections(settings, entry)
        market_id = str(market.get("market_id", "") or market.get("id", "")).strip()
        if not market_id or market_id in seen_market_ids:
            continue
        seen_market_ids.add(market_id)
        resolved = dict(market) if _extract_market_token_ids(market) else _resolve_intraday_market_tokens(market)
        if not isinstance(resolved, dict):
            continue
        record = _intraday_registry_market_record(resolved, source="raw-updown-load")
        if record is None:
            continue
        if not _registry_market_matches(
            record,
            min_minutes_to_resolution=min_minutes_to_resolution,
            max_minutes_to_resolution=max_minutes_to_resolution,
            require_updown=require_updown,
        ):
            continue
        eligible.append(record)
    eligible.sort(
        key=lambda market: (
            _extract_short_market_hours_to_resolution(market),
            -_extract_volume(market),
            _extract_question(market),
        )
    )
    return eligible


def _load_intraday_imminent_updown_markets(
    settings: Settings,
    *,
    min_minutes_to_resolution: int = 0,
    max_minutes_to_resolution: int,
    require_updown: bool,
) -> list[dict[str, Any]]:
    payload = _json_load(settings.intraday_registry_imminent_updown_path, {"entries": []})
    entries = payload.get("entries", []) if isinstance(payload, dict) else []
    if not isinstance(entries, list):
        return []
    now = datetime.now(timezone.utc)
    max_seen_age_minutes = max(int(settings.intraday_registry_imminent_seen_age_minutes), 1)
    eligible: list[dict[str, Any]] = []
    seen_market_ids: set[str] = set()
    for entry in reversed(entries):
        if not isinstance(entry, dict):
            continue
        seen_at = _normalize_iso(str(entry.get("seen_at", "")))
        if seen_at is None:
            continue
        age_minutes = max((now - seen_at).total_seconds() / 60.0, 0.0)
        if age_minutes > max_seen_age_minutes:
            continue
        market = _merge_intraday_raw_entry_from_rejections(settings, entry)
        market_id = str(market.get("market_id", "") or market.get("id", "")).strip()
        if not market_id or market_id in seen_market_ids:
            continue
        seen_market_ids.add(market_id)
        resolved = dict(market) if _extract_market_token_ids(market) else _resolve_intraday_market_tokens(market)
        if not isinstance(resolved, dict):
            continue
        record = _intraday_registry_market_record(resolved, source="imminent-updown-load")
        if record is None:
            continue
        if not _registry_market_matches(
            record,
            min_minutes_to_resolution=min_minutes_to_resolution,
            max_minutes_to_resolution=max_minutes_to_resolution,
            require_updown=require_updown,
        ):
            continue
        eligible.append(record)
    eligible.sort(
        key=lambda market: (
            _extract_short_market_hours_to_resolution(market),
            -_extract_volume(market),
            _extract_question(market),
        )
    )
    return eligible


def _load_intraday_threshold_live_markets(
    settings: Settings,
    *,
    max_minutes_to_resolution: int,
) -> list[dict[str, Any]]:
    payload = _json_load(settings.intraday_registry_threshold_live_path, {"entries": []})
    entries = payload.get("entries", []) if isinstance(payload, dict) else []
    if not isinstance(entries, list):
        return []
    eligible: list[dict[str, Any]] = []
    seen_market_ids: set[str] = set()
    for entry in reversed(entries):
        if not isinstance(entry, dict):
            continue
        market_id = str(entry.get("market_id", "") or entry.get("id", "")).strip()
        if not market_id or market_id in seen_market_ids:
            continue
        seen_market_ids.add(market_id)
        resolved = dict(entry) if _extract_market_token_ids(entry) else _resolve_intraday_market_tokens(entry)
        if not isinstance(resolved, dict):
            continue
        record = _intraday_registry_market_record(resolved, source="threshold-live-load")
        if record is None:
            continue
        if not _registry_market_matches(
            record,
            max_minutes_to_resolution=max_minutes_to_resolution,
            require_updown=False,
        ):
            continue
        eligible.append(record)
    eligible.sort(
        key=lambda market: (
            _extract_short_market_hours_to_resolution(market),
            -_extract_volume(market),
            _extract_question(market),
        )
    )
    return eligible


def _load_intraday_imminent_box_arb_tape_markets(
    settings: Settings,
    *,
    max_minutes_to_resolution: int,
) -> list[dict[str, Any]]:
    payload = _json_load(settings.intraday_registry_imminent_box_arb_tape_path, {"entries": []})
    entries = payload.get("entries", []) if isinstance(payload, dict) else []
    if not isinstance(entries, list):
        return []
    eligible: list[dict[str, Any]] = []
    seen_market_ids: set[str] = set()
    for entry in reversed(entries):
        if not isinstance(entry, dict):
            continue
        market_id = str(entry.get("market_id", "") or entry.get("id", "")).strip()
        if not market_id or market_id in seen_market_ids:
            continue
        seen_market_ids.add(market_id)
        resolved = dict(entry) if _extract_market_token_ids(entry) else _resolve_intraday_market_tokens(entry)
        if not isinstance(resolved, dict):
            continue
        record = _intraday_registry_market_record(resolved, source="imminent-box-load")
        if record is None:
            continue
        if not _registry_market_matches(
            record,
            max_minutes_to_resolution=max_minutes_to_resolution,
            require_updown=True,
        ):
            continue
        eligible.append(record)
    eligible.sort(
        key=lambda market: (
            _extract_short_market_hours_to_resolution(market),
            -_extract_volume(market),
            _extract_question(market),
        )
    )
    return eligible


def _load_intraday_registry_watchlist_markets(
    settings: Settings,
    *,
    min_minutes_to_resolution: int = 0,
    max_minutes_to_resolution: int,
    require_updown: bool,
) -> list[dict[str, Any]]:
    if not settings.intraday_registry_enabled:
        return []
    payload = _intraday_registry_payload(settings)
    if not payload or not _intraday_registry_is_fresh(settings, payload):
        return []
    markets = payload.get("markets", [])
    if not isinstance(markets, list):
        return []
    eligible: list[dict[str, Any]] = []
    for market in markets:
        if not isinstance(market, dict):
            continue
        if not bool(market.get("registry_watch_only")):
            continue
        if not _registry_market_matches(
            market,
            min_minutes_to_resolution=min_minutes_to_resolution,
            max_minutes_to_resolution=max_minutes_to_resolution,
            require_updown=require_updown,
        ):
            continue
        eligible.append(market)
    eligible.sort(
        key=lambda market: (
            _extract_short_market_hours_to_resolution(market),
            -_extract_volume(market),
            _extract_question(market),
        )
    )
    return eligible


def _dedupe_markets_by_id(markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for market in markets:
        if not isinstance(market, dict):
            continue
        market_id = _extract_market_id(market)
        if not market_id:
            continue
        existing = deduped.get(market_id)
        if existing is None or _extract_volume(market) >= _extract_volume(existing):
            deduped[market_id] = market
    return sorted(
        deduped.values(),
        key=lambda market: (
            _extract_short_market_hours_to_resolution(market),
            -_extract_volume(market),
            _extract_question(market),
        ),
    )


def _load_intraday_book_tape_markets(
    settings: Settings,
    *,
    min_minutes_to_resolution: int = 0,
    max_minutes_to_resolution: int,
    require_updown: bool,
) -> list[dict[str, Any]]:
    payload = _intraday_registry_book_tape_payload(settings)
    entries = payload.get("entries", []) if isinstance(payload, dict) else []
    if not isinstance(entries, list):
        return []
    now = datetime.now(timezone.utc)
    max_seen_age_minutes = max(int(settings.intraday_registry_imminent_seen_age_minutes), 1)
    min_hours = max(float(min_minutes_to_resolution), 0.0) / 60.0
    max_hours = max(float(max_minutes_to_resolution), 0.0) / 60.0
    eligible: list[dict[str, Any]] = []
    seen_market_ids: set[str] = set()
    for entry in reversed(entries):
        if not isinstance(entry, dict):
            continue
        seen_at = _normalize_iso(str(entry.get("seen_at", "")))
        if seen_at is None:
            continue
        age_minutes = max((now - seen_at).total_seconds() / 60.0, 0.0)
        if age_minutes > max_seen_age_minutes:
            continue
        market_id = str(entry.get("market_id", "") or entry.get("id", "")).strip()
        if not market_id or market_id in seen_market_ids:
            continue
        seen_market_ids.add(market_id)
        question = _extract_question(entry)
        slug = _extract_slug(entry)
        if require_updown and not _is_five_minute_updown_market(question, slug):
            continue
        hours_to_resolution = _extract_short_market_hours_to_resolution(entry)
        if hours_to_resolution < min_hours or hours_to_resolution > max_hours:
            continue
        if not bool(_first(entry, "active", default=True)) or not bool(_first(entry, "acceptingOrders", default=True)):
            continue
        if not _extract_yes_token_id(entry):
            continue
        eligible.append(dict(entry))
    eligible.sort(
        key=lambda market: (
            _extract_short_market_hours_to_resolution(market),
            -_extract_volume(market),
            _extract_question(market),
        )
    )
    return eligible


def _merge_intraday_raw_entry_from_rejections(settings: Settings, entry: dict[str, Any]) -> dict[str, Any]:
    market = dict(entry)
    if _extract_market_token_ids(market) and _first(market, "acceptingOrders", "accepting_orders", default=None) is not None:
        return market
    payload = _intraday_registry_rejections_payload(settings)
    candidates = payload.get("entries", []) if isinstance(payload, dict) else []
    if not isinstance(candidates, list):
        return market
    market_id = str(market.get("market_id", "") or market.get("id", "")).strip()
    slug = str(market.get("slug", "")).strip()
    question = str(market.get("question", "")).strip()
    for candidate in reversed(candidates):
        if not isinstance(candidate, dict):
            continue
        candidate_market_id = str(candidate.get("market_id", "")).strip()
        candidate_slug = str(candidate.get("slug", "")).strip()
        candidate_question = str(candidate.get("question", "")).strip()
        if market_id and candidate_market_id == market_id:
            match = True
        elif slug and candidate_slug == slug:
            match = True
        elif question and candidate_question == question:
            match = True
        else:
            match = False
        if not match:
            continue
        token_ids = candidate.get("token_ids", [])
        if token_ids and not _extract_market_token_ids(market):
            market["token_ids"] = token_ids
            market["clobTokenIds"] = token_ids
        for source_key, target_key in (
            ("active", "active"),
            ("closed", "closed"),
            ("archived", "archived"),
            ("accepting_orders", "accepting_orders"),
            ("accepting_orders", "acceptingOrders"),
        ):
            source_value = candidate.get(source_key)
            if source_value is not None:
                market[target_key] = source_value
        if market.get("hours_to_resolution") in (None, "", []):
            market["hours_to_resolution"] = candidate.get("hours_to_resolution")
        return market
    return market


def _write_intraday_registry(settings: Settings, markets: list[dict[str, Any]], metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    deduped: dict[str, dict[str, Any]] = {}
    for market in markets:
        record = _intraday_registry_market_record(market, source="registry-merge")
        if record is None:
            continue
        if not _registry_market_persistable(settings, record):
            continue
        existing = deduped.get(record["id"])
        if existing is None or _extract_volume(record) >= _extract_volume(existing):
            deduped[record["id"]] = record
    sorted_markets = sorted(
        deduped.values(),
        key=lambda market: (
            _extract_short_market_hours_to_resolution(market),
            -_extract_volume(market),
            _extract_question(market),
        ),
    )
    payload = {
        "updated_at": utc_now_iso(),
        "markets": sorted_markets,
        "market_count": len(sorted_markets),
        "metadata": metadata or {},
    }
    _json_dump(settings.intraday_registry_path, payload)
    return payload


def _bootstrap_intraday_registry(settings: Settings) -> dict[str, Any]:
    page_size = 500
    total_limit = max(settings.intraday_registry_bootstrap_limit, page_size)
    pages = max(1, math.ceil(total_limit / page_size))
    eligible: list[dict[str, Any]] = []
    fetched = 0
    for page_index in range(pages):
        page = _fetch_gamma_intraday_markets_page(page_size, page_index * page_size)
        if not page:
            break
        fetched += len(page)
        for market in page:
            _append_intraday_registry_raw_updown(settings, market, source="gamma", event_type="bootstrap")
            rejection_reason, rejection_details = _intraday_registry_rejection_reason(settings, market, require_updown=False)
            if rejection_reason:
                _append_intraday_registry_rejection(
                    settings,
                    market,
                    source="gamma",
                    reason=rejection_reason,
                    details=rejection_details,
                )
                continue
            record = _intraday_registry_market_record(market, source="gamma")
            if record is None:
                continue
            eligible.append(record)
        if len(page) < page_size:
            break
    return _write_intraday_registry(
        settings,
        eligible,
        metadata={"source": "gamma-bootstrap", "fetched_markets": fetched, "eligible_markets": len(eligible)},
    )


def _bootstrap_intraday_registry_from_cli(settings: Settings, cli: PolymarketCLI) -> dict[str, Any]:
    limit = max(int(settings.intraday_registry_bootstrap_limit), 1)
    try:
        markets = cli.list_markets(limit)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"cli bootstrap failed: {exc}") from exc
    eligible: list[dict[str, Any]] = []
    fetched = 0
    for market in markets:
        if not isinstance(market, dict):
            continue
        fetched += 1
        _append_intraday_registry_raw_updown(settings, market, source="cli", event_type="bootstrap")
        rejection_reason, rejection_details = _intraday_registry_rejection_reason(settings, market, require_updown=False)
        if rejection_reason:
            _append_intraday_registry_rejection(
                settings,
                market,
                source="cli",
                reason=rejection_reason,
                details=rejection_details,
            )
            continue
        record = _intraday_registry_market_record(market, source="cli")
        if record is None:
            continue
        eligible.append(record)
    return _write_intraday_registry(
        settings,
        eligible,
        metadata={"source": "cli-bootstrap", "fetched_markets": fetched, "eligible_markets": len(eligible)},
    )


def _registry_records_from_payload(payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    markets = payload.get("markets", []) if isinstance(payload, dict) else []
    if not isinstance(markets, list):
        return {}
    return {
        record["id"]: record
        for record in markets
        if isinstance(record, dict) and record.get("id")
    }


def _subscription_seed_records_from_markets(settings: Settings, markets: list[dict[str, Any]], *, source: str) -> dict[str, dict[str, Any]]:
    seeded: dict[str, dict[str, Any]] = {}
    for market in markets:
        if not isinstance(market, dict):
            continue
        _append_intraday_registry_raw_updown(settings, market, source=source, event_type="seed")
        _intraday_registry_merge_market(settings, seeded, market, source=source)
    return seeded


def _subscription_seed_fetch_limit(settings: Settings) -> int:
    return min(max(int(settings.intraday_registry_bootstrap_limit), 1), 200)


def _intraday_registry_watch_only_source(source: str) -> bool:
    return source.startswith("ws:") or source.startswith("gamma") or source.startswith("cli")


def _asset_allowed_for_5m_box(settings: Settings, asset: str) -> bool:
    allowed = tuple(str(item).strip().lower() for item in settings.ab_test_crypto_5m_box_assets if str(item).strip())
    if not allowed:
        return True
    return str(asset or "").strip().lower() in set(allowed)


def _refresh_gamma_watchlist_records(
    settings: Settings,
    records: dict[str, dict[str, Any]],
    *,
    source: str,
) -> dict[str, int]:
    refresh_limit = max(int(settings.intraday_registry_gamma_refresh_limit), 1)
    tag_payload = _gamma_tag_updown_market_payload(refresh_limit, settings)
    page = list(tag_payload.get("markets", []))
    fetched = 0
    accepted = 0
    new = 0
    updated = 0
    watch_only = 0
    fetched += len(page)
    for market in page:
        if not isinstance(market, dict):
            continue
        _append_intraday_registry_raw_updown(settings, market, source="gamma", event_type="refresh")
        result = _intraday_registry_merge_market(settings, records, market, source=source)
        if not result.get("accepted"):
            continue
        accepted += 1
        if result.get("is_new"):
            new += 1
        elif result.get("updated"):
            updated += 1
        if result.get("watch_only"):
            watch_only += 1
    return {
        "tag_candidate_count": int(tag_payload.get("tag_candidate_count", 0) or 0),
        "tag_live_upcoming_count": len(page),
        "tag_near_term_count": len(page),
        "fetched_markets": fetched,
        "raw_fetched_markets": int(tag_payload.get("fetched_count", 0) or 0),
        "accepted_markets": accepted,
        "new_markets": new,
        "updated_markets": updated,
        "watch_only_markets": watch_only,
    }


def _intraday_ws_subscription_payload(settings: Settings, markets: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = _intraday_ws_subscription_asset_ids(markets)
    return _intraday_ws_subscription_payload_for_assets(ordered)


def _intraday_ws_subscription_asset_ids(markets: list[dict[str, Any]]) -> list[str]:
    asset_ids: list[str] = []
    for market in markets:
        asset_ids.extend(_extract_market_token_ids(market))
    seen: set[str] = set()
    ordered = []
    for asset_id in asset_ids:
        if asset_id in seen:
            continue
        seen.add(asset_id)
        ordered.append(asset_id)
    return ordered


def _intraday_ws_subscription_payload_for_assets(asset_ids: list[str]) -> dict[str, Any]:
    return {
        "type": "market",
        "assets_ids": list(asset_ids),
        "custom_feature_enabled": True,
    }


def _intraday_ws_asset_batches(settings: Settings, markets: list[dict[str, Any]]) -> list[list[str]]:
    asset_ids = _intraday_ws_subscription_asset_ids(markets)
    batch_size = max(int(settings.intraday_registry_ws_batch_asset_limit), 1)
    if not asset_ids:
        return []
    return [asset_ids[index : index + batch_size] for index in range(0, len(asset_ids), batch_size)]


def _intraday_ws_market_keys(market: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    for value in (
        _extract_market_id(market),
        str(_first(market, "market", default="")).strip(),
        str(_first(market, "conditionId", "condition_id", default="")).strip(),
        str(_first(market, "slug", default="")).strip(),
    ):
        if value and value not in keys:
            keys.append(value)
    return keys


def _parse_intraday_ws_payloads(raw: str) -> list[Any]:
    if raw in (None, ""):
        return []
    try:
        return [json.loads(raw)]
    except json.JSONDecodeError as exc:
        if "Extra data" not in str(exc):
            return []
    decoder = json.JSONDecoder()
    index = 0
    payloads: list[Any] = []
    length = len(raw)
    while index < length:
        while index < length and raw[index].isspace():
            index += 1
        if index >= length:
            break
        try:
            payload, next_index = decoder.raw_decode(raw, index)
        except json.JSONDecodeError:
            break
        payloads.append(payload)
        index = next_index
    return payloads


def _index_intraday_ws_market(
    market: dict[str, Any],
    *,
    asset_lookup: dict[str, dict[str, Any]],
    market_lookup: dict[str, dict[str, Any]],
) -> None:
    for token_id in _extract_market_token_ids(market):
        if token_id:
            asset_lookup[str(token_id)] = market
    for key in _intraday_ws_market_keys(market):
        if key:
            market_lookup[key] = market


def _market_from_intraday_ws_event(
    event: dict[str, Any],
    *,
    asset_lookup: dict[str, dict[str, Any]],
    market_lookup: dict[str, dict[str, Any]],
) -> list[tuple[dict[str, Any], str]]:
    if not isinstance(event, dict):
        return []
    event_type = str(_first(event, "event_type", "eventType", "type", default="")).lower()
    direct_market = _normalize_intraday_ws_market(event)
    if direct_market is not None:
        materialized = dict(direct_market)
        if "clobTokenIds" not in materialized:
            token_ids = _extract_market_token_ids(event)
            if token_ids:
                materialized["clobTokenIds"] = token_ids
        return [(materialized, event_type or "market")]

    def base_market_for_asset(asset_id: str) -> dict[str, Any] | None:
        asset_id = str(asset_id or "").strip()
        if not asset_id:
            return None
        market = asset_lookup.get(asset_id)
        if market is not None:
            return dict(market)
        return None

    if event_type == "book":
        asset_id = str(event.get("asset_id", "")).strip()
        market = base_market_for_asset(asset_id)
        if market is None:
            market_key = str(event.get("market", "")).strip()
            matched = market_lookup.get(market_key)
            market = dict(matched) if matched is not None else None
        if market is None:
            return []
        market["asset_id"] = asset_id
        market["cached_bids"] = event.get("bids", []) if isinstance(event.get("bids"), list) else []
        market["cached_asks"] = event.get("asks", []) if isinstance(event.get("asks"), list) else []
        market["last_trade_price"] = _first(event, "last_trade_price", default=market.get("last_trade_price"))
        market["market"] = _first(event, "market", default=market.get("market"))
        return [(market, event_type)]

    if event_type == "best_bid_ask":
        asset_id = str(event.get("asset_id", "")).strip()
        market = base_market_for_asset(asset_id)
        if market is None:
            market_key = str(event.get("market", "")).strip()
            matched = market_lookup.get(market_key)
            market = dict(matched) if matched is not None else None
        if market is None:
            return []
        best_bid = _as_float(event.get("best_bid"), 0.0)
        best_ask = _as_float(event.get("best_ask"), 1.0)
        market["asset_id"] = asset_id
        market["cached_bids"] = [{"price": f"{best_bid:.6f}", "size": "1"}] if best_bid > 0 else []
        market["cached_asks"] = [{"price": f"{best_ask:.6f}", "size": "1"}] if best_ask > 0 else []
        market["market"] = _first(event, "market", default=market.get("market"))
        return [(market, event_type)]

    if event_type == "price_change":
        materialized: list[tuple[dict[str, Any], str]] = []
        changes = event.get("price_changes")
        if not isinstance(changes, list):
            return materialized
        for change in changes:
            if not isinstance(change, dict):
                continue
            asset_id = str(change.get("asset_id", "")).strip()
            market = base_market_for_asset(asset_id)
            if market is None:
                market_key = str(event.get("market", "")).strip()
                matched = market_lookup.get(market_key)
                market = dict(matched) if matched is not None else None
            if market is None:
                continue
            best_bid = _as_float(change.get("best_bid"), 0.0)
            best_ask = _as_float(change.get("best_ask"), 1.0)
            market["asset_id"] = asset_id
            market["cached_bids"] = [{"price": f"{best_bid:.6f}", "size": str(change.get("size", "1"))}] if best_bid > 0 else []
            market["cached_asks"] = [{"price": f"{best_ask:.6f}", "size": str(change.get("size", "1"))}] if best_ask > 0 else []
            market["last_trade_price"] = change.get("price")
            market["last_trade_side"] = change.get("side")
            market["market"] = _first(event, "market", default=market.get("market"))
            materialized.append((market, event_type))
        return materialized

    return []


def _normalize_intraday_ws_market(payload: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    market = payload.get("market")
    if isinstance(market, dict):
        return market
    if isinstance(payload.get("data"), dict):
        data = payload["data"]
        if isinstance(data.get("market"), dict):
            return data["market"]
        return data
    if payload.get("question") or payload.get("title"):
        return payload
    return None


def _intraday_audit_ws_entry(event: dict[str, Any], market: dict[str, Any], *, event_type: str) -> dict[str, Any]:
    book = _cached_book_from_market(market)
    midpoint = _as_float(market.get("midpoint"), 0.0) or (_midpoint_from_book(book) or 0.0)
    bids = book.get("bids", []) if isinstance(book, dict) else []
    asks = book.get("asks", []) if isinstance(book, dict) else []
    bids_depth = _sum_book_depth(bids)
    asks_depth = _sum_book_depth(asks)
    return {
        "recorded_at": utc_now_iso(),
        "event_type": event_type,
        "market_id": _extract_market_id(market),
        "asset_id": str(_first(market, "asset_id", default="")).strip(),
        "question": _extract_question(market),
        "slug": _extract_slug(market),
        "hours_to_resolution": round(_extract_short_market_hours_to_resolution(market), 6),
        "best_bid": round(_best_price(bids, side="bid"), 6),
        "best_ask": round(_best_price(asks, default=1.0, side="ask"), 6),
        "midpoint": round(midpoint, 6) if midpoint > 0 else 0.0,
        "bids_depth_usdc": round(bids_depth, 2),
        "asks_depth_usdc": round(asks_depth, 2),
        "min_depth_usdc": round(min(bids_depth, asks_depth), 2),
        "raw_event": event,
        "market": market,
    }


def _capture_intraday_registry_ws_updates(settings: Settings, base_markets: list[dict[str, Any]], *, timeout_seconds: int) -> dict[str, Any]:
    if websocket_connect is None or not settings.intraday_registry_ws_enabled:
        return {"connected": False, "messages": 0, "new_markets": 0, "updated_markets": 0, "enriched_markets": 0}
    endpoint = settings.intraday_registry_ws_endpoint.strip()
    if not endpoint:
        return {"connected": False, "messages": 0, "new_markets": 0, "updated_markets": 0, "enriched_markets": 0}

    records: dict[str, dict[str, Any]] = {}
    asset_lookup: dict[str, dict[str, Any]] = {}
    market_lookup: dict[str, dict[str, Any]] = {}
    for market in base_markets:
        record = _intraday_registry_market_record(market, source="gamma")
        if record is not None:
            records[record["id"]] = record
            _index_intraday_ws_market(record, asset_lookup=asset_lookup, market_lookup=market_lookup)

    stats = {"connected": False, "messages": 0, "new_markets": 0, "updated_markets": 0, "enriched_markets": 0}
    audit_entries: list[dict[str, Any]] = []
    asset_batches = _intraday_ws_asset_batches(settings, list(records.values()))
    deadline = time.monotonic() + max(timeout_seconds, 1)
    last_error = None
    for index, asset_batch in enumerate(asset_batches):
        remaining_batches = max(len(asset_batches) - index, 1)
        remaining_cycle = deadline - time.monotonic()
        if remaining_cycle <= 0:
            break
        subscription = _intraday_ws_subscription_payload_for_assets(asset_batch)
        batch_deadline = time.monotonic() + max(remaining_cycle / remaining_batches, 0.5)
        try:
            with websocket_connect(
                endpoint,
                open_timeout=5,
                close_timeout=2,
                max_size=max(int(settings.intraday_registry_ws_max_message_bytes), 1),
            ) as websocket:
                stats["connected"] = True
                websocket.send(json.dumps(subscription))
                while time.monotonic() < batch_deadline:
                    remaining = max(batch_deadline - time.monotonic(), 0.25)
                    try:
                        raw = websocket.recv(timeout=remaining)
                    except TimeoutError:
                        break
                    if raw in (None, ""):
                        continue
                    stats["messages"] += 1
                    for message in _parse_intraday_ws_payloads(raw):
                        events = message if isinstance(message, list) else [message]
                        for event in events:
                            if not isinstance(event, dict):
                                continue
                            event_type = str(_first(event, "event_type", "eventType", "type", default="")).lower()
                            materialized_markets = _market_from_intraday_ws_event(
                                event,
                                asset_lookup=asset_lookup,
                                market_lookup=market_lookup,
                            )
                            for market, normalized_event_type in materialized_markets:
                                audit_entries.append(
                                    _intraday_audit_ws_entry(
                                        event,
                                        market,
                                        event_type=normalized_event_type or event_type or "market",
                                    )
                                )
                                result = _intraday_registry_merge_market(
                                    settings,
                                    records,
                                    market,
                                    source=f"ws:{normalized_event_type or event_type or 'market'}",
                                )
                                if not result.get("accepted"):
                                    continue
                                accepted = records.get(str(result.get("market_id", "")))
                                if accepted is not None:
                                    _index_intraday_ws_market(accepted, asset_lookup=asset_lookup, market_lookup=market_lookup)
                                if result.get("was_enriched"):
                                    stats["enriched_markets"] += 1
                                if result.get("is_new"):
                                    stats["new_markets"] += 1
                                elif result.get("updated"):
                                    stats["updated_markets"] += 1
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
    if last_error and not stats["connected"]:
        stats["error"] = last_error
    elif last_error:
        stats["partial_error"] = last_error

    _record_intraday_audit_ws_events(settings, audit_entries)
    payload = _write_intraday_registry(
        settings,
        list(records.values()),
        metadata={
            "source": "gamma+ws",
            "websocket": stats,
            "bootstrap_markets": len(base_markets),
        },
    )
    stats["market_count"] = int(payload.get("market_count", 0))
    return stats


def _intraday_registry_records(settings: Settings) -> dict[str, dict[str, Any]]:
    payload = _intraday_registry_payload(settings)
    markets = payload.get("markets", []) if isinstance(payload, dict) else []
    records: dict[str, dict[str, Any]] = {}
    if not isinstance(markets, list):
        return records
    for market in markets:
        if not isinstance(market, dict):
            continue
        record = _intraday_registry_market_record(market, source="registry-load")
        if record is None:
            continue
        if not _registry_market_persistable(settings, record):
            continue
        records[record["id"]] = record
    return records


def _intraday_registry_merge_market(
    settings: Settings,
    records: dict[str, dict[str, Any]],
    market: dict[str, Any],
    *,
    source: str,
) -> dict[str, Any]:
    working_market = market
    was_enriched = False
    rejection_reason, rejection_details = _intraday_registry_rejection_reason(settings, working_market, require_updown=False)
    if rejection_reason in {"missing_token_ids", "inactive", "not_accepting_orders"}:
        enriched_market = _resolve_intraday_market_tokens(working_market)
        if enriched_market is not None:
            working_market = enriched_market
            was_enriched = True
            rejection_reason, rejection_details = _intraday_registry_rejection_reason(settings, working_market, require_updown=False)
    if rejection_reason:
        record = _intraday_registry_market_record(working_market, source=source)
        watch_window_hours = max(int(settings.intraday_registry_watchlist_max_minutes_to_resolution), 1) / 60.0
        watch_only = (
            _intraday_registry_watch_only_source(source)
            and record is not None
            and rejection_reason in {"inactive", "outside_max_minutes_window", "not_accepting_orders"}
            and 0.0 < _extract_short_market_hours_to_resolution(working_market) <= watch_window_hours
        )
        watch_existing = records.get(record["id"]) if record is not None else None
        if watch_only and record is not None:
            record["registry_watch_only"] = True
            record["registry_rejection_reason"] = rejection_reason
            if watch_existing is not None:
                record["registry_sources"] = sorted(
                    {*(watch_existing.get("registry_sources", []) or []), *(record.get("registry_sources", []) or [])}
                )
            records[record["id"]] = record
        _append_intraday_registry_rejection(
            settings,
            working_market,
            source=source,
            reason=rejection_reason,
            details=rejection_details,
        )
        return {
            "accepted": watch_only,
            "is_new": bool(watch_only and record is not None and watch_existing is None),
            "updated": bool(watch_only and record is not None and watch_existing is not None),
            "was_enriched": was_enriched,
            "market_id": _extract_market_id(working_market),
            "reason": rejection_reason,
            "watch_only": watch_only,
        }
    record = _intraday_registry_market_record(working_market, source=source)
    if record is None:
        return {
            "accepted": False,
            "is_new": False,
            "updated": False,
            "was_enriched": was_enriched,
            "market_id": _extract_market_id(working_market),
            "reason": "record_build_failed",
        }
    existing = records.get(record["id"])
    if existing is not None:
        record["registry_sources"] = sorted(
            {*(existing.get("registry_sources", []) or []), *(record.get("registry_sources", []) or [])}
        )
    records[record["id"]] = record
    return {
        "accepted": True,
        "is_new": existing is None,
        "updated": existing is not None,
        "was_enriched": was_enriched,
        "market_id": record["id"],
        "reason": None,
    }


def _record_intraday_book_tape(settings: Settings, cli: PolymarketCLI) -> dict[str, Any]:
    records = _intraday_registry_records(settings)
    shortlisted = sorted(
        records.values(),
        key=lambda market: (
            _extract_short_market_hours_to_resolution(market),
            -_extract_volume(market),
            _extract_question(market),
        ),
    )[: max(settings.intraday_registry_book_snapshot_topn, 1)]
    tape_payload = _intraday_registry_book_tape_payload(settings)
    tape_entries = tape_payload.get("entries", []) if isinstance(tape_payload, dict) else []
    if not isinstance(tape_entries, list):
        tape_entries = []
    current_snapshot_entries: list[dict[str, Any]] = []
    threshold_live_entries: list[dict[str, Any]] = []
    transition_entries: list[dict[str, Any]] = []
    imminent_box_arb_entries: list[dict[str, Any]] = []
    raw_imminent_box_entries: list[dict[str, Any]] = []
    maker_box_simulation_entries: list[dict[str, Any]] = []
    aggressive_box_simulation_entries: list[dict[str, Any]] = []
    snapshots_recorded = 0
    snapshots_with_midpoint = 0
    snapshots_with_depth = 0
    for market in shortlisted:
        token_id = _extract_yes_token_id(market)
        if not token_id:
            continue
        question = _extract_question(market)
        slug = _extract_slug(market)
        book = _safe_book(cli, token_id)
        bids = book.get("bids", []) if isinstance(book, dict) else []
        asks = book.get("asks", []) if isinstance(book, dict) else []
        midpoint = _safe_midpoint(cli, token_id)
        if midpoint is None:
            midpoint = _midpoint_from_book(book)
        bids_depth = _sum_book_depth(bids)
        asks_depth = _sum_book_depth(asks)
        min_depth = min(bids_depth, asks_depth)
        best_bid = _best_price(bids, side="bid")
        best_ask = _best_price(asks, default=1.0, side="ask")
        spread = max(best_ask - best_bid, 0.0)
        entry = {
            "seen_at": utc_now_iso(),
            "market_id": _extract_market_id(market),
            "question": question,
            "slug": slug,
            "asset": _short_crypto_market_asset(question, slug),
            "token_id": token_id,
            "clobTokenIds": _extract_market_token_ids(market),
            "hours_to_resolution": round(_extract_short_market_hours_to_resolution(market), 6),
            "midpoint": round(midpoint, 6) if midpoint is not None else None,
            "best_bid": round(best_bid, 6),
            "best_ask": round(best_ask, 6),
            "spread": round(spread, 6),
            "bids_depth_usdc": round(bids_depth, 2),
            "asks_depth_usdc": round(asks_depth, 2),
            "min_depth_usdc": round(min_depth, 2),
            "bid_levels": len(bids),
            "ask_levels": len(asks),
            "cached_bids": bids[:5],
            "cached_asks": asks[:5],
            "is_updown": _is_five_minute_updown_market(question, slug),
            "active": bool(_first(market, "active", default=True)),
            "acceptingOrders": bool(_first(market, "acceptingOrders", default=True)),
            "endDate": market.get("endDate"),
        }
        tape_entries.append(entry)
        current_snapshot_entries.append(entry)
        snapshots_recorded += 1
        if midpoint is not None:
            snapshots_with_midpoint += 1
        if min_depth > 0:
            snapshots_with_depth += 1
        if bool(market.get("registry_watch_only")) and midpoint is not None and min_depth > 0:
            transition_stage = None
            if (
                not entry["is_updown"]
                and 0.0 < entry["hours_to_resolution"] <= (max(settings.ab_test_crypto_threshold_snapshot_max_minutes_to_resolution, 1) / 60.0)
            ):
                transition_stage = "threshold"
            elif (
                entry["is_updown"]
                and 0.0 < entry["hours_to_resolution"] <= (max(settings.intraday_registry_imminent_max_minutes_to_resolution, 1) / 60.0)
            ):
                transition_stage = "imminent_updown"
            if transition_stage is not None:
                transition_entries.append(
                    {
                        **entry,
                        "transition_stage": transition_stage,
                        "registry_rejection_reason": market.get("registry_rejection_reason"),
                    }
                )
        if (
            not entry["is_updown"]
            and 0.0 < entry["hours_to_resolution"] <= (max(settings.intraday_registry_threshold_live_max_minutes_to_resolution, 1) / 60.0)
            and midpoint is not None
            and min_depth >= settings.ab_test_crypto_threshold_snapshot_min_book_depth_usdc
        ):
            threshold_live_entries.append(
                {
                    **market,
                    "token_id": token_id,
                    "clobTokenIds": _extract_market_token_ids(market),
                    "hours_to_resolution": entry["hours_to_resolution"],
                    "midpoint": entry["midpoint"],
                    "best_bid": entry["best_bid"],
                    "best_ask": entry["best_ask"],
                    "spread": entry["spread"],
                    "bids_depth_usdc": entry["bids_depth_usdc"],
                    "asks_depth_usdc": entry["asks_depth_usdc"],
                    "min_depth_usdc": entry["min_depth_usdc"],
                    "cached_bids": entry["cached_bids"],
                    "cached_asks": entry["cached_asks"],
                    "seen_at": entry["seen_at"],
                }
            )
        if (
            entry["is_updown"]
            and 0.0 < entry["hours_to_resolution"] <= (max(settings.intraday_registry_imminent_max_minutes_to_resolution, 1) / 60.0)
            and midpoint is not None
        ):
            asset = str(_short_crypto_market_asset(_extract_question(market), _extract_slug(market)) or market.get("asset") or "").strip().lower()
            if not _asset_allowed_for_5m_box(settings, asset):
                continue
            candidate = _candidate_from_cached_short_market({**market, **entry})
            raw_box_entry = {**market, **entry, "meets_min_depth": min_depth >= settings.ab_test_crypto_5m_box_min_book_depth_usdc}
            if candidate is not None:
                yes_levels = _book_contract_levels(book, "BUY", "open") or _fallback_contract_levels_from_candidate(candidate, "BUY", "open")
                no_levels = _book_contract_levels(book, "SELL", "open") or _fallback_contract_levels_from_candidate(candidate, "SELL", "open")
                execution = _simulate_box_pair_open(yes_levels, no_levels, settings.ab_test_crypto_5m_box_pair_budget_usdc)
                metrics = _box_arb_edge_metrics(
                    execution,
                    estimated_fee_per_share=settings.ab_test_crypto_5m_box_estimated_fee_per_share,
                    estimated_slippage_per_share=settings.ab_test_crypto_5m_box_estimated_slippage_per_share,
                )
                raw_box_entry.update(
                    {
                        "filled_shares": execution.get("filled_shares", 0.0),
                        "combined_contract_price": execution.get("combined_contract_price", 0.0),
                        "yes_contract_price": execution.get("avg_yes_contract_price", 0.0),
                        "no_contract_price": execution.get("avg_no_contract_price", 0.0),
                        **metrics,
                    }
                )
                raw_box_entry.update(
                    _simulate_box_pair_open_maker_proxy(
                        book,
                        settings.ab_test_crypto_5m_box_pair_budget_usdc,
                        estimated_fee_per_share=settings.ab_test_crypto_5m_box_estimated_fee_per_share,
                        estimated_slippage_per_share=settings.ab_test_crypto_5m_box_estimated_slippage_per_share,
                        best_bid=_as_float(entry.get("best_bid"), 0.0),
                        best_ask=_as_float(entry.get("best_ask"), 0.0),
                    )
                )
                raw_box_entry.update(
                    _simulate_box_pair_open_aggressive_proxy(
                        settings.ab_test_crypto_5m_box_pair_budget_usdc,
                        best_bid=_as_float(entry.get("best_bid"), 0.0),
                        best_ask=_as_float(entry.get("best_ask"), 0.0),
                        maker_fee_per_share=settings.ab_test_crypto_5m_box_aggressive_maker_fee_per_share,
                        estimated_slippage_per_share=settings.ab_test_crypto_5m_box_aggressive_estimated_slippage_per_share,
                        queue_fill_probability=settings.ab_test_crypto_5m_box_aggressive_queue_fill_probability,
                        tick_size=settings.ab_test_crypto_5m_box_aggressive_tick_size,
                        price_concession_ticks=settings.ab_test_crypto_5m_box_aggressive_price_concession_ticks,
                    )
                )
            raw_imminent_box_entries.append(raw_box_entry)
            maker_box_simulation_entries.append(raw_box_entry)
            aggressive_box_simulation_entries.append(raw_box_entry)
            if candidate is not None and min_depth >= settings.ab_test_crypto_5m_box_min_book_depth_usdc:
                imminent_box_arb_entries.append(raw_box_entry)
    tape_limit = max(settings.intraday_registry_book_tape_limit, 1)
    _json_dump(
        settings.intraday_registry_book_tape_path,
        {
            "updated_at": utc_now_iso(),
            "entries": tape_entries[-tape_limit:],
            "count": len(tape_entries[-tape_limit:]),
        },
    )
    deduped_threshold_live: dict[str, dict[str, Any]] = {}
    for entry in threshold_live_entries:
        market_id = str(entry.get("market_id", "") or entry.get("id", "")).strip()
        if not market_id:
            continue
        deduped_threshold_live[market_id] = entry
    sorted_threshold_live = sorted(
        deduped_threshold_live.values(),
        key=lambda market: (
            _extract_short_market_hours_to_resolution(market),
            -_extract_volume(market),
            _extract_question(market),
        ),
    )[: max(settings.intraday_registry_threshold_live_limit, 1)]
    _json_dump(
        settings.intraday_registry_threshold_live_path,
        {
            "updated_at": utc_now_iso(),
            "entries": sorted_threshold_live,
            "count": len(sorted_threshold_live),
        },
    )
    deduped_transition_entries: dict[tuple[str, str], dict[str, Any]] = {}
    for entry in transition_entries:
        market_id = str(entry.get("market_id", "") or entry.get("id", "")).strip()
        stage = str(entry.get("transition_stage", "")).strip()
        if not market_id or not stage:
            continue
        deduped_transition_entries[(market_id, stage)] = entry
    sorted_transition_entries = sorted(
        deduped_transition_entries.values(),
        key=lambda item: (
            str(item.get("transition_stage", "")),
            _extract_short_market_hours_to_resolution(item),
            -_extract_volume(item),
            _extract_question(item),
        ),
    )[: max(settings.intraday_registry_book_tape_limit, 1)]
    _json_dump(
        settings.intraday_registry_transition_tape_path,
        {
            "updated_at": utc_now_iso(),
            "entries": sorted_transition_entries,
            "count": len(sorted_transition_entries),
        },
    )
    deduped_box_entries: dict[str, dict[str, Any]] = {}
    for entry in imminent_box_arb_entries:
        market_id = str(entry.get("market_id", "") or entry.get("id", "")).strip()
        if not market_id:
            continue
        deduped_box_entries[market_id] = entry
    sorted_box_entries = sorted(
        deduped_box_entries.values(),
        key=lambda item: (
            -_as_float(item.get("gross_edge_per_share"), 0.0),
            _extract_short_market_hours_to_resolution(item),
            -_extract_volume(item),
            _extract_question(item),
        ),
    )[: max(settings.intraday_registry_threshold_live_limit, 1)]
    positive_gross_edge_count = sum(1 for item in sorted_box_entries if _as_float(item.get("gross_edge_per_share"), 0.0) > 0.0)
    positive_net_edge_count = sum(1 for item in sorted_box_entries if _as_float(item.get("net_edge_per_share"), 0.0) > 0.0)
    max_gross_edge_per_share = max((_as_float(item.get("gross_edge_per_share"), 0.0) for item in sorted_box_entries), default=0.0)
    max_net_edge_per_share = max((_as_float(item.get("net_edge_per_share"), 0.0) for item in sorted_box_entries), default=0.0)
    _json_dump(
        settings.intraday_registry_imminent_box_arb_tape_path,
        {
            "updated_at": utc_now_iso(),
            "entries": sorted_box_entries,
            "count": len(sorted_box_entries),
            "positive_gross_edge_count": positive_gross_edge_count,
            "positive_net_edge_count": positive_net_edge_count,
            "max_gross_edge_per_share": round(max_gross_edge_per_share, 6),
            "max_net_edge_per_share": round(max_net_edge_per_share, 6),
        },
    )
    _record_intraday_audit_book_snapshots(settings, current_snapshot_entries, source="official_clob_book")
    _record_intraday_audit_watchlist_transitions(
        settings,
        sorted_transition_entries,
        source="registry_watchlist_transition",
    )
    _record_intraday_audit_imminent_candidates(settings, raw_imminent_box_entries, source="official_clob_book_raw_imminent")
    _record_intraday_audit_arb_opportunities(settings, imminent_box_arb_entries, source="official_clob_book")
    _record_intraday_audit_maker_box_simulations(settings, maker_box_simulation_entries, source="official_clob_book_maker_proxy")
    _record_intraday_audit_aggressive_box_simulations(settings, aggressive_box_simulation_entries, source="official_clob_book_aggressive_proxy")
    return {
        "snapshots_recorded": snapshots_recorded,
        "snapshots_with_midpoint": snapshots_with_midpoint,
        "snapshots_with_depth": snapshots_with_depth,
        "threshold_live_markets": len(sorted_threshold_live),
        "transition_tape_entries": len(sorted_transition_entries),
        "raw_imminent_box_candidates": len(raw_imminent_box_entries),
        "imminent_box_arb_entries": len(sorted_box_entries),
        "positive_gross_edge_count": positive_gross_edge_count,
        "positive_net_edge_count": positive_net_edge_count,
        "max_gross_edge_per_share": round(max_gross_edge_per_share, 6),
        "max_net_edge_per_share": round(max_net_edge_per_share, 6),
    }


def _intraday_registry_population_metrics(settings: Settings) -> dict[str, int]:
    watchlist = _load_intraday_registry_watchlist_markets(
        settings,
        max_minutes_to_resolution=settings.intraday_registry_watchlist_max_minutes_to_resolution,
        require_updown=False,
    )
    imminent = _load_intraday_imminent_updown_markets(
        settings,
        max_minutes_to_resolution=settings.intraday_registry_imminent_max_minutes_to_resolution,
        require_updown=True,
    )
    watchlist_threshold_transition = _load_intraday_registry_watchlist_markets(
        settings,
        max_minutes_to_resolution=settings.ab_test_crypto_threshold_snapshot_max_minutes_to_resolution,
        require_updown=False,
    )
    watchlist_imminent_transition = _load_intraday_registry_watchlist_markets(
        settings,
        max_minutes_to_resolution=settings.intraday_registry_imminent_max_minutes_to_resolution,
        require_updown=True,
    )
    return {
        "watchlist_market_count": len(watchlist),
        "watchlist_updown_count": sum(1 for market in watchlist if _is_five_minute_updown_market(_extract_question(market), _extract_slug(market))),
        "imminent_updown_count": len(imminent),
        "watchlist_threshold_transition_count": len(watchlist_threshold_transition),
        "watchlist_imminent_transition_count": len(watchlist_imminent_transition),
        "watchlist_max_minutes_to_resolution": int(settings.intraday_registry_watchlist_max_minutes_to_resolution),
    }


def _intraday_box_execution_state(registry_result: dict[str, Any], box_tape: dict[str, Any] | None = None) -> str:
    imminent_count = int(registry_result.get("imminent_updown_count", 0) or 0)
    imminent_transition_count = int(registry_result.get("watchlist_imminent_transition_count", 0) or 0)
    imminent_box_entries = int(registry_result.get("imminent_box_arb_entries", 0) or 0)
    positive_gross_edge_count = int(registry_result.get("positive_gross_edge_count", 0) or 0)
    positive_net_edge_count = int(registry_result.get("positive_net_edge_count", 0) or 0)
    box_tape = box_tape if isinstance(box_tape, dict) else {}
    executed_pairs = int(box_tape.get("executed_pairs", 0) or 0)
    executable_pairs = int(box_tape.get("executable_pairs", 0) or 0)
    candidates_seen = int(box_tape.get("candidates_seen", 0) or 0)
    if executed_pairs > 0:
        return "arb executed"
    if positive_net_edge_count > 0:
        return "positive net arb observed"
    if positive_gross_edge_count > 0:
        return "positive gross arb observed"
    if imminent_box_entries > 0 or executable_pairs > 0 or candidates_seen > 0:
        return "imminent supply, no arb"
    if imminent_transition_count > 0 or imminent_count > 0:
        return "imminent supply, no arb"
    return "no imminent supply"


def _append_intraday_edge_alerts(
    settings: Settings,
    *,
    registry_result: dict[str, Any],
    previous_result: dict[str, Any] | None = None,
    box_tape: dict[str, Any] | None = None,
) -> dict[str, Any]:
    previous_result = previous_result if isinstance(previous_result, dict) else {}
    box_tape = box_tape if isinstance(box_tape, dict) else {}
    payload = _json_load(settings.intraday_registry_edge_alerts_path, {"updated_at": None, "summary": {}, "entries": []})
    entries = payload.get("entries", []) if isinstance(payload, dict) else []
    if not isinstance(entries, list):
        entries = []
    now = utc_now_iso()

    def _push(event_type: str, message: str, extra: dict[str, Any] | None = None) -> None:
        entries.append(
            {
                "seen_at": now,
                "event_type": event_type,
                "message": message,
                **(extra or {}),
            }
        )

    current_imminent_transitions = int(registry_result.get("watchlist_imminent_transition_count", 0) or 0)
    previous_imminent_transitions = int(previous_result.get("watchlist_imminent_transition_count", 0) or 0)
    if current_imminent_transitions > previous_imminent_transitions:
        _push(
            "watchlist_to_imminent",
            f"watchlist -> imminent transitions increased to {current_imminent_transitions}",
            {"count": current_imminent_transitions},
        )

    current_imminent_box_entries = int(registry_result.get("imminent_box_arb_entries", 0) or 0)
    previous_imminent_box_entries = int(previous_result.get("imminent_box_arb_entries", 0) or 0)
    if current_imminent_box_entries > previous_imminent_box_entries:
        _push(
            "imminent_box_observed",
            f"imminent box-arb tape entries increased to {current_imminent_box_entries}",
            {"count": current_imminent_box_entries},
        )

    current_positive_gross = int(registry_result.get("positive_gross_edge_count", 0) or 0)
    previous_positive_gross = int(previous_result.get("positive_gross_edge_count", 0) or 0)
    if current_positive_gross > previous_positive_gross:
        _push(
            "positive_gross_edge",
            f"positive gross edge observed on {current_positive_gross} imminent markets",
            {
                "count": current_positive_gross,
                "max_gross_edge_per_share": round(_as_float(registry_result.get("max_gross_edge_per_share"), 0.0), 6),
            },
        )

    current_positive_net = int(registry_result.get("positive_net_edge_count", 0) or 0)
    previous_positive_net = int(previous_result.get("positive_net_edge_count", 0) or 0)
    if current_positive_net > previous_positive_net:
        _push(
            "positive_net_edge",
            f"positive net edge observed on {current_positive_net} imminent markets",
            {
                "count": current_positive_net,
                "max_net_edge_per_share": round(_as_float(registry_result.get("max_net_edge_per_share"), 0.0), 6),
            },
        )

    current_executed_pairs = int(box_tape.get("executed_pairs", 0) or 0)
    previous_executed_pairs = int((payload.get("summary", {}) if isinstance(payload.get("summary", {}), dict) else {}).get("executed_pairs", 0) or 0)
    if current_executed_pairs > previous_executed_pairs:
        _push(
            "arb_executed",
            f"5m box arb executed pairs increased to {current_executed_pairs}",
            {"count": current_executed_pairs},
        )

    summary = {
        "imminent_updown_count": int(registry_result.get("imminent_updown_count", 0) or 0),
        "watchlist_imminent_transition_count": current_imminent_transitions,
        "imminent_box_arb_entries": current_imminent_box_entries,
        "positive_gross_edge_count": current_positive_gross,
        "positive_net_edge_count": current_positive_net,
        "max_gross_edge_per_share": round(_as_float(registry_result.get("max_gross_edge_per_share"), 0.0), 6),
        "max_net_edge_per_share": round(_as_float(registry_result.get("max_net_edge_per_share"), 0.0), 6),
        "execution_state": _intraday_box_execution_state(registry_result, box_tape),
        "executed_pairs": current_executed_pairs,
    }
    trimmed_entries = entries[-200:]
    alert_payload = {
        "updated_at": now,
        "summary": summary,
        "entries": trimmed_entries,
        "count": len(trimmed_entries),
    }
    _json_dump(settings.intraday_registry_edge_alerts_path, alert_payload)
    return alert_payload


def _cached_book_from_market(market: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(market, dict):
        return {}
    bids = market.get("cached_bids")
    asks = market.get("cached_asks")
    return {
        "bids": bids if isinstance(bids, list) else [],
        "asks": asks if isinstance(asks, list) else [],
    }


def _candidate_from_cached_short_market(market: dict[str, Any]) -> MarketCandidate | None:
    if not isinstance(market, dict):
        return None
    question = _extract_question(market)
    slug = _extract_slug(market)
    asset = _short_crypto_market_asset(question, slug)
    token_id = _extract_yes_token_id(market)
    if asset is None or not token_id:
        return None
    book = _cached_book_from_market(market)
    bids = book.get("bids", [])
    asks = book.get("asks", [])
    midpoint = _as_float(market.get("midpoint"), 0.0) or (_midpoint_from_book(book) or 0.0)
    if midpoint <= 0.0:
        return None
    bids_depth = _as_float(market.get("bids_depth_usdc"), 0.0) or _sum_book_depth(bids)
    asks_depth = _as_float(market.get("asks_depth_usdc"), 0.0) or _sum_book_depth(asks)
    best_bid = _as_float(market.get("best_bid"), 0.0) or _best_price(bids, side="bid")
    best_ask = _as_float(market.get("best_ask"), 0.0) or _best_price(asks, default=1.0, side="ask")
    spread = _as_float(market.get("spread"), 0.0) or max(best_ask - best_bid, 0.0)
    denom = bids_depth + asks_depth
    imbalance = ((bids_depth - asks_depth) / denom) if denom else 0.0
    hours_left = _extract_short_market_hours_to_resolution(market)
    depth = min(bids_depth, asks_depth)
    urgency = max(0.0, 1.0 - (hours_left * 60.0 / 45.0))
    score = (abs(imbalance) * 6.0) + min(depth / 2_500.0, 5.0) + (urgency * 4.0) + max(0.0, 0.05 - spread) * 20.0
    return MarketCandidate(
        market_id=_extract_market_id(market),
        question=question,
        slug=slug,
        token_id=token_id,
        midpoint=round(midpoint, 6),
        best_bid=round(best_bid, 6),
        best_ask=round(best_ask, 6),
        bids_depth=round(bids_depth, 2),
        asks_depth=round(asks_depth, 2),
        spread=round(spread, 6),
        hours_to_resolution=round(hours_left, 6),
        total_volume=round(_extract_volume(market), 2),
        book_imbalance=round(imbalance, 6),
        category="crypto",
        priority_score=round(score, 3),
        raw_market={**market, "asset": asset, "strategy_family": "crypto_threshold_snapshot"},
    )


def run_intraday_registry_cycle(settings: Settings) -> dict[str, Any]:
    if not settings.intraday_registry_enabled:
        return {
            "strategy": "intraday_registry",
            "market_count": 0,
            "bootstrap_market_count": 0,
            "websocket_connected": False,
            "websocket_messages": 0,
            "websocket_new_markets": 0,
            "websocket_updated_markets": 0,
            "websocket_enriched_markets": 0,
            "websocket_error": "disabled",
        }
    bootstrap: dict[str, Any] = {}
    try:
        bootstrap = _bootstrap_intraday_registry_from_local_cache(settings)
    except Exception:
        bootstrap = {}
    markets = bootstrap.get("markets", []) if isinstance(bootstrap, dict) else []
    if not markets:
        try:
            cli = _fast_intraday_cli(PolymarketCLI(settings.polymarket_cli_bin, timeout_seconds=settings.polymarket_cli_timeout_seconds))
            bootstrap = _bootstrap_intraday_registry_from_cli(settings, cli)
            markets = bootstrap.get("markets", []) if isinstance(bootstrap, dict) else []
        except Exception:
            markets = []
    if not markets:
        bootstrap = _bootstrap_intraday_registry(settings)
        markets = bootstrap.get("markets", []) if isinstance(bootstrap, dict) else []
    records = {record["id"]: record for record in markets if isinstance(record, dict) and record.get("id")}
    gamma_refresh_stats = _refresh_gamma_watchlist_records(settings, records, source="gamma-refresh-cycle")
    if records:
        markets = list(records.values())
    ws_stats = _capture_intraday_registry_ws_updates(settings, markets, timeout_seconds=max(settings.daemon_interval_seconds - 2, 2))
    tape_stats = _record_intraday_book_tape(
        settings,
        _fast_intraday_cli(PolymarketCLI(settings.polymarket_cli_bin, timeout_seconds=settings.polymarket_cli_timeout_seconds)),
    )
    current = _intraday_registry_payload(settings)
    population_metrics = _intraday_registry_population_metrics(settings)
    box_status = _json_load(_runner_status_path("ab_crypto_5m_box_arb"), {})
    box_result = box_status.get("last_cycle_result", {}) if isinstance(box_status.get("last_cycle_result"), dict) else {}
    box_tape = box_result.get("arb_tape", {}) if isinstance(box_result.get("arb_tape"), dict) else {}
    result = {
        "strategy": "intraday_registry",
        "market_count": int(current.get("market_count", 0) or 0),
        "bootstrap_market_count": len(markets),
        "websocket_connected": bool(ws_stats.get("connected", False)),
        "websocket_messages": int(ws_stats.get("messages", 0) or 0),
        "websocket_new_markets": int(ws_stats.get("new_markets", 0) or 0),
        "websocket_updated_markets": int(ws_stats.get("updated_markets", 0) or 0),
        "websocket_enriched_markets": int(ws_stats.get("enriched_markets", 0) or 0),
        "websocket_error": str(ws_stats.get("error", "")) or None,
        "gamma_tag_candidate_count": int(gamma_refresh_stats.get("tag_candidate_count", 0) or 0),
        "gamma_live_upcoming_count": int(gamma_refresh_stats.get("tag_live_upcoming_count", 0) or 0),
        "gamma_near_term_count": int(gamma_refresh_stats.get("tag_near_term_count", 0) or 0),
        "gamma_refresh_fetched_markets": int(gamma_refresh_stats.get("fetched_markets", 0) or 0),
        "gamma_refresh_raw_fetched_markets": int(gamma_refresh_stats.get("raw_fetched_markets", 0) or 0),
        "gamma_refresh_new_markets": int(gamma_refresh_stats.get("new_markets", 0) or 0),
        "gamma_refresh_watch_only_markets": int(gamma_refresh_stats.get("watch_only_markets", 0) or 0),
        "book_snapshots_recorded": int(tape_stats.get("snapshots_recorded", 0) or 0),
        "book_snapshots_with_midpoint": int(tape_stats.get("snapshots_with_midpoint", 0) or 0),
        "book_snapshots_with_depth": int(tape_stats.get("snapshots_with_depth", 0) or 0),
        "threshold_live_markets": int(tape_stats.get("threshold_live_markets", 0) or 0),
        "transition_tape_entries": int(tape_stats.get("transition_tape_entries", 0) or 0),
        "raw_imminent_box_candidates": int(tape_stats.get("raw_imminent_box_candidates", 0) or 0),
        "imminent_box_arb_entries": int(tape_stats.get("imminent_box_arb_entries", 0) or 0),
        "positive_gross_edge_count": int(tape_stats.get("positive_gross_edge_count", 0) or 0),
        "positive_net_edge_count": int(tape_stats.get("positive_net_edge_count", 0) or 0),
        "max_gross_edge_per_share": round(_as_float(tape_stats.get("max_gross_edge_per_share"), 0.0), 6),
        "max_net_edge_per_share": round(_as_float(tape_stats.get("max_net_edge_per_share"), 0.0), 6),
        "watchlist_market_count": int(population_metrics.get("watchlist_market_count", 0) or 0),
        "watchlist_updown_count": int(population_metrics.get("watchlist_updown_count", 0) or 0),
        "imminent_updown_count": int(population_metrics.get("imminent_updown_count", 0) or 0),
        "watchlist_threshold_transition_count": int(population_metrics.get("watchlist_threshold_transition_count", 0) or 0),
        "watchlist_imminent_transition_count": int(population_metrics.get("watchlist_imminent_transition_count", 0) or 0),
        "watchlist_max_minutes_to_resolution": int(population_metrics.get("watchlist_max_minutes_to_resolution", 0) or 0),
    }
    result["box_execution_state"] = _intraday_box_execution_state(result, box_tape)
    result["execution_state"] = result["box_execution_state"]
    _append_intraday_edge_alerts(settings, registry_result=result, previous_result=None, box_tape=box_tape)
    _record_intraday_audit_cycle(settings, runner="intraday_registry", payload={**result, "executed_pairs": int(box_tape.get("executed_pairs", 0) or 0)})
    return result


def _market_is_tradeable(market: dict[str, Any]) -> bool:
    if not bool(_first(market, "active", default=True)):
        return False
    if bool(_first(market, "closed", default=False)):
        return False
    if bool(_first(market, "archived", default=False)):
        return False
    if not bool(_first(market, "acceptingOrders", default=True)):
        return False
    return True


def _market_prefilter(settings: Settings, markets: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], str]:
    prepared_all: list[dict[str, Any]] = []
    target_hours = max(float(settings.min_hours_to_resolution), min(float(settings.max_hours_to_resolution), 24.0))
    strict_min = float(settings.min_hours_to_resolution)
    strict_max = float(settings.max_hours_to_resolution)
    relaxed_min = max(0.5, strict_min * 0.5)
    relaxed_max = max(strict_max * 2.0, 24.0)
    active_rotation = _active_rotation_category(settings)

    for market in markets:
        if not _market_is_tradeable(market):
            continue
        token_id = _extract_yes_token_id(market)
        if not token_id:
            continue
        hours = _extract_hours_to_resolution(market)
        if hours <= 0.0:
            continue
        category = _extract_market_category(market)
        allowed, category_reason = _market_allowed_by_category(settings, category, None)
        if not allowed:
            continue
        volume = _extract_volume(market)
        resolution_score = max(0.0, 1.0 - (abs(hours - target_hours) / max(target_hours, 1.0)))
        priority_score = round((min(volume / 100_000.0, 5.0) * 0.7) + (resolution_score * 5.0), 3)
        prepared_all.append(
            {
                "market": market,
                "token_id": token_id,
                "hours": hours,
                "volume": volume,
                "category": category,
                "category_reason": category_reason,
                "target_distance": abs(hours - target_hours),
                "priority_score": priority_score,
            }
        )

    def sort_key(item: dict[str, Any]) -> tuple[float, float, float, float]:
        return (item["target_distance"], item["hours"], -item["priority_score"], -item["volume"])

    prepared = [item for item in prepared_all if item["category"] == active_rotation] if active_rotation else list(prepared_all)
    scan_pool_size = max(settings.queue_max_candidates, settings.queue_max_candidates * max(settings.scan_pool_multiplier, 1))

    strict = [item for item in prepared if strict_min <= item["hours"] <= strict_max]
    strict.sort(key=sort_key)
    if strict:
        return strict[:scan_pool_size], f"strict:{active_rotation or 'all'}"

    if active_rotation:
        strict = [item for item in prepared_all if strict_min <= item["hours"] <= strict_max]
        strict.sort(key=sort_key)
        if strict:
            return strict[:scan_pool_size], "strict:fallback-all"

    relaxed = [item for item in prepared if relaxed_min <= item["hours"] <= relaxed_max]
    relaxed.sort(key=sort_key)
    if relaxed:
        return relaxed[:scan_pool_size], f"relaxed:{active_rotation or 'all'}"

    if active_rotation:
        relaxed = [item for item in prepared_all if relaxed_min <= item["hours"] <= relaxed_max]
        relaxed.sort(key=sort_key)
        return relaxed[:scan_pool_size], "relaxed:fallback-all"

    return [], "empty"


def _sum_book_depth(levels: list[dict[str, Any]]) -> float:
    depth = 0.0
    for level in levels:
        price = _as_float(_first(level, "price", default=0.0))
        size = _as_float(_first(level, "size", "amount", "quantity", default=0.0))
        depth += price * size
    return depth


def _best_price(levels: list[dict[str, Any]], default: float = 0.0, *, side: str) -> float:
    if not levels:
        return default
    prices = [_as_float(_first(level, "price", default=default), default) for level in levels]
    if side == "bid":
        return max(prices, default=default)
    return min(prices, default=default)


def _crypto_series_match_keys(question: str, slug: str) -> set[str]:
    normalized = _normalize_market_text(question)
    slug_text = str(slug or "").strip().lower()
    keys: set[str] = set()
    asset = ""
    for candidate_asset, markers in _CRYPTO_ASSET_MARKERS.items():
        if any(_text_contains_marker(normalized, slug_text, marker) for marker in markers):
            asset = candidate_asset
            break
    if not asset:
        return keys
    if "up or down" in normalized or "updown" in slug_text:
        keys.add(f"series:{asset}:updown")
        if "15m" in slug_text:
            keys.add(f"series:{asset}:updown:15m")
        if "5m" in slug_text:
            keys.add(f"series:{asset}:updown:5m")
    if "be between" in normalized:
        keys.add(f"series:{asset}:range")
    if "reach $" in question.lower():
        keys.add(f"series:{asset}:reach")
    if "above $" in question.lower():
        keys.add(f"series:{asset}:above")
    if "dip to $" in question.lower():
        keys.add(f"series:{asset}:dip")
    return keys


def _crypto_asset_theme_keys(question: str, slug: str) -> set[str]:
    normalized = _normalize_market_text(question)
    slug_text = str(slug or "").strip().lower()
    keys: set[str] = set()

    for asset, markers in _CRYPTO_ASSET_MARKERS.items():
        if any(_text_contains_marker(normalized, slug_text, marker) for marker in markers):
            keys.add(f"asset:{asset}")
    if "airdrop" in normalized:
        keys.add("theme:airdrop")
    if "fdv" in normalized:
        keys.add("theme:fdv")
    return keys


def _extract_trade_match_key(item: dict[str, Any]) -> set[str]:
    keys = set()
    for key in ("token_id", "tokenID", "asset_id", "assetId", "condition_id", "conditionId", "market_id", "marketId", "slug"):
        value = _first(item, key)
        if value not in (None, ""):
            keys.add(str(value))
    for key in ("question", "title", "market_question", "marketQuestion"):
        value = _first(item, key)
        normalized = _normalize_market_text(value)
        if normalized:
            keys.add(f"q:{normalized}")
    keys.update(_crypto_series_match_keys(str(_first(item, "question", "title", default="")), str(_first(item, "slug", default=""))))
    keys.update(_crypto_asset_theme_keys(str(_first(item, "question", "title", default="")), str(_first(item, "slug", default=""))))
    return keys


def _candidate_match_keys(candidate: MarketCandidate) -> set[str]:
    keys = {candidate.market_id, candidate.token_id, candidate.slug}
    normalized_question = _normalize_market_text(candidate.question)
    if normalized_question:
        keys.add(f"q:{normalized_question}")
    keys.update(_crypto_series_match_keys(candidate.question, candidate.slug))
    keys.update(_crypto_asset_theme_keys(candidate.question, candidate.slug))
    return {key for key in keys if key}


def _market_lookup_keys(market: dict[str, Any]) -> set[str]:
    keys = {
        _extract_market_id(market),
        str(_first(market, "conditionId", "condition_id", default="")),
        _extract_slug(market),
    }
    normalized_question = _normalize_market_text(_extract_question(market))
    if normalized_question:
        keys.add(f"q:{normalized_question}")
    keys.update(_crypto_series_match_keys(_extract_question(market), _extract_slug(market)))
    keys.update(_crypto_asset_theme_keys(_extract_question(market), _extract_slug(market)))
    return {key for key in keys if key}


def _trade_unique_key(trade: dict[str, Any]) -> str:
    raw = trade.get("raw", {}) if isinstance(trade.get("raw"), dict) else {}
    transaction_hash = str(_first(raw, "transaction_hash", "transactionHash", default=""))
    if transaction_hash:
        return transaction_hash
    return "|".join(
        [
            str(trade.get("wallet", "")),
            str(trade.get("market_id", "")),
            str(trade.get("slug", "")),
            str(trade.get("question", "")),
            str(trade.get("timestamp", "")),
            str(trade.get("side", "")),
            str(trade.get("size", "")),
        ]
    )


def _matched_activity_trades(activity: dict[str, Any], keys: set[str]) -> list[dict[str, Any]]:
    matched: list[dict[str, Any]] = []
    seen: set[str] = set()
    by_match_key = activity.get("by_match_key", {})
    for key in keys:
        for trade in by_match_key.get(key, []):
            unique_key = _trade_unique_key(trade)
            if unique_key in seen:
                continue
            seen.add(unique_key)
            matched.append(trade)
    return matched


def _low_price_liquidity_factor(contract_price: float) -> float:
    price = max(min(contract_price, 1.0), 0.0)
    if price >= 0.10:
        return 1.0
    return max(0.05, price / 0.10)


def _complement_contract_price(price: float) -> float:
    return 1.0 - price


def _book_contract_levels(book: dict[str, Any], side: str, action: str) -> list[tuple[float, float]]:
    side = str(side).upper()
    action = str(action).lower()
    bids = book.get("bids", []) if isinstance(book, dict) else []
    asks = book.get("asks", []) if isinstance(book, dict) else []
    levels: list[tuple[float, float]] = []

    if side == "BUY" and action == "open":
        source = asks
        transform = float
    elif side == "BUY" and action == "close":
        source = bids
        transform = float
    elif side == "SELL" and action == "open":
        source = bids
        transform = _complement_contract_price
    else:
        source = asks
        transform = _complement_contract_price

    for level in source:
        yes_price = _as_float(_first(level, "price", default=0.0))
        raw_size = _as_float(_first(level, "size", "amount", "quantity", default=0.0))
        contract_price = max(min(transform(yes_price), 0.99), 0.01)
        effective_size = raw_size * _low_price_liquidity_factor(contract_price)
        if contract_price <= 0 or effective_size <= 0:
            continue
        levels.append((contract_price, effective_size))

    if action == "open":
        levels.sort(key=lambda item: item[0])
    else:
        levels.sort(key=lambda item: item[0], reverse=True)
    return levels


def _passive_book_contract_levels(book: dict[str, Any], side: str) -> list[tuple[float, float]]:
    side = str(side).upper()
    bids = book.get("bids", []) if isinstance(book, dict) else []
    asks = book.get("asks", []) if isinstance(book, dict) else []
    levels: list[tuple[float, float]] = []

    if side == "BUY":
        source = bids
        transform = float
    else:
        source = asks
        transform = _complement_contract_price

    for level in source:
        yes_price = _as_float(_first(level, "price", default=0.0))
        raw_size = _as_float(_first(level, "size", "amount", "quantity", default=0.0))
        contract_price = max(min(transform(yes_price), 0.99), 0.01)
        effective_size = raw_size * _low_price_liquidity_factor(contract_price)
        if contract_price <= 0 or effective_size <= 0:
            continue
        levels.append((contract_price, effective_size))

    # Passive fills should start from the best resting price already on the book,
    # not the cheapest historical level in the snapshot.
    levels.sort(key=lambda item: item[0], reverse=True)
    return levels


def _simulate_contract_buy(levels: list[tuple[float, float]], budget_usdc: float) -> dict[str, float]:
    remaining_budget = max(budget_usdc, 0.0)
    spent = 0.0
    filled_shares = 0.0
    for price, size in levels:
        if remaining_budget <= 1e-9:
            break
        affordable_shares = remaining_budget / price
        fill_shares = min(size, affordable_shares)
        if fill_shares <= 1e-9:
            continue
        fill_cost = fill_shares * price
        spent += fill_cost
        filled_shares += fill_shares
        remaining_budget -= fill_cost
    avg_contract_price = (spent / filled_shares) if filled_shares > 0 else 0.0
    return {
        "filled_shares": round(filled_shares, 6),
        "filled_notional_usdc": round(spent, 2),
        "avg_contract_price": round(avg_contract_price, 6),
        "fill_ratio": round((spent / budget_usdc), 6) if budget_usdc > 0 else 0.0,
    }


def _simulate_contract_sell(levels: list[tuple[float, float]], shares: float) -> dict[str, float]:
    remaining_shares = max(shares, 0.0)
    proceeds = 0.0
    filled_shares = 0.0
    for price, size in levels:
        if remaining_shares <= 1e-9:
            break
        fill_shares = min(size, remaining_shares)
        if fill_shares <= 1e-9:
            continue
        proceeds += fill_shares * price
        filled_shares += fill_shares
        remaining_shares -= fill_shares
    avg_contract_price = (proceeds / filled_shares) if filled_shares > 0 else 0.0
    return {
        "filled_shares": round(filled_shares, 6),
        "proceeds_usdc": round(proceeds, 2),
        "avg_contract_price": round(avg_contract_price, 6),
        "fill_ratio": round((filled_shares / shares), 6) if shares > 0 else 0.0,
    }


def _contract_midpoint_to_yes_midpoint(side: str, contract_price: float) -> float:
    price = max(min(contract_price, 0.99), 0.01)
    return price if str(side).upper() == "BUY" else 1.0 - price


def _fallback_contract_levels_from_candidate(candidate: MarketCandidate, side: str, action: str) -> list[tuple[float, float]]:
    if str(side).upper() == "BUY" and action == "open":
        contract_price = max(candidate.best_ask, 0.01)
        depth_notional = candidate.asks_depth
    elif str(side).upper() == "BUY" and action == "close":
        contract_price = max(candidate.best_bid, 0.01)
        depth_notional = candidate.bids_depth
    elif str(side).upper() == "SELL" and action == "open":
        contract_price = max(1.0 - candidate.best_bid, 0.01)
        depth_notional = candidate.bids_depth
    else:
        contract_price = max(1.0 - candidate.best_ask, 0.01)
        depth_notional = candidate.asks_depth
    size = (depth_notional / contract_price) if contract_price > 0 else 0.0
    size *= _low_price_liquidity_factor(contract_price)
    return [(contract_price, size)] if size > 0 else []


def _index_wallet_trades(trades: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_match_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        for key in _extract_trade_match_key(trade):
            by_match_key[key].append(trade)
    return dict(by_match_key)


def _is_transient_cli_error(message: str) -> bool:
    lowered = message.lower()
    transient_markers = (
        "http error 429",
        "http error 500",
        "http error 502",
        "http error 503",
        "http error 504",
        "too many requests",
        "service unavailable",
        "bad gateway",
        "gateway timeout",
        "internal: error sending request for url",
        "connection reset",
        "temporarily unavailable",
    )
    return any(marker in lowered for marker in transient_markers)


class PolymarketCLI:
    def __init__(self, binary: str, timeout_seconds: int = 12, max_attempts: int = 3) -> None:
        self.binary = binary
        self.timeout_seconds = max(timeout_seconds, 1)
        self.max_attempts = max(max_attempts, 1)

    def _run_json(self, args: list[str]) -> Any:
        cmd = [self.binary, "-o", "json", *args]
        last_error: RuntimeError | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=self.timeout_seconds)
            except FileNotFoundError as exc:
                raise RuntimeError(f"{self.binary} is not installed or not on PATH") from exc
            except subprocess.TimeoutExpired as exc:
                message = f"polymarket cli timed out after {self.timeout_seconds}s: {' '.join(cmd)}"
                last_error = RuntimeError(message)
                if attempt < self.max_attempts:
                    time.sleep(float(attempt))
                    continue
                raise last_error from exc
            if proc.returncode == 0:
                return json.loads(proc.stdout)

            message = proc.stderr.strip() or proc.stdout.strip() or f"command failed: {' '.join(cmd)}"
            last_error = RuntimeError(message)
            if attempt < self.max_attempts and _is_transient_cli_error(message):
                time.sleep(float(attempt))
                continue
            raise last_error

        if last_error is not None:
            raise last_error
        raise RuntimeError(f"command failed: {' '.join(cmd)}")

    def list_markets(self, limit: int) -> list[dict[str, Any]]:
        payload = self._run_json(["markets", "list", "--active", "true", "--closed", "false", "--limit", str(limit)])
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            return payload.get("markets", [])
        return []

    def midpoint(self, token_id: str) -> float:
        payload = self._run_json(["clob", "midpoint", token_id])
        if isinstance(payload, dict):
            return _as_float(_first(payload, "mid", "midpoint", default=0.0))
        return _as_float(payload, 0.0)

    def book(self, token_id: str) -> dict[str, Any]:
        payload = self._run_json(["clob", "book", token_id])
        return payload if isinstance(payload, dict) else {}

    def wallet_trades(self, wallet: str, limit: int) -> list[dict[str, Any]]:
        payload = self._run_json(["data", "trades", wallet, "--limit", str(limit)])
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            return payload.get("trades", [])
        return []

    def leaderboard(self, period: str, order_by: str, limit: int) -> list[dict[str, Any]]:
        payload = self._run_json(["data", "leaderboard", "--period", period, "--order-by", order_by, "--limit", str(limit)])
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for key in ("leaderboard", "entries", "data", "users"):
                value = payload.get(key)
                if isinstance(value, list):
                    return value
        return []

    def create_limit_order(self, token_id: str, side: str, price: float, size: float) -> dict[str, Any]:
        return self._run_json(
            [
                "clob",
                "create-order",
                "--token",
                token_id,
                "--side",
                side.lower(),
                "--price",
                f"{price:.4f}",
                "--size",
                f"{size:.6f}",
            ]
        )


def _fast_intraday_cli(cli: Any, timeout_seconds: int = 4) -> Any:
    if isinstance(cli, PolymarketCLI):
        return PolymarketCLI(
            cli.binary,
            timeout_seconds=min(cli.timeout_seconds, max(timeout_seconds, 1)),
            max_attempts=1,
        )
    return cli


def _safe_midpoint(cli: Any, token_id: str) -> float | None:
    if not hasattr(cli, "midpoint"):
        return None
    try:
        midpoint = cli.midpoint(token_id)
    except Exception:
        return None
    try:
        value = float(midpoint)
    except (TypeError, ValueError):
        return None
    return value if value > 0.0 else None


def _midpoint_from_book(book: dict[str, Any]) -> float | None:
    if not isinstance(book, dict):
        return None
    bids = book.get("bids", [])
    asks = book.get("asks", [])
    best_bid = _best_price(bids, side="bid")
    best_ask = _best_price(asks, default=1.0, side="ask")
    if best_bid <= 0.0 and best_ask >= 1.0:
        return None
    if best_bid <= 0.0:
        return max(min(best_ask, 0.99), 0.01)
    if best_ask >= 1.0:
        return max(min(best_bid, 0.99), 0.01)
    return round((best_bid + best_ask) / 2.0, 6)


def _safe_book(cli: Any, token_id: str) -> dict[str, Any]:
    if hasattr(cli, "book"):
        try:
            payload = cli.book(token_id)
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}
    return {}


def check_geoblock() -> dict[str, Any]:
    req = urllib.request.Request(
        "https://polymarket.com/api/geoblock",
        headers={"User-Agent": "polymarket-bot-scaffold/0.1"},
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def ensure_live_allowed(settings: Settings) -> None:
    if settings.bot_mode != "live" or not settings.live_trading_enabled:
        return
    try:
        geo = check_geoblock()
    except urllib.error.URLError as exc:
        raise GeoblockedError(f"unable to verify geoblock status: {exc}") from exc
    if geo.get("blocked"):
        country = geo.get("country", "unknown")
        region = geo.get("region", "")
        raise GeoblockedError(f"live trading blocked for detected location {country}/{region}")


def discover_targets_from_csv(csv_path: Path, output_path: Path, min_trades: int = 100, top_n: int = 50) -> list[dict[str, Any]]:
    wallet_stats: dict[str, dict[str, float]] = defaultdict(lambda: {"trades": 0.0, "wins": 0.0, "pnl": 0.0, "notional": 0.0})
    pnl_fields = ("profit", "pnl", "total_pnl", "realized_pnl")
    wallet_fields = ("maker", "wallet", "address", "user", "trader")

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            wallet = None
            for field in wallet_fields:
                value = row.get(field)
                if value:
                    wallet = value.strip()
                    break
            if not wallet:
                continue

            stats = wallet_stats[wallet]
            stats["trades"] += 1
            stats["notional"] += _as_float(row.get("usd_amount"), 0.0)

            pnl_value = None
            for field in pnl_fields:
                if row.get(field) not in (None, ""):
                    pnl_value = _as_float(row.get(field), 0.0)
                    break
            if pnl_value is not None:
                stats["pnl"] += pnl_value
                if pnl_value > 0:
                    stats["wins"] += 1
            elif row.get("win_rate") not in (None, ""):
                stats["wins"] += _as_float(row.get("win_rate"), 0.0)

    ranked: list[dict[str, Any]] = []
    for wallet, stats in wallet_stats.items():
        trades = int(stats["trades"])
        if trades < min_trades:
            continue
        win_rate = stats["wins"] / trades if stats["wins"] > 1 else stats["wins"]
        ranked.append(
            {
                "wallet": wallet,
                "trades": trades,
                "win_rate": round(win_rate, 4),
                "total_pnl": round(stats["pnl"], 2),
                "notional_usdc": round(stats["notional"], 2),
            }
        )

    def sort_key(item: dict[str, Any]) -> tuple[float, float, int]:
        return (item["total_pnl"], item["notional_usdc"], item["trades"])

    ranked.sort(key=sort_key, reverse=True)
    ranked = ranked[:top_n]
    _json_dump(output_path, ranked)
    return ranked


def _leaderboard_targets(
    settings: Settings,
    cli: PolymarketCLI,
    *,
    limit: int,
    period: str | None = None,
    order_by: str | None = None,
) -> list[dict[str, Any]]:
    ranked = []
    entries = cli.leaderboard(
        period=period or settings.target_discovery_period,
        order_by=order_by or settings.target_discovery_order_by,
        limit=min(max(limit, 1), 50),
    )
    for item in entries:
        wallet = str(_first(item, "proxy_wallet", "wallet", "address", "user", "maker", default="")).strip()
        if not wallet:
            continue
        ranked.append(
            {
                "wallet": wallet,
                "username": str(_first(item, "user_name", "username", "name", "handle", default="")).strip(),
                "total_pnl": round(_as_float(_first(item, "pnl", "profit", "total_pnl", default=0.0)), 2),
                "volume": round(_as_float(_first(item, "volume", "traded", default=0.0)), 2),
                "rank": int(_as_float(_first(item, "rank", default=len(ranked) + 1), len(ranked) + 1)),
                "source": "leaderboard",
                "period": period or settings.target_discovery_period,
                "order_by": order_by or settings.target_discovery_order_by,
            }
        )
    return ranked


def discover_targets_from_leaderboard_to_path(settings: Settings, cli: PolymarketCLI, output_path: Path, limit: int) -> list[dict[str, Any]]:
    ranked = _leaderboard_targets(settings, cli, limit=limit)
    _json_dump(output_path, ranked)
    return ranked


def discover_targets_from_leaderboard(settings: Settings, cli: PolymarketCLI) -> list[dict[str, Any]]:
    ranked = _leaderboard_targets(settings, cli, limit=settings.target_discovery_limit)
    _json_dump(settings.targets_path, ranked)
    return ranked


def discover_expanded_targets_for_crypto(settings: Settings, cli: PolymarketCLI, output_path: Path, limit: int) -> list[dict[str, Any]]:
    requests = [
        (settings.target_discovery_period, settings.target_discovery_order_by),
        (settings.target_discovery_period, "vol"),
    ]
    merged: dict[str, dict[str, Any]] = {}
    for period, order_by in requests:
        for item in _leaderboard_targets(settings, cli, limit=limit, period=period, order_by=order_by):
            wallet = str(item.get("wallet", "")).lower()
            if not wallet:
                continue
            if wallet not in merged:
                merged[wallet] = item
    ranked = sorted(
        merged.values(),
        key=lambda item: (
            -_as_float(item.get("total_pnl"), 0.0),
            -_as_float(item.get("volume"), 0.0),
        ),
    )
    _json_dump(output_path, ranked)
    return ranked


def ensure_targets(settings: Settings, cli: PolymarketCLI) -> list[dict[str, Any]]:
    targets = _json_load(settings.targets_path, [])
    if targets:
        return targets
    return discover_targets_from_leaderboard(settings, cli)


def refresh_target_activity_for_paths(
    settings: Settings,
    cli: PolymarketCLI,
    *,
    targets_path: Path,
    activity_path: Path,
    limit: int = 25,
) -> dict[str, Any]:
    cached = _json_load(activity_path, {"updated_at": None, "wallets": [], "by_match_key": {}, "errors": []})
    fallback_cached = {"wallets": []}
    if activity_path != settings.target_activity_path:
        fallback_cached = _json_load(settings.target_activity_path, {"wallets": []})
    if (
        settings.target_activity_refresh_interval_seconds > 0
        and isinstance(cached, dict)
        and cached.get("wallets")
        and (
            cached.get("by_match_key")
            or any(isinstance(item, dict) and item.get("trades") for item in cached.get("wallets", []))
        )
        and _thesis_cache_entry_is_fresh(
            {"generated_at": cached.get("updated_at")},
            settings.target_activity_refresh_interval_seconds,
        )
    ):
        return cached

    targets = _json_load(targets_path, [])
    cached_wallets = {
        str(item.get("wallet", "")).lower(): item
        for item in cached.get("wallets", [])
        if isinstance(item, dict) and item.get("wallet")
    }
    fallback_cached_wallets = {
        str(item.get("wallet", "")).lower(): item
        for item in fallback_cached.get("wallets", [])
        if isinstance(item, dict) and item.get("wallet")
    }
    wallet_count = len(targets)
    wallets_per_refresh = wallet_count
    if wallet_count > 0:
        configured_refresh_count = max(settings.target_activity_wallets_per_refresh, 1)
        wallets_per_refresh = min(wallet_count, configured_refresh_count)
    refresh_cursor = int(cached.get("refresh_cursor", 0)) if isinstance(cached, dict) else 0
    selected_wallets: set[str] = set()
    if wallet_count <= wallets_per_refresh:
        targets_to_refresh = targets
    else:
        start = refresh_cursor % wallet_count
        targets_to_refresh = [targets[(start + offset) % wallet_count] for offset in range(wallets_per_refresh)]
        selected_wallets = {str(item.get("wallet", "")).lower() for item in targets_to_refresh if item.get("wallet")}

    activity: dict[str, Any] = {
        "updated_at": utc_now_iso(),
        "wallets": [],
        "by_match_key": {},
        "errors": [],
        "refresh_cursor": (refresh_cursor + wallets_per_refresh) % wallet_count if wallet_count else 0,
    }
    preserved_wallets: dict[str, dict[str, Any]] = {}

    for target in targets_to_refresh:
        wallet = target.get("wallet")
        if not wallet:
            continue
        wallet_key = str(wallet).lower()
        cached_wallet = cached_wallets.get(wallet_key)
        try:
            trades = cli.wallet_trades(wallet, limit)
            wallet_error = None
        except Exception as exc:  # noqa: BLE001
            trades = []
            wallet_error = str(exc)
            activity["errors"].append({"wallet": wallet, "error": wallet_error})

        normalized_trades = []
        for trade in trades:
            side = str(_first(trade, "side", "maker_direction", "direction", default="")).upper()
            size = _as_float(_first(trade, "size", "token_amount", "amount", default=0.0))
            price = _as_float(_first(trade, "price", default=0.0))
            normalized = {
                "wallet": wallet,
                "side": side,
                "size": size,
                "price": price,
                "market_id": str(_first(trade, "market_id", "marketId", "condition_id", "conditionId", default="")),
                "token_id": str(_first(trade, "token_id", "tokenID", "asset_id", "assetId", default="")),
                "slug": str(_first(trade, "slug", default="")),
                "question": str(_first(trade, "question", "title", "market_question", "marketQuestion", default="")),
                "timestamp": str(_first(trade, "timestamp", "created_at", default="")),
                "raw": trade,
            }
            normalized_trades.append(normalized)

        preserved = False
        if wallet_error and cached_wallet and cached_wallet.get("trades"):
            normalized_trades = cached_wallet.get("trades", [])
            preserved = True
        elif wallet_error and fallback_cached_wallets.get(wallet_key, {}).get("trades"):
            normalized_trades = fallback_cached_wallets[wallet_key].get("trades", [])
            preserved = True

        wallet_payload = {
            "wallet": wallet,
            "meta": target,
            "trades": normalized_trades,
            "error": wallet_error,
        }
        if preserved:
            wallet_payload["preserved_from_cache"] = True
        preserved_wallets[wallet_key] = wallet_payload

    for target in targets:
        wallet = target.get("wallet")
        if not wallet:
            continue
        wallet_key = str(wallet).lower()
        if wallet_key in preserved_wallets:
            wallet_payload = preserved_wallets[wallet_key]
        elif wallet_key in selected_wallets:
            continue
        else:
            cached_wallet = cached_wallets.get(wallet_key)
            if cached_wallet:
                wallet_payload = {
                    "wallet": wallet,
                    "meta": target,
                    "trades": cached_wallet.get("trades", []),
                    "error": cached_wallet.get("error"),
                    "preserved_from_cache": True,
                }
            else:
                wallet_payload = {
                    "wallet": wallet,
                    "meta": target,
                    "trades": [],
                    "error": "not refreshed yet",
                }
        activity["wallets"].append(wallet_payload)

    all_trades = [
        trade
        for wallet_payload in activity["wallets"]
        for trade in wallet_payload.get("trades", [])
        if isinstance(trade, dict)
    ]
    activity["by_match_key"] = _index_wallet_trades(all_trades)
    _json_dump(activity_path, activity)
    return activity


def refresh_target_activity(settings: Settings, cli: PolymarketCLI, limit: int = 25) -> dict[str, Any]:
    return refresh_target_activity_for_paths(
        settings,
        cli,
        targets_path=settings.targets_path,
        activity_path=settings.target_activity_path,
        limit=limit,
    )


def derive_crypto_target_activity(
    settings: Settings,
    *,
    source_targets_path: Path | None = None,
    source_activity_path: Path | None = None,
) -> dict[str, Any]:
    targets_path = source_targets_path or settings.targets_path
    activity_path = source_activity_path or settings.target_activity_path
    targets = _json_load(targets_path, [])
    activity = _json_load(activity_path, {"wallets": []})
    targets_by_wallet = {
        str(item.get("wallet", "")).lower(): item
        for item in targets
        if isinstance(item, dict) and item.get("wallet")
    }
    ranked_wallets: list[dict[str, Any]] = []
    filtered_wallets: list[dict[str, Any]] = []
    all_crypto_trades: list[dict[str, Any]] = []

    for wallet_payload in activity.get("wallets", []):
        if not isinstance(wallet_payload, dict):
            continue
        wallet = str(wallet_payload.get("wallet", ""))
        wallet_key = wallet.lower()
        meta = targets_by_wallet.get(wallet_key, wallet_payload.get("meta", {}))
        crypto_trades = [
            trade
            for trade in wallet_payload.get("trades", [])
            if isinstance(trade, dict)
            and _categorize_text(str(trade.get("question", "")), str(trade.get("slug", ""))) == "crypto"
        ]
        if not crypto_trades:
            continue
        crypto_notional = round(
            sum(max(_as_float(trade.get("size"), 0.0), 0.0) * max(_as_float(trade.get("price"), 0.0), 0.01) for trade in crypto_trades),
            2,
        )
        ranked_wallets.append(
            {
                **(meta if isinstance(meta, dict) else {}),
                "wallet": wallet,
                "crypto_trade_count": len(crypto_trades),
                "crypto_notional": crypto_notional,
            }
        )
        filtered_wallets.append(
            {
                "wallet": wallet,
                "meta": meta if isinstance(meta, dict) else {"wallet": wallet},
                "trades": crypto_trades,
                "error": wallet_payload.get("error"),
            }
        )
        all_crypto_trades.extend(crypto_trades)

    ranked_wallets.sort(
        key=lambda item: (
            -_as_float(item.get("crypto_notional"), 0.0),
            -int(item.get("crypto_trade_count", 0) or 0),
            -_as_float(item.get("total_pnl"), 0.0),
        )
    )
    selected = ranked_wallets[: max(settings.ab_test_crypto_top_wallets, 1)]
    selected_wallets = {str(item.get("wallet", "")).lower() for item in selected if item.get("wallet")}
    selected_wallet_payloads = [item for item in filtered_wallets if str(item.get("wallet", "")).lower() in selected_wallets]

    output_targets = []
    for idx, item in enumerate(selected, start=1):
        output_targets.append({**item, "rank": idx, "source": "derived_crypto_subset"})

    output_activity = {
        "updated_at": utc_now_iso(),
        "wallets": selected_wallet_payloads,
        "by_match_key": _index_wallet_trades(
            [
                trade
                for wallet_payload in selected_wallet_payloads
                for trade in wallet_payload.get("trades", [])
                if isinstance(trade, dict)
            ]
        ),
        "errors": [item for item in activity.get("errors", []) if str(item.get("wallet", "")).lower() in selected_wallets],
        "refresh_cursor": 0,
        "source": "derived_crypto_subset",
    }
    _json_dump(settings.crypto_targets_path, output_targets)
    _json_dump(settings.crypto_activity_path, output_activity)
    return {
        "updated_at": output_activity["updated_at"],
        "wallets_selected": len(output_targets),
        "trade_count": sum(len(item.get("trades", [])) for item in selected_wallet_payloads),
    }


def refresh_crypto_target_activity(
    settings: Settings,
    cli: PolymarketCLI,
    *,
    refresh_base_activity: bool = True,
    limit: int | None = None,
) -> dict[str, Any]:
    trade_limit = limit or settings.ab_test_crypto_trade_limit
    cached_activity = _json_load(settings.crypto_source_activity_path, {})
    if (
        settings.target_activity_refresh_interval_seconds > 0
        and settings.crypto_source_targets_path.exists()
        and isinstance(cached_activity, dict)
        and cached_activity.get("wallets")
        and _thesis_cache_entry_is_fresh(
            {"generated_at": cached_activity.get("updated_at")},
            settings.target_activity_refresh_interval_seconds,
        )
    ):
        return derive_crypto_target_activity(
            settings,
            source_targets_path=settings.crypto_source_targets_path,
            source_activity_path=settings.crypto_source_activity_path,
        )
    try:
        discover_expanded_targets_for_crypto(
            settings,
            cli,
            settings.crypto_source_targets_path,
            max(settings.ab_test_crypto_source_wallets_limit, settings.target_discovery_limit),
        )
    except Exception:  # noqa: BLE001
        if not settings.crypto_source_targets_path.exists():
            raise
    crypto_refresh_settings = replace(
        settings,
        target_activity_wallets_per_refresh=settings.ab_test_crypto_wallets_per_refresh,
    )
    try:
        refresh_target_activity_for_paths(
            crypto_refresh_settings,
            cli,
            targets_path=settings.crypto_source_targets_path,
            activity_path=settings.crypto_source_activity_path,
            limit=trade_limit,
        )
    except Exception:  # noqa: BLE001
        if not settings.crypto_source_activity_path.exists():
            raise
    if refresh_base_activity:
        try:
            refresh_target_activity(settings, cli, limit=trade_limit)
        except Exception:  # noqa: BLE001
            if not settings.target_activity_path.exists():
                raise
    return derive_crypto_target_activity(
        settings,
        source_targets_path=settings.crypto_source_targets_path,
        source_activity_path=settings.crypto_source_activity_path,
    )


def scan_markets(settings: Settings, cli: PolymarketCLI) -> list[dict[str, Any]]:
    queue: list[dict[str, Any]] = []
    selected, mode = _market_prefilter(settings, cli.list_markets(settings.markets_limit))
    effective_depth_threshold = settings.min_book_depth_usdc if mode.startswith("strict") else settings.min_book_depth_usdc * 0.25

    for selected_item in selected:
        market = selected_item["market"]
        token_id = selected_item["token_id"]
        midpoint = cli.midpoint(token_id)
        if midpoint <= 0.0:
            continue

        book = _safe_book(cli, token_id)
        bids = book.get("bids", []) if isinstance(book, dict) else []
        asks = book.get("asks", []) if isinstance(book, dict) else []
        bids_depth = _sum_book_depth(bids)
        asks_depth = _sum_book_depth(asks)
        depth = min(bids_depth, asks_depth)
        hours_left = selected_item["hours"]

        if depth < effective_depth_threshold:
            continue

        best_bid = _best_price(bids, side="bid")
        best_ask = _best_price(asks, default=1.0, side="ask")
        spread = max(best_ask - best_bid, 0.0)
        denom = bids_depth + asks_depth
        imbalance = ((bids_depth - asks_depth) / denom) if denom else 0.0
        candidate = MarketCandidate(
            market_id=_extract_market_id(market),
            question=_extract_question(market),
            slug=_extract_slug(market),
            token_id=token_id,
            midpoint=midpoint,
            best_bid=best_bid,
            best_ask=best_ask,
            bids_depth=round(bids_depth, 2),
            asks_depth=round(asks_depth, 2),
            spread=round(spread, 4),
            hours_to_resolution=round(hours_left, 2),
            total_volume=round(_extract_volume(market), 2),
            book_imbalance=round(imbalance, 4),
            category=str(selected_item.get("category", "unknown")),
            priority_score=round(float(selected_item.get("priority_score", 0.0)) + min(depth / 5_000.0, 5.0) + max(0.0, 0.05 - spread) * 20.0, 3),
            raw_market={**market, "_scan_mode": mode, "_depth_threshold": effective_depth_threshold},
        )
        queue.append(candidate.to_dict())

    queue.sort(key=lambda item: (-float(item.get("priority_score", 0.0)), abs(float(item["hours_to_resolution"]) - 24.0), -float(item["total_volume"])))
    queue = queue[: settings.queue_max_candidates]
    _json_dump(settings.queue_path, queue)
    return queue


def _wallet_trade_timestamp_value(trade: dict[str, Any]) -> float:
    raw = str(trade.get("timestamp", "")).strip()
    if raw.isdigit():
        return float(raw)
    parsed = _normalize_iso(raw)
    if parsed is not None:
        return parsed.timestamp()
    return 0.0


def _scan_short_crypto_updown_candidates(
    cli: PolymarketCLI,
    *,
    settings: Settings | None = None,
    markets_limit: int,
    min_minutes_to_resolution: int = 0,
    max_minutes_to_resolution: int,
    min_book_depth_usdc: float,
    require_updown: bool = True,
    market_fetcher: Callable[[int, int], list[dict[str, Any]]] | None = None,
    seed_markets: list[dict[str, Any]] | None = None,
    prebook_limit: int | None = None,
) -> list[MarketCandidate]:
    candidates: list[MarketCandidate] = []
    markets: list[dict[str, Any]] = []
    seen_market_ids: set[str] = set()
    errors: list[str] = []
    if seed_markets:
        for market in seed_markets[:markets_limit]:
            if not isinstance(market, dict):
                continue
            market_id = _extract_market_id(market)
            if not market_id or market_id in seen_market_ids:
                continue
            seen_market_ids.add(market_id)
            markets.append(market)
    if not markets and settings is not None and settings.intraday_registry_prefer_registry:
        registry_markets = _load_intraday_registry_markets(
            settings,
            min_minutes_to_resolution=min_minutes_to_resolution,
            max_minutes_to_resolution=max_minutes_to_resolution,
            require_updown=require_updown,
        )
        for market in registry_markets[:markets_limit]:
            market_id = _extract_market_id(market)
            if not market_id or market_id in seen_market_ids:
                continue
            seen_market_ids.add(market_id)
            markets.append(market)
    fetcher = market_fetcher or _fetch_gamma_intraday_markets_page
    if not markets:
        page_size = min(max(markets_limit, 1), 500)
        pages = max(1, math.ceil(max(markets_limit, page_size) / page_size))
        for page_index in range(pages):
            try:
                page = fetcher(page_size, page_index * page_size)
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))
                continue
            if not page:
                break
            for market in page:
                market_id = _extract_market_id(market)
                if not market_id or market_id in seen_market_ids:
                    continue
                seen_market_ids.add(market_id)
                markets.append(market)
            if len(page) < page_size:
                break
    if not markets:
        for attempt_limit in (min(markets_limit, 500), min(markets_limit, 250), 100):
            if attempt_limit <= 0:
                continue
            try:
                markets = cli.list_markets(attempt_limit)
                if markets:
                    break
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))
                continue
    if not markets and errors:
        return []
    shortlisted_markets: list[dict[str, Any]] = []
    for market in markets:
        if not _market_is_tradeable(market):
            continue
        question = _extract_question(market)
        slug = _extract_slug(market)
        asset = _short_crypto_updown_asset(question, slug) if require_updown else _short_crypto_market_asset(question, slug)
        if asset is None:
            continue
        if require_updown and not _is_five_minute_updown_market(question, slug):
            continue
        token_id = _extract_yes_token_id(market)
        if not token_id:
            continue
        hours_left = _extract_short_market_hours_to_resolution(market)
        if hours_left <= 0.0:
            continue
        if min_minutes_to_resolution > 0 and hours_left < (min_minutes_to_resolution / 60.0):
            continue
        if hours_left > (max_minutes_to_resolution / 60.0):
            continue
        shortlisted_markets.append(market)
    shortlisted_markets.sort(
        key=lambda market: (
            _extract_short_market_hours_to_resolution(market),
            -_extract_volume(market),
            _extract_question(market),
        )
    )
    if prebook_limit is not None and prebook_limit > 0:
        shortlisted_markets = shortlisted_markets[:prebook_limit]
    for market in shortlisted_markets:
        question = _extract_question(market)
        slug = _extract_slug(market)
        category = _extract_market_category(market)
        asset = _short_crypto_updown_asset(question, slug) if require_updown else _short_crypto_market_asset(question, slug)
        if asset is None:
            continue
        token_id = _extract_yes_token_id(market)
        hours_left = _extract_short_market_hours_to_resolution(market)
        book = _safe_book(cli, token_id)
        bids = book.get("bids", []) if isinstance(book, dict) else []
        asks = book.get("asks", []) if isinstance(book, dict) else []
        midpoint = _safe_midpoint(cli, token_id)
        if midpoint is None:
            midpoint = _midpoint_from_book(book)
        if midpoint is None:
            continue
        bids_depth = _sum_book_depth(bids)
        asks_depth = _sum_book_depth(asks)
        depth = min(bids_depth, asks_depth)
        if depth < min_book_depth_usdc:
            continue
        best_bid = _best_price(bids, side="bid")
        best_ask = _best_price(asks, default=1.0, side="ask")
        spread = max(best_ask - best_bid, 0.0)
        denom = bids_depth + asks_depth
        imbalance = ((bids_depth - asks_depth) / denom) if denom else 0.0
        urgency = max(0.0, 1.0 - (hours_left * 60.0 / max(max_minutes_to_resolution, 1)))
        score = (abs(imbalance) * 6.0) + min(depth / 2_500.0, 5.0) + (urgency * 4.0) + max(0.0, 0.05 - spread) * 20.0
        candidates.append(
            MarketCandidate(
                market_id=_extract_market_id(market),
                question=question,
                slug=slug,
                token_id=token_id,
                midpoint=midpoint,
                best_bid=best_bid,
                best_ask=best_ask,
                bids_depth=round(bids_depth, 2),
                asks_depth=round(asks_depth, 2),
                spread=round(spread, 4),
                hours_to_resolution=round(hours_left, 4),
                total_volume=round(_extract_volume(market), 2),
                book_imbalance=round(imbalance, 4),
                category="crypto" if category == "unknown" else category,
                priority_score=round(score, 3),
                raw_market={
                    **market,
                    "asset": asset,
                    "strategy_family": "crypto_short_binary",
                    "require_updown": require_updown,
                    "source_category": category,
                },
            )
        )
    candidates.sort(key=lambda item: (-item.priority_score, item.hours_to_resolution, -item.total_volume))
    return candidates


def _short_crypto_sniper_vote(
    candidate: MarketCandidate,
    *,
    strategy_name: str = "crypto_5m_sniper",
    min_book_imbalance: float,
    max_side_price: float,
) -> Vote:
    buy_contract = max(candidate.best_ask, 0.01)
    sell_contract = max(1.0 - candidate.best_bid, 0.01)
    if candidate.spread > 0.08:
        return Vote(strategy_name, "HOLD", 0.0, candidate.midpoint, "spread too wide")
    imbalance = candidate.book_imbalance
    if imbalance >= min_book_imbalance and buy_contract <= max_side_price:
        confidence = min(0.95, 0.5 + abs(imbalance))
        est = min(0.99, candidate.midpoint + min(0.18, abs(imbalance) * 0.2))
        return Vote(strategy_name, "BUY", confidence, est, "bid-side depth favors yes-side breakout")
    if imbalance <= -min_book_imbalance and sell_contract <= max_side_price:
        confidence = min(0.95, 0.5 + abs(imbalance))
        est = max(0.01, candidate.midpoint - min(0.18, abs(imbalance) * 0.2))
        return Vote(strategy_name, "SELL", confidence, est, "ask-side depth favors no-side breakout")
    return Vote(strategy_name, "HOLD", 0.0, candidate.midpoint, "book imbalance below entry threshold")


def _scheduled_intraday_crypto_vote(
    candidate: MarketCandidate,
    *,
    strategy_name: str = "crypto_intraday_scheduled",
    min_book_imbalance: float,
    min_midpoint_edge: float,
    max_side_price: float,
    max_hours_to_resolution: float,
) -> Vote:
    question = str(candidate.question or "")
    if _extract_intraday_question_window(question) is None:
        return Vote(strategy_name, "HOLD", 0.0, candidate.midpoint, "missing explicit intraday window")
    if candidate.hours_to_resolution > max_hours_to_resolution:
        return Vote(strategy_name, "HOLD", 0.0, candidate.midpoint, "window too far away")
    buy_contract = max(candidate.best_ask, 0.01)
    sell_contract = max(1.0 - candidate.best_bid, 0.01)
    if candidate.spread > 0.05:
        return Vote(strategy_name, "HOLD", 0.0, candidate.midpoint, "spread too wide")
    midpoint_edge = abs(candidate.midpoint - 0.5)
    if midpoint_edge < min_midpoint_edge:
        return Vote(strategy_name, "HOLD", 0.0, candidate.midpoint, "midpoint too close to fair value")
    imbalance = candidate.book_imbalance
    if candidate.midpoint < 0.5 and imbalance >= min_book_imbalance and buy_contract <= max_side_price:
        confidence = min(0.9, 0.52 + abs(imbalance) * 0.8)
        est = min(0.99, candidate.midpoint + min(0.14, abs(imbalance) * 0.18))
        return Vote(strategy_name, "BUY", confidence, est, "scheduled book leans yes-side")
    if candidate.midpoint > 0.5 and imbalance <= -min_book_imbalance and sell_contract <= max_side_price:
        confidence = min(0.9, 0.52 + abs(imbalance) * 0.8)
        est = max(0.01, candidate.midpoint - min(0.14, abs(imbalance) * 0.18))
        return Vote(strategy_name, "SELL", confidence, est, "scheduled book leans no-side")
    return Vote(strategy_name, "HOLD", 0.0, candidate.midpoint, "midpoint and imbalance not aligned")


def _threshold_snapshot_crypto_vote(
    candidate: MarketCandidate,
    *,
    strategy_name: str = "crypto_threshold_snapshot",
    min_book_imbalance: float,
    min_midpoint_edge: float,
    max_spread: float,
    max_side_price: float,
) -> Vote:
    question = str(candidate.question or "")
    slug = str(candidate.slug or "")
    if _is_five_minute_updown_market(question, slug):
        return Vote(strategy_name, "HOLD", 0.0, candidate.midpoint, "reserved for up/down strategies")
    if candidate.spread > max_spread:
        return Vote(strategy_name, "HOLD", 0.0, candidate.midpoint, "spread too wide")
    midpoint_edge = abs(candidate.midpoint - 0.5)
    if midpoint_edge < min_midpoint_edge:
        return Vote(strategy_name, "HOLD", 0.0, candidate.midpoint, "midpoint too close to fair value")
    buy_contract = max(candidate.best_ask, 0.01)
    sell_contract = max(1.0 - candidate.best_bid, 0.01)
    imbalance = candidate.book_imbalance
    if candidate.midpoint < 0.5 and imbalance >= min_book_imbalance and buy_contract <= max_side_price:
        confidence = min(0.9, 0.52 + abs(imbalance) * 0.8)
        est = min(0.99, candidate.midpoint + min(0.16, abs(imbalance) * 0.2))
        return Vote(strategy_name, "BUY", confidence, est, "threshold book leans yes-side")
    if candidate.midpoint > 0.5 and imbalance <= -min_book_imbalance and sell_contract <= max_side_price:
        confidence = min(0.9, 0.52 + abs(imbalance) * 0.8)
        est = max(0.01, candidate.midpoint - min(0.16, abs(imbalance) * 0.2))
        return Vote(strategy_name, "SELL", confidence, est, "threshold book leans no-side")
    return Vote(strategy_name, "HOLD", 0.0, candidate.midpoint, "midpoint and imbalance not aligned")


def _threshold_snapshot_spread_size_multiplier(settings: Settings, spread: float) -> float:
    soft_cap = max(float(settings.ab_test_crypto_threshold_snapshot_soft_max_spread), 0.0)
    hard_cap = max(float(settings.ab_test_crypto_threshold_snapshot_hard_max_spread), soft_cap + 1e-6)
    min_multiplier = min(max(float(settings.ab_test_crypto_threshold_snapshot_min_spread_size_multiplier), 0.05), 1.0)
    if spread <= soft_cap:
        return 1.0
    if spread >= hard_cap:
        return 0.0
    width = hard_cap - soft_cap
    if width <= 0:
        return min_multiplier
    ratio = (spread - soft_cap) / width
    return round(max(min_multiplier, 1.0 - (ratio * (1.0 - min_multiplier))), 4)


def _simulate_box_pair_open(
    yes_levels: list[tuple[float, float]],
    no_levels: list[tuple[float, float]],
    budget_usdc: float,
) -> dict[str, float]:
    if budget_usdc <= 0 or not yes_levels or not no_levels:
        return {
            "filled_shares": 0.0,
            "spent_yes_usdc": 0.0,
            "spent_no_usdc": 0.0,
            "avg_yes_contract_price": 0.0,
            "avg_no_contract_price": 0.0,
            "combined_contract_price": 0.0,
            "edge_per_share": 0.0,
        }

    yes_queue = [[price, size] for price, size in yes_levels]
    no_queue = [[price, size] for price, size in no_levels]
    i = 0
    j = 0
    remaining_budget = budget_usdc
    filled_shares = 0.0
    spent_yes = 0.0
    spent_no = 0.0
    while i < len(yes_queue) and j < len(no_queue) and remaining_budget > 1e-9:
        yes_price, yes_size = yes_queue[i]
        no_price, no_size = no_queue[j]
        combined = yes_price + no_price
        if combined <= 0:
            break
        affordable = remaining_budget / combined
        fill_shares = min(yes_size, no_size, affordable)
        if fill_shares <= 1e-9:
            break
        spent_yes += fill_shares * yes_price
        spent_no += fill_shares * no_price
        filled_shares += fill_shares
        remaining_budget -= fill_shares * combined
        yes_queue[i][1] -= fill_shares
        no_queue[j][1] -= fill_shares
        if yes_queue[i][1] <= 1e-9:
            i += 1
        if no_queue[j][1] <= 1e-9:
            j += 1
    total_spent = spent_yes + spent_no
    avg_yes = (spent_yes / filled_shares) if filled_shares > 0 else 0.0
    avg_no = (spent_no / filled_shares) if filled_shares > 0 else 0.0
    combined_avg = (total_spent / filled_shares) if filled_shares > 0 else 0.0
    return {
        "filled_shares": round(filled_shares, 6),
        "spent_yes_usdc": round(spent_yes, 2),
        "spent_no_usdc": round(spent_no, 2),
        "avg_yes_contract_price": round(avg_yes, 6),
        "avg_no_contract_price": round(avg_no, 6),
        "combined_contract_price": round(combined_avg, 6),
        "edge_per_share": round(max(1.0 - combined_avg, 0.0), 6),
    }


def _box_arb_edge_metrics(
    execution: dict[str, float],
    *,
    estimated_fee_per_share: float,
    estimated_slippage_per_share: float,
) -> dict[str, float]:
    gross_edge = max(_as_float(execution.get("edge_per_share"), 0.0), 0.0)
    fee = max(float(estimated_fee_per_share), 0.0)
    slippage = max(float(estimated_slippage_per_share), 0.0)
    net_edge = gross_edge - fee - slippage
    return {
        "gross_edge_per_share": round(gross_edge, 6),
        "estimated_fee_per_share": round(fee, 6),
        "estimated_slippage_per_share": round(slippage, 6),
        "net_edge_per_share": round(net_edge, 6),
    }


def _simulate_box_pair_open_maker_proxy(
    book: dict[str, Any],
    budget_usdc: float,
    *,
    estimated_fee_per_share: float,
    estimated_slippage_per_share: float,
    best_bid: float | None = None,
    best_ask: float | None = None,
) -> dict[str, float]:
    bid = _as_float(best_bid, 0.0) if best_bid is not None else _best_price(book.get("bids", []), side="bid")
    ask = _as_float(best_ask, 0.0) if best_ask is not None else _best_price(book.get("asks", []), default=0.0, side="ask")
    if budget_usdc <= 0 or bid <= 0 or ask <= 0 or ask < bid:
        execution = {
            "filled_shares": 0.0,
            "avg_yes_contract_price": 0.0,
            "avg_no_contract_price": 0.0,
            "combined_contract_price": 0.0,
            "edge_per_share": 0.0,
        }
    else:
        # Conservative proxy: one leg rests at the current best price and the
        # other still pays the current taker price. With a single displayed yes
        # book, this is the strongest passive improvement we can justify from
        # the public top-of-book data alone.
        scenarios = [
            {
                "avg_yes_contract_price": max(min(bid, 0.99), 0.01),
                "avg_no_contract_price": max(min(1.0 - bid, 0.99), 0.01),
            },
            {
                "avg_yes_contract_price": max(min(ask, 0.99), 0.01),
                "avg_no_contract_price": max(min(1.0 - ask, 0.99), 0.01),
            },
        ]
        for scenario in scenarios:
            scenario["combined_contract_price"] = round(
                _as_float(scenario["avg_yes_contract_price"], 0.0) + _as_float(scenario["avg_no_contract_price"], 0.0),
                6,
            )
            scenario["edge_per_share"] = round(max(1.0 - _as_float(scenario["combined_contract_price"], 0.0), 0.0), 6)
            scenario["filled_shares"] = round(
                (budget_usdc / _as_float(scenario["combined_contract_price"], 0.0))
                if _as_float(scenario["combined_contract_price"], 0.0) > 0
                else 0.0,
                6,
            )
        execution = min(scenarios, key=lambda item: _as_float(item.get("combined_contract_price"), 10.0))
    metrics = _box_arb_edge_metrics(
        execution,
        estimated_fee_per_share=estimated_fee_per_share,
        estimated_slippage_per_share=estimated_slippage_per_share,
    )
    return {
        "maker_filled_shares": execution.get("filled_shares", 0.0),
        "maker_yes_contract_price": execution.get("avg_yes_contract_price", 0.0),
        "maker_no_contract_price": execution.get("avg_no_contract_price", 0.0),
        "maker_combined_contract_price": execution.get("combined_contract_price", 0.0),
        "maker_gross_edge_per_share": metrics.get("gross_edge_per_share", 0.0),
        "maker_net_edge_per_share": metrics.get("net_edge_per_share", 0.0),
    }


def _simulate_box_pair_open_aggressive_proxy(
    budget_usdc: float,
    *,
    best_bid: float | None,
    best_ask: float | None,
    maker_fee_per_share: float,
    estimated_slippage_per_share: float,
    queue_fill_probability: float,
    tick_size: float,
    price_concession_ticks: int = 0,
) -> dict[str, float]:
    bid = max(min(_as_float(best_bid, 0.0), 0.99), 0.0)
    ask = max(min(_as_float(best_ask, 0.0), 0.99), 0.0)
    if budget_usdc <= 0 or bid <= 0 or ask <= 0 or ask < bid:
        return {
            "aggressive_filled_shares": 0.0,
            "aggressive_yes_contract_price": 0.0,
            "aggressive_no_contract_price": 0.0,
            "aggressive_combined_contract_price": 0.0,
            "aggressive_gross_edge_per_share": 0.0,
            "aggressive_expected_gross_edge_per_share": 0.0,
            "aggressive_net_edge_per_share": 0.0,
            "aggressive_expected_net_edge_per_share": 0.0,
            "aggressive_fill_probability": round(max(min(float(queue_fill_probability), 1.0), 0.0), 6),
            "aggressive_maker_fee_per_share": round(float(maker_fee_per_share), 6),
            "aggressive_estimated_slippage_per_share": round(max(float(estimated_slippage_per_share), 0.0), 6),
            "aggressive_price_concession_ticks": int(max(price_concession_ticks, 0)),
            "aggressive_tick_size": round(max(float(tick_size), 0.0), 6),
        }
    concession = max(float(tick_size), 0.0) * max(int(price_concession_ticks), 0)
    yes_price = min(max(bid + concession, 0.01), ask)
    no_price = min(max((1.0 - ask) + concession, 0.01), 1.0 - bid)
    combined = round(yes_price + no_price, 6)
    gross = round(max(1.0 - combined, 0.0), 6)
    fill_prob = round(max(min(float(queue_fill_probability), 1.0), 0.0), 6)
    maker_fee = round(float(maker_fee_per_share), 6)
    slippage = round(max(float(estimated_slippage_per_share), 0.0), 6)
    expected_gross = round(gross * fill_prob, 6)
    net = round(gross - maker_fee - slippage, 6)
    expected_net = round(expected_gross - maker_fee - slippage, 6)
    filled_shares = round((budget_usdc / combined), 6) if combined > 0 else 0.0
    return {
        "aggressive_filled_shares": filled_shares,
        "aggressive_yes_contract_price": round(yes_price, 6),
        "aggressive_no_contract_price": round(no_price, 6),
        "aggressive_combined_contract_price": combined,
        "aggressive_gross_edge_per_share": gross,
        "aggressive_expected_gross_edge_per_share": expected_gross,
        "aggressive_net_edge_per_share": net,
        "aggressive_expected_net_edge_per_share": expected_net,
        "aggressive_fill_probability": fill_prob,
        "aggressive_maker_fee_per_share": maker_fee,
        "aggressive_estimated_slippage_per_share": slippage,
        "aggressive_price_concession_ticks": int(max(price_concession_ticks, 0)),
        "aggressive_tick_size": round(max(float(tick_size), 0.0), 6),
    }


def _wallet_copy_candidate_pool(
    settings: Settings,
    cli: PolymarketCLI,
    activity: dict[str, Any],
    *,
    markets_limit: int | None = None,
    queue_limit: int | None = None,
    prebook_candidate_limit: int | None = None,
) -> list[MarketCandidate]:
    matched_candidates: list[MarketCandidate] = []
    shortlisted: list[dict[str, Any]] = []
    crypto_only = tuple(settings.category_include) == ("crypto",) and not settings.category_exclude
    depth_floor = 25.0 if crypto_only else 100.0
    effective_depth_threshold = max(settings.min_book_depth_usdc * 0.5, depth_floor)
    target_queue_limit = max(queue_limit or settings.queue_max_candidates, 1)
    target_prebook_limit = max(prebook_candidate_limit or (target_queue_limit * 3), target_queue_limit)

    for market in cli.list_markets(markets_limit or settings.markets_limit):
        if not _market_is_tradeable(market):
            continue
        token_id = _extract_yes_token_id(market)
        if not token_id:
            continue
        matched_trades = _matched_activity_trades(activity, _market_lookup_keys(market))
        if not matched_trades:
            continue
        hours_left = _extract_hours_to_resolution(market)
        if hours_left <= 0.0:
            continue
        category = _extract_market_category(market)
        allowed, _ = _market_allowed_by_category(settings, category, None)
        if not allowed:
            continue
        trade_flow_score = sum(max(_as_float(trade.get("size"), 0.0) * max(_as_float(trade.get("price"), 0.0), 0.01), 1.0) for trade in matched_trades)
        recency_score = max((_wallet_trade_timestamp_value(trade) for trade in matched_trades), default=0.0)
        shortlisted.append(
            {
                "market": market,
                "token_id": token_id,
                "hours_left": hours_left,
                "category": category,
                "matched_trades": matched_trades,
                "trade_flow_score": trade_flow_score,
                "recency_score": recency_score,
            }
        )

    shortlisted.sort(
        key=lambda item: (
            -float(item["trade_flow_score"]),
            -float(item["recency_score"]),
            abs(float(item["hours_left"]) - 24.0),
            -float(_extract_volume(item["market"])),
        )
    )

    for item in shortlisted[:target_prebook_limit]:
        market = item["market"]
        token_id = item["token_id"]
        matched_trades = item["matched_trades"]
        hours_left = item["hours_left"]
        category = item["category"]
        midpoint = cli.midpoint(token_id)
        if midpoint <= 0.0:
            continue

        book = _safe_book(cli, token_id)
        bids = book.get("bids", []) if isinstance(book, dict) else []
        asks = book.get("asks", []) if isinstance(book, dict) else []
        bids_depth = _sum_book_depth(bids)
        asks_depth = _sum_book_depth(asks)
        depth = min(bids_depth, asks_depth)
        if depth < effective_depth_threshold:
            continue

        best_bid = _best_price(bids, side="bid")
        best_ask = _best_price(asks, default=1.0, side="ask")
        spread = max(best_ask - best_bid, 0.0)
        denom = bids_depth + asks_depth
        imbalance = ((bids_depth - asks_depth) / denom) if denom else 0.0
        trade_flow_score = item["trade_flow_score"]
        recency_score = item["recency_score"]
        candidate = MarketCandidate(
            market_id=_extract_market_id(market),
            question=_extract_question(market),
            slug=_extract_slug(market),
            token_id=token_id,
            midpoint=midpoint,
            best_bid=best_bid,
            best_ask=best_ask,
            bids_depth=round(bids_depth, 2),
            asks_depth=round(asks_depth, 2),
            spread=round(spread, 4),
            hours_to_resolution=round(hours_left, 2),
            total_volume=round(_extract_volume(market), 2),
            book_imbalance=round(imbalance, 4),
            category=category,
            priority_score=round(min(trade_flow_score / 25_000.0, 12.0) + min(depth / 10_000.0, 5.0) + max(0.0, 0.05 - spread) * 20.0, 3),
            raw_market={**market, "_wallet_trade_count": len(matched_trades), "_wallet_trade_recency": recency_score},
        )
        matched_candidates.append(candidate)

    matched_candidates.sort(
        key=lambda item: (
            -float(item.priority_score),
            -float(item.raw_market.get("_wallet_trade_recency", 0.0)),
            abs(float(item.hours_to_resolution) - 24.0),
            -float(item.total_volume),
        )
    )
    return matched_candidates[:target_queue_limit]


def _extract_openai_output_text(data: dict[str, Any]) -> str:
    output = data.get("output", [])
    texts: list[str] = []
    for item in output:
        if item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                texts.append(content.get("text", ""))
            elif content.get("type") == "refusal":
                raise RuntimeError(content.get("refusal", "OpenAI refused the request"))
    return "\n".join(part for part in texts if part).strip()


def _openai_payload(settings: Settings, prompt: str, max_output_tokens: int) -> dict[str, Any]:
    return {
        "model": settings.openai_model,
        "instructions": (
            "You score Polymarket trade candidates. "
            "Return JSON only. "
            "Do not add markdown, prose, or commentary outside JSON."
        ),
        "input": prompt,
        "max_output_tokens": max_output_tokens,
        "reasoning": {"effort": "minimal"},
        "text": {"format": {"type": "json_object"}, "verbosity": "low"},
    }


def _openai_request(settings: Settings, prompt: str) -> Thesis | None:
    if not settings.openai_api_key:
        return None

    last_error: Exception | None = None
    for max_output_tokens in (400, 900):
        payload = _openai_payload(settings, prompt, max_output_tokens)
        body = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            "https://api.openai.com/v1/responses",
            data=body,
            method="POST",
            headers={
                "content-type": "application/json",
                "Authorization": f"Bearer {settings.openai_api_key}",
            },
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            data = json.loads(response.read().decode("utf-8"))
        text = _extract_openai_output_text(data)
        if text:
            try:
                parsed = json.loads(text)
                break
            except json.JSONDecodeError:
                last_error = RuntimeError(f"OpenAI response was not valid JSON: {text[:200]}")
                continue
        reason = _first(data.get("incomplete_details", {}), "reason", default="unknown")
        last_error = RuntimeError(f"OpenAI response incomplete with empty output_text: {reason}")
    else:
        if last_error is not None:
            raise last_error
        raise RuntimeError("OpenAI response did not contain usable output")

    return Thesis(
        market_id=str(parsed["market_id"]),
        token_id=str(parsed["token_id"]),
        estimated_probability=_as_float(parsed["estimated_probability"]),
        confidence=_as_float(parsed["confidence"]),
        thesis=str(parsed["thesis"]),
        catalysts=[str(item) for item in parsed.get("catalysts", [])],
        crowd_error=str(parsed.get("crowd_error", "")),
        source="openai",
    )


def build_theses(settings: Settings) -> list[dict[str, Any]]:
    queue = _json_load(settings.queue_path, [])[: max(settings.thesis_max_candidates_per_cycle, 1)]
    activity = _json_load(settings.target_activity_path, {"by_match_key": {}})
    cache_payload = _json_load(settings.thesis_cache_path, {"items": []})
    cached_items = cache_payload.get("items", []) if isinstance(cache_payload, dict) else []
    cached_by_market = {
        str(item.get("market_id", "")): item
        for item in cached_items
        if isinstance(item, dict) and item.get("market_id")
    }
    theses: list[dict[str, Any]] = []
    refreshed_cache = dict(cached_by_market)

    for item in queue:
        market_id = str(item["market_id"])
        cached = cached_by_market.get(market_id)
        if cached and _thesis_cache_entry_is_fresh(cached, settings.thesis_reuse_ttl_seconds):
            cached_thesis = {key: value for key, value in cached.items() if key != "generated_at"}
            theses.append(cached_thesis)
            continue

        match_keys = {str(item["market_id"]), str(item["token_id"]), str(item["slug"])}
        target_hits = []
        by_match_key = activity.get("by_match_key", {})
        for key in match_keys:
            target_hits.extend(by_match_key.get(key, []))

        if settings.openai_api_key:
            prompt = (
                "You are scoring a Polymarket trade candidate. "
                "Return strict JSON only with keys: market_id, token_id, estimated_probability, confidence, thesis, catalysts, crowd_error.\n\n"
                f"Candidate:\n{json.dumps(item, indent=2)}\n\n"
                f"Target wallet activity:\n{json.dumps(target_hits[:15], indent=2)}\n\n"
                "Confidence should be between 0 and 1. "
                "If the available evidence is thin, keep confidence low."
            )
            try:
                thesis = _openai_request(settings, prompt)
            except Exception:  # noqa: BLE001
                if cached:
                    cached_thesis = {key: value for key, value in cached.items() if key != "generated_at"}
                    theses.append(cached_thesis)
                    continue
                raise
            if thesis is None:
                if cached:
                    cached_thesis = {key: value for key, value in cached.items() if key != "generated_at"}
                    theses.append(cached_thesis)
                continue
        else:
            thesis = Thesis(
                market_id=str(item["market_id"]),
                token_id=str(item["token_id"]),
                estimated_probability=_as_float(item["midpoint"]),
                confidence=0.0,
                thesis="No OpenAI API key configured; no predictive edge estimated.",
                catalysts=[],
                crowd_error="No external reasoning source configured.",
                source="fallback",
            )
        thesis_dict = thesis.to_dict()
        theses.append(thesis_dict)
        refreshed_cache[market_id] = {**thesis_dict, "generated_at": utc_now_iso()}

    _json_dump(settings.theses_path, theses)
    _json_dump(
        settings.thesis_cache_path,
        {
            "updated_at": utc_now_iso(),
            "items": list(refreshed_cache.values()),
        },
    )
    return theses


def _persist_last_nonempty_snapshots(settings: Settings, queue: list[dict[str, Any]], theses: list[dict[str, Any]]) -> None:
    if queue:
        _json_dump(
            settings.last_nonempty_queue_path,
            {"updated_at": utc_now_iso(), "count": len(queue), "items": queue},
        )
    if theses:
        _json_dump(
            settings.last_nonempty_theses_path,
            {"updated_at": utc_now_iso(), "count": len(theses), "items": theses},
        )


def _load_thesis_map(settings: Settings) -> dict[str, Thesis]:
    payload = _json_load(settings.theses_path, [])
    result: dict[str, Thesis] = {}
    for item in payload:
        thesis = Thesis(**item)
        result[thesis.market_id] = thesis
    return result


def _find_target_trades(activity: dict[str, Any], candidate: MarketCandidate) -> list[dict[str, Any]]:
    return _matched_activity_trades(activity, _candidate_match_keys(candidate))


def convergence_vote(candidate: MarketCandidate, thesis: Thesis, settings: Settings) -> Vote:
    gap = thesis.estimated_probability - candidate.midpoint
    action = "HOLD"
    rationale = "edge below threshold"
    if abs(gap) >= settings.edge_threshold and thesis.confidence >= settings.min_thesis_confidence:
        action = "BUY" if gap > 0 else "SELL"
        rationale = f"probability gap {gap:.3f} exceeds threshold"
    return Vote(
        agent="convergence",
        action=action,
        confidence=thesis.confidence,
        estimated_probability=thesis.estimated_probability,
        rationale=rationale,
    )


def whale_copy_vote(candidate: MarketCandidate, activity: dict[str, Any]) -> Vote:
    trades = _find_target_trades(activity, candidate)
    buy_weight = 0.0
    sell_weight = 0.0
    for trade in trades:
        size = max(_as_float(trade.get("size"), 0.0), 1.0)
        side = str(trade.get("side", "")).upper()
        if side == "BUY":
            buy_weight += size
        elif side == "SELL":
            sell_weight += size

    total = buy_weight + sell_weight
    if total == 0:
        return Vote("whale_copy", "HOLD", 0.0, candidate.midpoint, "no matched target-wallet activity")
    if buy_weight > sell_weight:
        confidence = buy_weight / total
        est = min(0.99, candidate.midpoint + min(0.12, confidence * 0.12))
        return Vote("whale_copy", "BUY", confidence, est, "target wallets net buyers")
    if sell_weight > buy_weight:
        confidence = sell_weight / total
        est = max(0.01, candidate.midpoint - min(0.12, confidence * 0.12))
        return Vote("whale_copy", "SELL", confidence, est, "target wallets net sellers")
    return Vote("whale_copy", "HOLD", 0.0, candidate.midpoint, "target wallets balanced")


def wallet_copy_ab_vote(candidate: MarketCandidate, activity: dict[str, Any], settings: Settings) -> Vote:
    whale = whale_copy_vote(candidate, activity)
    if whale.action == "HOLD":
        return Vote("wallet_copy_ab", "HOLD", whale.confidence, whale.estimated_probability, whale.rationale)
    if whale.confidence < settings.ab_test_min_whale_confidence:
        return Vote("wallet_copy_ab", "HOLD", whale.confidence, whale.estimated_probability, "target-wallet confidence below threshold")
    return Vote("wallet_copy_ab", whale.action, whale.confidence, whale.estimated_probability, whale.rationale)


def wallet_copy_variant_vote(candidate: MarketCandidate, activity: dict[str, Any], *, min_confidence: float, agent_name: str) -> Vote:
    whale = whale_copy_vote(candidate, activity)
    if whale.action == "HOLD":
        return Vote(agent_name, "HOLD", whale.confidence, whale.estimated_probability, whale.rationale)
    if whale.confidence < min_confidence:
        return Vote(agent_name, "HOLD", whale.confidence, whale.estimated_probability, "target-wallet confidence below threshold")
    return Vote(agent_name, whale.action, whale.confidence, whale.estimated_probability, whale.rationale)


def microstructure_vote(candidate: MarketCandidate) -> Vote:
    if candidate.spread > 0.04:
        return Vote("microstructure", "HOLD", 0.0, candidate.midpoint, "spread too wide")
    adjustment = min(0.12, (abs(candidate.book_imbalance) * 0.12) + min(candidate.spread, 0.03))
    if candidate.book_imbalance >= 0.2:
        est = min(0.99, candidate.midpoint + adjustment)
        return Vote("microstructure", "BUY", min(0.9, abs(candidate.book_imbalance)), est, "bid-side depth dominates")
    if candidate.book_imbalance <= -0.2:
        est = max(0.01, candidate.midpoint - adjustment)
        return Vote("microstructure", "SELL", min(0.9, abs(candidate.book_imbalance)), est, "ask-side depth dominates")
    return Vote("microstructure", "HOLD", 0.0, candidate.midpoint, "book imbalance weak")


def kelly_size(p_win: float, market_price: float, bankroll: float, max_fraction: float) -> float:
    if market_price <= 0 or market_price >= 1:
        return 0.0
    b = (1 / market_price) - 1
    q = 1 - p_win
    f_star = (p_win * b - q) / b
    if f_star <= 0:
        return 0.0
    return round(bankroll * min(f_star, max_fraction), 2)


def _side_probability(side: str, estimated_probability: float) -> float:
    probability = estimated_probability if side == "BUY" else 1 - estimated_probability
    return min(0.99, max(0.01, probability))


def _side_market_price(side: str, midpoint: float) -> float:
    return side_contract_price(side, midpoint)


def _shadow_settings(settings: Settings, suffix: str) -> Settings:
    shadow = replace(
        settings,
        positions_path=Path(f"state/{suffix}_positions.json"),
        trades_path=Path(f"state/{suffix}_trades.json"),
        marks_path=Path(f"state/{suffix}_marks.json"),
        missed_opportunities_path=Path(f"state/{suffix}_missed_opportunities.json"),
    )
    shadow.ensure_dirs()
    return shadow


class PaperExecutor:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def load_positions(self) -> list[dict[str, Any]]:
        return _json_load(self.settings.positions_path, [])

    def save_positions(self, positions: list[dict[str, Any]]) -> None:
        _json_dump(self.settings.positions_path, positions)

    def append_trade(self, payload: dict[str, Any]) -> None:
        trades = _json_load(self.settings.trades_path, [])
        trades.append(payload)
        _json_dump(self.settings.trades_path, trades)

    def open_notional_usdc(self) -> float:
        return round(sum(_as_float(item.get("notional_usdc"), 0.0) for item in self.load_positions()), 2)

    def open_position(
        self,
        candidate: MarketCandidate,
        side: str,
        notional_usdc: float,
        confidence: float,
        expected_gap: float,
        book: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        levels = _book_contract_levels(book or {}, side, "open") or _fallback_contract_levels_from_candidate(candidate, side, "open")
        execution = _simulate_contract_buy(levels, notional_usdc)
        shares = execution["filled_shares"]
        filled_notional = execution["filled_notional_usdc"]
        entry_contract_price = execution["avg_contract_price"]
        if shares <= 0 or filled_notional <= 0 or entry_contract_price <= 0:
            return None
        entry_yes_price = _contract_midpoint_to_yes_midpoint(side, entry_contract_price)
        position = Position(
            position_id=uuid4().hex,
            market_id=candidate.market_id,
            token_id=candidate.token_id,
            question=candidate.question,
            side=side,
            shares=shares,
            notional_usdc=filled_notional,
            entry_price=entry_yes_price,
            expected_gap=expected_gap,
            opened_at=utc_now_iso(),
            thesis_confidence=confidence,
            mode=self.settings.bot_mode,
            category=candidate.category,
        )
        positions = self.load_positions()
        payload = position.to_dict()
        payload["entry_contract_price"] = entry_contract_price
        payload["requested_notional_usdc"] = round(notional_usdc, 2)
        payload["fill_ratio"] = execution["fill_ratio"]
        if metadata:
            payload.update(metadata)
        positions.append(payload)
        self.save_positions(positions)
        trade = {"type": "OPEN", **payload}
        self.append_trade(trade)
        return trade

    def open_position_exact(
        self,
        candidate: MarketCandidate,
        side: str,
        *,
        shares: float,
        entry_contract_price: float,
        confidence: float,
        expected_gap: float,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        shares = round(max(shares, 0.0), 6)
        entry_contract_price = round(max(entry_contract_price, 0.0), 6)
        if shares <= 0 or entry_contract_price <= 0:
            return None
        filled_notional = round(shares * entry_contract_price, 2)
        entry_yes_price = _contract_midpoint_to_yes_midpoint(side, entry_contract_price)
        position = Position(
            position_id=uuid4().hex,
            market_id=candidate.market_id,
            token_id=candidate.token_id,
            question=candidate.question,
            side=side,
            shares=shares,
            notional_usdc=filled_notional,
            entry_price=entry_yes_price,
            expected_gap=expected_gap,
            opened_at=utc_now_iso(),
            thesis_confidence=confidence,
            mode=self.settings.bot_mode,
            category=candidate.category,
        )
        positions = self.load_positions()
        payload = position.to_dict()
        payload["entry_contract_price"] = entry_contract_price
        payload["requested_notional_usdc"] = filled_notional
        payload["fill_ratio"] = 1.0
        if metadata:
            payload.update(metadata)
        positions.append(payload)
        self.save_positions(positions)
        trade = {"type": "OPEN", **payload}
        self.append_trade(trade)
        return trade

    def scale_position(
        self,
        position: dict[str, Any],
        candidate: MarketCandidate,
        add_notional_usdc: float,
        reason: str,
        book: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        positions = self.load_positions()
        levels = _book_contract_levels(book or {}, str(position.get("side", "BUY")), "open") or _fallback_contract_levels_from_candidate(candidate, str(position.get("side", "BUY")), "open")
        execution = _simulate_contract_buy(levels, add_notional_usdc)
        add_shares = execution["filled_shares"]
        filled_notional = execution["filled_notional_usdc"]
        entry_contract_price = execution["avg_contract_price"]
        if add_shares <= 0 or filled_notional <= 0 or entry_contract_price <= 0:
            return None
        entry_yes_price = _contract_midpoint_to_yes_midpoint(str(position.get("side", "BUY")), entry_contract_price)
        scaled_position: dict[str, Any] | None = None
        for item in positions:
            if item["position_id"] != position["position_id"]:
                continue
            old_shares = effective_position_shares(item)
            old_entry = _as_float(item.get("entry_price"), entry_yes_price)
            old_entry_contract = _as_float(item.get("entry_contract_price"), 0.0)
            if old_entry_contract <= 0:
                old_entry_contract = side_contract_price(item.get("side", "BUY"), old_entry)
            total_shares = old_shares + add_shares
            if total_shares <= 0:
                continue
            item["entry_price"] = round(((old_entry * old_shares) + (entry_yes_price * add_shares)) / total_shares, 6)
            item["entry_contract_price"] = round(((old_entry_contract * old_shares) + (entry_contract_price * add_shares)) / total_shares, 6)
            item["shares"] = round(total_shares, 6)
            item["notional_usdc"] = round(_as_float(item.get("notional_usdc"), 0.0) + filled_notional, 2)
            item["category"] = item.get("category") or candidate.category
            scaled_position = item
            break

        if scaled_position is None:
            raise RuntimeError(f"position not found for scale-in: {position.get('position_id')}")

        self.save_positions(positions)
        trade = {
            "type": "SCALE",
            "position_id": scaled_position["position_id"],
            "market_id": scaled_position["market_id"],
            "token_id": scaled_position["token_id"],
            "question": scaled_position["question"],
            "category": scaled_position.get("category", "unknown"),
            "side": scaled_position["side"],
            "shares_added": add_shares,
            "add_notional_usdc": filled_notional,
            "notional_usdc": scaled_position["notional_usdc"],
            "entry_price": entry_yes_price,
            "entry_contract_price": entry_contract_price,
            "new_avg_entry_price": scaled_position["entry_price"],
            "reason": reason,
            "scaled_at": utc_now_iso(),
            "mode": self.settings.bot_mode,
            "fill_ratio": execution["fill_ratio"],
        }
        self.append_trade(trade)
        return trade

    def close_position(
        self,
        position: dict[str, Any],
        exit_price: float,
        reason: str,
        book: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        positions = self.load_positions()
        updated_positions: list[dict[str, Any]] = []
        matched_position: dict[str, Any] | None = None
        for item in positions:
            if item["position_id"] == position["position_id"]:
                matched_position = dict(item)
            else:
                updated_positions.append(item)
        if matched_position is None:
            return None
        shares = effective_position_shares(matched_position)
        levels = _book_contract_levels(book or {}, matched_position["side"], "close")
        if not levels:
            synthetic_candidate = MarketCandidate(
                market_id=str(matched_position.get("market_id", "")),
                question=str(matched_position.get("question", "")),
                slug=str(matched_position.get("market_id", "")),
                token_id=str(matched_position.get("token_id", "")),
                midpoint=exit_price,
                best_bid=exit_price,
                best_ask=exit_price,
                bids_depth=max(_as_float(matched_position.get("notional_usdc"), 0.0), 0.0),
                asks_depth=max(_as_float(matched_position.get("notional_usdc"), 0.0), 0.0),
                spread=0.0,
                hours_to_resolution=0.0,
                total_volume=0.0,
                book_imbalance=0.0,
                category=str(matched_position.get("category", "")),
            )
            levels = _fallback_contract_levels_from_candidate(synthetic_candidate, matched_position["side"], "close")
        execution = _simulate_contract_sell(levels, shares)
        filled_shares = execution["filled_shares"]
        exit_contract_price = execution["avg_contract_price"]
        proceeds_usdc = execution["proceeds_usdc"]
        if filled_shares <= 0 or exit_contract_price <= 0:
            return None
        entry_contract_price = _as_float(matched_position.get("entry_contract_price"), 0.0)
        if entry_contract_price <= 0:
            entry_contract_price = side_contract_price(matched_position["side"], matched_position["entry_price"])
        closed_notional = round(entry_contract_price * filled_shares, 2)
        remaining_shares = round(max(shares - filled_shares, 0.0), 6)
        if remaining_shares > 1e-6:
            matched_position["shares"] = remaining_shares
            matched_position["notional_usdc"] = round(entry_contract_price * remaining_shares, 2)
            updated_positions.append(matched_position)
        self.save_positions(updated_positions)
        pnl = round(proceeds_usdc - closed_notional, 2)
        exit_yes_price = _contract_midpoint_to_yes_midpoint(matched_position["side"], exit_contract_price)
        trade = {
            "type": "CLOSE",
            "position_id": matched_position["position_id"],
            "market_id": matched_position["market_id"],
            "token_id": matched_position["token_id"],
            "question": matched_position["question"],
            "category": matched_position.get("category", "unknown"),
            "side": matched_position["side"],
            "entry_price": matched_position["entry_price"],
            "entry_contract_price": entry_contract_price,
            "exit_price": round(exit_yes_price, 6),
            "exit_contract_price": exit_contract_price,
            "shares": filled_shares,
            "notional_usdc": closed_notional,
            "proceeds_usdc": proceeds_usdc,
            "pnl_usdc": pnl,
            "reason": reason,
            "closed_at": utc_now_iso(),
            "mode": self.settings.bot_mode,
            "fill_ratio": execution["fill_ratio"],
            "remaining_shares": remaining_shares,
            "fully_closed": remaining_shares <= 1e-6,
        }
        for key in ("strategy_tag", "pair_id", "asset", "market_family"):
            if key in matched_position:
                trade[key] = matched_position[key]
        self.append_trade(trade)
        return trade


class LiveExecutor(PaperExecutor):
    def __init__(self, settings: Settings, cli: PolymarketCLI) -> None:
        super().__init__(settings)
        self.cli = cli
        ensure_live_allowed(settings)

    def open_position(
        self,
        candidate: MarketCandidate,
        side: str,
        notional_usdc: float,
        confidence: float,
        expected_gap: float,
        book: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        shares = round(notional_usdc / max(candidate.midpoint, 0.01), 6)
        self.cli.create_limit_order(candidate.token_id, side, candidate.midpoint, shares)
        return super().open_position(candidate, side, notional_usdc, confidence, expected_gap, book=book, metadata=metadata)

    def close_position(
        self,
        position: dict[str, Any],
        exit_price: float,
        reason: str,
        book: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        self.cli.create_limit_order(position["token_id"], "SELL" if position["side"] == "BUY" else "BUY", exit_price, position["shares"])
        return super().close_position(position, exit_price, reason, book=book)


def _executor(settings: Settings, cli: PolymarketCLI) -> PaperExecutor:
    if settings.bot_mode == "live" and settings.live_trading_enabled:
        return LiveExecutor(settings, cli)
    return PaperExecutor(settings)

def _realized_pnl_usdc(settings: Settings) -> float:
    trades = _json_load(settings.trades_path, [])
    return realized_pnl_from_trades(trades)


def _current_bankroll_usdc(settings: Settings) -> float:
    # Only realized closed PnL compounds. Open PnL remains separate until exit.
    return round(max(settings.bankroll_usdc + _realized_pnl_usdc(settings), 0.0), 2)


def _current_equity_usdc(settings: Settings) -> float:
    marks = _json_load(settings.marks_path, {"summary": {}})
    unrealized_pnl = _as_float(_first(marks.get("summary", {}), "total_unrealized_pnl_usdc", default=0.0), 0.0)
    return round(max(_current_bankroll_usdc(settings) + unrealized_pnl, 0.0), 2)


def _is_warmup_intraday_strategy(strategy_name: str, lifetime_closed_trades: int) -> bool:
    return (
        strategy_name
        in {
            "crypto_5m_sniper",
            "crypto_5m_box_arb",
            "crypto_latency_5m",
            "crypto_next_window_sniper",
            "crypto_next_window_box_arb",
            "crypto_intraday_scheduled",
            "crypto_threshold_snapshot",
            "sports_early_entry",
            "penny_longshot",
        }
        and lifetime_closed_trades < 3
    )


def _warmup_risk_reasons(
    settings: Settings,
    *,
    strategy_name: str,
    lifetime_closed_trades: int,
    open_positions_count: int,
    unrealized_pnl_usdc: float,
) -> list[str]:
    if not _is_warmup_intraday_strategy(strategy_name, lifetime_closed_trades):
        return []
    reasons: list[str] = []
    max_open = max(int(settings.strategy_warmup_max_open_positions), 0)
    max_drawdown = abs(float(settings.strategy_warmup_max_unrealized_drawdown_usdc))
    if max_open > 0 and open_positions_count > max_open:
        reasons.append(f"warmup open positions {open_positions_count} > {max_open}")
    if max_drawdown > 0 and unrealized_pnl_usdc <= -max_drawdown:
        reasons.append(f"warmup unrealized pnl {unrealized_pnl_usdc:.2f} <= -{max_drawdown:.2f}")
    return reasons


def _is_probation_reentry_strategy(strategy_name: str) -> bool:
    return strategy_name in {"crypto_next_window_sniper", "crypto_intraday_scheduled", "crypto_threshold_snapshot"}


def _probation_reentry_allowed(
    settings: Settings,
    strategy_name: str,
    reasons: list[str],
    *,
    open_positions_count: int,
) -> bool:
    if not _is_probation_reentry_strategy(strategy_name):
        return False
    probation_max_open = max(int(settings.strategy_probation_max_open_positions), 0)
    if strategy_name == "crypto_threshold_snapshot":
        if probation_max_open > 0 and open_positions_count > probation_max_open:
            return False
    elif open_positions_count > 0:
        return False
    if not reasons:
        return False
    allowed_prefixes = (
        "recent realized pnl ",
        "only ",
    )
    disallowed_fragments = (
        "runner degraded",
        "strategy quarantined",
        "warmup open positions",
        "warmup unrealized pnl",
    )
    if any(any(fragment in reason for fragment in disallowed_fragments) for reason in reasons):
        return False
    return all(any(reason.startswith(prefix) for prefix in allowed_prefixes) for reason in reasons)


def _quarantine_all_positions(settings: Settings, cli: PolymarketCLI, *, reason: str) -> list[dict[str, Any]]:
    executor = _executor(settings, cli)
    closed_positions: list[dict[str, Any]] = []
    for position in executor.load_positions():
        current_midpoint = _as_float(position.get("entry_price"), 0.0)
        book = _safe_book(cli, str(position.get("token_id", "")))
        liquidation = _simulate_contract_sell(
            _book_contract_levels(book, str(position.get("side", "BUY")), "close"),
            effective_position_shares(position),
        )
        current_contract_price = _as_float(liquidation.get("avg_contract_price"), 0.0)
        if current_contract_price > 0:
            current_midpoint = _contract_midpoint_to_yes_midpoint(str(position.get("side", "BUY")), current_contract_price)
        closed = executor.close_position(position, current_midpoint, reason, book=book)
        if closed is not None:
            closed_positions.append(closed)
    return closed_positions


def _force_close_position_zero_proceeds(
    settings: Settings,
    position: dict[str, Any],
    *,
    reason: str,
) -> dict[str, Any] | None:
    if settings.bot_mode != "paper":
        return None
    executor = PaperExecutor(settings)
    positions = executor.load_positions()
    updated_positions: list[dict[str, Any]] = []
    matched_position: dict[str, Any] | None = None
    for item in positions:
        if item.get("position_id") == position.get("position_id"):
            matched_position = dict(item)
        else:
            updated_positions.append(item)
    if matched_position is None:
        return None
    executor.save_positions(updated_positions)
    shares = effective_position_shares(matched_position)
    entry_contract_price = _as_float(matched_position.get("entry_contract_price"), 0.0)
    if entry_contract_price <= 0:
        entry_contract_price = side_contract_price(matched_position.get("side", "BUY"), _as_float(matched_position.get("entry_price"), 0.5))
    closed_notional = round(entry_contract_price * shares, 2)
    exit_yes_price = 0.0 if str(matched_position.get("side", "BUY")).upper() == "BUY" else 1.0
    trade = {
        "type": "CLOSE",
        "position_id": matched_position["position_id"],
        "market_id": matched_position["market_id"],
        "token_id": matched_position["token_id"],
        "question": matched_position["question"],
        "category": matched_position.get("category", "unknown"),
        "side": matched_position["side"],
        "entry_price": matched_position["entry_price"],
        "entry_contract_price": round(entry_contract_price, 6),
        "exit_price": round(exit_yes_price, 6),
        "exit_contract_price": 0.0,
        "shares": shares,
        "notional_usdc": closed_notional,
        "proceeds_usdc": 0.0,
        "pnl_usdc": round(-closed_notional, 2),
        "reason": reason,
        "closed_at": utc_now_iso(),
        "mode": settings.bot_mode,
        "fill_ratio": 1.0,
        "remaining_shares": 0.0,
        "fully_closed": True,
    }
    for key in ("strategy_tag", "pair_id", "asset", "market_family"):
        if key in matched_position:
            trade[key] = matched_position[key]
    executor.append_trade(trade)
    return trade


def _force_close_stale_threshold_positions(
    settings: Settings,
    cli: PolymarketCLI,
    *,
    hold_minutes: float,
    live_market_ids: set[str],
) -> list[dict[str, Any]]:
    if settings.bot_mode != "paper":
        return []
    executor = _executor(settings, cli)
    stale_exits: list[dict[str, Any]] = []
    stale_after_minutes = max(
        float(hold_minutes) * 2.0,
        float(settings.ab_test_crypto_threshold_snapshot_max_minutes_to_resolution) * 2.0,
        60.0,
    )
    for position in executor.load_positions():
        market_id = str(position.get("market_id", "")).strip()
        if market_id and market_id in live_market_ids:
            continue
        age_minutes = _age_minutes(str(position.get("opened_at", "")))
        if age_minutes < stale_after_minutes:
            continue
        stale_trade = _force_close_position_zero_proceeds(
            settings,
            position,
            reason="THRESHOLD_STALE_EXIT",
        )
        if stale_trade is not None:
            stale_exits.append(stale_trade)
    return stale_exits


def _strategy_health_summary(
    settings: Settings,
    *,
    trades_path: Path,
    status_path: Path | None,
    strategy_name: str,
) -> dict[str, Any]:
    trades = _json_load(trades_path, [])
    lifetime_closed_trades = sum(1 for trade in trades if trade.get("type") == "CLOSE")
    lookback_hours = max(int(settings.strategy_health_lookback_hours), 1)
    min_closed_trades = max(int(settings.strategy_health_min_closed_trades), 0)
    min_realized_pnl_usdc = float(settings.strategy_health_min_realized_pnl_usdc)
    positions_path = Path(str(trades_path).replace("_trades.json", "_positions.json"))
    marks_path = Path(str(trades_path).replace("_trades.json", "_marks.json"))
    open_positions_count = len(_json_load(positions_path, []))
    marks_payload = _json_load(marks_path, {"summary": {}})
    marks_summary = marks_payload.get("summary", {}) if isinstance(marks_payload, dict) else {}
    unrealized_pnl_usdc = _as_float(
        _first(marks_summary, "total_unrealized_pnl_usdc", default=0.0),
        0.0,
    )
    warmup_note = ""
    if _is_warmup_intraday_strategy(strategy_name, lifetime_closed_trades):
        lookback_hours = max(lookback_hours, 72)
        min_closed_trades = 0
        min_realized_pnl_usdc = 0.0
        warmup_note = "warmup mode until 3 lifetime closes"
    elif strategy_name == "wallet_copy":
        min_realized_pnl_usdc = 0.0
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    recent_closes: list[dict[str, Any]] = []
    for trade in trades:
        if trade.get("type") != "CLOSE":
            continue
        closed_at = _normalize_iso(str(trade.get("closed_at") or ""))
        if closed_at is None or closed_at < cutoff:
            continue
        recent_closes.append(trade)
    recent_realized_pnl = round(sum(_as_float(item.get("pnl_usdc"), 0.0) for item in recent_closes), 2)
    status_payload = _json_load(status_path, {}) if status_path is not None else {}
    runner_status = str(status_payload.get("status") or "").strip().lower()
    last_error = str(status_payload.get("last_error") or "").strip()
    reasons: list[str] = []
    if settings.strategy_health_block_degraded and runner_status == "degraded":
        reasons.append(f"runner degraded{f': {last_error}' if last_error else ''}")
    if strategy_name == "crypto_wallet_copy" and settings.crypto_wallet_copy_quarantined:
        reasons.append("strategy quarantined")
    reasons.extend(
        _warmup_risk_reasons(
            settings,
            strategy_name=strategy_name,
            lifetime_closed_trades=lifetime_closed_trades,
            open_positions_count=open_positions_count,
            unrealized_pnl_usdc=unrealized_pnl_usdc,
        )
    )
    if len(recent_closes) < min_closed_trades:
        reasons.append(f"only {len(recent_closes)} closes in {lookback_hours}h ({min_closed_trades} required)")
    if recent_realized_pnl < min_realized_pnl_usdc:
        reasons.append(f"recent realized pnl {recent_realized_pnl:.2f} < {min_realized_pnl_usdc:.2f}")
    probation_allowed = _probation_reentry_allowed(
        settings,
        strategy_name,
        reasons,
        open_positions_count=open_positions_count,
    )
    reason = "; ".join(reasons)
    if probation_allowed:
        reason = f"probation mode: {reason}"
    elif not reason and warmup_note:
        reason = warmup_note
    return {
        "strategy": strategy_name,
        "healthy": (not reasons) or probation_allowed,
        "lookback_hours": lookback_hours,
        "lifetime_closed_trades": lifetime_closed_trades,
        "recent_closed_trades": len(recent_closes),
        "recent_realized_pnl_usdc": recent_realized_pnl,
        "open_positions_count": open_positions_count,
        "unrealized_pnl_usdc": round(unrealized_pnl_usdc, 2),
        "runner_status": runner_status or "unknown",
        "last_error": last_error or None,
        "reason": reason,
        "probation_allowed": probation_allowed,
    }


def _strategy_health_gate(
    settings: Settings,
    *,
    trades_path: Path,
    status_path: Path | None,
    strategy_name: str,
) -> dict[str, Any]:
    summary = _strategy_health_summary(
        settings,
        trades_path=trades_path,
        status_path=status_path,
        strategy_name=strategy_name,
    )
    blocked = bool(settings.strategy_health_gate_enabled and not summary["healthy"])
    status_label = "probation" if summary.get("probation_allowed") else ("healthy" if not blocked else "blocked")
    return {**summary, "blocked": blocked, "status_label": status_label}


def _risk_bankroll_usdc(settings: Settings) -> float:
    bankroll = _current_bankroll_usdc(settings)
    if not settings.use_equity_for_risk_caps:
        return bankroll
    equity = _current_equity_usdc(settings)
    return round(max(min(bankroll, equity), 0.0), 2)


def _remaining_bankroll_usdc(settings: Settings, executor: PaperExecutor) -> float:
    portfolio_cap = _risk_bankroll_usdc(settings) * max(settings.max_portfolio_fraction, 0.0)
    remaining = round(portfolio_cap - executor.open_notional_usdc(), 2)
    return max(remaining, 0.0)


def _position_cap_usdc(settings: Settings) -> float:
    return round(_risk_bankroll_usdc(settings) * max(settings.max_position_fraction, 0.0), 2)


def _market_family_label(question: str, slug: str) -> str:
    normalized = _normalize_market_text(question)
    slug_text = str(slug or "").strip().lower()
    if "up or down" in normalized or "up-or-down" in slug_text or "updown" in slug_text:
        return "updown"
    if "mvp" in normalized:
        return "mvp"
    if "conference finals" in normalized:
        return "conference_finals"
    if "nomination" in normalized:
        return "nomination"
    if "fdv" in normalized:
        return "fdv"
    if "airdrop" in normalized:
        return "airdrop"
    if "between" in normalized:
        return "range"
    if "win" in normalized and "will" in normalized:
        return "winner"
    return "other"


def _scan_filtered_shadow_candidates(
    cli: PolymarketCLI,
    *,
    markets_limit: int,
    allowed_categories: set[str] | None = None,
    min_hours_to_resolution: float = 0.0,
    max_hours_to_resolution: float = 10_000.0,
    min_book_depth_usdc: float = 0.0,
    min_midpoint: float = 0.01,
    max_midpoint: float = 0.99,
    market_filter: Callable[[dict[str, Any]], bool] | None = None,
    prebook_limit: int = 24,
) -> list[MarketCandidate]:
    candidates: list[MarketCandidate] = []
    markets = cli.list_markets(max(markets_limit, 1))
    shortlisted: list[dict[str, Any]] = []
    for market in markets:
        if not _market_is_tradeable(market):
            continue
        token_id = _extract_yes_token_id(market)
        if not token_id:
            continue
        question = _extract_question(market)
        slug = _extract_slug(market)
        category = _extract_market_category(market)
        if category == "unknown":
            category = _categorize_text(question, slug)
        if allowed_categories and category not in allowed_categories:
            continue
        hours_left = _extract_hours_to_resolution(market)
        if hours_left <= 0.0 or hours_left < min_hours_to_resolution or hours_left > max_hours_to_resolution:
            continue
        if market_filter is not None and not market_filter(market):
            continue
        shortlisted.append(market)
    shortlisted.sort(
        key=lambda market: (
            _extract_hours_to_resolution(market),
            -_extract_volume(market),
            _extract_question(market),
        )
    )
    shortlisted = shortlisted[: max(prebook_limit, 1)]
    for market in shortlisted:
        token_id = _extract_yes_token_id(market)
        midpoint = _safe_midpoint(cli, token_id)
        book = _safe_book(cli, token_id)
        if midpoint is None:
            midpoint = _midpoint_from_book(book)
        if midpoint is None or midpoint < min_midpoint or midpoint > max_midpoint:
            continue
        bids = book.get("bids", []) if isinstance(book, dict) else []
        asks = book.get("asks", []) if isinstance(book, dict) else []
        bids_depth = _sum_book_depth(bids)
        asks_depth = _sum_book_depth(asks)
        depth = min(bids_depth, asks_depth)
        if depth < min_book_depth_usdc:
            continue
        best_bid = _best_price(bids, side="bid")
        best_ask = _best_price(asks, default=1.0, side="ask")
        spread = max(best_ask - best_bid, 0.0)
        denom = bids_depth + asks_depth
        imbalance = ((bids_depth - asks_depth) / denom) if denom else 0.0
        question = _extract_question(market)
        slug = _extract_slug(market)
        category = _extract_market_category(market)
        if category == "unknown":
            category = _categorize_text(question, slug)
        score = min(depth / 2_500.0, 5.0) + (abs(imbalance) * 6.0) + max(0.0, 0.05 - spread) * 20.0
        candidates.append(
            MarketCandidate(
                market_id=_extract_market_id(market),
                question=question,
                slug=slug,
                token_id=token_id,
                midpoint=midpoint,
                best_bid=best_bid,
                best_ask=best_ask,
                bids_depth=round(bids_depth, 2),
                asks_depth=round(asks_depth, 2),
                spread=round(spread, 4),
                hours_to_resolution=round(_extract_hours_to_resolution(market), 2),
                total_volume=round(_extract_volume(market), 2),
                book_imbalance=round(imbalance, 4),
                category=category,
                priority_score=round(score, 3),
                raw_market={
                    **market,
                    "market_family": _market_family_label(question, slug),
                    "source_category": category,
                },
            )
        )
    candidates.sort(key=lambda item: (-item.priority_score, item.hours_to_resolution, -item.total_volume))
    return candidates


def _sports_early_entry_vote(candidate: MarketCandidate) -> Vote:
    if candidate.spread > 0.04:
        return Vote("sports_early_entry", "HOLD", 0.0, candidate.midpoint, "spread too wide")
    if candidate.midpoint < 0.08 or candidate.midpoint > 0.50:
        return Vote("sports_early_entry", "HOLD", 0.0, candidate.midpoint, "outside sports early-entry price band")
    return microstructure_vote(candidate)


def _penny_longshot_vote(candidate: MarketCandidate) -> Vote:
    if candidate.spread > 0.03:
        return Vote("penny_longshot", "HOLD", 0.0, candidate.midpoint, "spread too wide")
    if candidate.best_ask > 0.05:
        return Vote("penny_longshot", "HOLD", 0.0, candidate.midpoint, "contract not cheap enough")
    confidence = min(0.75, 0.55 + max(candidate.book_imbalance, 0.0) * 0.5)
    est = min(0.15, max(candidate.midpoint + 0.03, candidate.best_ask))
    return Vote("penny_longshot", "BUY", confidence, est, "cheap convex longshot with executable asks")


def _run_simple_shadow_cycle(
    settings: Settings,
    cli: PolymarketCLI,
    *,
    suffix: str,
    strategy_name: str,
    hold_minutes: float,
    take_profit_contract_price: float,
    candidates: list[MarketCandidate],
    fixed_position_usdc: float,
    vote_fn: Callable[[MarketCandidate], Vote],
    metadata_family: str,
    max_open_positions: int | None = None,
) -> dict[str, Any]:
    shadow_settings = _shadow_settings(settings, suffix)
    exits = monitor_crypto_5m_sniper_exits(
        shadow_settings,
        cli,
        hold_minutes=hold_minutes,
        take_profit_contract_price=take_profit_contract_price,
    )
    health_gate = _strategy_health_gate(
        settings,
        trades_path=shadow_settings.trades_path,
        status_path=_runner_status_path(suffix),
        strategy_name=strategy_name,
    )
    executor = _executor(shadow_settings, cli)
    existing_positions = executor.load_positions()
    open_lookup = _market_open_lookup(existing_positions)
    decisions: list[dict[str, Any]] = []
    opened_positions: list[dict[str, Any]] = []
    if health_gate["blocked"]:
        marks = mark_open_positions(shadow_settings, cli)
        return {
            "strategy": strategy_name,
            "queue_count": len(candidates),
            "trade_decisions": decisions,
            "exits": exits,
            "opened_positions_count": 0,
            "scaled_positions_count": 0,
            "marked_positions_count": marks["summary"]["open_positions"],
            "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
            "health_gate": health_gate,
        }
    for candidate in candidates[: max(settings.queue_max_candidates, 1)]:
        decision_base = {
            "market_id": candidate.market_id,
            "token_id": candidate.token_id,
            "question": candidate.question,
            "midpoint": candidate.midpoint,
            "priority_score": candidate.priority_score,
            "market_family": str(candidate.raw_market.get("market_family", "")),
        }
        vote = vote_fn(candidate)
        if vote.action == "HOLD":
            decisions.append({**decision_base, "action": "SKIP", "reason": vote.rationale, "votes": [vote.to_dict()]})
            continue
        if (candidate.market_id, vote.action) in open_lookup:
            decisions.append({**decision_base, "action": "SKIP", "reason": "position already open for this market side", "votes": [vote.to_dict()]})
            continue
        if max_open_positions is not None and len(existing_positions) + len(opened_positions) >= max_open_positions:
            decisions.append({**decision_base, "action": "SKIP", "reason": "strategy max open positions reached", "votes": [vote.to_dict()]})
            continue
        remaining_bankroll = _remaining_bankroll_usdc(shadow_settings, executor)
        size = round(min(fixed_position_usdc, remaining_bankroll, _position_cap_usdc(shadow_settings)), 2)
        if size <= 0:
            decisions.append({**decision_base, "action": "SKIP", "reason": "no remaining bankroll available", "votes": [vote.to_dict()]})
            continue
        book = _safe_book(cli, candidate.token_id)
        side_price = side_contract_price(vote.action, candidate.midpoint)
        opened = executor.open_position(
            candidate,
            vote.action,
            size,
            vote.confidence,
            max(0.01, abs(vote.estimated_probability - candidate.midpoint)),
            book=book,
            metadata={"strategy_tag": strategy_name, "market_family": metadata_family},
        )
        if opened is None:
            decisions.append({**decision_base, "action": "SKIP", "reason": "insufficient executable depth for open", "votes": [vote.to_dict()]})
            continue
        decisions.append({**decision_base, "action": "OPEN", "trade": opened, "votes": [vote.to_dict()], "entry_contract_price": round(side_price, 6)})
        opened_positions.append(opened)
        open_lookup[(candidate.market_id, vote.action)] = opened
    marks = mark_open_positions(shadow_settings, cli)
    return {
        "strategy": strategy_name,
        "queue_count": len(candidates),
        "trade_decisions": decisions,
        "exits": exits,
        "opened_positions_count": len(opened_positions),
        "scaled_positions_count": 0,
        "marked_positions_count": marks["summary"]["open_positions"],
        "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
        "health_gate": health_gate,
    }


def _strategy_style_profile(trades: list[dict[str, Any]]) -> dict[str, Any]:
    opens = [trade for trade in trades if trade.get("type") == "OPEN"]
    closes = [trade for trade in trades if trade.get("type") == "CLOSE"]
    categories: dict[str, int] = defaultdict(int)
    sides: dict[str, int] = defaultdict(int)
    families: dict[str, int] = defaultdict(int)
    entry_prices: list[float] = []
    holds: list[float] = []
    opened_by_position = {str(item.get("position_id", "")): item for item in opens if item.get("position_id")}
    for trade in opens:
        category = _normalize_category(str(trade.get("category", "") or "")) or "unknown"
        categories[category] += 1
        sides[str(trade.get("side", "")).upper() or "UNKNOWN"] += 1
        families[str(trade.get("market_family", _market_family_label(str(trade.get("question", "")), str(trade.get("slug", ""))))) or "other"] += 1
        entry_prices.append(_as_float(trade.get("entry_contract_price", trade.get("entry_price")), 0.0))
    for trade in closes:
        opened = opened_by_position.get(str(trade.get("position_id", "")))
        if not opened:
            continue
        opened_at = _normalize_iso(str(opened.get("opened_at", "")))
        closed_at = _normalize_iso(str(trade.get("closed_at", "")))
        if opened_at and closed_at and closed_at >= opened_at:
            holds.append((closed_at - opened_at).total_seconds() / 60.0)
    total_open = max(len(opens), 1)
    def top_mix(counter: dict[str, int]) -> list[dict[str, Any]]:
        return [
            {"key": key, "count": count, "share_pct": round((count / total_open) * 100.0, 1)}
            for key, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:5]
        ]
    nonzero_prices = [price for price in entry_prices if price > 0]
    return {
        "opens": len(opens),
        "closes": len(closes),
        "category_mix": top_mix(categories),
        "side_mix": top_mix(sides),
        "market_family_mix": top_mix(families),
        "entry_contract_price_min": round(min(nonzero_prices), 4) if nonzero_prices else 0.0,
        "entry_contract_price_avg": round(sum(nonzero_prices) / len(nonzero_prices), 4) if nonzero_prices else 0.0,
        "entry_contract_price_max": round(max(nonzero_prices), 4) if nonzero_prices else 0.0,
        "avg_hold_minutes": round(sum(holds) / len(holds), 1) if holds else 0.0,
    }


def write_strategy_style_profiles(settings: Settings) -> dict[str, Any]:
    specs = {
        "primary": settings.trades_path,
        "wallet_copy": Path("state/ab_wallet_copy_trades.json"),
        "wallet_copy_aggressive": Path("state/ab_wallet_copy_aggressive_trades.json"),
        "crypto_wallet_copy": Path("state/ab_crypto_wallet_copy_trades.json"),
        "crypto_5m_sniper": Path("state/ab_crypto_5m_sniper_trades.json"),
        "crypto_5m_box_arb": Path("state/ab_crypto_5m_box_arb_trades.json"),
        "crypto_next_window_sniper": Path("state/ab_crypto_next_window_sniper_trades.json"),
        "crypto_next_window_box_arb": Path("state/ab_crypto_next_window_box_arb_trades.json"),
        "crypto_intraday_scheduled": Path("state/ab_crypto_intraday_scheduled_trades.json"),
        "crypto_threshold_snapshot": Path("state/ab_crypto_threshold_snapshot_trades.json"),
        "sports_early_entry": Path("state/ab_sports_early_entry_trades.json"),
        "penny_longshot": Path("state/ab_penny_longshot_trades.json"),
        "crypto_latency_5m": Path("state/ab_crypto_latency_5m_trades.json"),
    }
    profiles = {}
    for name, path in specs.items():
        profiles[name] = _strategy_style_profile(_json_load(path, []))
    payload = {"generated_at": utc_now_iso(), "profiles": profiles}
    _json_dump(Path("state/strategy_style_profiles.json"), payload)
    return payload


def _is_primary_book(settings: Settings) -> bool:
    return settings.positions_path.name == "positions.json" and settings.trades_path.name == "trades.json"


def _forced_derisk_reason(
    settings: Settings,
    position: dict[str, Any],
    *,
    current_contract_price: float,
    unrealized_pnl: float,
    age_hours: float,
) -> str | None:
    if not settings.forced_derisk_enabled or not _is_primary_book(settings):
        return None
    if age_hours * 60.0 < settings.forced_derisk_min_holding_minutes:
        return None
    entry_contract_price = _as_float(position.get("entry_contract_price"), 0.0)
    if entry_contract_price <= 0:
        entry_contract_price = side_contract_price(str(position.get("side", "BUY")), _as_float(position.get("entry_price"), 0.0))
    adverse_contract_move = max(entry_contract_price - current_contract_price, 0.0)
    notional = max(_as_float(position.get("notional_usdc"), 0.0), 0.0)
    if notional <= 0:
        return None
    drawdown_fraction = max(-unrealized_pnl / notional, 0.0)
    if adverse_contract_move >= settings.forced_derisk_max_contract_loss:
        return f"FORCED_DERISK_CONTRACT ({adverse_contract_move:.3f} >= {settings.forced_derisk_max_contract_loss:.3f})"
    if drawdown_fraction >= settings.forced_derisk_max_drawdown_fraction:
        return f"FORCED_DERISK_DRAWDOWN ({drawdown_fraction:.2%} >= {settings.forced_derisk_max_drawdown_fraction:.2%})"
    return None


def _target_trade_size_usdc(
    settings: Settings,
    side: str,
    side_votes: list[Vote],
    candidate: MarketCandidate,
    remaining_bankroll: float,
) -> float:
    avg_probability = sum(_side_probability(side, vote.estimated_probability) for vote in side_votes) / len(side_votes)
    size = kelly_size(avg_probability, _side_market_price(side, candidate.midpoint), remaining_bankroll, settings.max_kelly_fraction)
    if len(side_votes) == 1:
        size *= settings.single_vote_size_multiplier
    size = min(size, remaining_bankroll, _position_cap_usdc(settings))
    if 0 < size < settings.min_position_usdc <= remaining_bankroll:
        size = min(settings.min_position_usdc, remaining_bankroll, _position_cap_usdc(settings))
    depth = min(candidate.bids_depth, candidate.asks_depth)
    max_vote_confidence = max((vote.confidence for vote in side_votes), default=0.0)
    if (
        settings.conviction_min_notional_usdc > 0
        and max_vote_confidence >= settings.conviction_confidence_threshold
        and depth >= settings.conviction_depth_threshold_usdc
        and size > 0
    ):
        size = max(size, min(settings.conviction_min_notional_usdc, remaining_bankroll, _position_cap_usdc(settings)))
    return round(size, 2)


def _last_trade_by_market(trades: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for trade in trades:
        market_id = str(trade.get("market_id", ""))
        if not market_id:
            continue
        timestamp = str(trade.get("closed_at") or trade.get("opened_at") or "")
        current = latest.get(market_id)
        current_ts = str(current.get("closed_at") or current.get("opened_at") or "") if current else ""
        if current is None or timestamp >= current_ts:
            latest[market_id] = trade
    return latest


def _trade_history_by_market(trades: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    history: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        market_id = str(trade.get("market_id", ""))
        if not market_id:
            continue
        history[market_id].append(trade)
    for market_id, items in history.items():
        items.sort(key=lambda trade: str(trade.get("closed_at") or trade.get("scaled_at") or trade.get("opened_at") or ""))
        history[market_id] = items
    return history


def _stale_reentry_guard(settings: Settings, candidate: MarketCandidate, trade_history: dict[str, list[dict[str, Any]]]) -> tuple[bool, str]:
    market_trades = trade_history.get(candidate.market_id, [])
    if not market_trades or settings.stale_reentry_max_closes <= 0:
        return False, ""

    now = datetime.now(timezone.utc)
    lookback_cutoff = now.timestamp() - (max(settings.stale_reentry_lookback_hours, 1) * 3600)
    stale_closes: list[dict[str, Any]] = []
    cumulative_loss = 0.0
    latest_stale_close: dict[str, Any] | None = None
    for trade in market_trades:
        if trade.get("type") != "CLOSE" or trade.get("reason") != "STALE_THESIS":
            continue
        closed_at = _normalize_iso(str(trade.get("closed_at") or ""))
        if closed_at is None or closed_at.timestamp() < lookback_cutoff:
            continue
        stale_closes.append(trade)
        pnl = _as_float(trade.get("pnl_usdc"), 0.0)
        if pnl < 0:
            cumulative_loss += abs(pnl)
        latest_stale_close = trade

    if len(stale_closes) < settings.stale_reentry_max_closes:
        return False, ""
    if cumulative_loss < settings.stale_reentry_min_cumulative_loss_usdc:
        return False, ""
    if latest_stale_close is None:
        return False, ""
    closed_at = _normalize_iso(str(latest_stale_close.get("closed_at") or ""))
    if closed_at is None:
        return False, ""
    elapsed_minutes = max((now - closed_at).total_seconds() / 60.0, 0.0)
    if elapsed_minutes < settings.stale_reentry_block_minutes:
        return True, (
            f"stale re-entry blocked ({len(stale_closes)} stale closes, "
            f"${cumulative_loss:.2f} loss, {elapsed_minutes:.0f}m < {settings.stale_reentry_block_minutes}m)"
        )
    return False, ""


def _high_midpoint_sell_guard(settings: Settings, side: str, candidate: MarketCandidate) -> tuple[bool, str]:
    if side != "SELL":
        return False, ""
    if candidate.midpoint > settings.max_sell_midpoint:
        return True, f"sell midpoint too high ({candidate.midpoint:.3f} > {settings.max_sell_midpoint:.3f})"
    return False, ""


def _primary_book_entry_guard(settings: Settings, candidate: MarketCandidate, existing_position: dict[str, Any] | None) -> tuple[bool, str]:
    if existing_position is not None or not _is_primary_book(settings):
        return False, ""
    if settings.primary_book_block_politics and candidate.category == "politics":
        return True, "primary book politics entries disabled"
    return False, ""


def _latest_close_for_market(trade_history: dict[str, list[dict[str, Any]]], market_id: str) -> dict[str, Any] | None:
    for trade in reversed(trade_history.get(market_id, [])):
        if trade.get("type") == "CLOSE":
            return trade
    return None


def _aggressive_reentry_guard(
    candidate: MarketCandidate,
    trade_history: dict[str, list[dict[str, Any]]],
    *,
    require_profit: bool,
    min_profit_usdc: float,
) -> tuple[bool, str]:
    if not require_profit:
        return False, ""
    latest_close = _latest_close_for_market(trade_history, candidate.market_id)
    if latest_close is None:
        return False, ""
    pnl = _as_float(latest_close.get("pnl_usdc"), 0.0)
    if pnl >= min_profit_usdc:
        return False, ""
    return True, f"re-entry requires prior profit ({pnl:.2f} < {min_profit_usdc:.2f})"


def _aggressive_market_guard(candidate: MarketCandidate) -> tuple[bool, str]:
    question = str(candidate.question or "").lower()
    category = str(candidate.category or "").lower()
    weak_markers = (
        "eastern conference finals",
        "western conference finals",
        "conference finals",
        "nomination",
    )
    if category == "politics":
        return True, "aggressive excludes politics markets"
    if any(marker in question for marker in weak_markers):
        return True, "aggressive excludes weak market family"
    return False, ""


def _market_theme_key(question: str, category: str | None = None) -> str:
    text = " ".join(str(question or "").strip().lower().replace("?", "").split())
    if not text:
        return (category or "unknown").strip().lower() or "unknown"
    if text.startswith("will ") and " win the " in text:
        return text.split(" win the ", 1)[1]
    if text.startswith("will ") and " by " in text:
        return text.split("will ", 1)[1]
    return text


def _select_rotation_position(
    settings: Settings,
    candidate: MarketCandidate,
    candidate_score: float,
    positions: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not settings.rotation_enabled:
        return None

    eligible: list[dict[str, Any]] = []
    for position in positions:
        if str(position.get("market_id", "")) == candidate.market_id:
            continue
        hold_minutes = _age_minutes(str(position.get("opened_at")))
        if hold_minutes < settings.rotation_min_holding_minutes:
            continue
        quality_score = (
            float(position.get("thesis_confidence", 0.0)) * 10.0
            + float(position.get("expected_gap", 0.0)) * 100.0
            - min(hold_minutes / 60.0, 24.0) * 0.05
        )
        eligible.append(
            {
                "position": position,
                "hold_minutes": hold_minutes,
                "quality_score": round(quality_score, 3),
            }
        )

    if not eligible:
        return None

    eligible.sort(key=lambda item: (item["quality_score"], -item["hold_minutes"]))
    weakest_choice = eligible[0]
    weakest = dict(weakest_choice["position"])
    weakest_score = float(weakest_choice["quality_score"])
    if candidate_score < weakest_score + settings.rotation_min_priority_score_delta:
        return None
    weakest["_rotation_quality_score"] = weakest_score
    return weakest


def _within_market_cooldown(settings: Settings, candidate: MarketCandidate, last_trade: dict[str, Any] | None) -> tuple[bool, str]:
    if last_trade is None or settings.market_cooldown_minutes <= 0:
        return False, ""
    timestamp = _normalize_iso(str(last_trade.get("closed_at") or last_trade.get("opened_at") or ""))
    if timestamp is None:
        return False, ""
    elapsed_minutes = max((datetime.now(timezone.utc) - timestamp).total_seconds() / 60.0, 0.0)
    cooldown_minutes = settings.market_cooldown_minutes
    if last_trade.get("type") == "CLOSE":
        pnl = _as_float(last_trade.get("pnl_usdc"), 0.0)
        cooldown_minutes = settings.winner_cooldown_minutes if pnl > 0 else settings.loser_cooldown_minutes
    if elapsed_minutes < cooldown_minutes:
        return True, f"cooldown active ({elapsed_minutes:.0f}m < {cooldown_minutes}m)"
    return False, ""


def mark_open_positions(settings: Settings, cli: PolymarketCLI) -> dict[str, Any]:
    positions = _json_load(settings.positions_path, [])
    marked_positions = []
    total_unrealized_pnl = 0.0
    total_mark_value = 0.0

    for position in positions:
        current_midpoint = _as_float(position.get("entry_price"), 0.0)
        mark_error = None
        current_contract_price = 0.0
        liquidation_fill_ratio = 0.0
        shares = effective_position_shares(position)
        mark_value = 0.0
        unrealized_pnl = 0.0
        try:
            book = _safe_book(cli, position["token_id"])
            liquidation = _simulate_contract_sell(_book_contract_levels(book, position["side"], "close"), shares)
            current_contract_price = liquidation["avg_contract_price"]
            liquidation_fill_ratio = liquidation["fill_ratio"]
            mark_value = liquidation["proceeds_usdc"]
            unrealized_pnl = round(mark_value - _as_float(position.get("notional_usdc"), 0.0), 2)
            current_midpoint = _contract_midpoint_to_yes_midpoint(position["side"], current_contract_price)
        except Exception as exc:  # noqa: BLE001
            mark_error = str(exc)
            fallback = dict(position)
            try:
                current_midpoint = cli.midpoint(position["token_id"])
                fallback["current_contract_price"] = side_contract_price(position["side"], current_midpoint)
                shares, mark_value, unrealized_pnl = position_mark(fallback, current_midpoint)
                current_contract_price = fallback["current_contract_price"]
            except Exception as midpoint_exc:  # noqa: BLE001
                mark_error = f"{mark_error}; midpoint fallback failed: {midpoint_exc}"
                fallback["current_contract_price"] = side_contract_price(position["side"], current_midpoint)
                shares, mark_value, unrealized_pnl = position_mark(fallback, current_midpoint)
                current_contract_price = fallback["current_contract_price"]
        total_unrealized_pnl += unrealized_pnl
        total_mark_value += mark_value
        marked_positions.append(
            {
                "position_id": position["position_id"],
                "market_id": position["market_id"],
                "token_id": position["token_id"],
                "question": position["question"],
                "side": position["side"],
                "entry_price": position["entry_price"],
                "current_midpoint": round(current_midpoint, 4),
                "current_contract_price": round(side_contract_price(position["side"], current_midpoint), 4),
                "liquidation_contract_price": round(current_contract_price, 4),
                "shares": shares,
                "mark_value_usdc": mark_value,
                "unrealized_pnl_usdc": unrealized_pnl,
                "opened_at": position["opened_at"],
                "liquidation_fill_ratio": liquidation_fill_ratio,
                "mark_error": mark_error,
            }
        )

    payload = {
        "updated_at": utc_now_iso(),
        "positions": marked_positions,
        "summary": {
            "open_positions": len(marked_positions),
            "total_unrealized_pnl_usdc": round(total_unrealized_pnl, 2),
            "total_mark_value_usdc": round(total_mark_value, 2),
        },
    }
    _json_dump(settings.marks_path, payload)
    return payload


def trade_candidates(settings: Settings, cli: PolymarketCLI) -> list[dict[str, Any]]:
    queue = [MarketCandidate(**item) for item in _json_load(settings.queue_path, [])]
    thesis_map = _load_thesis_map(settings)
    activity = _json_load(settings.target_activity_path, {"by_match_key": {}})
    executor = _executor(settings, cli)
    existing_positions = executor.load_positions()
    trades = _json_load(settings.trades_path, [])
    last_trade_map = _last_trade_by_market(trades)
    trade_history = _trade_history_by_market(trades)
    open_token_ids = {str(item.get("token_id", "")) for item in existing_positions}
    open_market_ids = {str(item.get("market_id", "")) for item in existing_positions}
    position_by_market = {str(item.get("market_id", "")): item for item in existing_positions}
    position_by_token = {str(item.get("token_id", "")): item for item in existing_positions}
    decisions: list[dict[str, Any]] = []
    scale_ins_this_cycle = 0
    health_gate = _strategy_health_gate(
        settings,
        trades_path=settings.trades_path,
        status_path=settings.status_path,
        strategy_name="primary",
    )
    if health_gate["blocked"]:
        reason = f"strategy health gate blocked new entries: {health_gate['reason']}"
        for candidate in queue:
            decisions.append(
                {
                    "market_id": candidate.market_id,
                    "question": candidate.question,
                    "category": candidate.category,
                    "token_id": candidate.token_id,
                    "priority_score": candidate.priority_score,
                    "midpoint": candidate.midpoint,
                    "action": "SKIP",
                    "reason": reason,
                }
            )
        return decisions

    for candidate in queue:
        decision_base = {
            "market_id": candidate.market_id,
            "question": candidate.question,
            "category": candidate.category,
            "token_id": candidate.token_id,
            "priority_score": candidate.priority_score,
            "midpoint": candidate.midpoint,
        }
        existing_position = position_by_token.get(candidate.token_id) or position_by_market.get(candidate.market_id)
        entry_blocked, entry_block_reason = _primary_book_entry_guard(settings, candidate, existing_position)
        if entry_blocked:
            decisions.append({**decision_base, "action": "SKIP", "reason": entry_block_reason})
            continue
        cooldown_active, cooldown_reason = _within_market_cooldown(settings, candidate, last_trade_map.get(candidate.market_id))
        if cooldown_active and existing_position is None:
            decisions.append({**decision_base, "action": "SKIP", "reason": cooldown_reason})
            continue
        stale_blocked, stale_reason = _stale_reentry_guard(settings, candidate, trade_history)
        if stale_blocked and existing_position is None:
            decisions.append({**decision_base, "action": "SKIP", "reason": stale_reason})
            continue
        thesis = thesis_map.get(candidate.market_id)
        if thesis is None:
            continue
        votes = [
            convergence_vote(candidate, thesis, settings),
            whale_copy_vote(candidate, activity),
            microstructure_vote(candidate),
        ]
        buy_votes = [vote for vote in votes if vote.action == "BUY"]
        sell_votes = [vote for vote in votes if vote.action == "SELL"]
        side = "BUY" if len(buy_votes) > len(sell_votes) else "SELL"
        side_votes = buy_votes if side == "BUY" else sell_votes
        if len(side_votes) < settings.consensus_votes_required:
            decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "consensus threshold not met"})
            continue
        sell_blocked, sell_reason = _high_midpoint_sell_guard(settings, side, candidate)
        if sell_blocked and existing_position is None:
            decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": sell_reason})
            continue

        avg_confidence = sum(vote.confidence for vote in side_votes) / len(side_votes)
        candidate_rotation_score = round(
            float(candidate.priority_score)
            + (avg_confidence * 10.0)
            + (abs(thesis.estimated_probability - candidate.midpoint) * 100.0),
            3,
        )
        remaining_bankroll = _remaining_bankroll_usdc(settings, executor)
        if len(existing_positions) >= settings.max_open_positions and existing_position is None:
            rotation_target = _select_rotation_position(settings, candidate, candidate_rotation_score, existing_positions)
            if rotation_target is None:
                decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": f"max open positions reached ({settings.max_open_positions})"})
                continue
            exit_price = cli.midpoint(rotation_target["token_id"])
            rotation_entry_contract = _as_float(rotation_target.get("entry_contract_price"), 0.0)
            if rotation_entry_contract <= 0:
                rotation_entry_contract = side_contract_price(rotation_target["side"], rotation_target["entry_price"])
            rotation_pnl = round((side_contract_price(rotation_target["side"], exit_price) - rotation_entry_contract) * effective_position_shares(rotation_target), 2)
            max_rotation_loss = max(10.0, _as_float(rotation_target.get("notional_usdc"), 0.0) * 0.02)
            if rotation_pnl < -max_rotation_loss:
                decisions.append(
                    {
                        **decision_base,
                        "action": "SKIP",
                        "votes": [vote.to_dict() for vote in votes],
                        "reason": f"rotation loss guard ({rotation_pnl:.2f} < -{max_rotation_loss:.2f})",
                    }
                )
                continue
            rotation_book = _safe_book(cli, rotation_target["token_id"])
            rotated = executor.close_position(rotation_target, exit_price, "ROTATED_OUT", book=rotation_book)
            if rotated is None:
                decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "rotation exit lacked executable depth"})
                continue
            existing_positions = executor.load_positions()
            position_by_market = {str(item.get("market_id", "")): item for item in existing_positions}
            position_by_token = {str(item.get("token_id", "")): item for item in existing_positions}
            last_trade_map[str(rotation_target.get("market_id", ""))] = rotated
            decisions.append(
                {
                    **decision_base,
                    "action": "ROTATE_OUT",
                    "trade": rotated,
                    "candidate_rotation_score": candidate_rotation_score,
                    "rotated_position_score": rotation_target.get("_rotation_quality_score"),
                    "reason": f"freed slot for stronger candidate {candidate_rotation_score:.2f}",
                }
            )
            remaining_bankroll = _remaining_bankroll_usdc(settings, executor)

        if remaining_bankroll <= 0 and existing_position is None:
            rotation_target = _select_rotation_position(settings, candidate, candidate_rotation_score, existing_positions)
            if rotation_target is None:
                decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "no remaining bankroll available"})
                continue
            exit_price = cli.midpoint(rotation_target["token_id"])
            rotation_entry_contract = _as_float(rotation_target.get("entry_contract_price"), 0.0)
            if rotation_entry_contract <= 0:
                rotation_entry_contract = side_contract_price(rotation_target["side"], rotation_target["entry_price"])
            rotation_pnl = round((side_contract_price(rotation_target["side"], exit_price) - rotation_entry_contract) * effective_position_shares(rotation_target), 2)
            max_rotation_loss = max(10.0, _as_float(rotation_target.get("notional_usdc"), 0.0) * 0.02)
            if rotation_pnl < -max_rotation_loss:
                decisions.append(
                    {
                        **decision_base,
                        "action": "SKIP",
                        "votes": [vote.to_dict() for vote in votes],
                        "reason": f"rotation loss guard ({rotation_pnl:.2f} < -{max_rotation_loss:.2f})",
                    }
                )
                continue
            rotation_book = _safe_book(cli, rotation_target["token_id"])
            rotated = executor.close_position(rotation_target, exit_price, "ROTATED_OUT", book=rotation_book)
            if rotated is None:
                decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "rotation exit lacked executable depth"})
                continue
            existing_positions = executor.load_positions()
            position_by_market = {str(item.get("market_id", "")): item for item in existing_positions}
            position_by_token = {str(item.get("token_id", "")): item for item in existing_positions}
            last_trade_map[str(rotation_target.get("market_id", ""))] = rotated
            decisions.append(
                {
                    **decision_base,
                    "action": "ROTATE_OUT",
                    "trade": rotated,
                    "candidate_rotation_score": candidate_rotation_score,
                    "rotated_position_score": rotation_target.get("_rotation_quality_score"),
                    "reason": f"freed capital for stronger candidate {candidate_rotation_score:.2f}",
                }
            )
            remaining_bankroll = _remaining_bankroll_usdc(settings, executor)

        size = _target_trade_size_usdc(settings, side, side_votes, candidate, remaining_bankroll)
        if size <= 0:
            decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "negative or zero Kelly size"})
            continue

        if existing_position is not None:
            if not settings.scale_in_enabled:
                decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "position already open for this market"})
                continue
            if scale_ins_this_cycle >= settings.max_scale_ins_per_cycle:
                decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "max scale-ins reached"})
                continue
            if existing_position.get("side") != side:
                decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "open position side conflicts with current signal"})
                continue
            current_notional = _as_float(existing_position.get("notional_usdc"), 0.0)
            target_notional = min(size, _position_cap_usdc(settings))
            if current_notional >= target_notional * settings.scale_in_threshold_fraction:
                decisions.append(
                    {
                        **decision_base,
                        "action": "SKIP",
                        "votes": [vote.to_dict() for vote in votes],
                        "reason": f"position already sized ({current_notional:.2f}/{target_notional:.2f})",
                    }
                )
                continue
            add_notional = round(min(target_notional - current_notional, remaining_bankroll, _position_cap_usdc(settings) - current_notional), 2)
            if add_notional <= 0:
                decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "no scale-in room available"})
                continue
            execution_book = _safe_book(cli, candidate.token_id)
            scaled = executor.scale_position(existing_position, candidate, add_notional, "SCALE_TO_TARGET", book=execution_book)
            if scaled is None:
                decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "insufficient executable depth for scale-in"})
                continue
            existing_positions = executor.load_positions()
            position_by_market = {str(item.get("market_id", "")): item for item in existing_positions}
            position_by_token = {str(item.get("token_id", "")): item for item in existing_positions}
            scale_ins_this_cycle += 1
            decisions.append({**decision_base, "action": "SCALE", "trade": scaled, "votes": [vote.to_dict() for vote in votes]})
            continue

        expected_gap = abs(thesis.estimated_probability - candidate.midpoint)
        execution_book = _safe_book(cli, candidate.token_id)
        opened = executor.open_position(candidate, side, size, avg_confidence, expected_gap, book=execution_book)
        if opened is None:
            decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "insufficient executable depth for open"})
            continue
        existing_positions = executor.load_positions()
        open_token_ids.add(candidate.token_id)
        open_market_ids.add(candidate.market_id)
        position_by_market = {str(item.get("market_id", "")): item for item in existing_positions}
        position_by_token = {str(item.get("token_id", "")): item for item in existing_positions}
        last_trade_map[candidate.market_id] = opened
        decisions.append({**decision_base, "action": "OPEN", "trade": opened, "votes": [vote.to_dict() for vote in votes]})

    return decisions


def trade_candidates_wallet_copy_variant(
    settings: Settings,
    cli: PolymarketCLI,
    *,
    suffix: str,
    min_confidence: float,
    require_microstructure_alignment: bool,
    fixed_position_usdc: float | None = None,
    agent_name: str = "wallet_copy_ab",
    activity_path: Path | None = None,
    allow_multi_tranche: bool = False,
    max_tranches_per_market: int = 1,
    max_theme_positions: int = 0,
    max_theme_notional_usdc: float = 0.0,
    max_new_positions_per_theme_per_cycle: int = 0,
    reentry_requires_profit: bool = False,
    reentry_min_profit_usdc: float = 0.0,
    markets_limit_override: int | None = None,
    queue_limit_override: int | None = None,
    prebook_candidate_limit_override: int | None = None,
    market_guard: Callable[[MarketCandidate], tuple[bool, str]] | None = None,
) -> list[dict[str, Any]]:
    shadow_settings = _shadow_settings(settings, suffix)
    source_activity_path = activity_path or settings.target_activity_path
    activity = _json_load(source_activity_path, {"by_match_key": {}})
    queue = _wallet_copy_candidate_pool(
        settings,
        cli,
        activity,
        markets_limit=markets_limit_override,
        queue_limit=queue_limit_override,
        prebook_candidate_limit=prebook_candidate_limit_override,
    )
    executor = PaperExecutor(shadow_settings)
    existing_positions = executor.load_positions()
    trades = _json_load(shadow_settings.trades_path, [])
    last_trade_map = _last_trade_by_market(trades)
    trade_history = _trade_history_by_market(trades)
    decisions: list[dict[str, Any]] = []
    scale_ins_this_cycle = 0
    theme_new_positions_this_cycle: dict[str, int] = {}

    for candidate in queue:
        decision_base = {
            "market_id": candidate.market_id,
            "question": candidate.question,
            "category": candidate.category,
            "token_id": candidate.token_id,
            "priority_score": candidate.priority_score,
            "midpoint": candidate.midpoint,
        }
        if market_guard is not None:
            blocked, block_reason = market_guard(candidate)
            if blocked:
                decisions.append({**decision_base, "action": "SKIP", "reason": block_reason})
                continue
        theme_key = _market_theme_key(candidate.question, candidate.category)
        matching_positions = [
            item
            for item in existing_positions
            if str(item.get("token_id", "")) == candidate.token_id or str(item.get("market_id", "")) == candidate.market_id
        ]
        existing_position = matching_positions[0] if matching_positions else None
        cooldown_active, cooldown_reason = _within_market_cooldown(settings, candidate, last_trade_map.get(candidate.market_id))
        if cooldown_active and existing_position is None:
            decisions.append({**decision_base, "action": "SKIP", "reason": cooldown_reason})
            continue
        stale_blocked, stale_reason = _stale_reentry_guard(settings, candidate, trade_history)
        if stale_blocked and existing_position is None:
            decisions.append({**decision_base, "action": "SKIP", "reason": stale_reason})
            continue
        reentry_blocked, reentry_reason = _aggressive_reentry_guard(
            candidate,
            trade_history,
            require_profit=reentry_requires_profit,
            min_profit_usdc=reentry_min_profit_usdc,
        )
        if reentry_blocked and existing_position is None:
            decisions.append({**decision_base, "action": "SKIP", "reason": reentry_reason})
            continue

        whale_vote = wallet_copy_variant_vote(candidate, activity, min_confidence=min_confidence, agent_name=agent_name)
        micro_vote = microstructure_vote(candidate)
        votes = [whale_vote, micro_vote]
        if whale_vote.action == "HOLD":
            decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "no strong target-wallet signal"})
            continue
        if require_microstructure_alignment and micro_vote.action not in {whale_vote.action, "HOLD"}:
            decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "microstructure conflicts with wallet flow"})
            continue

        side = whale_vote.action
        side_votes = [whale_vote]
        same_side_positions = [item for item in matching_positions if str(item.get("side", "")).upper() == side]
        opposite_side_positions = [item for item in matching_positions if str(item.get("side", "")).upper() != side]
        if opposite_side_positions:
            decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "open position side conflicts with current signal"})
            continue
        if allow_multi_tranche and same_side_positions:
            if len(same_side_positions) >= max(max_tranches_per_market, 1):
                decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": f"max tranches reached ({len(same_side_positions)}/{max_tranches_per_market})"})
                continue
            existing_position = None
        elif same_side_positions:
            existing_position = same_side_positions[0]
        theme_positions = [
            item
            for item in existing_positions
            if _market_theme_key(str(item.get("question", "")), str(item.get("category", ""))) == theme_key
        ]
        theme_open_count = len(theme_positions)
        theme_open_notional = round(sum(_as_float(item.get("notional_usdc"), 0.0) for item in theme_positions), 2)
        if existing_position is None and max_theme_positions > 0 and theme_open_count >= max_theme_positions:
            decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": f"theme position cap reached ({theme_open_count}/{max_theme_positions})"})
            continue
        if existing_position is None and max_theme_notional_usdc > 0 and theme_open_notional >= max_theme_notional_usdc:
            decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": f"theme notional cap reached (${theme_open_notional:.2f}/${max_theme_notional_usdc:.2f})"})
            continue
        theme_cycle_count = theme_new_positions_this_cycle.get(theme_key, 0)
        if existing_position is None and max_new_positions_per_theme_per_cycle > 0 and theme_cycle_count >= max_new_positions_per_theme_per_cycle:
            decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": f"theme cycle cap reached ({theme_cycle_count}/{max_new_positions_per_theme_per_cycle})"})
            continue
        remaining_bankroll = _remaining_bankroll_usdc(shadow_settings, executor)
        if remaining_bankroll <= 0 and existing_position is None:
            decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "no remaining bankroll available"})
            continue

        size = _target_trade_size_usdc(shadow_settings, side, side_votes, candidate, remaining_bankroll)
        if fixed_position_usdc is not None and fixed_position_usdc > 0:
            size = round(min(size if size > 0 else fixed_position_usdc, fixed_position_usdc, remaining_bankroll, _position_cap_usdc(shadow_settings)), 2)
        if size <= 0:
            decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "negative or zero Kelly size"})
            continue

        if existing_position is not None:
            if not shadow_settings.scale_in_enabled:
                decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "position already open for this market"})
                continue
            if scale_ins_this_cycle >= shadow_settings.max_scale_ins_per_cycle:
                decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "max scale-ins reached"})
                continue
            if existing_position.get("side") != side:
                decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "open position side conflicts with current signal"})
                continue
            current_notional = _as_float(existing_position.get("notional_usdc"), 0.0)
            target_notional = min(size, _position_cap_usdc(shadow_settings))
            if current_notional >= target_notional * shadow_settings.scale_in_threshold_fraction:
                decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": f"position already sized ({current_notional:.2f}/{target_notional:.2f})"})
                continue
            add_notional = round(min(target_notional - current_notional, remaining_bankroll, _position_cap_usdc(shadow_settings) - current_notional), 2)
            if add_notional <= 0:
                decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "no scale-in room available"})
                continue
            execution_book = _safe_book(cli, candidate.token_id)
            scaled = executor.scale_position(existing_position, candidate, add_notional, "AB_SCALE_TO_TARGET", book=execution_book)
            if scaled is None:
                decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "insufficient executable depth for scale-in"})
                continue
            existing_positions = executor.load_positions()
            scale_ins_this_cycle += 1
            decisions.append({**decision_base, "action": "SCALE", "trade": scaled, "votes": [vote.to_dict() for vote in votes]})
            continue

        expected_gap = abs(whale_vote.estimated_probability - candidate.midpoint)
        execution_book = _safe_book(cli, candidate.token_id)
        opened = executor.open_position(candidate, side, size, whale_vote.confidence, expected_gap, book=execution_book)
        if opened is None:
            decisions.append({**decision_base, "action": "SKIP", "votes": [vote.to_dict() for vote in votes], "reason": "insufficient executable depth for open"})
            continue
        existing_positions = executor.load_positions()
        theme_new_positions_this_cycle[theme_key] = theme_cycle_count + 1
        last_trade_map[candidate.market_id] = opened
        decisions.append({**decision_base, "action": "OPEN", "trade": opened, "votes": [vote.to_dict() for vote in votes]})

    return decisions


def run_ab_wallet_copy_cycle(
    settings: Settings,
    cli: PolymarketCLI,
    *,
    suffix: str,
    strategy_name: str,
    min_confidence: float,
    require_microstructure_alignment: bool,
    fixed_position_usdc: float | None = None,
    agent_name: str = "wallet_copy_ab",
    activity_path: Path | None = None,
    allow_multi_tranche: bool = False,
    max_tranches_per_market: int = 1,
    max_theme_positions: int = 0,
    max_theme_notional_usdc: float = 0.0,
    max_new_positions_per_theme_per_cycle: int = 0,
    reentry_requires_profit: bool = False,
    reentry_min_profit_usdc: float = 0.0,
    markets_limit_override: int | None = None,
    queue_limit_override: int | None = None,
    prebook_candidate_limit_override: int | None = None,
    run_pre_trade_exits: bool = True,
    run_post_trade_exits: bool = True,
    run_marks: bool = True,
    market_guard: Callable[[MarketCandidate], tuple[bool, str]] | None = None,
) -> dict[str, Any]:
    shadow_settings = _shadow_settings(settings, suffix)
    pre_trade_exits = monitor_exits(shadow_settings, cli) if run_pre_trade_exits else []
    health_gate = _strategy_health_gate(
        settings,
        trades_path=shadow_settings.trades_path,
        status_path=_runner_status_path(suffix),
        strategy_name=strategy_name,
    )
    decisions = [] if health_gate["blocked"] else trade_candidates_wallet_copy_variant(
        settings,
        cli,
        suffix=suffix,
        min_confidence=min_confidence,
        require_microstructure_alignment=require_microstructure_alignment,
        fixed_position_usdc=fixed_position_usdc,
        agent_name=agent_name,
        activity_path=activity_path,
        allow_multi_tranche=allow_multi_tranche,
        max_tranches_per_market=max_tranches_per_market,
        max_theme_positions=max_theme_positions,
        max_theme_notional_usdc=max_theme_notional_usdc,
        max_new_positions_per_theme_per_cycle=max_new_positions_per_theme_per_cycle,
        reentry_requires_profit=reentry_requires_profit,
        reentry_min_profit_usdc=reentry_min_profit_usdc,
        markets_limit_override=markets_limit_override,
        queue_limit_override=queue_limit_override,
        prebook_candidate_limit_override=prebook_candidate_limit_override,
        market_guard=market_guard,
    )
    exits = pre_trade_exits + (monitor_exits(shadow_settings, cli) if run_post_trade_exits else [])
    marks = mark_open_positions(shadow_settings, cli) if run_marks else _json_load(
        shadow_settings.marks_path,
        {
            "updated_at": utc_now_iso(),
            "positions": [],
            "summary": {
                "open_positions": len(_json_load(shadow_settings.positions_path, [])),
                "total_unrealized_pnl_usdc": 0.0,
                "total_mark_value_usdc": 0.0,
            },
        },
    )
    opened_positions = [item["trade"] for item in decisions if item.get("action") == "OPEN" and item.get("trade")]
    scaled_positions = [item["trade"] for item in decisions if item.get("action") == "SCALE" and item.get("trade")]
    return {
        "strategy": strategy_name,
        "queue_count": len(decisions),
        "trade_decisions": decisions,
        "exits": exits,
        "opened_positions_count": len(opened_positions),
        "scaled_positions_count": len(scaled_positions),
        "marked_positions_count": marks["summary"]["open_positions"],
        "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
        "health_gate": health_gate,
    }


def run_aggressive_ab_cycle(settings: Settings, cli: PolymarketCLI, *, full_sync: bool = True) -> dict[str, Any]:
    shadow_settings = _shadow_settings(settings, "ab_wallet_copy_aggressive")
    health_gate = _strategy_health_gate(
        settings,
        trades_path=shadow_settings.trades_path,
        status_path=_runner_status_path("ab_wallet_copy_aggressive"),
        strategy_name="wallet_copy_aggressive",
    )
    if health_gate["blocked"]:
        marks = _json_load(
            shadow_settings.marks_path,
            {
                "updated_at": utc_now_iso(),
                "positions": [],
                "summary": {
                    "open_positions": len(_json_load(shadow_settings.positions_path, [])),
                    "total_unrealized_pnl_usdc": 0.0,
                    "total_mark_value_usdc": 0.0,
                },
            },
        )
        return {
            "strategy": "wallet_copy_aggressive",
            "queue_count": 0,
            "trade_decisions": [],
            "exits": [],
            "opened_positions_count": 0,
            "scaled_positions_count": 0,
            "marked_positions_count": marks["summary"]["open_positions"],
            "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
            "health_gate": health_gate,
        }
    if not health_gate["blocked"]:
        refresh_target_activity(settings, cli)
    result = run_ab_wallet_copy_cycle(
        settings,
        cli,
        suffix="ab_wallet_copy_aggressive",
        strategy_name="wallet_copy_aggressive",
        min_confidence=settings.ab_test_aggressive_min_whale_confidence,
        require_microstructure_alignment=settings.ab_test_aggressive_require_microstructure_alignment,
        fixed_position_usdc=settings.ab_test_aggressive_fixed_position_usdc,
        agent_name="wallet_copy_aggressive_ab",
        allow_multi_tranche=settings.ab_test_aggressive_allow_multi_tranche,
        max_tranches_per_market=settings.ab_test_aggressive_max_tranches_per_market,
        max_theme_positions=settings.ab_test_aggressive_max_theme_positions,
        max_theme_notional_usdc=settings.ab_test_aggressive_max_theme_notional_usdc,
        max_new_positions_per_theme_per_cycle=settings.ab_test_aggressive_max_new_positions_per_theme_per_cycle,
        reentry_requires_profit=settings.ab_test_aggressive_reentry_requires_profit,
        reentry_min_profit_usdc=settings.ab_test_aggressive_reentry_min_profit_usdc,
        markets_limit_override=settings.ab_test_aggressive_markets_limit,
        queue_limit_override=settings.ab_test_aggressive_queue_max_candidates,
        prebook_candidate_limit_override=settings.ab_test_aggressive_prebook_candidate_limit,
        run_pre_trade_exits=full_sync,
        run_post_trade_exits=full_sync,
        run_marks=full_sync,
        market_guard=_aggressive_market_guard,
    )
    return result


def run_crypto_ab_cycle(settings: Settings, cli: PolymarketCLI, *, refresh_base_activity: bool = True) -> dict[str, Any]:
    crypto_settings = replace(
        settings,
        category_include=("crypto",),
        category_exclude=(),
        category_rotation=(),
        min_book_depth_usdc=settings.ab_test_crypto_min_book_depth_usdc,
        min_hours_to_resolution=settings.ab_test_crypto_min_hours_to_resolution,
    )
    shadow_settings = _shadow_settings(crypto_settings, "ab_crypto_wallet_copy")
    health_gate = _strategy_health_gate(
        settings,
        trades_path=shadow_settings.trades_path,
        status_path=_runner_status_path("ab_crypto_wallet_copy"),
        strategy_name="crypto_wallet_copy",
    )
    if settings.crypto_wallet_copy_quarantined:
        exits = _quarantine_all_positions(shadow_settings, cli, reason="STRATEGY_QUARANTINED")
        marks = mark_open_positions(shadow_settings, cli)
        return {
            "strategy": "crypto_wallet_copy",
            "queue_count": 0,
            "trade_decisions": [],
            "exits": exits,
            "opened_positions_count": 0,
            "scaled_positions_count": 0,
            "marked_positions_count": marks["summary"]["open_positions"],
            "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
            "health_gate": health_gate,
        }
    if health_gate["blocked"]:
        marks = _json_load(
            shadow_settings.marks_path,
            {
                "updated_at": utc_now_iso(),
                "positions": [],
                "summary": {
                    "open_positions": len(_json_load(shadow_settings.positions_path, [])),
                    "total_unrealized_pnl_usdc": 0.0,
                    "total_mark_value_usdc": 0.0,
                },
            },
        )
        return {
            "strategy": "crypto_wallet_copy",
            "queue_count": 0,
            "trade_decisions": [],
            "exits": [],
            "opened_positions_count": 0,
            "scaled_positions_count": 0,
            "marked_positions_count": marks["summary"]["open_positions"],
            "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
            "health_gate": health_gate,
        }
    if not health_gate["blocked"]:
        ensure_targets(settings, cli)
        refresh_crypto_target_activity(settings, cli, refresh_base_activity=refresh_base_activity)
    return run_ab_wallet_copy_cycle(
        crypto_settings,
        cli,
        suffix="ab_crypto_wallet_copy",
        strategy_name="crypto_wallet_copy",
        min_confidence=settings.ab_test_crypto_min_whale_confidence,
        require_microstructure_alignment=settings.ab_test_crypto_require_microstructure_alignment,
        fixed_position_usdc=settings.ab_test_crypto_fixed_position_usdc,
        agent_name="crypto_wallet_copy_ab",
        activity_path=settings.crypto_activity_path,
        markets_limit_override=settings.ab_test_crypto_markets_limit,
        queue_limit_override=settings.ab_test_crypto_queue_max_candidates,
        prebook_candidate_limit_override=settings.ab_test_crypto_prebook_candidate_limit,
    )


def _market_open_lookup(positions: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    return {
        (str(item.get("market_id", "")), str(item.get("side", "")).upper()): item
        for item in positions
        if item.get("market_id") and item.get("side")
    }


def monitor_crypto_5m_sniper_exits(
    settings: Settings,
    cli: PolymarketCLI,
    *,
    hold_minutes: float,
    take_profit_contract_price: float,
) -> list[dict[str, Any]]:
    executor = _executor(settings, cli)
    positions = executor.load_positions()
    exits: list[dict[str, Any]] = []
    for position in positions:
        age_minutes = _age_minutes(str(position.get("opened_at", "")))
        book = _safe_book(cli, str(position.get("token_id", "")))
        shares = effective_position_shares(position)
        liquidation = _simulate_contract_sell(_book_contract_levels(book, str(position.get("side", "BUY")), "close"), shares)
        current_contract_price = liquidation.get("avg_contract_price", 0.0)
        if current_contract_price <= 0:
            continue
        current_yes_price = _contract_midpoint_to_yes_midpoint(str(position.get("side", "BUY")), current_contract_price)
        if current_contract_price >= take_profit_contract_price:
            closed = executor.close_position(position, current_yes_price, "SNIPER_TARGET_HIT", book=book)
            if closed is not None:
                exits.append(closed)
            continue
        if age_minutes >= hold_minutes:
            closed = executor.close_position(position, current_yes_price, "SNIPER_TIME_EXIT", book=book)
            if closed is not None:
                exits.append(closed)
    return exits


def _scheduled_intraday_exit_deadline(position: dict[str, Any], grace_minutes: float = 5.0) -> datetime | None:
    question = str(position.get("question", ""))
    opened_at = _normalize_iso(str(position.get("opened_at", ""))) or datetime.now(timezone.utc)
    window = _extract_intraday_question_window(question, now=opened_at)
    if window is None:
        return None
    _start, end = window
    return end + timedelta(minutes=max(grace_minutes, 0.0))


def monitor_crypto_intraday_scheduled_exits(
    settings: Settings,
    cli: PolymarketCLI,
    *,
    hold_minutes: float,
    take_profit_contract_price: float,
    window_exit_grace_minutes: float = 5.0,
) -> list[dict[str, Any]]:
    executor = _executor(settings, cli)
    positions = executor.load_positions()
    exits: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    for position in positions:
        age_minutes = _age_minutes(str(position.get("opened_at", "")))
        book = _safe_book(cli, str(position.get("token_id", "")))
        shares = effective_position_shares(position)
        liquidation = _simulate_contract_sell(_book_contract_levels(book, str(position.get("side", "BUY")), "close"), shares)
        current_contract_price = liquidation.get("avg_contract_price", 0.0)
        if current_contract_price <= 0:
            continue
        current_yes_price = _contract_midpoint_to_yes_midpoint(str(position.get("side", "BUY")), current_contract_price)
        if current_contract_price >= take_profit_contract_price:
            closed = executor.close_position(position, current_yes_price, "SCHEDULED_TARGET_HIT", book=book)
            if closed is not None:
                exits.append(closed)
            continue
        deadline = _scheduled_intraday_exit_deadline(position, grace_minutes=window_exit_grace_minutes)
        if deadline is not None:
            if now >= deadline:
                closed = executor.close_position(position, current_yes_price, "SCHEDULED_WINDOW_EXIT", book=book)
                if closed is not None:
                    exits.append(closed)
            continue
        if age_minutes >= hold_minutes:
            closed = executor.close_position(position, current_yes_price, "SNIPER_TIME_EXIT", book=book)
            if closed is not None:
                exits.append(closed)
    return exits


def monitor_crypto_5m_box_exits(
    settings: Settings,
    cli: PolymarketCLI,
    *,
    hold_minutes: float,
    capture_ratio: float,
) -> list[dict[str, Any]]:
    executor = _executor(settings, cli)
    positions = executor.load_positions()
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for position in positions:
        pair_id = str(position.get("pair_id", ""))
        if pair_id:
            grouped[pair_id].append(position)
    exits: list[dict[str, Any]] = []
    for pair_positions in grouped.values():
        if len(pair_positions) < 2:
            continue
        total_entry = round(sum(_as_float(item.get("notional_usdc"), 0.0) for item in pair_positions), 2)
        pair_shares = min(effective_position_shares(item) for item in pair_positions)
        locked_profit = round(max(pair_shares - total_entry, 0.0), 2)
        current_proceeds = 0.0
        close_args: list[tuple[dict[str, Any], float, dict[str, Any]]] = []
        for position in pair_positions:
            book = _safe_book(cli, str(position.get("token_id", "")))
            shares = effective_position_shares(position)
            liquidation = _simulate_contract_sell(_book_contract_levels(book, str(position.get("side", "BUY")), "close"), shares)
            current_contract_price = liquidation.get("avg_contract_price", 0.0)
            if current_contract_price <= 0 or liquidation.get("filled_shares", 0.0) <= 0:
                close_args = []
                break
            current_proceeds += _as_float(liquidation.get("proceeds_usdc"), 0.0)
            current_yes_price = _contract_midpoint_to_yes_midpoint(str(position.get("side", "BUY")), current_contract_price)
            close_args.append((position, current_yes_price, book))
        if len(close_args) != len(pair_positions):
            continue
        age_minutes = max((_age_minutes(str(item.get("opened_at", ""))) for item in pair_positions), default=0.0)
        capture_target = locked_profit * max(min(capture_ratio, 1.0), 0.0)
        reason = None
        if capture_target > 0 and current_proceeds >= (total_entry + capture_target):
            reason = "BOX_CAPTURE"
        elif age_minutes >= hold_minutes:
            reason = "BOX_TIME_EXIT"
        if not reason:
            continue
        for position, exit_yes_price, book in close_args:
            closed = executor.close_position(position, exit_yes_price, reason, book=book)
            if closed is not None:
                exits.append(closed)
    return exits


def run_crypto_5m_sniper_ab_cycle(settings: Settings, cli: PolymarketCLI) -> dict[str, Any]:
    intraday_cli = _fast_intraday_cli(cli)
    shadow_settings = _shadow_settings(settings, "ab_crypto_5m_sniper")
    exits = monitor_crypto_5m_sniper_exits(
        shadow_settings,
        intraday_cli,
        hold_minutes=settings.ab_test_crypto_5m_sniper_hold_minutes,
        take_profit_contract_price=settings.ab_test_crypto_5m_sniper_take_profit_contract_price,
    )
    health_gate = _strategy_health_gate(
        settings,
        trades_path=shadow_settings.trades_path,
        status_path=_runner_status_path("ab_crypto_5m_sniper"),
        strategy_name="crypto_5m_sniper",
    )
    seed_markets = _load_intraday_imminent_updown_markets(
        settings,
        max_minutes_to_resolution=settings.ab_test_crypto_5m_sniper_max_minutes_to_resolution,
        require_updown=False,
    ) or _load_intraday_raw_updown_markets(
        settings,
        max_minutes_to_resolution=settings.ab_test_crypto_5m_sniper_max_minutes_to_resolution,
        require_updown=False,
    )
    candidates = _scan_short_crypto_updown_candidates(
        intraday_cli,
        settings=settings,
        markets_limit=settings.ab_test_crypto_5m_sniper_markets_limit,
        max_minutes_to_resolution=settings.ab_test_crypto_5m_sniper_max_minutes_to_resolution,
        min_book_depth_usdc=settings.ab_test_crypto_5m_sniper_min_book_depth_usdc,
        require_updown=False,
        seed_markets=seed_markets,
        prebook_limit=16,
    )
    executor = _executor(shadow_settings, intraday_cli)
    existing_positions = executor.load_positions()
    open_lookup = _market_open_lookup(existing_positions)
    decisions: list[dict[str, Any]] = []
    opened_positions: list[dict[str, Any]] = []
    if health_gate["blocked"]:
        marks = mark_open_positions(shadow_settings, intraday_cli)
        return {
            "strategy": "crypto_5m_sniper",
            "queue_count": len(candidates),
            "trade_decisions": decisions,
            "exits": exits,
            "opened_positions_count": 0,
            "scaled_positions_count": 0,
            "marked_positions_count": marks["summary"]["open_positions"],
            "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
            "health_gate": health_gate,
        }
    for candidate in candidates[: max(settings.queue_max_candidates, 1)]:
        asset = str(candidate.raw_market.get("asset", ""))
        decision_base = {
            "market_id": candidate.market_id,
            "token_id": candidate.token_id,
            "question": candidate.question,
            "midpoint": candidate.midpoint,
            "priority_score": candidate.priority_score,
            "asset": asset,
        }
        vote = _short_crypto_sniper_vote(
            candidate,
            min_book_imbalance=settings.ab_test_crypto_5m_sniper_min_book_imbalance,
            max_side_price=settings.ab_test_crypto_5m_sniper_max_side_price,
        )
        if vote.action == "HOLD":
            decisions.append({**decision_base, "action": "SKIP", "reason": vote.rationale, "votes": [vote.to_dict()]})
            continue
        if (candidate.market_id, vote.action) in open_lookup:
            decisions.append({**decision_base, "action": "SKIP", "reason": "position already open for this market side", "votes": [vote.to_dict()]})
            continue
        remaining_bankroll = _remaining_bankroll_usdc(shadow_settings, executor)
        size = round(min(settings.ab_test_crypto_5m_sniper_fixed_position_usdc, remaining_bankroll, _position_cap_usdc(shadow_settings)), 2)
        if size <= 0:
            decisions.append({**decision_base, "action": "SKIP", "reason": "no remaining bankroll available", "votes": [vote.to_dict()]})
            continue
        book = _safe_book(intraday_cli, candidate.token_id)
        side_price = side_contract_price(vote.action, candidate.midpoint)
        opened = executor.open_position(
            candidate,
            vote.action,
            size,
            vote.confidence,
            max(0.02, abs(vote.estimated_probability - candidate.midpoint)),
            book=book,
            metadata={"strategy_tag": "crypto_5m_sniper", "asset": asset, "market_family": "crypto_5m_updown"},
        )
        if opened is None:
            decisions.append({**decision_base, "action": "SKIP", "reason": "insufficient executable depth for open", "votes": [vote.to_dict()]})
            continue
        decisions.append({**decision_base, "action": "OPEN", "trade": opened, "votes": [vote.to_dict()], "entry_contract_price": round(side_price, 6)})
        opened_positions.append(opened)
        open_lookup[(candidate.market_id, vote.action)] = opened
    marks = mark_open_positions(shadow_settings, intraday_cli)
    return {
        "strategy": "crypto_5m_sniper",
        "queue_count": len(candidates),
        "trade_decisions": decisions,
        "exits": exits,
        "opened_positions_count": len(opened_positions),
        "scaled_positions_count": 0,
        "marked_positions_count": marks["summary"]["open_positions"],
        "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
        "health_gate": health_gate,
    }


def run_crypto_5m_box_ab_cycle(settings: Settings, cli: PolymarketCLI) -> dict[str, Any]:
    intraday_cli = _fast_intraday_cli(cli)
    shadow_settings = _shadow_settings(settings, "ab_crypto_5m_box_arb")
    exits = monitor_crypto_5m_box_exits(
        shadow_settings,
        intraday_cli,
        hold_minutes=settings.ab_test_crypto_5m_box_hold_minutes,
        capture_ratio=settings.ab_test_crypto_5m_box_capture_ratio,
    )
    health_gate = _strategy_health_gate(
        settings,
        trades_path=shadow_settings.trades_path,
        status_path=_runner_status_path("ab_crypto_5m_box_arb"),
        strategy_name="crypto_5m_box_arb",
    )
    cached_seed_markets = _load_intraday_imminent_box_arb_tape_markets(
        settings,
        max_minutes_to_resolution=settings.ab_test_crypto_5m_box_max_minutes_to_resolution,
    ) or _load_intraday_book_tape_markets(
        settings,
        max_minutes_to_resolution=settings.ab_test_crypto_5m_box_max_minutes_to_resolution,
        require_updown=False,
    )
    if cached_seed_markets:
        candidates = []
        for market in cached_seed_markets[: settings.ab_test_crypto_5m_box_markets_limit]:
            candidate = _candidate_from_cached_short_market(market)
            if candidate is None:
                continue
            if not _asset_allowed_for_5m_box(settings, str(candidate.raw_market.get("asset", ""))):
                continue
            if min(candidate.bids_depth, candidate.asks_depth) < settings.ab_test_crypto_5m_box_min_book_depth_usdc:
                continue
            candidates.append(candidate)
    else:
        seed_markets = _load_intraday_imminent_updown_markets(
            settings,
            max_minutes_to_resolution=settings.ab_test_crypto_5m_box_max_minutes_to_resolution,
            require_updown=False,
        ) or _load_intraday_raw_updown_markets(
            settings,
            max_minutes_to_resolution=settings.ab_test_crypto_5m_box_max_minutes_to_resolution,
            require_updown=False,
        )
        candidates = _scan_short_crypto_updown_candidates(
            intraday_cli,
            settings=settings,
            markets_limit=settings.ab_test_crypto_5m_box_markets_limit,
            max_minutes_to_resolution=settings.ab_test_crypto_5m_box_max_minutes_to_resolution,
            min_book_depth_usdc=settings.ab_test_crypto_5m_box_min_book_depth_usdc,
            require_updown=False,
            seed_markets=seed_markets,
            prebook_limit=16,
        )
        candidates = [
            candidate
            for candidate in candidates
            if _asset_allowed_for_5m_box(settings, str(candidate.raw_market.get("asset", "")))
        ]
    executor = _executor(shadow_settings, intraday_cli)
    existing_positions = executor.load_positions()
    open_market_ids = {str(item.get("market_id", "")) for item in existing_positions if item.get("market_id")}
    hot_path_cached_only = bool(cached_seed_markets)
    decisions: list[dict[str, Any]] = []
    opened_pairs = 0
    gross_edges: list[float] = []
    net_edges: list[float] = []
    arb_tape = {
        "strategy": "crypto_5m_box_arb",
        "candidates_seen": len(candidates),
        "already_open_skips": 0,
        "no_budget_skips": 0,
        "depth_rejects": 0,
        "gross_edge_rejects": 0,
        "net_edge_rejects": 0,
        "gross_edge_eligible": 0,
        "net_edge_eligible": 0,
        "executable_pairs": 0,
        "executed_pairs": 0,
        "gross_edge_threshold": round(float(settings.ab_test_crypto_5m_box_min_edge_per_share), 6),
        "net_edge_threshold": round(float(settings.ab_test_crypto_5m_box_min_net_edge_per_share), 6),
        "estimated_fee_per_share": round(float(settings.ab_test_crypto_5m_box_estimated_fee_per_share), 6),
        "estimated_slippage_per_share": round(float(settings.ab_test_crypto_5m_box_estimated_slippage_per_share), 6),
    }
    if health_gate["blocked"]:
        marks = mark_open_positions(shadow_settings, intraday_cli)
        return {
            "strategy": "crypto_5m_box_arb",
            "queue_count": len(candidates),
            "trade_decisions": decisions,
            "exits": exits,
            "opened_positions_count": 0,
            "scaled_positions_count": 0,
            "marked_positions_count": marks["summary"]["open_positions"],
            "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
            "health_gate": health_gate,
            "arb_tape": arb_tape,
        }
    for candidate in candidates[: max(settings.queue_max_candidates, 1)]:
        asset = str(candidate.raw_market.get("asset", ""))
        decision_base = {
            "market_id": candidate.market_id,
            "token_id": candidate.token_id,
            "question": candidate.question,
            "midpoint": candidate.midpoint,
            "priority_score": candidate.priority_score,
            "asset": asset,
        }
        if candidate.market_id in open_market_ids:
            arb_tape["already_open_skips"] += 1
            decisions.append({**decision_base, "action": "SKIP", "reason": "paired position already open for market"})
            continue
        remaining_bankroll = _remaining_bankroll_usdc(shadow_settings, executor)
        pair_budget = round(min(settings.ab_test_crypto_5m_box_pair_budget_usdc, remaining_bankroll, _position_cap_usdc(shadow_settings)), 2)
        if pair_budget <= 0:
            arb_tape["no_budget_skips"] += 1
            decisions.append({**decision_base, "action": "SKIP", "reason": "no remaining bankroll available"})
            continue
        cached_book = _cached_book_from_market(candidate.raw_market)
        if hot_path_cached_only and not cached_book.get("bids") and not cached_book.get("asks"):
            decisions.append({**decision_base, "action": "SKIP", "reason": "missing cached book on hot path"})
            continue
        book = cached_book or _safe_book(intraday_cli, candidate.token_id)
        yes_levels = _book_contract_levels(book, "BUY", "open") or _fallback_contract_levels_from_candidate(candidate, "BUY", "open")
        no_levels = _book_contract_levels(book, "SELL", "open") or _fallback_contract_levels_from_candidate(candidate, "SELL", "open")
        execution = _simulate_box_pair_open(yes_levels, no_levels, pair_budget)
        metrics = _box_arb_edge_metrics(
            execution,
            estimated_fee_per_share=settings.ab_test_crypto_5m_box_estimated_fee_per_share,
            estimated_slippage_per_share=settings.ab_test_crypto_5m_box_estimated_slippage_per_share,
        )
        if execution["filled_shares"] <= 0:
            arb_tape["depth_rejects"] += 1
            decisions.append({**decision_base, "action": "SKIP", "reason": "insufficient executable depth for paired fill", **metrics})
            continue
        arb_tape["executable_pairs"] += 1
        gross_edges.append(metrics["gross_edge_per_share"])
        if metrics["gross_edge_per_share"] >= settings.ab_test_crypto_5m_box_min_edge_per_share:
            arb_tape["gross_edge_eligible"] += 1
        if metrics["gross_edge_per_share"] < settings.ab_test_crypto_5m_box_min_edge_per_share:
            arb_tape["gross_edge_rejects"] += 1
            decisions.append(
                {
                    **decision_base,
                    "action": "SKIP",
                    "reason": f"gross edge too small ({metrics['gross_edge_per_share']:.4f} < {settings.ab_test_crypto_5m_box_min_edge_per_share:.4f})",
                    **metrics,
                }
            )
            continue
        net_edges.append(metrics["net_edge_per_share"])
        if metrics["net_edge_per_share"] >= settings.ab_test_crypto_5m_box_min_net_edge_per_share:
            arb_tape["net_edge_eligible"] += 1
        if metrics["net_edge_per_share"] < settings.ab_test_crypto_5m_box_min_net_edge_per_share:
            arb_tape["net_edge_rejects"] += 1
            decisions.append(
                {
                    **decision_base,
                    "action": "SKIP",
                    "reason": f"net edge too small after costs ({metrics['net_edge_per_share']:.4f} < {settings.ab_test_crypto_5m_box_min_net_edge_per_share:.4f})",
                    **metrics,
                }
            )
            continue
        pair_id = uuid4().hex
        metadata = {
            "strategy_tag": "crypto_5m_box_arb",
            "asset": asset,
            "pair_id": pair_id,
            "market_family": "crypto_5m_updown",
        }
        buy_open = executor.open_position_exact(
            candidate,
            "BUY",
            shares=execution["filled_shares"],
            entry_contract_price=execution["avg_yes_contract_price"],
            confidence=1.0,
            expected_gap=execution["edge_per_share"],
            metadata=metadata,
        )
        sell_open = executor.open_position_exact(
            candidate,
            "SELL",
            shares=execution["filled_shares"],
            entry_contract_price=execution["avg_no_contract_price"],
            confidence=1.0,
            expected_gap=execution["edge_per_share"],
            metadata=metadata,
        )
        if buy_open is None or sell_open is None:
            decisions.append({**decision_base, "action": "SKIP", "reason": "paired execution failed"})
            continue
        decisions.append(
            {
                **decision_base,
                "action": "OPEN",
                "pair_id": pair_id,
                "trade": {"buy": buy_open, "sell": sell_open},
                "locked_edge_per_share": metrics["net_edge_per_share"],
                **metrics,
                "filled_shares": execution["filled_shares"],
                "combined_contract_price": execution["combined_contract_price"],
            }
        )
        opened_pairs += 1
        arb_tape["executed_pairs"] += 1
        open_market_ids.add(candidate.market_id)
    marks = mark_open_positions(shadow_settings, intraday_cli)
    arb_tape["avg_gross_edge_per_share"] = round(sum(gross_edges) / len(gross_edges), 6) if gross_edges else 0.0
    arb_tape["best_gross_edge_per_share"] = round(max(gross_edges), 6) if gross_edges else 0.0
    arb_tape["avg_net_edge_per_share"] = round(sum(net_edges) / len(net_edges), 6) if net_edges else 0.0
    arb_tape["best_net_edge_per_share"] = round(max(net_edges), 6) if net_edges else 0.0
    return {
        "strategy": "crypto_5m_box_arb",
        "queue_count": len(candidates),
        "trade_decisions": decisions,
        "exits": exits,
        "opened_positions_count": opened_pairs * 2,
        "scaled_positions_count": 0,
        "marked_positions_count": marks["summary"]["open_positions"],
        "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
        "health_gate": health_gate,
        "arb_tape": arb_tape,
    }


def run_crypto_next_window_sniper_ab_cycle(settings: Settings, cli: PolymarketCLI) -> dict[str, Any]:
    intraday_cli = _fast_intraday_cli(cli)
    shadow_settings = _shadow_settings(settings, "ab_crypto_next_window_sniper")
    exits = monitor_crypto_intraday_scheduled_exits(
        shadow_settings,
        intraday_cli,
        hold_minutes=settings.ab_test_crypto_next_window_sniper_hold_minutes,
        take_profit_contract_price=settings.ab_test_crypto_next_window_sniper_take_profit_contract_price,
    )
    health_gate = _strategy_health_gate(
        settings,
        trades_path=shadow_settings.trades_path,
        status_path=_runner_status_path("ab_crypto_next_window_sniper"),
        strategy_name="crypto_next_window_sniper",
    )
    seed_markets = _dedupe_markets_by_id(
        _load_intraday_registry_watchlist_markets(
            settings,
            min_minutes_to_resolution=settings.ab_test_crypto_next_window_sniper_min_minutes_to_resolution,
            max_minutes_to_resolution=settings.ab_test_crypto_next_window_sniper_max_minutes_to_resolution,
            require_updown=True,
        )
        + _load_intraday_raw_updown_markets(
            settings,
            min_minutes_to_resolution=settings.ab_test_crypto_next_window_sniper_min_minutes_to_resolution,
            max_minutes_to_resolution=settings.ab_test_crypto_next_window_sniper_max_minutes_to_resolution,
            require_updown=True,
        )
    )
    candidates = _scan_short_crypto_updown_candidates(
        intraday_cli,
        settings=settings,
        markets_limit=settings.ab_test_crypto_next_window_sniper_markets_limit,
        min_minutes_to_resolution=settings.ab_test_crypto_next_window_sniper_min_minutes_to_resolution,
        max_minutes_to_resolution=settings.ab_test_crypto_next_window_sniper_max_minutes_to_resolution,
        min_book_depth_usdc=settings.ab_test_crypto_next_window_sniper_min_book_depth_usdc,
        require_updown=True,
        seed_markets=seed_markets,
        prebook_limit=12,
    )
    executor = _executor(shadow_settings, intraday_cli)
    existing_positions = executor.load_positions()
    open_lookup = _market_open_lookup(existing_positions)
    decisions: list[dict[str, Any]] = []
    opened_positions: list[dict[str, Any]] = []
    probation_max_open = max(int(settings.strategy_probation_max_open_positions), 0) if health_gate.get("probation_allowed") else 0
    probation_size_multiplier = max(min(float(settings.strategy_probation_size_multiplier), 1.0), 0.0) if health_gate.get("probation_allowed") else 1.0
    if health_gate["blocked"]:
        marks = mark_open_positions(shadow_settings, intraday_cli)
        return {
            "strategy": "crypto_next_window_sniper",
            "queue_count": len(candidates),
            "trade_decisions": decisions,
            "exits": exits,
            "opened_positions_count": 0,
            "scaled_positions_count": 0,
            "marked_positions_count": marks["summary"]["open_positions"],
            "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
            "health_gate": health_gate,
        }
    for candidate in candidates[: max(settings.queue_max_candidates, 1)]:
        asset = str(candidate.raw_market.get("asset", ""))
        decision_base = {
            "market_id": candidate.market_id,
            "token_id": candidate.token_id,
            "question": candidate.question,
            "midpoint": candidate.midpoint,
            "priority_score": candidate.priority_score,
            "asset": asset,
        }
        vote = _scheduled_intraday_crypto_vote(
            candidate,
            strategy_name="crypto_next_window_sniper",
            min_book_imbalance=settings.ab_test_crypto_next_window_sniper_min_book_imbalance,
            min_midpoint_edge=settings.ab_test_crypto_next_window_sniper_min_midpoint_edge,
            max_side_price=settings.ab_test_crypto_next_window_sniper_max_side_price,
            max_hours_to_resolution=settings.ab_test_crypto_next_window_sniper_max_minutes_to_resolution / 60.0,
        )
        if vote.action == "HOLD":
            decisions.append({**decision_base, "action": "SKIP", "reason": vote.rationale, "votes": [vote.to_dict()]})
            continue
        if (candidate.market_id, vote.action) in open_lookup:
            decisions.append({**decision_base, "action": "SKIP", "reason": "position already open for this market side", "votes": [vote.to_dict()]})
            continue
        if probation_max_open > 0 and len(existing_positions) + len(opened_positions) >= probation_max_open:
            decisions.append({**decision_base, "action": "SKIP", "reason": f"probation max open positions reached ({probation_max_open})", "votes": [vote.to_dict()]})
            continue
        remaining_bankroll = _remaining_bankroll_usdc(shadow_settings, executor)
        size = round(
            min(
                settings.ab_test_crypto_next_window_sniper_fixed_position_usdc * probation_size_multiplier,
                remaining_bankroll,
                _position_cap_usdc(shadow_settings),
            ),
            2,
        )
        if size <= 0:
            decisions.append({**decision_base, "action": "SKIP", "reason": "no remaining bankroll available", "votes": [vote.to_dict()]})
            continue
        book = _safe_book(intraday_cli, candidate.token_id)
        side_price = side_contract_price(vote.action, candidate.midpoint)
        opened = executor.open_position(
            candidate,
            vote.action,
            size,
            vote.confidence,
            max(0.02, abs(vote.estimated_probability - candidate.midpoint)),
            book=book,
            metadata={"strategy_tag": "crypto_next_window_sniper", "asset": asset, "market_family": "crypto_next_window_updown"},
        )
        if opened is None:
            decisions.append({**decision_base, "action": "SKIP", "reason": "insufficient executable depth for open", "votes": [vote.to_dict()]})
            continue
        decisions.append({**decision_base, "action": "OPEN", "trade": opened, "votes": [vote.to_dict()], "entry_contract_price": round(side_price, 6)})
        opened_positions.append(opened)
        open_lookup[(candidate.market_id, vote.action)] = opened
    marks = mark_open_positions(shadow_settings, intraday_cli)
    return {
        "strategy": "crypto_next_window_sniper",
        "queue_count": len(candidates),
        "trade_decisions": decisions,
        "exits": exits,
        "opened_positions_count": len(opened_positions),
        "scaled_positions_count": 0,
        "marked_positions_count": marks["summary"]["open_positions"],
        "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
        "health_gate": health_gate,
    }


def run_crypto_next_window_box_ab_cycle(settings: Settings, cli: PolymarketCLI) -> dict[str, Any]:
    intraday_cli = _fast_intraday_cli(cli)
    shadow_settings = _shadow_settings(settings, "ab_crypto_next_window_box_arb")
    exits = monitor_crypto_5m_box_exits(
        shadow_settings,
        intraday_cli,
        hold_minutes=settings.ab_test_crypto_next_window_box_hold_minutes,
        capture_ratio=settings.ab_test_crypto_next_window_box_capture_ratio,
    )
    health_gate = _strategy_health_gate(
        settings,
        trades_path=shadow_settings.trades_path,
        status_path=_runner_status_path("ab_crypto_next_window_box_arb"),
        strategy_name="crypto_next_window_box_arb",
    )
    seed_markets = _dedupe_markets_by_id(
        _load_intraday_registry_watchlist_markets(
            settings,
            min_minutes_to_resolution=settings.ab_test_crypto_next_window_box_min_minutes_to_resolution,
            max_minutes_to_resolution=settings.ab_test_crypto_next_window_box_max_minutes_to_resolution,
            require_updown=True,
        )
        + _load_intraday_raw_updown_markets(
            settings,
            min_minutes_to_resolution=settings.ab_test_crypto_next_window_box_min_minutes_to_resolution,
            max_minutes_to_resolution=settings.ab_test_crypto_next_window_box_max_minutes_to_resolution,
            require_updown=True,
        )
    )
    candidates = _scan_short_crypto_updown_candidates(
        intraday_cli,
        settings=settings,
        markets_limit=settings.ab_test_crypto_next_window_box_markets_limit,
        min_minutes_to_resolution=settings.ab_test_crypto_next_window_box_min_minutes_to_resolution,
        max_minutes_to_resolution=settings.ab_test_crypto_next_window_box_max_minutes_to_resolution,
        min_book_depth_usdc=settings.ab_test_crypto_next_window_box_min_book_depth_usdc,
        require_updown=True,
        seed_markets=seed_markets,
        prebook_limit=12,
    )
    executor = _executor(shadow_settings, intraday_cli)
    existing_positions = executor.load_positions()
    open_market_ids = {str(item.get("market_id", "")) for item in existing_positions if item.get("market_id")}
    decisions: list[dict[str, Any]] = []
    opened_pairs = 0
    gross_edges: list[float] = []
    net_edges: list[float] = []
    arb_tape = {
        "strategy": "crypto_next_window_box_arb",
        "candidates_seen": len(candidates),
        "already_open_skips": 0,
        "no_budget_skips": 0,
        "depth_rejects": 0,
        "gross_edge_rejects": 0,
        "net_edge_rejects": 0,
        "gross_edge_eligible": 0,
        "net_edge_eligible": 0,
        "executable_pairs": 0,
        "executed_pairs": 0,
        "gross_edge_threshold": round(float(settings.ab_test_crypto_next_window_box_min_edge_per_share), 6),
        "net_edge_threshold": round(float(settings.ab_test_crypto_next_window_box_min_net_edge_per_share), 6),
        "estimated_fee_per_share": round(float(settings.ab_test_crypto_next_window_box_estimated_fee_per_share), 6),
        "estimated_slippage_per_share": round(float(settings.ab_test_crypto_next_window_box_estimated_slippage_per_share), 6),
    }
    if health_gate["blocked"]:
        marks = mark_open_positions(shadow_settings, intraday_cli)
        return {
            "strategy": "crypto_next_window_box_arb",
            "queue_count": len(candidates),
            "trade_decisions": decisions,
            "exits": exits,
            "opened_positions_count": 0,
            "scaled_positions_count": 0,
            "marked_positions_count": marks["summary"]["open_positions"],
            "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
            "health_gate": health_gate,
            "arb_tape": arb_tape,
        }
    for candidate in candidates[: max(settings.queue_max_candidates, 1)]:
        asset = str(candidate.raw_market.get("asset", ""))
        decision_base = {
            "market_id": candidate.market_id,
            "token_id": candidate.token_id,
            "question": candidate.question,
            "midpoint": candidate.midpoint,
            "priority_score": candidate.priority_score,
            "asset": asset,
        }
        if candidate.market_id in open_market_ids:
            arb_tape["already_open_skips"] += 1
            decisions.append({**decision_base, "action": "SKIP", "reason": "paired position already open for market"})
            continue
        remaining_bankroll = _remaining_bankroll_usdc(shadow_settings, executor)
        pair_budget = round(min(settings.ab_test_crypto_next_window_box_pair_budget_usdc, remaining_bankroll, _position_cap_usdc(shadow_settings)), 2)
        if pair_budget <= 0:
            arb_tape["no_budget_skips"] += 1
            decisions.append({**decision_base, "action": "SKIP", "reason": "no remaining bankroll available"})
            continue
        book = _safe_book(intraday_cli, candidate.token_id)
        yes_levels = _book_contract_levels(book, "BUY", "open") or _fallback_contract_levels_from_candidate(candidate, "BUY", "open")
        no_levels = _book_contract_levels(book, "SELL", "open") or _fallback_contract_levels_from_candidate(candidate, "SELL", "open")
        execution = _simulate_box_pair_open(yes_levels, no_levels, pair_budget)
        metrics = _box_arb_edge_metrics(
            execution,
            estimated_fee_per_share=settings.ab_test_crypto_next_window_box_estimated_fee_per_share,
            estimated_slippage_per_share=settings.ab_test_crypto_next_window_box_estimated_slippage_per_share,
        )
        if execution["filled_shares"] <= 0:
            arb_tape["depth_rejects"] += 1
            decisions.append({**decision_base, "action": "SKIP", "reason": "insufficient executable depth for paired fill", **metrics})
            continue
        arb_tape["executable_pairs"] += 1
        gross_edges.append(metrics["gross_edge_per_share"])
        if metrics["gross_edge_per_share"] >= settings.ab_test_crypto_next_window_box_min_edge_per_share:
            arb_tape["gross_edge_eligible"] += 1
        if metrics["gross_edge_per_share"] < settings.ab_test_crypto_next_window_box_min_edge_per_share:
            arb_tape["gross_edge_rejects"] += 1
            decisions.append(
                {
                    **decision_base,
                    "action": "SKIP",
                    "reason": f"gross edge too small ({metrics['gross_edge_per_share']:.4f} < {settings.ab_test_crypto_next_window_box_min_edge_per_share:.4f})",
                    **metrics,
                }
            )
            continue
        net_edges.append(metrics["net_edge_per_share"])
        if metrics["net_edge_per_share"] >= settings.ab_test_crypto_next_window_box_min_net_edge_per_share:
            arb_tape["net_edge_eligible"] += 1
        if metrics["net_edge_per_share"] < settings.ab_test_crypto_next_window_box_min_net_edge_per_share:
            arb_tape["net_edge_rejects"] += 1
            decisions.append(
                {
                    **decision_base,
                    "action": "SKIP",
                    "reason": f"net edge too small after costs ({metrics['net_edge_per_share']:.4f} < {settings.ab_test_crypto_next_window_box_min_net_edge_per_share:.4f})",
                    **metrics,
                }
            )
            continue
        pair_id = uuid4().hex
        metadata = {
            "strategy_tag": "crypto_next_window_box_arb",
            "asset": asset,
            "pair_id": pair_id,
            "market_family": "crypto_next_window_updown",
        }
        buy_open = executor.open_position_exact(
            candidate,
            "BUY",
            shares=execution["filled_shares"],
            entry_contract_price=execution["avg_yes_contract_price"],
            confidence=1.0,
            expected_gap=execution["edge_per_share"],
            metadata=metadata,
        )
        sell_open = executor.open_position_exact(
            candidate,
            "SELL",
            shares=execution["filled_shares"],
            entry_contract_price=execution["avg_no_contract_price"],
            confidence=1.0,
            expected_gap=execution["edge_per_share"],
            metadata=metadata,
        )
        if buy_open is None or sell_open is None:
            decisions.append({**decision_base, "action": "SKIP", "reason": "paired execution failed"})
            continue
        decisions.append(
            {
                **decision_base,
                "action": "OPEN",
                "pair_id": pair_id,
                "trade": {"buy": buy_open, "sell": sell_open},
                "locked_edge_per_share": metrics["net_edge_per_share"],
                **metrics,
                "filled_shares": execution["filled_shares"],
                "combined_contract_price": execution["combined_contract_price"],
            }
        )
        opened_pairs += 1
        arb_tape["executed_pairs"] += 1
        open_market_ids.add(candidate.market_id)
    marks = mark_open_positions(shadow_settings, intraday_cli)
    arb_tape["avg_gross_edge_per_share"] = round(sum(gross_edges) / len(gross_edges), 6) if gross_edges else 0.0
    arb_tape["best_gross_edge_per_share"] = round(max(gross_edges), 6) if gross_edges else 0.0
    arb_tape["avg_net_edge_per_share"] = round(sum(net_edges) / len(net_edges), 6) if net_edges else 0.0
    arb_tape["best_net_edge_per_share"] = round(max(net_edges), 6) if net_edges else 0.0
    return {
        "strategy": "crypto_next_window_box_arb",
        "queue_count": len(candidates),
        "trade_decisions": decisions,
        "exits": exits,
        "opened_positions_count": opened_pairs * 2,
        "scaled_positions_count": 0,
        "marked_positions_count": marks["summary"]["open_positions"],
        "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
        "health_gate": health_gate,
        "arb_tape": arb_tape,
    }


def run_crypto_intraday_scheduled_ab_cycle(settings: Settings, cli: PolymarketCLI) -> dict[str, Any]:
    intraday_cli = _fast_intraday_cli(cli)
    shadow_settings = _shadow_settings(settings, "ab_crypto_intraday_scheduled")
    exits = monitor_crypto_intraday_scheduled_exits(
        shadow_settings,
        intraday_cli,
        hold_minutes=settings.ab_test_crypto_intraday_scheduled_hold_minutes,
        take_profit_contract_price=settings.ab_test_crypto_intraday_scheduled_take_profit_contract_price,
    )
    health_gate = _strategy_health_gate(
        settings,
        trades_path=shadow_settings.trades_path,
        status_path=_runner_status_path("ab_crypto_intraday_scheduled"),
        strategy_name="crypto_intraday_scheduled",
    )
    seed_markets = _load_intraday_raw_updown_markets(
        settings,
        max_minutes_to_resolution=settings.ab_test_crypto_intraday_scheduled_max_minutes_to_resolution,
        require_updown=True,
    )
    candidates = _scan_short_crypto_updown_candidates(
        intraday_cli,
        settings=settings,
        markets_limit=settings.ab_test_crypto_intraday_scheduled_markets_limit,
        max_minutes_to_resolution=settings.ab_test_crypto_intraday_scheduled_max_minutes_to_resolution,
        min_book_depth_usdc=settings.ab_test_crypto_intraday_scheduled_min_book_depth_usdc,
        require_updown=True,
        seed_markets=seed_markets,
        prebook_limit=12,
    )
    executor = _executor(shadow_settings, intraday_cli)
    existing_positions = executor.load_positions()
    open_lookup = _market_open_lookup(existing_positions)
    decisions: list[dict[str, Any]] = []
    opened_positions: list[dict[str, Any]] = []
    probation_max_open = max(int(settings.strategy_probation_max_open_positions), 0) if health_gate.get("probation_allowed") else 0
    probation_size_multiplier = max(min(float(settings.strategy_probation_size_multiplier), 1.0), 0.0) if health_gate.get("probation_allowed") else 1.0
    if health_gate["blocked"]:
        marks = mark_open_positions(shadow_settings, intraday_cli)
        return {
            "strategy": "crypto_intraday_scheduled",
            "queue_count": len(candidates),
            "trade_decisions": decisions,
            "exits": exits,
            "opened_positions_count": 0,
            "scaled_positions_count": 0,
            "marked_positions_count": marks["summary"]["open_positions"],
            "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
            "health_gate": health_gate,
        }
    for candidate in candidates[: max(settings.queue_max_candidates, 1)]:
        asset = str(candidate.raw_market.get("asset", ""))
        decision_base = {
            "market_id": candidate.market_id,
            "token_id": candidate.token_id,
            "question": candidate.question,
            "midpoint": candidate.midpoint,
            "priority_score": candidate.priority_score,
            "asset": asset,
        }
        vote = _scheduled_intraday_crypto_vote(
            candidate,
            min_book_imbalance=settings.ab_test_crypto_intraday_scheduled_min_book_imbalance,
            min_midpoint_edge=settings.ab_test_crypto_intraday_scheduled_min_midpoint_edge,
            max_side_price=settings.ab_test_crypto_intraday_scheduled_max_side_price,
            max_hours_to_resolution=settings.ab_test_crypto_intraday_scheduled_max_minutes_to_resolution / 60.0,
        )
        if vote.action == "HOLD":
            decisions.append({**decision_base, "action": "SKIP", "reason": vote.rationale, "votes": [vote.to_dict()]})
            continue
        if (candidate.market_id, vote.action) in open_lookup:
            decisions.append({**decision_base, "action": "SKIP", "reason": "position already open for this market side", "votes": [vote.to_dict()]})
            continue
        if probation_max_open > 0 and len(existing_positions) + len(opened_positions) >= probation_max_open:
            decisions.append({**decision_base, "action": "SKIP", "reason": f"probation max open positions reached ({probation_max_open})", "votes": [vote.to_dict()]})
            continue
        remaining_bankroll = _remaining_bankroll_usdc(shadow_settings, executor)
        size = round(
            min(
                settings.ab_test_crypto_intraday_scheduled_fixed_position_usdc * probation_size_multiplier,
                remaining_bankroll,
                _position_cap_usdc(shadow_settings),
            ),
            2,
        )
        if size <= 0:
            decisions.append({**decision_base, "action": "SKIP", "reason": "no remaining bankroll available", "votes": [vote.to_dict()]})
            continue
        book = _safe_book(intraday_cli, candidate.token_id)
        side_price = side_contract_price(vote.action, candidate.midpoint)
        opened = executor.open_position(
            candidate,
            vote.action,
            size,
            vote.confidence,
            max(0.02, abs(vote.estimated_probability - candidate.midpoint)),
            book=book,
            metadata={"strategy_tag": "crypto_intraday_scheduled", "asset": asset, "market_family": "crypto_scheduled_updown"},
        )
        if opened is None:
            decisions.append({**decision_base, "action": "SKIP", "reason": "insufficient executable depth for open", "votes": [vote.to_dict()]})
            continue
        decisions.append({**decision_base, "action": "OPEN", "trade": opened, "votes": [vote.to_dict()], "entry_contract_price": round(side_price, 6)})
        opened_positions.append(opened)
        open_lookup[(candidate.market_id, vote.action)] = opened
    marks = mark_open_positions(shadow_settings, intraday_cli)
    return {
        "strategy": "crypto_intraday_scheduled",
        "queue_count": len(candidates),
        "trade_decisions": decisions,
        "exits": exits,
        "opened_positions_count": len(opened_positions),
        "scaled_positions_count": 0,
        "marked_positions_count": marks["summary"]["open_positions"],
        "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
        "health_gate": health_gate,
    }


def run_crypto_threshold_snapshot_ab_cycle(settings: Settings, cli: PolymarketCLI) -> dict[str, Any]:
    intraday_cli = _fast_intraday_cli(cli)
    shadow_settings = _shadow_settings(settings, "ab_crypto_threshold_snapshot")
    threshold_live_markets = _load_intraday_threshold_live_markets(
        settings,
        max_minutes_to_resolution=settings.ab_test_crypto_threshold_snapshot_max_minutes_to_resolution,
    )
    taped_markets = _load_intraday_book_tape_markets(
        settings,
        min_minutes_to_resolution=0,
        max_minutes_to_resolution=settings.ab_test_crypto_threshold_snapshot_max_minutes_to_resolution,
        require_updown=False,
    )
    watchlist_markets = _load_intraday_registry_watchlist_markets(
        settings,
        min_minutes_to_resolution=0,
        max_minutes_to_resolution=settings.ab_test_crypto_threshold_snapshot_max_minutes_to_resolution,
        require_updown=False,
    )
    registry_markets = _load_intraday_registry_markets(
        settings,
        min_minutes_to_resolution=0,
        max_minutes_to_resolution=settings.ab_test_crypto_threshold_snapshot_max_minutes_to_resolution,
        require_updown=False,
    )
    live_seed_markets = _dedupe_markets_by_id(
        threshold_live_markets + taped_markets + watchlist_markets + registry_markets
    )
    exits = monitor_crypto_5m_sniper_exits(
        shadow_settings,
        intraday_cli,
        hold_minutes=settings.ab_test_crypto_threshold_snapshot_hold_minutes,
        take_profit_contract_price=settings.ab_test_crypto_threshold_snapshot_take_profit_contract_price,
    )
    stale_exits = _force_close_stale_threshold_positions(
        shadow_settings,
        intraday_cli,
        hold_minutes=settings.ab_test_crypto_threshold_snapshot_hold_minutes,
        live_market_ids={_extract_market_id(market) for market in live_seed_markets if _extract_market_id(market)},
    )
    if stale_exits:
        exits.extend(stale_exits)
    health_gate = _strategy_health_gate(
        settings,
        trades_path=shadow_settings.trades_path,
        status_path=_runner_status_path("ab_crypto_threshold_snapshot"),
        strategy_name="crypto_threshold_snapshot",
    )
    candidates = []
    for market in live_seed_markets[: settings.ab_test_crypto_threshold_snapshot_markets_limit]:
        candidate = _candidate_from_cached_short_market(market)
        if candidate is None:
            continue
        if _is_five_minute_updown_market(candidate.question, candidate.slug):
            continue
        if min(candidate.bids_depth, candidate.asks_depth) < settings.ab_test_crypto_threshold_snapshot_min_book_depth_usdc:
            continue
        candidates.append(candidate)
    if not candidates:
        scan_seed_markets = _dedupe_markets_by_id(watchlist_markets + registry_markets + taped_markets + threshold_live_markets)
        candidates = _scan_short_crypto_updown_candidates(
            intraday_cli,
            settings=settings,
            markets_limit=settings.ab_test_crypto_threshold_snapshot_markets_limit,
            max_minutes_to_resolution=settings.ab_test_crypto_threshold_snapshot_max_minutes_to_resolution,
            min_book_depth_usdc=settings.ab_test_crypto_threshold_snapshot_min_book_depth_usdc,
            require_updown=False,
            seed_markets=scan_seed_markets,
            prebook_limit=16,
        )
        candidates = [candidate for candidate in candidates if not _is_five_minute_updown_market(candidate.question, candidate.slug)]
    executor = _executor(shadow_settings, intraday_cli)
    existing_positions = executor.load_positions()
    open_lookup = _market_open_lookup(existing_positions)
    decisions: list[dict[str, Any]] = []
    opened_positions: list[dict[str, Any]] = []
    probation_max_open = max(int(settings.strategy_probation_max_open_positions), 0) if health_gate.get("probation_allowed") else 0
    probation_size_multiplier = max(min(float(settings.strategy_probation_size_multiplier), 1.0), 0.0) if health_gate.get("probation_allowed") else 1.0
    if health_gate["blocked"]:
        marks = mark_open_positions(shadow_settings, intraday_cli)
        return {
            "strategy": "crypto_threshold_snapshot",
            "queue_count": len(candidates),
            "trade_decisions": decisions,
            "exits": exits,
            "opened_positions_count": 0,
            "scaled_positions_count": 0,
            "marked_positions_count": marks["summary"]["open_positions"],
            "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
            "health_gate": health_gate,
        }
    for candidate in candidates[: max(settings.queue_max_candidates, 1)]:
        asset = str(candidate.raw_market.get("asset", ""))
        decision_base = {
            "market_id": candidate.market_id,
            "token_id": candidate.token_id,
            "question": candidate.question,
            "midpoint": candidate.midpoint,
            "priority_score": candidate.priority_score,
            "asset": asset,
        }
        vote = _threshold_snapshot_crypto_vote(
            candidate,
            min_book_imbalance=settings.ab_test_crypto_threshold_snapshot_min_book_imbalance,
            min_midpoint_edge=settings.ab_test_crypto_threshold_snapshot_min_midpoint_edge,
            max_spread=settings.ab_test_crypto_threshold_snapshot_hard_max_spread,
            max_side_price=settings.ab_test_crypto_threshold_snapshot_max_side_price,
        )
        if vote.action == "HOLD":
            decisions.append({**decision_base, "action": "SKIP", "reason": vote.rationale, "votes": [vote.to_dict()]})
            continue
        if (candidate.market_id, vote.action) in open_lookup:
            decisions.append({**decision_base, "action": "SKIP", "reason": "position already open for this market side", "votes": [vote.to_dict()]})
            continue
        if probation_max_open > 0 and len(existing_positions) + len(opened_positions) >= probation_max_open:
            decisions.append({**decision_base, "action": "SKIP", "reason": f"probation max open positions reached ({probation_max_open})", "votes": [vote.to_dict()]})
            continue
        remaining_bankroll = _remaining_bankroll_usdc(shadow_settings, executor)
        base_size = round(
            min(
                settings.ab_test_crypto_threshold_snapshot_fixed_position_usdc * probation_size_multiplier,
                remaining_bankroll,
                _position_cap_usdc(shadow_settings),
            ),
            2,
        )
        spread_size_multiplier = _threshold_snapshot_spread_size_multiplier(settings, candidate.spread)
        size = round(base_size * spread_size_multiplier, 2)
        if size <= 0:
            reason = "no remaining bankroll available" if base_size <= 0 else "spread-adjusted size too small"
            decisions.append({**decision_base, "action": "SKIP", "reason": reason, "votes": [vote.to_dict()]})
            continue
        book = _cached_book_from_market(candidate.raw_market) or _safe_book(intraday_cli, candidate.token_id)
        side_price = side_contract_price(vote.action, candidate.midpoint)
        opened = executor.open_position(
            candidate,
            vote.action,
            size,
            vote.confidence,
            max(0.02, abs(vote.estimated_probability - candidate.midpoint)),
            book=book,
            metadata={"strategy_tag": "crypto_threshold_snapshot", "asset": asset, "market_family": "crypto_threshold_snapshot"},
        )
        if opened is None:
            decisions.append({**decision_base, "action": "SKIP", "reason": "insufficient executable depth for open", "votes": [vote.to_dict()]})
            continue
        decisions.append(
            {
                **decision_base,
                "action": "OPEN",
                "trade": opened,
                "votes": [vote.to_dict()],
                "entry_contract_price": round(side_price, 6),
                "spread_size_multiplier": round(spread_size_multiplier, 4),
                "base_size_usdc": round(base_size, 2),
                "final_size_usdc": round(size, 2),
            }
        )
        opened_positions.append(opened)
        open_lookup[(candidate.market_id, vote.action)] = opened
    marks = mark_open_positions(shadow_settings, intraday_cli)
    return {
        "strategy": "crypto_threshold_snapshot",
        "queue_count": len(candidates),
        "trade_decisions": decisions,
        "exits": exits,
        "opened_positions_count": len(opened_positions),
        "scaled_positions_count": 0,
        "marked_positions_count": marks["summary"]["open_positions"],
        "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
        "health_gate": health_gate,
    }


def run_sports_early_entry_ab_cycle(settings: Settings, cli: PolymarketCLI) -> dict[str, Any]:
    shadow_cli = _fast_intraday_cli(cli)
    candidates = _scan_filtered_shadow_candidates(
        shadow_cli,
        markets_limit=settings.ab_test_sports_early_entry_markets_limit,
        allowed_categories={"sports"},
        min_hours_to_resolution=float(settings.ab_test_sports_early_entry_min_hours_to_resolution),
        max_hours_to_resolution=float(settings.ab_test_sports_early_entry_max_hours_to_resolution),
        min_book_depth_usdc=settings.ab_test_sports_early_entry_min_book_depth_usdc,
        min_midpoint=settings.ab_test_sports_early_entry_min_midpoint,
        max_midpoint=settings.ab_test_sports_early_entry_max_midpoint,
        prebook_limit=16,
    )
    return _run_simple_shadow_cycle(
        settings,
        shadow_cli,
        suffix="ab_sports_early_entry",
        strategy_name="sports_early_entry",
        hold_minutes=settings.ab_test_sports_early_entry_hold_minutes,
        take_profit_contract_price=settings.ab_test_sports_early_entry_take_profit_contract_price,
        candidates=candidates,
        fixed_position_usdc=settings.ab_test_sports_early_entry_fixed_position_usdc,
        vote_fn=_sports_early_entry_vote,
        metadata_family="sports_early_entry",
    )


def run_penny_longshot_ab_cycle(settings: Settings, cli: PolymarketCLI) -> dict[str, Any]:
    shadow_cli = _fast_intraday_cli(cli)
    candidates = _scan_filtered_shadow_candidates(
        shadow_cli,
        markets_limit=settings.ab_test_penny_longshot_markets_limit,
        min_hours_to_resolution=float(settings.ab_test_penny_longshot_min_hours_to_resolution),
        max_hours_to_resolution=float(settings.ab_test_penny_longshot_max_hours_to_resolution),
        min_book_depth_usdc=settings.ab_test_penny_longshot_min_book_depth_usdc,
        min_midpoint=0.01,
        max_midpoint=settings.ab_test_penny_longshot_max_midpoint,
        prebook_limit=24,
    )
    return _run_simple_shadow_cycle(
        settings,
        shadow_cli,
        suffix="ab_penny_longshot",
        strategy_name="penny_longshot",
        hold_minutes=settings.ab_test_penny_longshot_hold_minutes,
        take_profit_contract_price=settings.ab_test_penny_longshot_take_profit_contract_price,
        candidates=candidates,
        fixed_position_usdc=settings.ab_test_penny_longshot_fixed_position_usdc,
        vote_fn=_penny_longshot_vote,
        metadata_family="penny_longshot",
        max_open_positions=settings.ab_test_penny_longshot_max_open_positions,
    )


def run_crypto_latency_5m_ab_cycle(settings: Settings, cli: PolymarketCLI) -> dict[str, Any]:
    intraday_cli = _fast_intraday_cli(cli)
    shadow_settings = _shadow_settings(settings, "ab_crypto_latency_5m")
    exits = monitor_crypto_5m_sniper_exits(
        shadow_settings,
        intraday_cli,
        hold_minutes=settings.ab_test_crypto_latency_5m_hold_minutes,
        take_profit_contract_price=settings.ab_test_crypto_latency_5m_take_profit_contract_price,
    )
    health_gate = _strategy_health_gate(
        settings,
        trades_path=shadow_settings.trades_path,
        status_path=_runner_status_path("ab_crypto_latency_5m"),
        strategy_name="crypto_latency_5m",
    )
    seed_markets = _load_intraday_imminent_updown_markets(
        settings,
        max_minutes_to_resolution=settings.ab_test_crypto_latency_5m_max_minutes_to_resolution,
        require_updown=True,
    ) or _load_intraday_raw_updown_markets(
        settings,
        max_minutes_to_resolution=settings.ab_test_crypto_latency_5m_max_minutes_to_resolution,
        require_updown=True,
    )
    candidates = _scan_short_crypto_updown_candidates(
        intraday_cli,
        settings=settings,
        markets_limit=settings.ab_test_crypto_latency_5m_markets_limit,
        max_minutes_to_resolution=settings.ab_test_crypto_latency_5m_max_minutes_to_resolution,
        min_book_depth_usdc=settings.ab_test_crypto_latency_5m_min_book_depth_usdc,
        require_updown=True,
        seed_markets=seed_markets,
        prebook_limit=16,
    )
    executor = _executor(shadow_settings, intraday_cli)
    existing_positions = executor.load_positions()
    open_lookup = _market_open_lookup(existing_positions)
    decisions: list[dict[str, Any]] = []
    opened_positions: list[dict[str, Any]] = []
    if health_gate["blocked"]:
        marks = mark_open_positions(shadow_settings, intraday_cli)
        return {
            "strategy": "crypto_latency_5m",
            "queue_count": len(candidates),
            "trade_decisions": decisions,
            "exits": exits,
            "opened_positions_count": 0,
            "scaled_positions_count": 0,
            "marked_positions_count": marks["summary"]["open_positions"],
            "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
            "health_gate": health_gate,
        }
    for candidate in candidates[: max(settings.queue_max_candidates, 1)]:
        asset = str(candidate.raw_market.get("asset", ""))
        decision_base = {
            "market_id": candidate.market_id,
            "token_id": candidate.token_id,
            "question": candidate.question,
            "midpoint": candidate.midpoint,
            "priority_score": candidate.priority_score,
            "asset": asset,
        }
        vote = _short_crypto_sniper_vote(
            candidate,
            strategy_name="crypto_latency_5m",
            min_book_imbalance=settings.ab_test_crypto_latency_5m_min_book_imbalance,
            max_side_price=settings.ab_test_crypto_latency_5m_max_side_price,
        )
        if vote.action == "HOLD":
            decisions.append({**decision_base, "action": "SKIP", "reason": vote.rationale, "votes": [vote.to_dict()]})
            continue
        if (candidate.market_id, vote.action) in open_lookup:
            decisions.append({**decision_base, "action": "SKIP", "reason": "position already open for this market side", "votes": [vote.to_dict()]})
            continue
        remaining_bankroll = _remaining_bankroll_usdc(shadow_settings, executor)
        size = round(min(settings.ab_test_crypto_latency_5m_fixed_position_usdc, remaining_bankroll, _position_cap_usdc(shadow_settings)), 2)
        if size <= 0:
            decisions.append({**decision_base, "action": "SKIP", "reason": "no remaining bankroll available", "votes": [vote.to_dict()]})
            continue
        book = _safe_book(intraday_cli, candidate.token_id)
        side_price = side_contract_price(vote.action, candidate.midpoint)
        opened = executor.open_position(
            candidate,
            vote.action,
            size,
            vote.confidence,
            max(0.02, abs(vote.estimated_probability - candidate.midpoint)),
            book=book,
            metadata={"strategy_tag": "crypto_latency_5m", "asset": asset, "market_family": "crypto_latency_5m"},
        )
        if opened is None:
            decisions.append({**decision_base, "action": "SKIP", "reason": "insufficient executable depth for open", "votes": [vote.to_dict()]})
            continue
        decisions.append({**decision_base, "action": "OPEN", "trade": opened, "votes": [vote.to_dict()], "entry_contract_price": round(side_price, 6)})
        opened_positions.append(opened)
        open_lookup[(candidate.market_id, vote.action)] = opened
    marks = mark_open_positions(shadow_settings, intraday_cli)
    return {
        "strategy": "crypto_latency_5m",
        "queue_count": len(candidates),
        "trade_decisions": decisions,
        "exits": exits,
        "opened_positions_count": len(opened_positions),
        "scaled_positions_count": 0,
        "marked_positions_count": marks["summary"]["open_positions"],
        "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
        "health_gate": health_gate,
    }


def monitor_exits(settings: Settings, cli: PolymarketCLI) -> list[dict[str, Any]]:
    executor = _executor(settings, cli)
    positions = executor.load_positions()
    exits: list[dict[str, Any]] = []

    for position in positions:
        book = _safe_book(cli, position["token_id"])
        bids = book.get("bids", []) if isinstance(book, dict) else []
        asks = book.get("asks", []) if isinstance(book, dict) else []
        depth = min(_sum_book_depth(bids), _sum_book_depth(asks))
        shares = effective_position_shares(position)
        liquidation = _simulate_contract_sell(_book_contract_levels(book, position["side"], "close"), shares)
        current_contract_price = liquidation["avg_contract_price"]
        if current_contract_price <= 0:
            continue
        current_yes_price = _contract_midpoint_to_yes_midpoint(position["side"], current_contract_price)
        opened_at = _normalize_iso(position["opened_at"])
        age_hours = 0.0
        if opened_at is not None:
            age_hours = max((datetime.now(timezone.utc) - opened_at).total_seconds() / 3600.0, 0.0)

        entry_contract_price = _as_float(position.get("entry_contract_price"), 0.0)
        if entry_contract_price <= 0:
            entry_contract_price = side_contract_price(position["side"], position["entry_price"])
        unrealized_pnl = round(_as_float(liquidation.get("proceeds_usdc"), 0.0) - _as_float(position.get("notional_usdc"), 0.0), 2)
        target_contract_price = entry_contract_price + (position["expected_gap"] * 0.85)
        if current_contract_price >= target_contract_price:
            closed = executor.close_position(position, current_yes_price, "TARGET_HIT", book=book)
            if closed is not None:
                exits.append(closed)
            continue
        forced_derisk_reason = _forced_derisk_reason(
            settings,
            position,
            current_contract_price=current_contract_price,
            unrealized_pnl=unrealized_pnl,
            age_hours=age_hours,
        )
        if forced_derisk_reason:
            closed = executor.close_position(position, current_yes_price, forced_derisk_reason, book=book)
            if closed is not None:
                exits.append(closed)
            continue
        if depth < settings.min_book_depth_usdc * 0.5:
            closed = executor.close_position(position, current_yes_price, "ORDER_FLOW_SPIKE_PROXY", book=book)
            if closed is not None:
                exits.append(closed)
            continue
        if age_hours > 24 and abs(current_contract_price - entry_contract_price) < 0.02:
            closed = executor.close_position(position, current_yes_price, "STALE_THESIS", book=book)
            if closed is not None:
                exits.append(closed)

    return exits


def run_cycle(settings: Settings, cli: PolymarketCLI) -> dict[str, Any]:
    starting_bankroll = settings.bankroll_usdc
    realized_pnl = _realized_pnl_usdc(settings)
    current_bankroll = _current_bankroll_usdc(settings)
    ensure_targets(settings, cli)
    refresh_target_activity(settings, cli)
    if settings.ab_test_crypto_enabled and not settings.ab_test_crypto_separate_daemon:
        refresh_crypto_target_activity(settings, cli, refresh_base_activity=False)
    if settings.ab_test_verified_public_enabled:
        refresh_target_activity_for_paths(
            settings,
            cli,
            targets_path=settings.verified_public_traders_path,
            activity_path=settings.verified_public_activity_path,
        )
    pre_trade_exits = monitor_exits(settings, cli)
    queue = scan_markets(settings, cli)
    theses = build_theses(settings)
    _persist_last_nonempty_snapshots(settings, queue, theses)
    decisions = trade_candidates(settings, cli)
    exits = pre_trade_exits + monitor_exits(settings, cli)
    marks = mark_open_positions(settings, cli)
    ab_tests: dict[str, Any] = {}
    if settings.ab_test_enabled and settings.ab_test_strategy == "wallet_copy":
        ab_tests["wallet_copy"] = run_ab_wallet_copy_cycle(
            settings,
            cli,
            suffix="ab_wallet_copy",
            strategy_name="wallet_copy",
            min_confidence=settings.ab_test_min_whale_confidence,
            require_microstructure_alignment=settings.ab_test_require_microstructure_alignment,
            agent_name="wallet_copy_ab",
        )
        if not settings.ab_test_aggressive_separate_daemon:
            ab_tests["wallet_copy_aggressive"] = run_aggressive_ab_cycle(settings, cli)
        if settings.ab_test_crypto_enabled and not settings.ab_test_crypto_separate_daemon:
            ab_tests["crypto_wallet_copy"] = run_crypto_ab_cycle(settings, cli, refresh_base_activity=False)
        if settings.ab_test_crypto_5m_sniper_enabled and not settings.ab_test_crypto_5m_sniper_separate_daemon:
            ab_tests["crypto_5m_sniper"] = run_crypto_5m_sniper_ab_cycle(settings, cli)
        if settings.ab_test_crypto_5m_box_enabled and not settings.ab_test_crypto_5m_box_separate_daemon:
            ab_tests["crypto_5m_box_arb"] = run_crypto_5m_box_ab_cycle(settings, cli)
        if settings.ab_test_crypto_next_window_sniper_enabled and not settings.ab_test_crypto_next_window_sniper_separate_daemon:
            ab_tests["crypto_next_window_sniper"] = run_crypto_next_window_sniper_ab_cycle(settings, cli)
        if settings.ab_test_crypto_next_window_box_enabled and not settings.ab_test_crypto_next_window_box_separate_daemon:
            ab_tests["crypto_next_window_box_arb"] = run_crypto_next_window_box_ab_cycle(settings, cli)
        if settings.ab_test_crypto_intraday_scheduled_enabled and not settings.ab_test_crypto_intraday_scheduled_separate_daemon:
            ab_tests["crypto_intraday_scheduled"] = run_crypto_intraday_scheduled_ab_cycle(settings, cli)
        if settings.ab_test_crypto_threshold_snapshot_enabled and not settings.ab_test_crypto_threshold_snapshot_separate_daemon:
            ab_tests["crypto_threshold_snapshot"] = run_crypto_threshold_snapshot_ab_cycle(settings, cli)
        if settings.ab_test_sports_early_entry_enabled and not settings.ab_test_sports_early_entry_separate_daemon:
            ab_tests["sports_early_entry"] = run_sports_early_entry_ab_cycle(settings, cli)
        if settings.ab_test_penny_longshot_enabled and not settings.ab_test_penny_longshot_separate_daemon:
            ab_tests["penny_longshot"] = run_penny_longshot_ab_cycle(settings, cli)
        if settings.ab_test_crypto_latency_5m_enabled and not settings.ab_test_crypto_latency_5m_separate_daemon:
            ab_tests["crypto_latency_5m"] = run_crypto_latency_5m_ab_cycle(settings, cli)
        if settings.ab_test_verified_public_enabled:
            ab_tests["verified_public"] = run_ab_wallet_copy_cycle(
                settings,
                cli,
                suffix="ab_verified_public",
                strategy_name="verified_public",
                min_confidence=settings.ab_test_verified_public_min_whale_confidence,
                require_microstructure_alignment=settings.ab_test_verified_public_require_microstructure_alignment,
                fixed_position_usdc=settings.ab_test_verified_public_fixed_position_usdc,
                agent_name="verified_public_ab",
                activity_path=settings.verified_public_activity_path,
            )
    opened_positions = [item["trade"] for item in decisions if item.get("action") == "OPEN" and item.get("trade")]
    scaled_positions = [item["trade"] for item in decisions if item.get("action") == "SCALE" and item.get("trade")]
    missed_opportunities = sorted(
        [item for item in decisions if item.get("action") == "SKIP"],
        key=lambda item: float(item.get("priority_score", 0.0)),
        reverse=True,
    )[:10]
    _json_dump(
        settings.missed_opportunities_path,
        {
            "updated_at": utc_now_iso(),
            "items": missed_opportunities,
        },
    )
    result = {
        "queue_count": len(queue),
        "thesis_count": len(theses),
        "trade_decisions": decisions,
        "missed_opportunities": missed_opportunities,
        "exits": exits,
        "opened_positions_count": len(opened_positions),
        "scaled_positions_count": len(scaled_positions),
        "marked_positions_count": marks["summary"]["open_positions"],
        "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
        "starting_bankroll_usdc": starting_bankroll,
        "realized_pnl_usdc": realized_pnl,
        "current_bankroll_usdc": current_bankroll,
        "ab_tests": ab_tests,
        "health_gate": _strategy_health_gate(
            settings,
            trades_path=settings.trades_path,
            status_path=settings.status_path,
            strategy_name="primary",
        ),
    }
    _write_status(
        settings,
        {
            "last_nonempty_queue_count": len(_json_load(settings.last_nonempty_queue_path, {}).get("items", [])),
            "last_nonempty_thesis_count": len(_json_load(settings.last_nonempty_theses_path, {}).get("items", [])),
            "last_opened_positions": opened_positions,
            "last_scaled_positions": scaled_positions,
            "marks_summary": marks["summary"],
        },
    )
    return result


def _write_status(settings: Settings, payload: dict[str, Any]) -> None:
    current = _json_load(settings.status_path, {})
    current.update(payload)
    _json_dump(settings.status_path, current)


def _log(message: str) -> None:
    print(f"[{utc_now_iso()}] {message}", flush=True)


def _current_cycle_result_from_files(settings: Settings, *, error: str | None = None) -> dict[str, Any]:
    queue = _json_load(settings.queue_path, [])
    theses = _json_load(settings.theses_path, [])
    marks = _json_load(settings.marks_path, {"summary": {}})
    missed = _json_load(settings.missed_opportunities_path, {"items": []})
    realized_pnl = _realized_pnl_usdc(settings)
    payload = {
        "queue_count": len(queue),
        "thesis_count": len(theses),
        "trade_decisions": [],
        "missed_opportunities": missed.get("items", []),
        "exits": [],
        "opened_positions_count": 0,
        "scaled_positions_count": 0,
        "marked_positions_count": int(_as_float(marks.get("summary", {}).get("open_positions"), 0.0)),
        "unrealized_pnl_usdc": round(_as_float(marks.get("summary", {}).get("total_unrealized_pnl_usdc"), 0.0), 2),
        "starting_bankroll_usdc": settings.bankroll_usdc,
        "realized_pnl_usdc": realized_pnl,
        "current_bankroll_usdc": round(max(settings.bankroll_usdc + realized_pnl, 0.0), 2),
    }
    if error:
        payload["error"] = error
    return payload


def _empty_cycle_result(*, error: str | None = None) -> dict[str, Any]:
    payload = {
        "queue_count": 0,
        "thesis_count": 0,
        "trade_decisions": [],
        "missed_opportunities": [],
        "exits": [],
        "opened_positions_count": 0,
        "scaled_positions_count": 0,
        "marked_positions_count": 0,
        "unrealized_pnl_usdc": 0.0,
        "starting_bankroll_usdc": 0.0,
        "realized_pnl_usdc": 0.0,
        "current_bankroll_usdc": 0.0,
    }
    if error:
        payload["error"] = error
    return payload


def reset_paper_book(settings: Settings) -> dict[str, Any]:
    _json_dump(settings.positions_path, [])
    _json_dump(settings.trades_path, [])
    _json_dump(settings.queue_path, [])
    _json_dump(settings.theses_path, [])
    _json_dump(settings.missed_opportunities_path, {"updated_at": utc_now_iso(), "items": []})
    _json_dump(settings.marks_path, {"updated_at": utc_now_iso(), "positions": [], "summary": {"open_positions": 0, "total_unrealized_pnl_usdc": 0.0, "total_mark_value_usdc": 0.0}})
    _json_dump(settings.last_nonempty_queue_path, {"updated_at": utc_now_iso(), "count": 0, "items": []})
    _json_dump(settings.last_nonempty_theses_path, {"updated_at": utc_now_iso(), "count": 0, "items": []})
    _json_dump(
        settings.status_path,
        {
            "status": "idle",
            "last_error": None,
            "runner": "reset",
            "reset_at": utc_now_iso(),
            "last_cycle_started_at": None,
            "last_cycle_completed_at": None,
            "last_cycle_result": _empty_cycle_result(),
            "last_nonempty_queue_count": 0,
            "last_nonempty_thesis_count": 0,
            "last_opened_positions": [],
            "last_scaled_positions": [],
            "marks_summary": {
                "open_positions": 0,
                "total_unrealized_pnl_usdc": 0.0,
                "total_mark_value_usdc": 0.0,
            },
        },
    )
    return {
        "positions_cleared": True,
        "trades_cleared": True,
        "queue_cleared": True,
        "theses_cleared": True,
        "marks_cleared": True,
        "targets_preserved": True,
        "target_activity_preserved": True,
    }


def dedupe_open_positions(settings: Settings) -> dict[str, Any]:
    positions = _json_load(settings.positions_path, [])
    seen_keys = set()
    deduped = []
    removed = []
    for position in positions:
        key = (position.get("market_id"), position.get("token_id"), position.get("side"))
        if key in seen_keys:
            removed.append(position)
            continue
        seen_keys.add(key)
        deduped.append(position)
    _json_dump(settings.positions_path, deduped)
    mark_payload = {
        "updated_at": utc_now_iso(),
        "positions": [],
        "summary": {
            "open_positions": len(deduped),
            "total_unrealized_pnl_usdc": 0.0,
            "total_mark_value_usdc": round(sum(_as_float(item.get("notional_usdc"), 0.0) for item in deduped), 2),
        },
    }
    _json_dump(settings.marks_path, mark_payload)
    return {"positions_before": len(positions), "positions_after": len(deduped), "duplicates_removed": len(removed)}


def _runner_status_path(name: str) -> Path:
    return Path(f"state/{name}_status.json")


def run_intraday_registry_daemon(settings: Settings, interval_seconds: int | None = None) -> None:
    interval = interval_seconds or settings.daemon_interval_seconds
    status_path = _runner_status_path("intraday_registry")
    cli = _fast_intraday_cli(PolymarketCLI(settings.polymarket_cli_bin, timeout_seconds=settings.polymarket_cli_timeout_seconds))
    _log(f"intraday_registry daemon starting interval={interval}s")
    current = _json_load(status_path, {})
    current.update(
        {
            "runner": "intraday_registry",
            "started_at": utc_now_iso(),
            "pid": os.getpid(),
            "interval_seconds": interval,
            "status": "running",
            "last_error": None,
        }
    )
    _json_dump(status_path, current)
    if not settings.intraday_registry_enabled:
        _log("intraday_registry disabled")
        return

    records = _intraday_registry_records(settings)
    if not records:
        try:
            bootstrap_payload = _bootstrap_intraday_registry_from_local_cache(settings)
            records = _registry_records_from_payload(bootstrap_payload)
            _log(f"intraday_registry local-cache bootstrapped {len(records)} markets before websocket subscribe")
        except Exception as exc:  # noqa: BLE001
            _log(f"intraday_registry local-cache bootstrap failed before websocket subscribe: {exc}")
    if not records:
        try:
            seed_markets = list(_gamma_tag_updown_market_payload(_subscription_seed_fetch_limit(settings), settings).get("markets", []))
            records = _subscription_seed_records_from_markets(settings, seed_markets, source="gamma-seed")
            _log(f"intraday_registry gamma seeded {len(records)} crypto markets for websocket subscribe")
        except Exception as exc:  # noqa: BLE001
            _log(f"intraday_registry gamma seed failed before websocket subscribe: {exc}")
    _log(f"intraday_registry using {len(records)} cached registry markets; websocket-first mode enabled")
    _log(
        "intraday_registry websocket config "
        f"imported={websocket_connect is not None} "
        f"enabled={settings.intraday_registry_ws_enabled} "
        f"endpoint={settings.intraday_registry_ws_endpoint.strip() or '-'}"
    )

    endpoint = settings.intraday_registry_ws_endpoint.strip()
    asset_lookup: dict[str, dict[str, Any]] = {}
    market_lookup: dict[str, dict[str, Any]] = {}

    def rebuild_ws_lookups() -> None:
        asset_lookup.clear()
        market_lookup.clear()
        for market in records.values():
            _index_intraday_ws_market(market, asset_lookup=asset_lookup, market_lookup=market_lookup)

    rebuild_ws_lookups()
    flush_started = utc_now_iso()
    flush_clock = time.monotonic()
    interval_messages = 0
    interval_new = 0
    interval_updated = 0
    interval_enriched = 0
    gamma_refresh_stats: dict[str, int] = {}

    def flush_status(*, websocket_connected: bool, websocket_error: str | None = None) -> None:
        nonlocal flush_started, flush_clock, interval_messages, interval_new, interval_updated, interval_enriched, gamma_refresh_stats
        tape_stats = _record_intraday_book_tape(settings, cli)
        population_metrics = _intraday_registry_population_metrics(settings)
        box_status = _json_load(_runner_status_path("ab_crypto_5m_box_arb"), {})
        box_result = box_status.get("last_cycle_result", {}) if isinstance(box_status.get("last_cycle_result"), dict) else {}
        box_tape = box_result.get("arb_tape", {}) if isinstance(box_result.get("arb_tape"), dict) else {}
        payload = _write_intraday_registry(
            settings,
            list(records.values()),
            metadata={
                "source": "gamma+ws-persistent",
                "websocket": {
                    "connected": websocket_connected,
                    "messages": interval_messages,
                    "new_markets": interval_new,
                    "updated_markets": interval_updated,
                    "enriched_markets": interval_enriched,
                    "error": websocket_error,
                },
                "bootstrap_markets": 0,
            },
        )
        result = {
            "strategy": "intraday_registry",
            "market_count": int(payload.get("market_count", 0)),
            "bootstrap_market_count": 0,
            "websocket_connected": websocket_connected,
            "websocket_messages": interval_messages,
            "websocket_new_markets": interval_new,
            "websocket_updated_markets": interval_updated,
            "websocket_enriched_markets": interval_enriched,
            "websocket_error": websocket_error,
            "gamma_tag_candidate_count": int(gamma_refresh_stats.get("tag_candidate_count", 0) or 0),
            "gamma_live_upcoming_count": int(gamma_refresh_stats.get("tag_live_upcoming_count", 0) or 0),
            "gamma_near_term_count": int(gamma_refresh_stats.get("tag_near_term_count", 0) or 0),
            "gamma_refresh_raw_fetched_markets": int(gamma_refresh_stats.get("raw_fetched_markets", 0) or 0),
            "book_snapshots_recorded": int(tape_stats.get("snapshots_recorded", 0) or 0),
            "book_snapshots_with_midpoint": int(tape_stats.get("snapshots_with_midpoint", 0) or 0),
            "book_snapshots_with_depth": int(tape_stats.get("snapshots_with_depth", 0) or 0),
            "threshold_live_markets": int(tape_stats.get("threshold_live_markets", 0) or 0),
            "transition_tape_entries": int(tape_stats.get("transition_tape_entries", 0) or 0),
            "raw_imminent_box_candidates": int(tape_stats.get("raw_imminent_box_candidates", 0) or 0),
            "imminent_box_arb_entries": int(tape_stats.get("imminent_box_arb_entries", 0) or 0),
            "positive_gross_edge_count": int(tape_stats.get("positive_gross_edge_count", 0) or 0),
            "positive_net_edge_count": int(tape_stats.get("positive_net_edge_count", 0) or 0),
            "max_gross_edge_per_share": round(_as_float(tape_stats.get("max_gross_edge_per_share"), 0.0), 6),
            "max_net_edge_per_share": round(_as_float(tape_stats.get("max_net_edge_per_share"), 0.0), 6),
            "watchlist_market_count": int(population_metrics.get("watchlist_market_count", 0) or 0),
            "watchlist_updown_count": int(population_metrics.get("watchlist_updown_count", 0) or 0),
            "imminent_updown_count": int(population_metrics.get("imminent_updown_count", 0) or 0),
            "watchlist_threshold_transition_count": int(population_metrics.get("watchlist_threshold_transition_count", 0) or 0),
            "watchlist_imminent_transition_count": int(population_metrics.get("watchlist_imminent_transition_count", 0) or 0),
            "watchlist_max_minutes_to_resolution": int(population_metrics.get("watchlist_max_minutes_to_resolution", 0) or 0),
        }
        result["box_execution_state"] = _intraday_box_execution_state(result, box_tape)
        result["execution_state"] = result["box_execution_state"]
        completed = utc_now_iso()
        _log(
            "intraday_registry cycle completed "
            f"markets={result.get('market_count', 0)} "
            f"ws_connected={result.get('websocket_connected', False)} "
            f"ws_messages={result.get('websocket_messages', 0)} "
            f"ws_new={result.get('websocket_new_markets', 0)} "
            f"ws_enriched={result.get('websocket_enriched_markets', 0)}"
        )
        current = _json_load(status_path, {})
        previous_result = current.get("last_cycle_result", {}) if isinstance(current.get("last_cycle_result"), dict) else {}
        _append_intraday_edge_alerts(settings, registry_result=result, previous_result=previous_result, box_tape=box_tape)
        _record_intraday_audit_cycle(settings, runner="intraday_registry", payload={**result, "executed_pairs": int(box_tape.get("executed_pairs", 0) or 0)})
        current.update(
            {
                "status": "running" if websocket_error is None else "degraded",
                "last_error": websocket_error,
                "last_cycle_started_at": flush_started,
                "last_cycle_completed_at": completed,
                "last_cycle_result": result,
            }
        )
        _json_dump(status_path, current)
        flush_started = completed
        flush_clock = time.monotonic()
        interval_messages = 0
        interval_new = 0
        interval_updated = 0
        interval_enriched = 0
        _log("intraday_registry cycle started")

    if websocket_connect is None or not settings.intraday_registry_ws_enabled or not endpoint:
        disabled_reasons: list[str] = []
        if websocket_connect is None:
            disabled_reasons.append("import_missing")
        if not settings.intraday_registry_ws_enabled:
            disabled_reasons.append("ws_flag_off")
        if not endpoint:
            disabled_reasons.append("missing_endpoint")
        _log(f"intraday_registry websocket disabled reason={','.join(disabled_reasons) or 'unknown'}")
        while True:
            flush_status(websocket_connected=False, websocket_error="websocket disabled")
            time.sleep(interval)

    while True:
        current = _json_load(status_path, {})
        current.update({"last_cycle_started_at": flush_started, "status": "running", "last_error": None})
        _json_dump(status_path, current)
        try:
            if not records:
                try:
                    bootstrap_payload = _bootstrap_intraday_registry_from_local_cache(settings)
                    records = _registry_records_from_payload(bootstrap_payload)
                    _log(f"intraday_registry refreshed local-cache bootstrap to {len(records)} markets before websocket connect")
                except Exception as exc:  # noqa: BLE001
                    _log(f"intraday_registry refresh local-cache bootstrap failed before websocket connect: {exc}")
            if not records:
                try:
                    seed_markets = list(_gamma_tag_updown_market_payload(_subscription_seed_fetch_limit(settings), settings).get("markets", []))
                    records = _subscription_seed_records_from_markets(settings, seed_markets, source="gamma-seed")
                    _log(f"intraday_registry refreshed gamma seed to {len(records)} crypto markets before websocket connect")
                except Exception as exc:  # noqa: BLE001
                    _log(f"intraday_registry refresh gamma seed failed before websocket connect: {exc}")
            if not records:
                _log("intraday_registry no seed markets available; retrying bootstrap next cycle")
                flush_status(websocket_connected=False, websocket_error="no seed markets available")
                time.sleep(interval)
                continue
            gamma_refresh_stats = _refresh_gamma_watchlist_records(settings, records, source="gamma-refresh-daemon")
            if gamma_refresh_stats.get("new_markets", 0) or gamma_refresh_stats.get("watch_only_markets", 0):
                _log(
                    "intraday_registry gamma refresh "
                    f"fetched={gamma_refresh_stats.get('fetched_markets', 0)} "
                    f"new={gamma_refresh_stats.get('new_markets', 0)} "
                    f"watch_only={gamma_refresh_stats.get('watch_only_markets', 0)}"
                )
            rebuild_ws_lookups()
            asset_batches = _intraday_ws_asset_batches(settings, list(records.values()))
            if not asset_batches:
                _log("intraday_registry no asset batches available after seeding")
                flush_status(websocket_connected=False, websocket_error="no asset batches available")
                time.sleep(interval)
                continue
            cycle_connected = False
            cycle_error = None
            interval_audit_entries: list[dict[str, Any]] = []
            cycle_deadline = time.monotonic() + max(interval, 1)
            for batch_index, asset_batch in enumerate(asset_batches):
                remaining_batches = max(len(asset_batches) - batch_index, 1)
                remaining_cycle = cycle_deadline - time.monotonic()
                if remaining_cycle <= 0:
                    break
                subscription = _intraday_ws_subscription_payload_for_assets(asset_batch)
                batch_deadline = time.monotonic() + max(remaining_cycle / remaining_batches, 0.75)
                try:
                    with websocket_connect(
                        endpoint,
                        open_timeout=5,
                        close_timeout=2,
                        max_size=max(int(settings.intraday_registry_ws_max_message_bytes), 1),
                    ) as websocket:
                        cycle_connected = True
                        _log(
                            "intraday_registry websocket connected "
                            f"batch={batch_index + 1}/{len(asset_batches)} "
                            f"assets={len(asset_batch)}"
                        )
                        websocket.send(json.dumps(subscription))
                        last_ping = time.monotonic()
                        while time.monotonic() < batch_deadline:
                            timeout = max(batch_deadline - time.monotonic(), 0.25)
                            raw = None
                            try:
                                raw = websocket.recv(timeout=timeout)
                            except TimeoutError:
                                if (time.monotonic() - last_ping) >= 8.0:
                                    try:
                                        websocket.send("PING")
                                        last_ping = time.monotonic()
                                    except Exception:
                                        raise
                                continue
                            if raw in (None, ""):
                                continue
                            if raw == "PONG":
                                continue
                            for message in _parse_intraday_ws_payloads(raw):
                                events = message if isinstance(message, list) else [message]
                                for event in events:
                                    if not isinstance(event, dict):
                                        continue
                                    interval_messages += 1
                                    event_type = str(_first(event, "event_type", "eventType", "type", default="")).lower()
                                    materialized_markets = _market_from_intraday_ws_event(
                                        event,
                                        asset_lookup=asset_lookup,
                                        market_lookup=market_lookup,
                                    )
                                    for market, normalized_event_type in materialized_markets:
                                        interval_audit_entries.append(
                                            _intraday_audit_ws_entry(
                                                event,
                                                market,
                                                event_type=normalized_event_type or event_type or "market",
                                            )
                                        )
                                        _append_intraday_registry_raw_updown(
                                            settings,
                                            market,
                                            source="ws",
                                            event_type=normalized_event_type or event_type,
                                        )
                                        result = _intraday_registry_merge_market(
                                            settings,
                                            records,
                                            market,
                                            source=f"ws:{normalized_event_type or event_type or 'market'}",
                                        )
                                        if not result.get("accepted"):
                                            continue
                                        accepted = records.get(str(result.get("market_id", "")))
                                        if accepted is not None:
                                            _index_intraday_ws_market(accepted, asset_lookup=asset_lookup, market_lookup=market_lookup)
                                        if result.get("was_enriched"):
                                            interval_enriched += 1
                                        if result.get("is_new"):
                                            interval_new += 1
                                        elif result.get("updated"):
                                            interval_updated += 1
                except Exception as exc:  # noqa: BLE001
                    cycle_error = str(exc)
                    _log(
                        "intraday_registry websocket batch error "
                        f"batch={batch_index + 1}/{len(asset_batches)} "
                        f"assets={len(asset_batch)} "
                        f"error={cycle_error}"
                    )
            _record_intraday_audit_ws_events(settings, interval_audit_entries)
            flush_status(websocket_connected=cycle_connected, websocket_error=None if cycle_connected else cycle_error)
            sleep_seconds = max(cycle_deadline - time.monotonic(), 0.0)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
        except KeyboardInterrupt:
            raise
        except Exception as exc:  # noqa: BLE001
            _log(f"intraday_registry cycle error {exc}")
            flush_status(websocket_connected=False, websocket_error=str(exc))
            time.sleep(2)


def run_shadow_daemon(
    settings: Settings,
    cli: PolymarketCLI,
    *,
    runner_name: str,
    interval_seconds: int | None,
    cycle_fn: Callable[[Settings, PolymarketCLI], dict[str, Any]],
) -> None:
    interval = interval_seconds or settings.daemon_interval_seconds
    status_path = _runner_status_path(runner_name)
    _log(f"{runner_name} daemon starting interval={interval}s")
    current = _json_load(status_path, {})
    current.update(
        {
            "runner": runner_name,
            "started_at": utc_now_iso(),
            "pid": os.getpid(),
            "interval_seconds": interval,
            "status": "running",
            "last_error": None,
        }
    )
    _json_dump(status_path, current)
    while True:
        started = utc_now_iso()
        _log(f"{runner_name} cycle started")
        current = _json_load(status_path, {})
        current.update({"last_cycle_started_at": started, "status": "running", "last_error": None})
        _json_dump(status_path, current)
        try:
            with _daemon_cycle_timeout(settings.daemon_cycle_timeout_seconds, runner_name):
                result = cycle_fn(settings, cli)
            completed = utc_now_iso()
            _log(
                f"{runner_name} cycle completed "
                f"queue={result.get('queue_count', 0)} "
                f"opened={result.get('opened_positions_count', 0)} "
                f"scaled={result.get('scaled_positions_count', 0)} "
                f"exits={len(result.get('exits', []))} "
                f"open_positions={result.get('marked_positions_count', 0)} "
                f"unrealized_pnl={result.get('unrealized_pnl_usdc', 0.0)}"
            )
            current = _json_load(status_path, {})
            current.update(
                {
                    "status": "running",
                    "last_error": None,
                    "last_cycle_completed_at": completed,
                    "last_cycle_result": result,
                }
            )
            _json_dump(status_path, current)
        except KeyboardInterrupt:
            raise
        except Exception as exc:  # noqa: BLE001
            errored_at = utc_now_iso()
            _log(f"{runner_name} cycle error {exc}")
            current = _json_load(status_path, {})
            current.update(
                {
                    "status": "degraded",
                    "last_error": str(exc),
                    "last_cycle_started_at": started,
                    "last_cycle_completed_at": errored_at,
                }
            )
            _json_dump(status_path, current)
        _log(f"{runner_name} sleeping {interval}s")
        time.sleep(interval)


def run_aggressive_ab_daemon(settings: Settings, cli: PolymarketCLI, interval_seconds: int | None = None) -> None:
    interval = interval_seconds or settings.daemon_interval_seconds
    status_path = _runner_status_path("ab_wallet_copy_aggressive")
    full_sync_every = max(settings.ab_test_aggressive_full_sync_every_cycles, 1)
    cycle_index = 0
    _log(f"ab_wallet_copy_aggressive daemon starting interval={interval}s full_sync_every={full_sync_every}")
    current = _json_load(status_path, {})
    current.update(
        {
            "runner": "ab_wallet_copy_aggressive",
            "started_at": utc_now_iso(),
            "pid": os.getpid(),
            "interval_seconds": interval,
            "status": "running",
            "last_error": None,
        }
    )
    _json_dump(status_path, current)
    while True:
        cycle_index += 1
        full_sync = (cycle_index % full_sync_every) == 1
        started = utc_now_iso()
        _log(f"ab_wallet_copy_aggressive cycle started full_sync={str(full_sync).lower()}")
        current = _json_load(status_path, {})
        current.update(
            {
                "last_cycle_started_at": started,
                "status": "running",
                "last_error": None,
                "cycle_index": cycle_index,
                "full_sync": full_sync,
            }
        )
        _json_dump(status_path, current)
        try:
            with _daemon_cycle_timeout(settings.daemon_cycle_timeout_seconds, "ab_wallet_copy_aggressive"):
                result = run_aggressive_ab_cycle(settings, cli, full_sync=full_sync)
            completed = utc_now_iso()
            _log(
                "ab_wallet_copy_aggressive cycle completed "
                f"full_sync={str(full_sync).lower()} "
                f"queue={result.get('queue_count', 0)} "
                f"opened={result.get('opened_positions_count', 0)} "
                f"scaled={result.get('scaled_positions_count', 0)} "
                f"exits={len(result.get('exits', []))} "
                f"open_positions={result.get('marked_positions_count', 0)} "
                f"unrealized_pnl={result.get('unrealized_pnl_usdc', 0.0)}"
            )
            current = _json_load(status_path, {})
            current.update(
                {
                    "status": "running",
                    "last_error": None,
                    "last_cycle_completed_at": completed,
                    "last_cycle_result": result,
                    "cycle_index": cycle_index,
                    "full_sync": full_sync,
                }
            )
            _json_dump(status_path, current)
        except KeyboardInterrupt:
            raise
        except Exception as exc:  # noqa: BLE001
            errored_at = utc_now_iso()
            _log(f"ab_wallet_copy_aggressive cycle error {exc}")
            current = _json_load(status_path, {})
            current.update(
                {
                    "status": "degraded",
                    "last_error": str(exc),
                    "last_cycle_started_at": started,
                    "last_cycle_completed_at": errored_at,
                    "cycle_index": cycle_index,
                    "full_sync": full_sync,
                }
            )
            _json_dump(status_path, current)
        _log(f"ab_wallet_copy_aggressive sleeping {interval}s")
        time.sleep(interval)


def run_crypto_ab_daemon(settings: Settings, cli: PolymarketCLI, interval_seconds: int | None = None) -> None:
    run_shadow_daemon(
        settings,
        cli,
        runner_name="ab_crypto_wallet_copy",
        interval_seconds=interval_seconds,
        cycle_fn=lambda active_settings, active_cli: run_crypto_ab_cycle(active_settings, active_cli, refresh_base_activity=True),
    )


def run_crypto_5m_sniper_ab_daemon(settings: Settings, cli: PolymarketCLI, interval_seconds: int | None = None) -> None:
    run_shadow_daemon(
        settings,
        cli,
        runner_name="ab_crypto_5m_sniper",
        interval_seconds=interval_seconds,
        cycle_fn=run_crypto_5m_sniper_ab_cycle,
    )


def run_crypto_5m_box_ab_daemon(settings: Settings, cli: PolymarketCLI, interval_seconds: int | None = None) -> None:
    run_shadow_daemon(
        settings,
        cli,
        runner_name="ab_crypto_5m_box_arb",
        interval_seconds=interval_seconds,
        cycle_fn=run_crypto_5m_box_ab_cycle,
    )


def run_crypto_next_window_sniper_ab_daemon(settings: Settings, cli: PolymarketCLI, interval_seconds: int | None = None) -> None:
    run_shadow_daemon(
        settings,
        cli,
        runner_name="ab_crypto_next_window_sniper",
        interval_seconds=interval_seconds,
        cycle_fn=run_crypto_next_window_sniper_ab_cycle,
    )


def run_crypto_next_window_box_ab_daemon(settings: Settings, cli: PolymarketCLI, interval_seconds: int | None = None) -> None:
    run_shadow_daemon(
        settings,
        cli,
        runner_name="ab_crypto_next_window_box_arb",
        interval_seconds=interval_seconds,
        cycle_fn=run_crypto_next_window_box_ab_cycle,
    )


def run_crypto_intraday_scheduled_ab_daemon(settings: Settings, cli: PolymarketCLI, interval_seconds: int | None = None) -> None:
    run_shadow_daemon(
        settings,
        cli,
        runner_name="ab_crypto_intraday_scheduled",
        interval_seconds=interval_seconds,
        cycle_fn=run_crypto_intraday_scheduled_ab_cycle,
    )


def run_crypto_threshold_snapshot_ab_daemon(settings: Settings, cli: PolymarketCLI, interval_seconds: int | None = None) -> None:
    run_shadow_daemon(
        settings,
        cli,
        runner_name="ab_crypto_threshold_snapshot",
        interval_seconds=interval_seconds,
        cycle_fn=run_crypto_threshold_snapshot_ab_cycle,
    )


def run_sports_early_entry_ab_daemon(settings: Settings, cli: PolymarketCLI, interval_seconds: int | None = None) -> None:
    run_shadow_daemon(
        settings,
        cli,
        runner_name="ab_sports_early_entry",
        interval_seconds=interval_seconds,
        cycle_fn=run_sports_early_entry_ab_cycle,
    )


def run_penny_longshot_ab_daemon(settings: Settings, cli: PolymarketCLI, interval_seconds: int | None = None) -> None:
    run_shadow_daemon(
        settings,
        cli,
        runner_name="ab_penny_longshot",
        interval_seconds=interval_seconds,
        cycle_fn=run_penny_longshot_ab_cycle,
    )


def run_crypto_latency_5m_ab_daemon(settings: Settings, cli: PolymarketCLI, interval_seconds: int | None = None) -> None:
    run_shadow_daemon(
        settings,
        cli,
        runner_name="ab_crypto_latency_5m",
        interval_seconds=interval_seconds,
        cycle_fn=run_crypto_latency_5m_ab_cycle,
    )


def _primary_daemon_full_sync_settings(settings: Settings) -> Settings:
    return replace(
        settings,
        markets_limit=min(settings.markets_limit, 300),
        queue_max_candidates=min(settings.queue_max_candidates, 40),
        thesis_max_candidates_per_cycle=min(settings.thesis_max_candidates_per_cycle, 6),
    )


def run_primary_daemon_cycle(settings: Settings, cli: PolymarketCLI, *, full_sync: bool) -> dict[str, Any]:
    active_settings = _primary_daemon_full_sync_settings(settings) if full_sync else settings
    if full_sync:
        return run_cycle(active_settings, cli)
    starting_bankroll = active_settings.bankroll_usdc
    realized_pnl = _realized_pnl_usdc(active_settings)
    current_bankroll = _current_bankroll_usdc(active_settings)
    pre_trade_exits = monitor_exits(active_settings, cli)
    queue = _json_load(active_settings.queue_path, [])
    theses = _json_load(active_settings.theses_path, [])
    if not isinstance(queue, list):
        queue = []
    if not isinstance(theses, list):
        theses = []
    _persist_last_nonempty_snapshots(active_settings, queue, theses)
    exits = pre_trade_exits + monitor_exits(active_settings, cli)
    marks = mark_open_positions(active_settings, cli)
    missed_payload = _json_load(active_settings.missed_opportunities_path, {"items": []})
    missed_opportunities = missed_payload.get("items", []) if isinstance(missed_payload, dict) else []
    result = {
        "queue_count": len(queue),
        "thesis_count": len(theses),
        "trade_decisions": [],
        "missed_opportunities": missed_opportunities,
        "exits": exits,
        "opened_positions_count": 0,
        "scaled_positions_count": 0,
        "marked_positions_count": marks["summary"]["open_positions"],
        "unrealized_pnl_usdc": marks["summary"]["total_unrealized_pnl_usdc"],
        "starting_bankroll_usdc": starting_bankroll,
        "realized_pnl_usdc": realized_pnl,
        "current_bankroll_usdc": current_bankroll,
        "ab_tests": {},
        "health_gate": _strategy_health_gate(
            active_settings,
            trades_path=active_settings.trades_path,
            status_path=active_settings.status_path,
            strategy_name="primary",
        ),
    }
    _write_status(
        active_settings,
        {
            "last_nonempty_queue_count": len(_json_load(active_settings.last_nonempty_queue_path, {}).get("items", [])),
            "last_nonempty_thesis_count": len(_json_load(active_settings.last_nonempty_theses_path, {}).get("items", [])),
            "last_opened_positions": [],
            "last_scaled_positions": [],
            "marks_summary": marks["summary"],
        },
    )
    return result


def run_daemon(settings: Settings, cli: PolymarketCLI, interval_seconds: int | None = None) -> None:
    interval = interval_seconds or settings.daemon_interval_seconds
    current_bankroll = _current_bankroll_usdc(settings)
    full_sync_every = max(settings.main_daemon_full_sync_every_cycles, 1)
    cycle_index = 0
    _log(
        "daemon starting "
        f"mode={settings.bot_mode} starting_bankroll={settings.bankroll_usdc} "
        f"current_bankroll={current_bankroll} "
        f"markets_limit={settings.markets_limit} queue_max={settings.queue_max_candidates} "
        f"max_open_positions={settings.max_open_positions} interval={interval}s "
        f"full_sync_every={full_sync_every}"
    )
    _write_status(
        settings,
        {
            "runner": "daemon",
            "started_at": utc_now_iso(),
            "pid": os.getpid(),
            "interval_seconds": interval,
            "status": "running",
            "last_error": None,
        },
    )
    while True:
        cycle_index += 1
        full_sync = (cycle_index % full_sync_every) == 1
        started = utc_now_iso()
        _log(f"cycle started full_sync={str(full_sync).lower()}")
        _write_status(
            settings,
            {
                "last_cycle_started_at": started,
                "status": "running",
                "last_error": None,
                "cycle_index": cycle_index,
                "full_sync": full_sync,
            },
        )
        try:
            with _daemon_cycle_timeout(settings.daemon_cycle_timeout_seconds, "daemon"):
                result = run_primary_daemon_cycle(settings, cli, full_sync=full_sync)
            _log(
                "cycle completed "
                f"full_sync={str(full_sync).lower()} "
                f"queue={result.get('queue_count', 0)} "
                f"theses={result.get('thesis_count', 0)} "
                f"opened={result.get('opened_positions_count', 0)} "
                f"scaled={result.get('scaled_positions_count', 0)} "
                f"exits={len(result.get('exits', []))} "
                f"open_positions={result.get('marked_positions_count', 0)} "
                f"current_bankroll={result.get('current_bankroll_usdc', 0.0)} "
                f"unrealized_pnl={result.get('unrealized_pnl_usdc', 0.0)}"
            )
            _write_status(
                settings,
                {
                    "last_cycle_completed_at": utc_now_iso(),
                    "last_cycle_result": result,
                    "last_error": None,
                    "status": "running",
                    "cycle_index": cycle_index,
                    "full_sync": full_sync,
                },
            )
        except Exception as exc:
            _log(f"cycle error {exc}")
            errored_at = utc_now_iso()
            preserved_result = _current_cycle_result_from_files(settings, error=str(exc))
            _write_status(
                settings,
                {
                    "last_cycle_started_at": started,
                    "last_cycle_completed_at": errored_at,
                    "last_cycle_result": preserved_result,
                    "last_error": str(exc),
                    "status": "degraded",
                    "cycle_index": cycle_index,
                    "full_sync": full_sync,
                },
            )
        _log(f"sleeping {interval}s")
        time.sleep(interval)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Paper-first prediction-market research and execution simulation engine"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    discover = subparsers.add_parser("discover-targets")
    discover.add_argument("--csv")
    discover.add_argument("--leaderboard", action="store_true")
    discover.add_argument("--min-trades", type=int, default=100)
    discover.add_argument("--top-n", type=int, default=50)

    refresh = subparsers.add_parser("refresh-target-activity")
    refresh.add_argument("--limit", type=int, default=25)

    subparsers.add_parser("scan")
    subparsers.add_parser("brain")
    subparsers.add_parser("trade")
    subparsers.add_parser("monitor-exits")
    subparsers.add_parser("cycle")
    subparsers.add_parser("cycle-aggressive-ab")
    subparsers.add_parser("cycle-crypto-ab")
    subparsers.add_parser("cycle-crypto-5m-sniper-ab")
    subparsers.add_parser("cycle-crypto-5m-box-ab")
    subparsers.add_parser("cycle-crypto-next-window-sniper-ab")
    subparsers.add_parser("cycle-crypto-next-window-box-ab")
    subparsers.add_parser("cycle-crypto-intraday-scheduled-ab")
    subparsers.add_parser("cycle-crypto-threshold-snapshot-ab")
    subparsers.add_parser("cycle-sports-early-entry-ab")
    subparsers.add_parser("cycle-penny-longshot-ab")
    subparsers.add_parser("cycle-crypto-latency-5m-ab")
    subparsers.add_parser("cycle-intraday-registry")
    subparsers.add_parser("latency-bot-init")
    subparsers.add_parser("latency-bot-discovery-cycle")
    subparsers.add_parser("latency-bot-engine-cycle")
    subparsers.add_parser("latency-bot-us-arb-probe-cycle")
    subparsers.add_parser("latency-bot-kalshi-arb-probe-cycle")
    latency_bot_summarize_parser = subparsers.add_parser("latency-bot-summarize")
    latency_bot_summarize_parser.add_argument("--minutes", type=int, default=60)
    latency_bot_db_parser = subparsers.add_parser("latency-bot-db-maintain")
    latency_bot_db_parser.add_argument("--retention-days", type=int, default=14)
    latency_bot_db_parser.add_argument("--batch-size", type=int, default=25_000)
    latency_bot_db_parser.add_argument("--archive-dir")
    latency_bot_db_parser.add_argument("--apply", action="store_true")
    latency_bot_db_parser.add_argument("--confirm", default="")
    latency_bot_db_parser.add_argument("--checkpoint", action="store_true")
    latency_bot_db_parser.add_argument("--vacuum", action="store_true")
    latency_bot_db_parser.add_argument("--migrate", action="store_true")
    latency_bot_demo_parser = subparsers.add_parser("latency-bot-create-demo")
    latency_bot_demo_parser.add_argument("--output-dir", default="demo/runtime")
    latency_bot_demo_parser.add_argument("--force", action="store_true")
    latency_bot_validation_parser = subparsers.add_parser("latency-bot-validate")
    latency_bot_validation_parser.add_argument("--input", required=True)
    latency_bot_validation_parser.add_argument("--output")
    summarize_intraday_audit_parser = subparsers.add_parser("summarize-intraday-audit")
    summarize_intraday_audit_parser.add_argument("--minutes", type=int, default=60)
    summarize_intraday_lifecycle_parser = subparsers.add_parser("summarize-intraday-lifecycle")
    summarize_intraday_lifecycle_parser.add_argument("--limit", type=int, default=10)
    compare_intraday_discovery_parser = subparsers.add_parser("compare-intraday-discovery")
    compare_intraday_discovery_parser.add_argument("--limit", type=int, default=3)
    summarize_intraday_discovery_parser = subparsers.add_parser("summarize-intraday-discovery")
    summarize_intraday_discovery_parser.add_argument("--limit", type=int, default=3)
    subparsers.add_parser("backfill-intraday-lifecycle")
    subparsers.add_parser("profile-strategy-styles")
    subparsers.add_parser("reset-paper-book")
    subparsers.add_parser("dedupe-open-positions")
    daemon = subparsers.add_parser("daemon")
    daemon.add_argument("--interval", type=int)
    intraday_registry_daemon = subparsers.add_parser("daemon-intraday-registry")
    intraday_registry_daemon.add_argument("--interval", type=int)
    aggressive_daemon = subparsers.add_parser("daemon-aggressive-ab")
    aggressive_daemon.add_argument("--interval", type=int)
    crypto_daemon = subparsers.add_parser("daemon-crypto-ab")
    crypto_daemon.add_argument("--interval", type=int)
    crypto_5m_sniper_daemon = subparsers.add_parser("daemon-crypto-5m-sniper-ab")
    crypto_5m_sniper_daemon.add_argument("--interval", type=int)
    crypto_5m_box_daemon = subparsers.add_parser("daemon-crypto-5m-box-ab")
    crypto_5m_box_daemon.add_argument("--interval", type=int)
    crypto_next_window_sniper_daemon = subparsers.add_parser("daemon-crypto-next-window-sniper-ab")
    crypto_next_window_sniper_daemon.add_argument("--interval", type=int)
    crypto_next_window_box_daemon = subparsers.add_parser("daemon-crypto-next-window-box-ab")
    crypto_next_window_box_daemon.add_argument("--interval", type=int)
    crypto_intraday_scheduled_daemon = subparsers.add_parser("daemon-crypto-intraday-scheduled-ab")
    crypto_intraday_scheduled_daemon.add_argument("--interval", type=int)
    crypto_threshold_snapshot_daemon = subparsers.add_parser("daemon-crypto-threshold-snapshot-ab")
    crypto_threshold_snapshot_daemon.add_argument("--interval", type=int)
    sports_early_entry_daemon = subparsers.add_parser("daemon-sports-early-entry-ab")
    sports_early_entry_daemon.add_argument("--interval", type=int)
    penny_longshot_daemon = subparsers.add_parser("daemon-penny-longshot-ab")
    penny_longshot_daemon.add_argument("--interval", type=int)
    crypto_latency_5m_daemon = subparsers.add_parser("daemon-crypto-latency-5m-ab")
    crypto_latency_5m_daemon.add_argument("--interval", type=int)
    latency_bot_engine_daemon = subparsers.add_parser("daemon-latency-bot-engine")
    latency_bot_engine_daemon.add_argument("--interval", type=int)
    latency_bot_us_arb_probe_daemon = subparsers.add_parser("daemon-latency-bot-us-arb-probe")
    latency_bot_us_arb_probe_daemon.add_argument("--interval", type=int)
    latency_bot_kalshi_arb_probe_daemon = subparsers.add_parser("daemon-latency-bot-kalshi-arb-probe")
    latency_bot_kalshi_arb_probe_daemon.add_argument("--interval", type=int)
    dashboard = subparsers.add_parser("serve-dashboard")
    dashboard.add_argument("--host")
    dashboard.add_argument("--port", type=int)
    latency_dashboard = subparsers.add_parser("serve-latency-bot-dashboard")
    latency_dashboard.add_argument("--host")
    latency_dashboard.add_argument("--port", type=int)
    latency_demo_dashboard = subparsers.add_parser("serve-latency-bot-demo-dashboard")
    latency_demo_dashboard.add_argument("--data-dir", default="demo/runtime")
    latency_demo_dashboard.add_argument("--host")
    latency_demo_dashboard.add_argument("--port", type=int, default=8091)
    return parser


def main() -> int:
    _parse_env_file(Path(".env"))
    settings = Settings.from_env()
    settings.ensure_dirs()
    cli = PolymarketCLI(settings.polymarket_cli_bin, timeout_seconds=settings.polymarket_cli_timeout_seconds)
    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.command == "discover-targets":
            if args.csv:
                ranked = discover_targets_from_csv(Path(args.csv), settings.targets_path, args.min_trades, args.top_n)
            else:
                ranked = discover_targets_from_leaderboard(settings, cli)
            print(json.dumps(ranked, indent=2))
        elif args.command == "refresh-target-activity":
            payload = refresh_target_activity(settings, cli, args.limit)
            print(json.dumps(payload, indent=2))
        elif args.command == "scan":
            print(json.dumps(scan_markets(settings, cli), indent=2))
        elif args.command == "brain":
            print(json.dumps(build_theses(settings), indent=2))
        elif args.command == "trade":
            print(json.dumps(trade_candidates(settings, cli), indent=2))
        elif args.command == "monitor-exits":
            print(json.dumps(monitor_exits(settings, cli), indent=2))
        elif args.command == "cycle":
            print(json.dumps(run_cycle(settings, cli), indent=2))
        elif args.command == "cycle-aggressive-ab":
            print(json.dumps(run_aggressive_ab_cycle(settings, cli), indent=2))
        elif args.command == "cycle-crypto-ab":
            print(json.dumps(run_crypto_ab_cycle(settings, cli), indent=2))
        elif args.command == "cycle-crypto-5m-sniper-ab":
            print(json.dumps(run_crypto_5m_sniper_ab_cycle(settings, cli), indent=2))
        elif args.command == "cycle-crypto-5m-box-ab":
            print(json.dumps(run_crypto_5m_box_ab_cycle(settings, cli), indent=2))
        elif args.command == "cycle-crypto-next-window-sniper-ab":
            print(json.dumps(run_crypto_next_window_sniper_ab_cycle(settings, cli), indent=2))
        elif args.command == "cycle-crypto-next-window-box-ab":
            print(json.dumps(run_crypto_next_window_box_ab_cycle(settings, cli), indent=2))
        elif args.command == "cycle-crypto-intraday-scheduled-ab":
            print(json.dumps(run_crypto_intraday_scheduled_ab_cycle(settings, cli), indent=2))
        elif args.command == "cycle-crypto-threshold-snapshot-ab":
            print(json.dumps(run_crypto_threshold_snapshot_ab_cycle(settings, cli), indent=2))
        elif args.command == "cycle-sports-early-entry-ab":
            print(json.dumps(run_sports_early_entry_ab_cycle(settings, cli), indent=2))
        elif args.command == "cycle-penny-longshot-ab":
            print(json.dumps(run_penny_longshot_ab_cycle(settings, cli), indent=2))
        elif args.command == "cycle-crypto-latency-5m-ab":
            print(json.dumps(run_crypto_latency_5m_ab_cycle(settings, cli), indent=2))
        elif args.command == "cycle-intraday-registry":
            print(json.dumps(run_intraday_registry_cycle(settings), indent=2))
        elif args.command == "latency-bot-init":
            from latency_bot import LatencyBotSettings, latency_bot_init

            print(json.dumps(latency_bot_init(LatencyBotSettings.from_env()), indent=2))
        elif args.command == "latency-bot-discovery-cycle":
            from latency_bot import LatencyBotSettings, latency_bot_discovery_cycle

            print(json.dumps(latency_bot_discovery_cycle(LatencyBotSettings.from_env()), indent=2))
        elif args.command == "latency-bot-engine-cycle":
            from latency_bot import LatencyBotSettings, latency_bot_engine_cycle

            print(json.dumps(latency_bot_engine_cycle(LatencyBotSettings.from_env()), indent=2))
        elif args.command == "latency-bot-us-arb-probe-cycle":
            from latency_bot import LatencyBotSettings, latency_bot_us_arb_probe_cycle

            print(json.dumps(latency_bot_us_arb_probe_cycle(LatencyBotSettings.from_env()), indent=2))
        elif args.command == "latency-bot-kalshi-arb-probe-cycle":
            from latency_bot import LatencyBotSettings, latency_bot_kalshi_arb_probe_cycle

            print(json.dumps(latency_bot_kalshi_arb_probe_cycle(LatencyBotSettings.from_env()), indent=2))
        elif args.command == "latency-bot-summarize":
            from latency_bot import LatencyBotSettings, latency_bot_summarize

            print(json.dumps(latency_bot_summarize(LatencyBotSettings.from_env(), args.minutes), indent=2))
        elif args.command == "latency-bot-db-maintain":
            from latency_bot import LatencyBotSettings
            from latency_bot.maintenance import (
                apply_retention,
                checkpoint_database,
                database_inventory,
                retention_plan,
                vacuum_database,
            )
            from latency_bot.storage import init_latency_bot_db

            latency_settings = LatencyBotSettings.from_env()
            result: dict[str, Any] = {}
            if args.migrate:
                result["migration"] = init_latency_bot_db(latency_settings)
            result["inventory"] = database_inventory(latency_settings.db_path)
            result["retention"] = retention_plan(
                latency_settings.db_path,
                retention_days=args.retention_days,
                batch_size=args.batch_size,
            )
            if args.apply:
                result["retention"] = apply_retention(
                    latency_settings.db_path,
                    retention_days=args.retention_days,
                    batch_size=args.batch_size,
                    archive_dir=Path(args.archive_dir) if args.archive_dir else latency_settings.db_path.parent / "archive",
                    confirmation=args.confirm,
                )
            if args.checkpoint:
                result["checkpoint"] = checkpoint_database(latency_settings.db_path)
            if args.vacuum:
                if not args.apply:
                    parser.error("--vacuum requires --apply and --confirm RETENTION_APPLY")
                result["vacuum"] = vacuum_database(latency_settings.db_path, confirmation=args.confirm)
            print(json.dumps(result, indent=2))
        elif args.command == "latency-bot-create-demo":
            from latency_bot.demo import create_demo_workspace

            print(json.dumps(create_demo_workspace(Path(args.output_dir), force=args.force), indent=2))
        elif args.command == "latency-bot-validate":
            from latency_bot.validation import write_validation_report

            output_path = Path(args.output) if args.output else None
            print(json.dumps(write_validation_report(Path(args.input), output_path), indent=2))
        elif args.command == "summarize-intraday-audit":
            print(json.dumps(summarize_intraday_audit(settings, args.minutes), indent=2))
        elif args.command == "summarize-intraday-lifecycle":
            print(json.dumps(summarize_intraday_lifecycle(settings, args.limit), indent=2))
        elif args.command == "compare-intraday-discovery":
            print(json.dumps(compare_intraday_discovery_sources(settings, cli), indent=2))
        elif args.command == "summarize-intraday-discovery":
            print(json.dumps(summarize_intraday_discovery_comparator(settings, args.limit), indent=2))
        elif args.command == "backfill-intraday-lifecycle":
            print(json.dumps(backfill_intraday_lifecycle(settings), indent=2))
        elif args.command == "profile-strategy-styles":
            print(json.dumps(write_strategy_style_profiles(settings), indent=2))
        elif args.command == "reset-paper-book":
            print(json.dumps(reset_paper_book(settings), indent=2))
        elif args.command == "dedupe-open-positions":
            print(json.dumps(dedupe_open_positions(settings), indent=2))
        elif args.command == "daemon":
            run_daemon(settings, cli, args.interval)
        elif args.command == "daemon-intraday-registry":
            run_intraday_registry_daemon(settings, args.interval)
        elif args.command == "daemon-aggressive-ab":
            run_aggressive_ab_daemon(settings, cli, args.interval)
        elif args.command == "daemon-crypto-ab":
            run_crypto_ab_daemon(settings, cli, args.interval)
        elif args.command == "daemon-crypto-5m-sniper-ab":
            run_crypto_5m_sniper_ab_daemon(settings, cli, args.interval)
        elif args.command == "daemon-crypto-5m-box-ab":
            run_crypto_5m_box_ab_daemon(settings, cli, args.interval)
        elif args.command == "daemon-crypto-next-window-sniper-ab":
            run_crypto_next_window_sniper_ab_daemon(settings, cli, args.interval)
        elif args.command == "daemon-crypto-next-window-box-ab":
            run_crypto_next_window_box_ab_daemon(settings, cli, args.interval)
        elif args.command == "daemon-crypto-intraday-scheduled-ab":
            run_crypto_intraday_scheduled_ab_daemon(settings, cli, args.interval)
        elif args.command == "daemon-crypto-threshold-snapshot-ab":
            run_crypto_threshold_snapshot_ab_daemon(settings, cli, args.interval)
        elif args.command == "daemon-sports-early-entry-ab":
            run_sports_early_entry_ab_daemon(settings, cli, args.interval)
        elif args.command == "daemon-penny-longshot-ab":
            run_penny_longshot_ab_daemon(settings, cli, args.interval)
        elif args.command == "daemon-crypto-latency-5m-ab":
            run_crypto_latency_5m_ab_daemon(settings, cli, args.interval)
        elif args.command == "daemon-latency-bot-engine":
            from latency_bot import LatencyBotSettings, run_latency_bot_engine_daemon

            run_latency_bot_engine_daemon(LatencyBotSettings.from_env(), args.interval)
        elif args.command == "daemon-latency-bot-us-arb-probe":
            from latency_bot import LatencyBotSettings, run_latency_bot_us_arb_probe_daemon

            run_latency_bot_us_arb_probe_daemon(LatencyBotSettings.from_env(), args.interval)
        elif args.command == "daemon-latency-bot-kalshi-arb-probe":
            from latency_bot import LatencyBotSettings, run_latency_bot_kalshi_arb_probe_daemon

            run_latency_bot_kalshi_arb_probe_daemon(LatencyBotSettings.from_env(), args.interval)
        elif args.command == "serve-dashboard":
            serve_dashboard(settings, host=args.host or settings.dashboard_host, port=args.port or settings.dashboard_port)
        elif args.command == "serve-latency-bot-dashboard":
            from latency_bot import LatencyBotSettings, serve_latency_bot_dashboard

            latency_settings = LatencyBotSettings.from_env()
            serve_latency_bot_dashboard(
                latency_settings,
                host=args.host or latency_settings.dashboard_host,
                port=args.port or latency_settings.dashboard_port,
            )
        elif args.command == "serve-latency-bot-demo-dashboard":
            from latency_bot import LatencyBotSettings, serve_latency_bot_dashboard
            from latency_bot.demo import create_demo_workspace, demo_settings

            data_dir = Path(args.data_dir)
            latency_settings = demo_settings(data_dir, LatencyBotSettings.from_env())
            if not latency_settings.db_path.exists():
                create_demo_workspace(data_dir)
            serve_latency_bot_dashboard(
                latency_settings,
                host=args.host or "127.0.0.1",
                port=args.port,
            )
        else:
            parser.error(f"unknown command {args.command}")
    except GeoblockedError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0
