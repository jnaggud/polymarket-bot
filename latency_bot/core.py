from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any

from bot.core import PolymarketCLI

from .config import LatencyBotSettings
from .execution.paper import (
    run_btc_fair_value_paper_cycle,
    run_cex_latency_paper_cycle,
    run_complete_set_arb_paper_cycle,
    run_late_resolution_capture_paper_cycle,
    run_paper_execution_cycle,
    run_promoted_variant_paper_cycle,
    run_shadow_btc_no_paper_cycle,
    run_shadow_btc_yes_variant_paper_cycle,
    run_temporal_inventory_maker_paper_cycle,
    run_wallet_teacher_sniper_paper_cycle,
)
from .execution.live_complete_set_arb import run_live_complete_set_arb_pilot_cycle
from .execution.live_temporal_inventory_maker import run_live_temporal_inventory_maker_cycle
from .feeds.binance import refresh_binance_cache
from .feeds.discovery import discover_latency_markets
from .feeds.kalshi import fetch_kalshi_arb_ticks
from .feeds.polymarket import refresh_polymarket_cache
from .feeds.polymarket_us import fetch_polymarket_us_arb_ticks
from .feeds.wallet_activity import fetch_wallet_teacher_trades
from .models import LatencyBotStatus
from .risk.limits import risk_snapshot
from .storage import (
    append_cex_latency_paper_signals,
    append_complete_set_arb_signals,
    append_fair_values,
    append_kalshi_arb_ticks,
    append_late_resolution_capture_signals,
    append_polymarket_us_arb_ticks,
    append_signals,
    append_shadow_signals,
    append_shadow_variant_signals,
    init_latency_bot_db,
    latency_bot_portfolio_summary,
    latency_bot_shadow_portfolio_summary,
    load_open_orders,
    load_open_positions,
    record_engine_cycle,
    record_equity_snapshot,
    replace_binance_ticks,
    replace_markets,
    replace_polymarket_books,
    summarize_latency_bot_db,
)
from .strategy.complete_set_arb import build_complete_set_arb_signals
from .strategy.fair_value import build_fair_values
from .strategy.signals import (
    build_btc_fair_value_paper_signals,
    build_cex_latency_paper_signals,
    build_late_resolution_capture_paper_signals,
    build_shadow_btc_no_signals,
    build_shadow_btc_yes_variant_signals,
    build_signals,
    build_temporal_inventory_maker_paper_signals,
    build_wallet_teacher_sniper_signals,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json(path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text())
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _write_json(path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def _acquire_engine_daemon_lock(settings: LatencyBotSettings):
    lock_path = settings.db_path.parent / "latency_bot_engine.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = lock_path.open("w", encoding="utf-8")
    try:
        import fcntl

        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        lock_file.close()
        raise RuntimeError(f"latency bot engine daemon already running; lock held at {lock_path}") from exc
    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(str(os.getpid()))
    lock_file.flush()
    return lock_file


def _parse_iso(raw: str | None) -> datetime | None:
    if not raw:
        return None
    text = str(raw).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _filter_live_market_items(
    items: list[dict[str, Any]],
    *,
    now: datetime | None = None,
    lookahead_minutes: int | None = None,
) -> list[dict[str, Any]]:
    current = now or datetime.now(timezone.utc)
    max_seconds = None if lookahead_minutes is None else max(lookahead_minutes, 1) * 60
    live_items: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        expiry = _parse_iso(str(item.get("expiry_ts") or ""))
        if expiry is None:
            continue
        seconds_left = (expiry - current).total_seconds()
        if seconds_left <= 0:
            continue
        if max_seconds is not None and seconds_left > max_seconds:
            continue
        updated = dict(item)
        updated["hours_to_expiry"] = round(seconds_left / 3600.0, 6)
        updated["status"] = "tracked"
        live_items.append(updated)
    live_items.sort(key=lambda item: (float(item.get("hours_to_expiry", 0.0)), str(item.get("asset", "")), str(item.get("question", ""))))
    return live_items


def _markets_payload_needs_refresh(settings: LatencyBotSettings, payload: dict[str, Any], live_items: list[dict[str, Any]]) -> bool:
    generated_at = _parse_iso(str(payload.get("generated_at") or ""))
    if generated_at is None:
        return True
    age_seconds = (datetime.now(timezone.utc) - generated_at).total_seconds()
    if age_seconds >= max(settings.discovery_refresh_seconds, settings.daemon_interval_seconds):
        return True
    return not live_items


def _refresh_live_market_payload(settings: LatencyBotSettings, existing_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    current_payload = existing_payload if isinstance(existing_payload, dict) else _read_json(settings.markets_path)
    current_items = current_payload.get("items", []) if isinstance(current_payload.get("items"), list) else []
    live_items = _filter_live_market_items(current_items, lookahead_minutes=settings.discovery_lookahead_minutes)
    if _markets_payload_needs_refresh(settings, current_payload, live_items):
        try:
            discovered = discover_latency_markets(settings)
            discovered_live_items = _filter_live_market_items(
                discovered.get("items", []) if isinstance(discovered.get("items"), list) else [],
                lookahead_minutes=settings.discovery_lookahead_minutes,
            )
            if not discovered_live_items and live_items:
                source = discovered.get("source", {}) if isinstance(discovered.get("source"), dict) else {}
                payload = {
                    **current_payload,
                    "generated_at": current_payload.get("generated_at") or _now_iso(),
                    "items": live_items,
                    "count": len(live_items),
                    "source": {
                        **source,
                        "name": source.get("name", "gamma_events_by_tag"),
                        "warning": "empty discovery result preserved existing live registry",
                        "preserved_existing_count": len(live_items),
                    },
                }
            else:
                live_items = discovered_live_items
                payload = {
                    **discovered,
                    "generated_at": discovered.get("generated_at") or _now_iso(),
                    "items": live_items,
                    "count": len(live_items),
                }
        except Exception as exc:
            source = current_payload.get("source", {}) if isinstance(current_payload.get("source"), dict) else {}
            payload = {
                **current_payload,
                "generated_at": current_payload.get("generated_at") or _now_iso(),
                "items": live_items,
                "count": len(live_items),
                "source": {
                    **source,
                    "name": source.get("name", "gamma_events_by_tag"),
                    "error": str(exc),
                },
            }
    else:
        payload = {
            **current_payload,
            "generated_at": current_payload.get("generated_at") or _now_iso(),
            "items": live_items,
            "count": len(live_items),
        }
    replace_markets(settings, payload.get("items", []))
    _write_json(settings.markets_path, payload)
    return payload


def _status_payload(
    *,
    runner_status: str,
    phase: str,
    started_at: str | None,
    completed_at: str | None,
    last_error: str | None,
    tracked_markets_count: int,
    open_orders_count: int,
    open_positions_count: int,
    realized_pnl_usdc: float,
    unrealized_pnl_usdc: float,
    risk_state: str,
    notes: list[str],
    last_cycle_result: dict[str, Any],
) -> dict[str, Any]:
    status = LatencyBotStatus(
        runner_status=runner_status,
        phase=phase,
        last_cycle_started_at=started_at,
        last_cycle_completed_at=completed_at,
        last_error=last_error,
        tracked_markets_count=tracked_markets_count,
        open_orders_count=open_orders_count,
        open_positions_count=open_positions_count,
        realized_pnl_usdc=realized_pnl_usdc,
        unrealized_pnl_usdc=unrealized_pnl_usdc,
        risk_state=risk_state,
        notes=notes,
        last_cycle_result=last_cycle_result,
    )
    return status.to_dict()


def latency_bot_init(settings: LatencyBotSettings) -> dict[str, Any]:
    settings.ensure_dirs()
    db_init = init_latency_bot_db(settings)
    markets_payload = {
        "generated_at": _now_iso(),
        "items": [],
        "count": 0,
        "assets": list(settings.assets),
        "tenors_minutes": list(settings.tenors_minutes),
    }
    _write_json(settings.markets_path, markets_payload)
    result = {
        "initialized_at": _now_iso(),
        "db_path": str(settings.db_path),
        "status_path": str(settings.status_path),
        "markets_path": str(settings.markets_path),
        "assets": list(settings.assets),
        "tenors_minutes": list(settings.tenors_minutes),
    }
    _write_json(
        settings.status_path,
        _status_payload(
            runner_status="ready",
            phase="bootstrap",
            started_at=result["initialized_at"],
            completed_at=result["initialized_at"],
            last_error=None,
            tracked_markets_count=0,
            open_orders_count=0,
            open_positions_count=0,
            realized_pnl_usdc=0.0,
            unrealized_pnl_usdc=0.0,
            risk_state="bootstrap",
            notes=[
                "Latency bot scaffold initialized.",
                "Feeds, fair value, and paper execution will be layered on top of this schema.",
            ],
            last_cycle_result=result,
        ),
    )
    return {
        "ok": True,
        "db": db_init,
        "status": result,
    }


def latency_bot_discovery_cycle(settings: LatencyBotSettings) -> dict[str, Any]:
    settings.ensure_dirs()
    init_latency_bot_db(settings)
    started_at = _now_iso()
    last_error: str | None = None
    notes: list[str]
    try:
        markets_payload = _refresh_live_market_payload(settings)
        tracked_markets_count = int(markets_payload.get("count", 0) or 0)
        source = markets_payload.get("source", {}) if isinstance(markets_payload.get("source"), dict) else {}
        notes = [
            "Tag-driven Gamma discovery active for configured short-duration crypto markets.",
            f"Fetched {int(source.get('fetched_count', 0) or 0)} candidate markets across {int(source.get('tag_candidate_count', 0) or 0)} tags.",
        ]
        result = {
            "phase": "discovery",
            "cycle_started_at": started_at,
            "cycle_completed_at": _now_iso(),
            "tracked_markets_count": tracked_markets_count,
            "assets": list(settings.assets),
            "tenors_minutes": list(settings.tenors_minutes),
            "source": source,
            "notes": notes,
        }
    except Exception as exc:
        tracked_markets_count = 0
        last_error = str(exc)
        notes = [
            "Discovery cycle failed.",
            f"Error: {last_error}",
        ]
        result = {
            "phase": "discovery",
            "cycle_started_at": started_at,
            "cycle_completed_at": _now_iso(),
            "tracked_markets_count": 0,
            "assets": list(settings.assets),
            "tenors_minutes": list(settings.tenors_minutes),
            "source": {"name": "gamma_events_by_tag", "error": last_error},
            "notes": notes,
        }
    record_engine_cycle(
        settings,
        ts=result["cycle_completed_at"],
        phase="discovery",
        markets_tracked=tracked_markets_count,
        signals_seen=0,
        orders_open=0,
        positions_open=0,
        risk_state="bootstrap",
        notes="scaffold discovery cycle",
    )
    _write_json(
        settings.status_path,
        _status_payload(
            runner_status="running" if last_error is None else "degraded",
            phase="discovery",
            started_at=started_at,
            completed_at=result["cycle_completed_at"],
            last_error=last_error,
            tracked_markets_count=tracked_markets_count,
            open_orders_count=0,
            open_positions_count=0,
            realized_pnl_usdc=0.0,
            unrealized_pnl_usdc=0.0,
            risk_state="bootstrap",
            notes=notes,
            last_cycle_result=result,
        ),
    )
    return result


def latency_bot_engine_cycle(settings: LatencyBotSettings) -> dict[str, Any]:
    settings.ensure_dirs()
    init_latency_bot_db(settings)
    started_at = _now_iso()
    markets_payload = _refresh_live_market_payload(settings, _read_json(settings.markets_path))
    market_items = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    tracked_markets_count = len(market_items)
    cli = PolymarketCLI("polymarket", timeout_seconds=settings.polymarket_cli_timeout_seconds)
    polymarket_cache = refresh_polymarket_cache(settings, markets_payload, cli=cli)
    replace_polymarket_books(settings, polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else [], ts=_now_iso())
    binance_cache = refresh_binance_cache(settings)
    replace_binance_ticks(settings, binance_cache.get("items", []) if isinstance(binance_cache.get("items"), list) else [], ts=_now_iso())
    fair_values = build_fair_values(settings, markets_payload=markets_payload, polymarket_cache=polymarket_cache, binance_cache=binance_cache)
    append_fair_values(settings, fair_values, ts=_now_iso())
    signals = build_signals(settings, markets_payload=markets_payload, polymarket_cache=polymarket_cache, fair_values=fair_values)
    append_signals(settings, signals, ts=_now_iso())
    shadow_no_signals = build_shadow_btc_no_signals(
        settings,
        markets_payload=markets_payload,
        polymarket_cache=polymarket_cache,
        fair_values=fair_values,
    )
    append_shadow_signals(settings, shadow_no_signals, ts=_now_iso())
    shadow_yes_variant_signals = build_shadow_btc_yes_variant_signals(
        settings,
        markets_payload=markets_payload,
        polymarket_cache=polymarket_cache,
        fair_values=fair_values,
    )
    append_shadow_variant_signals(settings, shadow_yes_variant_signals, ts=_now_iso())
    complete_set_arb_signals = build_complete_set_arb_signals(
        settings,
        markets_payload=markets_payload,
        polymarket_cache=polymarket_cache,
    )
    append_complete_set_arb_signals(settings, complete_set_arb_signals, ts=_now_iso())
    cex_latency_paper_signals = build_cex_latency_paper_signals(
        settings,
        markets_payload=markets_payload,
        polymarket_cache=polymarket_cache,
        fair_values=fair_values,
    )
    append_cex_latency_paper_signals(settings, cex_latency_paper_signals, ts=_now_iso())
    btc_fair_value_paper_signals = build_btc_fair_value_paper_signals(
        settings,
        markets_payload=markets_payload,
        polymarket_cache=polymarket_cache,
        fair_values=fair_values,
    )
    append_cex_latency_paper_signals(settings, btc_fair_value_paper_signals, ts=_now_iso())
    temporal_inventory_maker_paper_signals = build_temporal_inventory_maker_paper_signals(
        settings,
        markets_payload=markets_payload,
        polymarket_cache=polymarket_cache,
        fair_values=fair_values,
    )
    late_resolution_capture_paper_signals = build_late_resolution_capture_paper_signals(
        settings,
        markets_payload=markets_payload,
        polymarket_cache=polymarket_cache,
        fair_values=fair_values,
    )
    append_late_resolution_capture_signals(settings, late_resolution_capture_paper_signals, ts=_now_iso())
    wallet_teacher_trades = fetch_wallet_teacher_trades(settings)
    wallet_teacher_sniper_signals = build_wallet_teacher_sniper_signals(
        settings,
        markets_payload=markets_payload,
        polymarket_cache=polymarket_cache,
        wallet_trades_payload=wallet_teacher_trades,
    )
    append_cex_latency_paper_signals(settings, wallet_teacher_sniper_signals, ts=_now_iso())
    execution = run_paper_execution_cycle(
        settings,
        markets_payload=markets_payload,
        polymarket_cache=polymarket_cache,
        signals=signals,
        ts=_now_iso(),
    )
    shadow_execution = run_shadow_btc_no_paper_cycle(
        settings,
        markets_payload=markets_payload,
        polymarket_cache=polymarket_cache,
        signals=shadow_no_signals,
        ts=_now_iso(),
    )
    shadow_yes_variant_execution = run_shadow_btc_yes_variant_paper_cycle(
        settings,
        markets_payload=markets_payload,
        polymarket_cache=polymarket_cache,
        signals=shadow_yes_variant_signals,
        ts=_now_iso(),
    )
    promoted_variant_execution = run_promoted_variant_paper_cycle(
        settings,
        markets_payload=markets_payload,
        polymarket_cache=polymarket_cache,
        signals=shadow_yes_variant_signals,
        ts=_now_iso(),
    )
    complete_set_arb_execution = run_complete_set_arb_paper_cycle(
        settings,
        signals=complete_set_arb_signals,
        ts=_now_iso(),
    )
    cex_latency_paper_execution = run_cex_latency_paper_cycle(
        settings,
        markets_payload=markets_payload,
        polymarket_cache=polymarket_cache,
        signals=cex_latency_paper_signals,
        ts=_now_iso(),
    )
    btc_fair_value_paper_execution = run_btc_fair_value_paper_cycle(
        settings,
        markets_payload=markets_payload,
        polymarket_cache=polymarket_cache,
        signals=btc_fair_value_paper_signals,
        ts=_now_iso(),
    )
    temporal_inventory_maker_paper_execution = run_temporal_inventory_maker_paper_cycle(
        settings,
        markets_payload=markets_payload,
        polymarket_cache=polymarket_cache,
        signals=temporal_inventory_maker_paper_signals,
        ts=_now_iso(),
    )
    live_temporal_inventory_maker_execution = run_live_temporal_inventory_maker_cycle(
        settings,
        markets_payload=markets_payload,
        signals=temporal_inventory_maker_paper_signals,
        ts=_now_iso(),
    )
    late_resolution_capture_paper_execution = run_late_resolution_capture_paper_cycle(
        settings,
        markets_payload=markets_payload,
        polymarket_cache=polymarket_cache,
        signals=late_resolution_capture_paper_signals,
        ts=_now_iso(),
    )
    wallet_teacher_sniper_execution = run_wallet_teacher_sniper_paper_cycle(
        settings,
        markets_payload=markets_payload,
        polymarket_cache=polymarket_cache,
        signals=wallet_teacher_sniper_signals,
        ts=_now_iso(),
    )
    live_complete_set_arb_pilot = run_live_complete_set_arb_pilot_cycle(
        settings,
        markets_payload=markets_payload,
        signals=complete_set_arb_signals,
        ts=_now_iso(),
        cli=cli,
    )
    risk = risk_snapshot(settings)
    polymarket_items = polymarket_cache.get("items", []) if isinstance(polymarket_cache.get("items"), list) else []
    binance_items = binance_cache.get("items", []) if isinstance(binance_cache.get("items"), list) else []
    open_orders = load_open_orders(settings)
    open_positions = load_open_positions(settings)
    risk_state = "active" if open_positions else "quoting" if open_orders else "bootstrap"
    portfolio = latency_bot_portfolio_summary(settings, cache_items=polymarket_items)
    shadow_portfolio = latency_bot_shadow_portfolio_summary(settings, cache_items=polymarket_items)
    cycle_completed_at = _now_iso()
    record_equity_snapshot(
        settings,
        ts=cycle_completed_at,
        realized_pnl_usdc=float(portfolio.get("realized_pnl_usdc", 0.0)),
        unrealized_pnl_usdc=float(portfolio.get("unrealized_pnl_usdc", 0.0)),
        equity_usdc=float(portfolio.get("equity_usdc", settings.bankroll_usdc)),
    )
    result = {
        "phase": "engine",
        "cycle_started_at": started_at,
        "cycle_completed_at": cycle_completed_at,
        "tracked_markets_count": tracked_markets_count,
        "polymarket_cache_count": len(polymarket_items),
        "binance_cache_count": len(binance_items),
        "polymarket_errors": list(polymarket_cache.get("errors", []))[:10] if isinstance(polymarket_cache.get("errors"), list) else [],
        "binance_errors": list(binance_cache.get("errors", []))[:10] if isinstance(binance_cache.get("errors"), list) else [],
        "fair_values_count": len(fair_values),
        "signals_seen": len(signals),
        "open_orders_count": len(open_orders),
        "open_positions_count": len(open_positions),
        "risk_state": risk_state,
        "execution": execution,
        "shadow_no": {
            "signals_seen": len(shadow_no_signals),
            "execution": shadow_execution,
            "portfolio": shadow_portfolio,
        },
        "shadow_yes_variants": {
            "signals_seen": len(shadow_yes_variant_signals),
            "execution": shadow_yes_variant_execution,
        },
        "promoted_variants": {
            "signals_seen": len(shadow_yes_variant_signals),
            "execution": promoted_variant_execution,
        },
        "complete_set_arb": {
            "signals_seen": len(complete_set_arb_signals),
            "execution": complete_set_arb_execution,
        },
        "cex_latency_paper": {
            "signals_seen": len(cex_latency_paper_signals),
            "execution": cex_latency_paper_execution,
        },
        "btc_fair_value_paper": {
            "signals_seen": len(btc_fair_value_paper_signals),
            "execution": btc_fair_value_paper_execution,
        },
        "temporal_inventory_maker_paper": {
            "signals_seen": len(temporal_inventory_maker_paper_signals),
            "execution": temporal_inventory_maker_paper_execution,
        },
        "live_temporal_inventory_maker": {
            "signals_seen": len(temporal_inventory_maker_paper_signals),
            "execution": live_temporal_inventory_maker_execution,
        },
        "late_resolution_capture_paper": {
            "signals_seen": len(late_resolution_capture_paper_signals),
            "execution": late_resolution_capture_paper_execution,
        },
        "wallet_teacher_sniper": {
            "signals_seen": len(wallet_teacher_sniper_signals),
            "trade_fetch_errors": list(wallet_teacher_trades.get("errors", []))[:10]
            if isinstance(wallet_teacher_trades.get("errors"), list)
            else [],
            "execution": wallet_teacher_sniper_execution,
        },
        "live_complete_set_arb_pilot": {
            "signals_seen": len(complete_set_arb_signals),
            "execution": live_complete_set_arb_pilot,
        },
        "risk": risk,
        "notes": [
            "Direct feed adapters active.",
            "Discovery auto-refresh runs when the local market registry is empty.",
            "Polymarket books prefer public CLOB REST snapshots, with CLI and shared-tape fallback behind them.",
            "Binance prices come from direct REST snapshots.",
            "Taker paper execution is active when sufficient Binance history exists.",
            "Current live strategy posture is BTC-first 5m YES taker only unless ETH, 15m, maker, or NO paths are explicitly re-enabled in config.",
            "Shadow BTC 5m NO taker evaluation runs in parallel without affecting live paper execution.",
            "Shadow taker strategy/model variants run in parallel across configured assets, sides, and tenors without affecting live paper execution.",
            "Configured promoted variants execute through live paper positions and feed realized revenue projections.",
            "Complete-set arb prototype scans paired YES/NO asks and paper-locks paired positions when net cost is below $1.",
            "CEX-latency directional paper bot runs separately with its own $1,000 paper bankroll.",
            "Temporal inventory maker paper bot is paper-only and only counts locked pairs after both sides are actually owned.",
            "Live temporal inventory maker is fail-closed behind dry-run/live mode, reconciliation, heartbeat, cancel-all, and confirmation gates.",
            "Late-resolution capture is a separate paper-only module with capped risk and its own PnL.",
            "Wallet-teacher sniper paper bot watches a public target wallet and paper-copies recent matching 5m market buys.",
            "Live complete-set arb pilot is tracked separately and defaults to dry-run/safety-blocked mode.",
        ],
    }
    record_engine_cycle(
        settings,
        ts=result["cycle_completed_at"],
        phase="engine",
        markets_tracked=tracked_markets_count,
        signals_seen=len(signals),
        orders_open=len(open_orders),
        positions_open=len(open_positions),
        risk_state=risk_state,
        notes="engine cycle",
    )
    _write_json(
        settings.status_path,
        _status_payload(
            runner_status="running",
        phase="engine",
        started_at=started_at,
        completed_at=result["cycle_completed_at"],
        last_error=None,
        tracked_markets_count=tracked_markets_count,
        open_orders_count=len(open_orders),
        open_positions_count=len(open_positions),
        realized_pnl_usdc=float(portfolio.get("realized_pnl_usdc") or 0.0),
        unrealized_pnl_usdc=float(portfolio.get("unrealized_pnl_usdc") or 0.0),
        risk_state=risk_state,
            notes=result["notes"],
            last_cycle_result=result,
        ),
    )
    return result


def run_latency_bot_engine_daemon(settings: LatencyBotSettings, interval: int | None = None) -> None:
    _lock_handle = _acquire_engine_daemon_lock(settings)
    delay = max(1, int(interval or settings.daemon_interval_seconds))
    while True:
        try:
            latency_bot_engine_cycle(settings)
        except Exception as exc:
            last_status = _read_json(settings.status_path)
            notes = [
                "Latency bot engine cycle failed; daemon will retry.",
                f"Error: {exc}",
            ]
            _write_json(
                settings.status_path,
                _status_payload(
                    runner_status="degraded",
                    phase="engine",
                    started_at=last_status.get("last_cycle_started_at"),
                    completed_at=last_status.get("last_cycle_completed_at"),
                    last_error=str(exc),
                    tracked_markets_count=int(last_status.get("tracked_markets_count", 0) or 0),
                    open_orders_count=int(last_status.get("open_orders_count", 0) or 0),
                    open_positions_count=int(last_status.get("open_positions_count", 0) or 0),
                    realized_pnl_usdc=float(last_status.get("realized_pnl_usdc", 0.0) or 0.0),
                    unrealized_pnl_usdc=float(last_status.get("unrealized_pnl_usdc", 0.0) or 0.0),
                    risk_state=str(last_status.get("risk_state") or "degraded"),
                    notes=notes,
                    last_cycle_result=last_status.get("last_cycle_result", {}) if isinstance(last_status.get("last_cycle_result"), dict) else {},
                ),
            )
        time.sleep(delay)


def latency_bot_us_arb_probe_cycle(settings: LatencyBotSettings) -> dict[str, Any]:
    settings.ensure_dirs()
    init_latency_bot_db(settings)
    ts = _now_iso()
    ticks = fetch_polymarket_us_arb_ticks(settings, ts=ts)
    append_polymarket_us_arb_ticks(settings, ticks, ts=ts)
    errors = [str(item.get("error") or "") for item in ticks if str(item.get("error") or "").strip()]
    crossed = [item for item in ticks if float(item.get("gross_edge") or 0.0) > 0.0]
    return {
        "phase": "polymarket_us_arb_probe",
        "cycle_completed_at": _now_iso(),
        "symbols_configured": len(settings.polymarket_us_arb_symbols),
        "ticks_recorded": len(ticks),
        "crossed_books_seen": len(crossed),
        "best_gross_edge": max((float(item.get("gross_edge") or 0.0) for item in ticks), default=0.0),
        "errors": errors[:10],
        "notes": [
            "Research-only Polymarket US crossed-book probe.",
            "No live orders are submitted.",
            "Dashboard simulation replays these raw ticks with FOK-style depth, latency, slippage, and failure assumptions.",
        ],
    }


def run_latency_bot_us_arb_probe_daemon(settings: LatencyBotSettings, interval: int | None = None) -> None:
    delay = max(1, int(interval or settings.daemon_interval_seconds))
    while True:
        try:
            latency_bot_us_arb_probe_cycle(settings)
        except Exception:
            pass
        time.sleep(delay)


def latency_bot_kalshi_arb_probe_cycle(settings: LatencyBotSettings) -> dict[str, Any]:
    settings.ensure_dirs()
    init_latency_bot_db(settings)
    ts = _now_iso()
    ticks = fetch_kalshi_arb_ticks(settings, ts=ts)
    append_kalshi_arb_ticks(settings, ticks, ts=ts)
    errors = [str(item.get("error") or "") for item in ticks if str(item.get("error") or "").strip()]
    crossed = [item for item in ticks if float(item.get("gross_edge") or 0.0) > 0.0]
    tickers = {str(item.get("ticker") or "") for item in ticks if str(item.get("ticker") or "").strip()}
    return {
        "phase": "kalshi_arb_probe",
        "cycle_completed_at": _now_iso(),
        "auto_discover": bool(settings.kalshi_arb_auto_discover),
        "assets": list(settings.kalshi_arb_assets),
        "tickers_configured": len(settings.kalshi_arb_tickers),
        "tickers_seen": len(tickers),
        "ticks_recorded": len(ticks),
        "complete_set_edges_seen": len(crossed),
        "best_gross_edge": max((float(item.get("gross_edge") or 0.0) for item in ticks), default=0.0),
        "errors": errors[:10],
        "notes": [
            "Research-only Kalshi complete-set arb probe.",
            "No live orders are submitted.",
            "Dashboard simulation replays raw Kalshi ticks with depth, latency, fees, capital lockup, and failure assumptions.",
        ],
    }


def run_latency_bot_kalshi_arb_probe_daemon(settings: LatencyBotSettings, interval: int | None = None) -> None:
    delay = max(1, int(interval or settings.daemon_interval_seconds))
    while True:
        try:
            latency_bot_kalshi_arb_probe_cycle(settings)
        except Exception:
            pass
        time.sleep(delay)


def latency_bot_summarize(settings: LatencyBotSettings, minutes: int = 60) -> dict[str, Any]:
    settings.ensure_dirs()
    init_latency_bot_db(settings)
    return {
        "status": _read_json(settings.status_path),
        "markets": _read_json(settings.markets_path),
        "db": summarize_latency_bot_db(settings, minutes),
    }
