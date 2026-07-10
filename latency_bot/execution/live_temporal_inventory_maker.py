from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from ..config import LatencyBotSettings
from ..storage import (
    connect_latency_bot_db,
    latency_bot_temporal_inventory_maker_paper_stats,
    load_live_temporal_inventory_maker_open_orders,
    record_live_temporal_inventory_maker_heartbeat,
    record_live_temporal_inventory_maker_order,
    record_maker_rebate_event,
    update_live_temporal_inventory_maker_order,
)
from ..strategy_truth import strategy_validation_gate
from .polymarket_live_client import PolymarketLiveCompleteSetClient, PolymarketLivePreflight


CONFIRM_LIVE_TEMPORAL_INVENTORY_MAKER = "LIVE_TEMPORAL_INVENTORY_MAKER"


def _parse_ts(raw: str) -> datetime:
    text = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _json_payload(payload: Any) -> str:
    try:
        return json.dumps(payload, sort_keys=True)
    except TypeError:
        return json.dumps({"repr": repr(payload)}, sort_keys=True)


def _order_id(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    for key in ("orderID", "orderId", "order_id", "id", "hash"):
        value = payload.get(key)
        if value:
            return str(value)
    for key in ("response", "order"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            found = _order_id(nested)
            if found:
                return found
    return ""


def _money_from_micro(raw: Any) -> float:
    try:
        return float(raw or 0.0) / 1_000_000.0
    except (TypeError, ValueError):
        return 0.0


def _latest_heartbeat_ts(settings: LatencyBotSettings) -> datetime | None:
    with connect_latency_bot_db(settings) as conn:
        row = conn.execute(
            """
            SELECT ts
            FROM live_temporal_inventory_maker_heartbeats
            ORDER BY ts DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
    if row is None:
        return None
    try:
        return _parse_ts(str(row["ts"] or ""))
    except Exception:
        return None


def _local_live_loss_24h(settings: LatencyBotSettings, *, ts: str) -> float:
    cutoff = (_parse_ts(ts).timestamp() - 24.0 * 3600.0)
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT ts_created, notional_usdc, status
            FROM live_temporal_inventory_maker_orders
            WHERE status IN ('failed', 'cancel_failed')
            ORDER BY ts_created ASC
            """
        ).fetchall()
    loss = 0.0
    for row in rows:
        try:
            if _parse_ts(str(row["ts_created"] or "")).timestamp() < cutoff:
                continue
        except Exception:
            continue
        loss += float(row["notional_usdc"] or 0.0)
    return round(loss, 6)


def _book_ask_for_side(signal: dict[str, Any], side: str) -> float:
    return float(signal.get("yes_ask") or 0.0) if side == "YES" else float(signal.get("no_ask") or 0.0)


def _token_id_for_side(market: dict[str, Any] | None, side: str) -> str:
    if not isinstance(market, dict):
        return ""
    return str(market.get("yes_token_id") or "") if side == "YES" else str(market.get("no_token_id") or "")


def _cancel_local_open_orders(
    settings: LatencyBotSettings,
    *,
    ts: str,
    client: PolymarketLiveCompleteSetClient | None,
    reason: str,
) -> list[dict[str, Any]]:
    cancelled: list[dict[str, Any]] = []
    open_orders = load_live_temporal_inventory_maker_open_orders(settings)
    if not open_orders:
        return cancelled
    if client is None:
        for order in open_orders:
            update_live_temporal_inventory_maker_order(
                settings,
                local_order_id=str(order.get("local_order_id") or ""),
                ts=ts,
                status="cancel_failed",
                reason=reason,
                error="cancel client unavailable",
            )
        return [{"reason": reason, "error": "cancel client unavailable", "count": len(open_orders)}]
    try:
        payload = client.cancel_all_orders()
        status = "cancelled" if bool(payload.get("success")) else "cancel_failed"
        for order in open_orders:
            update_live_temporal_inventory_maker_order(
                settings,
                local_order_id=str(order.get("local_order_id") or ""),
                ts=ts,
                status=status,
                reason=reason,
                cancel_payload=payload,
            )
        cancelled.append({"reason": reason, "count": len(open_orders), "payload": payload})
    except Exception as exc:
        for order in open_orders:
            update_live_temporal_inventory_maker_order(
                settings,
                local_order_id=str(order.get("local_order_id") or ""),
                ts=ts,
                status="cancel_failed",
                reason=reason,
                error=str(exc),
            )
        cancelled.append({"reason": reason, "error": str(exc), "count": len(open_orders)})
    return cancelled


def run_live_temporal_inventory_maker_cycle(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    signals: list[dict[str, Any]],
    ts: str,
) -> dict[str, Any]:
    mode = str(settings.live_temporal_inventory_maker_mode or "dry_run").lower()
    enabled = bool(settings.live_temporal_inventory_maker_enabled)
    armed = enabled and mode == "live" and settings.live_temporal_inventory_maker_confirm == CONFIRM_LIVE_TEMPORAL_INVENTORY_MAKER
    markets = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    market_by_id = {str(item.get("market_id") or ""): item for item in markets if isinstance(item, dict)}
    open_orders = load_live_temporal_inventory_maker_open_orders(settings)
    submitted: list[dict[str, Any]] = []
    blocks: list[dict[str, Any]] = []
    cancelled: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    notes: list[str] = []

    if not enabled:
        record_live_temporal_inventory_maker_heartbeat(
            settings,
            ts=ts,
            status="disabled",
            armed=False,
            open_orders=len(open_orders),
            cancel_all_ok=False,
            reason="live temporal inventory maker disabled",
        )
        return {
            "enabled": False,
            "mode": mode,
            "armed": False,
            "submitted_count": 0,
            "dry_run_count": 0,
            "blocked_count": 0,
            "cancelled_count": 0,
            "failed_count": 0,
            "submitted": [],
            "blocks": [],
            "cancelled": [],
            "failures": [],
            "notes": ["Live temporal inventory maker disabled."],
        }

    live_client: PolymarketLiveCompleteSetClient | None = None
    preflight = PolymarketLivePreflight(True, "dry-run mode; live CLOB client not required")
    cancel_all_ok = False
    if mode == "live":
        live_client = PolymarketLiveCompleteSetClient(settings)
        preflight = live_client.preflight_maker()
        cancel_all_ok = preflight.ok
        if not preflight.ok:
            notes.append(f"live maker preflight failed: {preflight.reason}")
            live_client = None
    else:
        notes.append("dry-run mode; no live orders submitted")

    heartbeat_timeout = max(int(settings.live_temporal_inventory_maker_heartbeat_timeout_seconds), 1)
    latest_heartbeat = _latest_heartbeat_ts(settings)
    if latest_heartbeat is not None and (_parse_ts(ts) - latest_heartbeat).total_seconds() > heartbeat_timeout:
        cancelled.extend(
            _cancel_local_open_orders(
                settings,
                ts=ts,
                client=live_client,
                reason="heartbeat stale; cancel-all protection triggered",
            )
        )

    max_order_age = max(int(settings.live_temporal_inventory_maker_max_order_age_seconds), 1)
    for order in load_live_temporal_inventory_maker_open_orders(settings):
        try:
            age = (_parse_ts(ts) - _parse_ts(str(order.get("ts_created") or ts))).total_seconds()
        except Exception:
            age = max_order_age + 1
        if age <= max_order_age:
            continue
        clob_order_id = str(order.get("clob_order_id") or "")
        if live_client is None or not clob_order_id:
            update_live_temporal_inventory_maker_order(
                settings,
                local_order_id=str(order.get("local_order_id") or ""),
                ts=ts,
                status="cancel_failed",
                reason="stale live maker order; cancel unavailable",
                error="missing client or CLOB order id",
            )
            failures.append({"market_id": str(order.get("market_id") or ""), "reason": "stale cancel unavailable"})
            continue
        try:
            payload = live_client.cancel_order(clob_order_id)
            update_live_temporal_inventory_maker_order(
                settings,
                local_order_id=str(order.get("local_order_id") or ""),
                ts=ts,
                status="cancelled" if bool(payload.get("success")) else "cancel_failed",
                reason="stale live maker order cancelled",
                cancel_payload=payload,
            )
            cancelled.append({"market_id": str(order.get("market_id") or ""), "payload": payload})
        except Exception as exc:
            update_live_temporal_inventory_maker_order(
                settings,
                local_order_id=str(order.get("local_order_id") or ""),
                ts=ts,
                status="cancel_failed",
                reason="stale live maker order cancel failed",
                error=str(exc),
            )
            failures.append({"market_id": str(order.get("market_id") or ""), "reason": str(exc)})

    paper_stats = latency_bot_temporal_inventory_maker_paper_stats(settings)
    paper_summary = paper_stats.get("summary", {}) if isinstance(paper_stats.get("summary"), dict) else {}
    if bool(settings.live_temporal_inventory_maker_require_positive_paper_pnl) and float(paper_summary.get("marked_pnl_usdc") or 0.0) < 0.0:
        notes.append("paper temporal inventory maker marked PnL is negative")

    clob_cash = 0.0
    clob_open_orders = 0
    clob_open_positions = 0
    clob_recent_trades = 0
    reconciliation_ok = mode != "live" or not bool(settings.live_temporal_inventory_maker_require_reconciliation)
    if live_client is not None:
        try:
            balance = live_client.get_collateral_balance_allowance()
            clob_cash = _money_from_micro(balance.get("balance"))
            clob_open_orders = len(live_client.get_open_orders())
            clob_open_positions = len(live_client.get_positions())
            clob_recent_trades = len(live_client.get_recent_trades())
            reconciliation_ok = True
        except Exception as exc:
            notes.append(f"live maker reconciliation probe failed: {exc}")
            reconciliation_ok = False

    local_open_orders = load_live_temporal_inventory_maker_open_orders(settings)
    heartbeat_status = "armed" if armed and preflight.ok and reconciliation_ok else "dry_run" if mode != "live" else "blocked"
    record_live_temporal_inventory_maker_heartbeat(
        settings,
        ts=ts,
        status=heartbeat_status,
        armed=armed,
        open_orders=len(local_open_orders),
        cancel_all_ok=cancel_all_ok,
        reason="; ".join(notes),
    )

    def block_signal(signal: dict[str, Any], market: dict[str, Any] | None, reason: str) -> None:
        side = str(signal.get("side") or "").upper()
        price = float(signal.get("order_price") or 0.0)
        size = float(settings.live_temporal_inventory_maker_base_order_usdc) / price if price > 0.0 else 0.0
        blocks.append(
            record_live_temporal_inventory_maker_order(
                settings,
                ts=ts,
                market_id=str(signal.get("market_id") or ""),
                asset=str(signal.get("asset") or ""),
                side=side,
                token_id=_token_id_for_side(market, side),
                price=price,
                size=size,
                mode=mode,
                decision="BLOCKED",
                status="blocked",
                edge=float(signal.get("edge") or 0.0),
                reason=reason,
            )
        )

    validation_gate = (
        strategy_validation_gate(settings, "temporal_inventory_maker")
        if settings.live_temporal_inventory_maker_require_validation_gate
        else {"passed": True, "status": "disabled", "reasons": []}
    )
    gate_reason = ""
    if mode == "live" and not armed:
        gate_reason = "live maker mode not armed; set confirmation string locally"
    elif mode == "live" and not preflight.ok:
        gate_reason = f"live maker preflight failed: {preflight.reason}"
    elif mode == "live" and not reconciliation_ok:
        gate_reason = "live maker reconciliation probe failed"
    elif bool(settings.live_temporal_inventory_maker_require_positive_paper_pnl) and float(paper_summary.get("marked_pnl_usdc") or 0.0) < 0.0:
        gate_reason = "paper temporal inventory maker marked PnL is negative"
    elif mode == "live" and not bool(validation_gate.get("passed")):
        gate_reason = "strategy validation blocked: " + "; ".join(str(item) for item in validation_gate.get("reasons", []))
    elif mode == "live" and clob_cash > 0.0 and clob_cash < float(settings.live_temporal_inventory_maker_base_order_usdc):
        gate_reason = "CLOB cash below base live maker order"
    elif _local_live_loss_24h(settings, ts=ts) >= float(settings.live_temporal_inventory_maker_daily_loss_limit_usdc):
        gate_reason = "live maker daily loss limit reached"
    if gate_reason and mode == "live":
        cancelled.extend(_cancel_local_open_orders(settings, ts=ts, client=live_client, reason=f"safety gate failed: {gate_reason}"))

    candidates = [signal for signal in signals if isinstance(signal, dict) and bool(signal.get("eligible"))]
    candidates.sort(key=lambda signal: float(signal.get("edge") or 0.0), reverse=True)
    max_per_cycle = max(int(settings.live_temporal_inventory_maker_max_orders_per_cycle), 0)
    max_open_orders = max(int(settings.live_temporal_inventory_maker_max_open_orders), 0)
    placed = 0
    for signal in candidates:
        if placed >= max_per_cycle:
            break
        local_open_orders = load_live_temporal_inventory_maker_open_orders(settings)
        if len(local_open_orders) >= max_open_orders:
            break
        market_id = str(signal.get("market_id") or "")
        market = market_by_id.get(market_id)
        side = str(signal.get("side") or "").upper()
        price = float(signal.get("order_price") or 0.0)
        edge = float(signal.get("edge") or 0.0)
        if not market_id or side not in {"YES", "NO"} or price <= 0.0:
            continue
        if gate_reason:
            block_signal(signal, market, gate_reason)
            continue
        if edge < float(settings.live_temporal_inventory_maker_min_edge):
            block_signal(signal, market, "live maker edge below live floor")
            continue
        if float(signal.get("seconds_left") or 0.0) < float(settings.live_temporal_inventory_maker_min_seconds_left):
            block_signal(signal, market, "live maker market too close to expiry")
            continue
        ask = _book_ask_for_side(signal, side)
        if ask > 0.0 and price >= ask:
            block_signal(signal, market, "post-only guard blocked crossing quote")
            continue
        token_id = _token_id_for_side(market, side)
        if not token_id:
            block_signal(signal, market, "missing token id for live maker side")
            continue
        notional = min(float(settings.live_temporal_inventory_maker_base_order_usdc), float(settings.live_temporal_inventory_maker_capital_usdc))
        size = notional / price if price > 0.0 else 0.0
        if size <= 0.0:
            block_signal(signal, market, "invalid live maker order size")
            continue
        if mode != "live":
            submitted.append(
                record_live_temporal_inventory_maker_order(
                    settings,
                    ts=ts,
                    market_id=market_id,
                    asset=str(signal.get("asset") or ""),
                    side=side,
                    token_id=token_id,
                    price=price,
                    size=size,
                    mode=mode,
                    decision="DRY_RUN",
                    status="dry_run",
                    edge=edge,
                    reason="dry-run post-only maker candidate; no order submitted",
                )
            )
            placed += 1
            continue
        assert live_client is not None
        try:
            payload = live_client.post_limit_buy(token_id=token_id, price=price, size=size)
            order_id = _order_id(payload)
            status = "open" if bool(payload.get("success")) else "failed"
            decision = "SUBMITTED" if bool(payload.get("success")) else "FAILED"
            row = record_live_temporal_inventory_maker_order(
                settings,
                ts=ts,
                market_id=market_id,
                asset=str(signal.get("asset") or ""),
                side=side,
                token_id=token_id,
                price=price,
                size=size,
                mode=mode,
                decision=decision,
                status=status,
                edge=edge,
                reason="post-only GTC maker order submitted" if status == "open" else "post-only maker order rejected",
                clob_order_id=order_id,
                order_payload=payload,
                error="" if status == "open" else _json_payload(payload),
            )
            if status == "open":
                submitted.append(row)
                try:
                    scoring = live_client.get_order_scoring_status(order_id) if order_id else {"available": False, "scoring": None}
                    scoring_value = scoring.get("scoring")
                    scoring_status = "scoring" if scoring_value is True else "not_scoring" if scoring_value is False else "unavailable"
                    record_maker_rebate_event(
                        settings,
                        ts=ts,
                        strategy_id="temporal_inventory_maker",
                        market_id=market_id,
                        order_id=order_id,
                        scoring_status=scoring_status,
                        rebate_usdc=0.0,
                        metadata=scoring,
                    )
                except Exception as scoring_exc:
                    record_maker_rebate_event(
                        settings,
                        ts=ts,
                        strategy_id="temporal_inventory_maker",
                        market_id=market_id,
                        order_id=order_id,
                        scoring_status="error",
                        rebate_usdc=0.0,
                        metadata={"error": str(scoring_exc)},
                    )
            else:
                failures.append(row)
            placed += 1
        except Exception as exc:
            failures.append(
                record_live_temporal_inventory_maker_order(
                    settings,
                    ts=ts,
                    market_id=market_id,
                    asset=str(signal.get("asset") or ""),
                    side=side,
                    token_id=token_id,
                    price=price,
                    size=size,
                    mode=mode,
                    decision="FAILED",
                    status="failed",
                    edge=edge,
                    reason="post-only maker order submit failed",
                    error=str(exc),
                )
            )
            placed += 1

    return {
        "enabled": enabled,
        "mode": mode,
        "armed": armed,
        "submitted_count": sum(1 for item in submitted if str(item.get("decision") or "") == "SUBMITTED"),
        "dry_run_count": sum(1 for item in submitted if str(item.get("decision") or "") == "DRY_RUN"),
        "blocked_count": len(blocks),
        "cancelled_count": len(cancelled),
        "failed_count": len(failures),
        "submitted": submitted,
        "blocks": blocks,
        "cancelled": cancelled,
        "failures": failures,
        "validation_gate": validation_gate,
        "clob_cash_usdc": round(clob_cash, 6),
        "clob_open_orders": clob_open_orders,
        "notes": notes,
        "clob_open_positions": clob_open_positions,
        "clob_recent_trades": clob_recent_trades,
    }
