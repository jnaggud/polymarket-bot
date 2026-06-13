from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from bot.core import PolymarketCLI

from ..config import LatencyBotSettings
from ..storage import (
    _live_pilot_extract_matched_legs,
    _live_pilot_get_market_resolution,
    connect_latency_bot_db,
    record_live_complete_set_arb_pilot_attempt,
)
from .polymarket_live_client import PolymarketLiveCompleteSetClient


CONFIRM_LIVE_COMPLETE_SET_ARB_PILOT = "LIVE_COMPLETE_SET_ARB_PILOT"


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
    for key in ("orderID", "order_id", "id", "hash"):
        value = payload.get(key)
        if value:
            return str(value)
    nested = payload.get("order")
    if isinstance(nested, dict):
        return _order_id(nested)
    return ""


def _paired_payload_has_matched_leg(payload: Any) -> bool:
    response = payload.get("response") if isinstance(payload, dict) else None
    if isinstance(response, list):
        legs = response
    elif isinstance(response, dict):
        legs = [response]
    else:
        legs = [payload.get("yes_order"), payload.get("no_order")] if isinstance(payload, dict) else []
    for leg in legs:
        if not isinstance(leg, dict):
            continue
        status = str(leg.get("status") or "").lower()
        if status in {"matched", "filled", "partially_matched"}:
            return True
    return False


def _float_from_payload(payload: Any, key: str) -> float:
    if not isinstance(payload, dict):
        return 0.0
    try:
        return float(payload.get(key) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _matched_leg_details(payload: Any, *, yes_token_id: str, no_token_id: str) -> dict[str, Any] | None:
    response = payload.get("response") if isinstance(payload, dict) else None
    if not isinstance(response, list):
        return None
    leg_specs = (
        ("yes", yes_token_id),
        ("no", no_token_id),
    )
    for index, (side, token_id) in enumerate(leg_specs):
        if index >= len(response) or not isinstance(response[index], dict):
            continue
        item = response[index]
        status = str(item.get("status") or "").lower()
        if status not in {"matched", "filled", "partially_matched"}:
            continue
        shares = _float_from_payload(item, "takingAmount")
        entry_spend = _float_from_payload(item, "makingAmount")
        if shares <= 0.0 or entry_spend <= 0.0:
            continue
        return {
            "side": side,
            "token_id": token_id,
            "shares": shares,
            "entry_spend_usdc": entry_spend,
            "response": item,
        }
    return None


def _rescue_proceeds_usdc(payload: Any) -> float:
    response = payload.get("response") if isinstance(payload, dict) else None
    if isinstance(response, list):
        response = response[0] if response else {}
    if not isinstance(response, dict):
        return 0.0
    return _float_from_payload(response, "takingAmount")


def _recent_live_pilot_market_cooldown(settings: LatencyBotSettings, *, market_id: str, ts: str) -> bool:
    cooldown_seconds = max(int(settings.live_complete_set_arb_pilot_same_market_cooldown_seconds), 0)
    if cooldown_seconds <= 0:
        return False
    cutoff = _parse_ts(ts).timestamp() - float(cooldown_seconds)
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT ts
            FROM live_complete_set_arb_pilot_attempts
            WHERE market_id = ?
              AND decision IN ('DRY_RUN', 'SUBMITTED')
            ORDER BY ts DESC, id DESC
            LIMIT 5
            """,
            (market_id,),
        ).fetchall()
    for row in rows:
        try:
            if _parse_ts(str(row["ts"] or "")).timestamp() >= cutoff:
                return True
        except Exception:
            continue
    return False


def _live_pilot_24h_loss(settings: LatencyBotSettings, *, ts: str) -> float:
    cutoff = (_parse_ts(ts).timestamp() - 24.0 * 3600.0)
    with connect_latency_bot_db(settings) as conn:
        rows = conn.execute(
            """
            SELECT ts, market_id, decision, realized_pnl_usdc, error
            FROM live_complete_set_arb_pilot_attempts
            WHERE decision IN ('FAILED', 'ONE_LEG_FAILED', 'SUBMITTED')
            ORDER BY ts ASC, id ASC
            """
        ).fetchall()
    realized_pnl_24h = 0.0
    unresolved_risk = 0.0
    for row in rows:
        row_in_window = True
        try:
            row_in_window = _parse_ts(str(row["ts"] or "")).timestamp() >= cutoff
        except Exception:
            row_in_window = False
        matched_legs = _live_pilot_extract_matched_legs(str(row["error"] or ""))
        if matched_legs:
            resolution = _live_pilot_get_market_resolution(settings, str(row["market_id"] or ""))
            winner = str(resolution.get("winner") or "")
            resolved = bool(resolution.get("resolved")) and winner in {"up", "down"}
            for matched_leg in matched_legs:
                spend = max(float(matched_leg.get("entry_spend_usdc") or 0.0), 0.0)
                shares = max(float(matched_leg.get("shares") or 0.0), 0.0)
                side = str(matched_leg.get("side") or "")
                side_outcome = "up" if side == "yes" else "down" if side == "no" else ""
                if not resolved:
                    # Unresolved one-leg exposure can still expire worthless, so
                    # reserve worst-case spend against the live loss budget.
                    unresolved_risk += spend
                    continue
                pnl = (shares if side_outcome == winner else 0.0) - spend
                if row_in_window:
                    realized_pnl_24h += pnl
            continue
        if not row_in_window:
            continue
        pnl = float(row["realized_pnl_usdc"] or 0.0)
        realized_pnl_24h += pnl
    return round(unresolved_risk + max(0.0, -realized_pnl_24h), 6)


def _record_attempt(
    settings: LatencyBotSettings,
    *,
    ts: str,
    signal: dict[str, Any],
    market: dict[str, Any] | None,
    mode: str,
    decision: str,
    reason: str,
    adjusted_edge: float,
    effective_depth_usdc: float,
    notional_usdc: float,
    size: float,
    expected_pnl_usdc: float,
    realized_pnl_usdc: float = 0.0,
    yes_payload: Any = None,
    no_payload: Any = None,
    error: str = "",
) -> dict[str, Any]:
    return record_live_complete_set_arb_pilot_attempt(
        settings,
        ts=ts,
        market_id=str(signal.get("market_id") or ""),
        asset=str(signal.get("asset") or ""),
        tenor_minutes=int(signal.get("tenor_minutes") or 0),
        mode=mode,
        decision=decision,
        reason=reason,
        yes_token_id=str((market or {}).get("yes_token_id") or ""),
        no_token_id=str((market or {}).get("no_token_id") or ""),
        yes_price=float(signal.get("yes_ask") or 0.0),
        no_price=float(signal.get("no_ask") or 0.0),
        total_cost=float(signal.get("total_cost") or 0.0),
        gross_edge=float(signal.get("gross_edge") or 0.0),
        net_edge=float(signal.get("net_edge") or 0.0),
        adjusted_edge=adjusted_edge,
        executable_depth_usdc=float(signal.get("executable_depth_usdc") or 0.0),
        effective_depth_usdc=effective_depth_usdc,
        notional_usdc=notional_usdc,
        size=size,
        expected_pnl_usdc=expected_pnl_usdc,
        realized_pnl_usdc=realized_pnl_usdc,
        yes_order_id=_order_id(yes_payload),
        no_order_id=_order_id(no_payload),
        yes_order_payload=_json_payload(yes_payload) if yes_payload is not None else "",
        no_order_payload=_json_payload(no_payload) if no_payload is not None else "",
        error=error,
    )


def run_live_complete_set_arb_pilot_cycle(
    settings: LatencyBotSettings,
    *,
    markets_payload: dict[str, Any],
    signals: list[dict[str, Any]],
    ts: str,
    cli: PolymarketCLI | None = None,
) -> dict[str, Any]:
    mode = str(settings.live_complete_set_arb_pilot_mode or "dry_run").lower()
    enabled = bool(settings.live_complete_set_arb_pilot_enabled)
    armed = enabled and mode == "live" and settings.live_complete_set_arb_pilot_confirm == CONFIRM_LIVE_COMPLETE_SET_ARB_PILOT
    markets = markets_payload.get("items", []) if isinstance(markets_payload.get("items"), list) else []
    market_by_id = {str(item.get("market_id") or ""): item for item in markets if isinstance(item, dict)}
    candidates = [signal for signal in signals if isinstance(signal, dict)]
    candidates.sort(key=lambda signal: float(signal.get("net_edge") or 0.0), reverse=True)
    max_attempts = max(int(settings.live_complete_set_arb_pilot_max_sets_per_cycle), 0)
    opened: list[dict[str, Any]] = []
    blocks: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    attempted = 0
    reserved_notional_usdc = 0.0

    if not enabled:
        return {
            "enabled": False,
            "mode": mode,
            "armed": False,
            "submitted_count": 0,
            "dry_run_count": 0,
            "blocked_count": 0,
            "failed_count": 0,
            "submitted": [],
            "blocks": [],
            "failures": [],
            "notes": ["Live complete-set arb pilot disabled."],
        }

    for signal in candidates:
        if attempted >= max_attempts:
            break
        market_id = str(signal.get("market_id") or "")
        market = market_by_id.get(market_id)
        yes_price = float(signal.get("yes_ask") or 0.0)
        no_price = float(signal.get("no_ask") or 0.0)
        total_cost = yes_price + no_price
        if not market_id or total_cost <= 0.0:
            continue
        adjusted_edge = float(signal.get("net_edge") or 0.0) - max(float(settings.live_complete_set_arb_pilot_extra_slippage_per_share), 0.0)
        effective_depth = float(signal.get("executable_depth_usdc") or 0.0) * min(max(float(settings.live_complete_set_arb_pilot_depth_haircut), 0.0), 1.0)
        target_notional = max(float(settings.live_complete_set_arb_pilot_notional_usdc), 0.0)
        remaining_capital = max(float(settings.live_complete_set_arb_pilot_capital_usdc), 0.0) - reserved_notional_usdc
        notional = min(target_notional, effective_depth, max(remaining_capital, 0.0))
        size = notional / total_cost if total_cost > 0.0 else 0.0
        expected_pnl = round(adjusted_edge * size, 6)
        seconds_left = float(signal.get("seconds_left") or 0.0)
        yes_amount = round(notional * (yes_price / total_cost), 6) if total_cost > 0.0 else 0.0
        no_amount = round(notional * (no_price / total_cost), 6) if total_cost > 0.0 else 0.0

        def block(reason: str, decision: str = "BLOCKED", error: str = "") -> None:
            blocks.append(
                _record_attempt(
                    settings,
                    ts=ts,
                    signal=signal,
                    market=market,
                    mode=mode,
                    decision=decision,
                    reason=reason,
                    adjusted_edge=adjusted_edge,
                    effective_depth_usdc=effective_depth,
                    notional_usdc=notional,
                    size=size,
                    expected_pnl_usdc=expected_pnl,
                    error=error,
                )
            )

        if _recent_live_pilot_market_cooldown(settings, market_id=market_id, ts=ts):
            block("same-market live pilot cooldown")
            continue
        original_reason = str(signal.get("reason") or "not eligible")
        if not bool(signal.get("eligible")) and expected_pnl <= 0.0:
            block(f"original complete-set skip: {original_reason}")
            continue
        if adjusted_edge < float(settings.live_complete_set_arb_pilot_min_edge_per_share):
            block("adjusted edge below live pilot floor")
            continue
        if expected_pnl <= 0.0:
            block("expected live pnl not positive")
            continue
        if effective_depth < float(settings.live_complete_set_arb_pilot_min_depth_usdc):
            block("effective paired depth below live pilot minimum")
            continue
        if notional < float(settings.live_complete_set_arb_pilot_min_depth_usdc):
            block("available notional below live pilot minimum")
            continue
        if seconds_left < float(settings.live_complete_set_arb_pilot_min_seconds_left):
            block("live pilot too close to expiry")
            continue
        min_leg_amount = max(float(settings.live_complete_set_arb_pilot_min_leg_amount_usdc), 0.0)
        if yes_amount < min_leg_amount or no_amount < min_leg_amount:
            block("live pilot per-leg amount below CLOB minimum")
            continue
        live_pilot_loss_24h = _live_pilot_24h_loss(settings, ts=ts)
        daily_loss_limit = float(settings.live_complete_set_arb_pilot_daily_loss_limit_usdc)
        if live_pilot_loss_24h >= daily_loss_limit:
            block("daily live pilot loss limit reached")
            continue
        if live_pilot_loss_24h + max(yes_amount, no_amount) > daily_loss_limit:
            block("daily live pilot loss budget insufficient")
            continue
        if market is None:
            block("market metadata missing")
            continue
        yes_token_id = str(market.get("yes_token_id") or "")
        no_token_id = str(market.get("no_token_id") or "")
        if not yes_token_id or not no_token_id:
            block("YES/NO token ids missing")
            continue

        if mode != "live":
            opened.append(
                _record_attempt(
                    settings,
                    ts=ts,
                    signal=signal,
                    market=market,
                    mode=mode,
                    decision="DRY_RUN",
                    reason="dry-run candidate; no orders submitted",
                    adjusted_edge=adjusted_edge,
                    effective_depth_usdc=effective_depth,
                    notional_usdc=notional,
                    size=size,
                    expected_pnl_usdc=expected_pnl,
                )
            )
            reserved_notional_usdc = round(reserved_notional_usdc + notional, 6)
            attempted += 1
            continue

        if not armed:
            block("live mode not armed; set confirmation string locally")
            continue
        if settings.live_complete_set_arb_pilot_require_fok:
            live_client = PolymarketLiveCompleteSetClient(settings)
            preflight = live_client.preflight()
            if not preflight.ok:
                block(f"live CLOB preflight failed: {preflight.reason}")
                attempted += 1
                continue
            try:
                paired_payload = live_client.post_paired_fok_market_buys(
                    yes_token_id=yes_token_id,
                    no_token_id=no_token_id,
                    yes_worst_price=yes_price,
                    no_worst_price=no_price,
                    yes_amount_usdc=yes_amount,
                    no_amount_usdc=no_amount,
                )
            except Exception as exc:
                failed = _record_attempt(
                    settings,
                    ts=ts,
                    signal=signal,
                    market=market,
                    mode=mode,
                    decision="FAILED",
                    reason="paired FOK batch submit failed",
                    adjusted_edge=adjusted_edge,
                    effective_depth_usdc=effective_depth,
                    notional_usdc=notional,
                    size=size,
                    expected_pnl_usdc=expected_pnl,
                    realized_pnl_usdc=0.0,
                    error=str(exc),
                )
                failures.append(failed)
                reserved_notional_usdc = round(reserved_notional_usdc + notional, 6)
                attempted += 1
                continue
            if not bool(paired_payload.get("success")):
                one_leg_matched = _paired_payload_has_matched_leg(paired_payload)
                matched_leg = _matched_leg_details(
                    paired_payload,
                    yes_token_id=yes_token_id,
                    no_token_id=no_token_id,
                )
                rescue_payload: dict[str, Any] | None = None
                realized_pnl = 0.0
                rescue_succeeded = False
                rescue_error = ""
                if (
                    one_leg_matched
                    and matched_leg is not None
                    and bool(settings.live_complete_set_arb_pilot_enable_rescue)
                ):
                    try:
                        rescue_payload = live_client.post_fok_market_sell(
                            token_id=str(matched_leg["token_id"]),
                            shares=float(matched_leg["shares"]),
                        )
                        rescue_succeeded = bool(rescue_payload.get("success"))
                        if rescue_succeeded:
                            realized_pnl = round(
                                _rescue_proceeds_usdc(rescue_payload) - float(matched_leg["entry_spend_usdc"]),
                                6,
                            )
                    except Exception as exc:
                        rescue_error = str(exc)
                decision = "FAILED"
                reason = "paired FOK batch returned unsuccessful response"
                if one_leg_matched:
                    decision = "RESCUED_ONE_LEG" if rescue_succeeded else "ONE_LEG_FAILED"
                    reason = (
                        "one matched leg flattened with FOK rescue sell"
                        if rescue_succeeded
                        else "paired FOK batch returned partial/unfilled leg response"
                    )
                    if one_leg_matched and not bool(settings.live_complete_set_arb_pilot_enable_rescue):
                        reason = "paired FOK partial fill; rescue disabled"
                    elif one_leg_matched and matched_leg is None:
                        reason = "paired FOK partial fill; matched leg details unavailable"
                    elif rescue_error:
                        reason = "paired FOK partial fill; rescue submit failed"
                failed = _record_attempt(
                    settings,
                    ts=ts,
                    signal=signal,
                    market=market,
                    mode=mode,
                    decision=decision,
                    reason=reason,
                    adjusted_edge=adjusted_edge,
                    effective_depth_usdc=effective_depth,
                    notional_usdc=notional,
                    size=size,
                    expected_pnl_usdc=expected_pnl,
                    realized_pnl_usdc=realized_pnl,
                    yes_payload=paired_payload.get("yes_order"),
                    no_payload=paired_payload.get("no_order"),
                    error=_json_payload(
                        {
                            "paired_response": paired_payload.get("response"),
                            "matched_leg": matched_leg,
                            "rescue_response": rescue_payload,
                            "rescue_error": rescue_error,
                        }
                    ),
                )
                failures.append(failed)
                attempted += 1
                continue
            opened.append(
                _record_attempt(
                    settings,
                    ts=ts,
                    signal=signal,
                    market=market,
                    mode=mode,
                    decision="SUBMITTED",
                    reason="paired FOK batch submitted through official CLOB client",
                    adjusted_edge=adjusted_edge,
                    effective_depth_usdc=effective_depth,
                    notional_usdc=notional,
                    size=size,
                    expected_pnl_usdc=expected_pnl,
                    yes_payload=paired_payload.get("yes_order"),
                    no_payload=paired_payload.get("no_order"),
                    error=_json_payload(paired_payload.get("response")),
                )
            )
            reserved_notional_usdc = round(reserved_notional_usdc + notional, 6)
            attempted += 1
            continue
        if not settings.live_complete_set_arb_pilot_allow_sequential_orders:
            block("sequential live orders disabled")
            attempted += 1
            continue
        if cli is None:
            block("polymarket cli unavailable")
            attempted += 1
            continue

        try:
            yes_payload = cli.create_limit_order(yes_token_id, "BUY", yes_price, size)
        except Exception as exc:
            failed = _record_attempt(
                settings,
                ts=ts,
                signal=signal,
                market=market,
                mode=mode,
                decision="FAILED",
                reason="YES leg submit failed",
                adjusted_edge=adjusted_edge,
                effective_depth_usdc=effective_depth,
                notional_usdc=notional,
                size=size,
                expected_pnl_usdc=expected_pnl,
                realized_pnl_usdc=0.0,
                error=str(exc),
            )
            failures.append(failed)
            reserved_notional_usdc = round(reserved_notional_usdc + notional, 6)
            attempted += 1
            continue
        try:
            no_payload = cli.create_limit_order(no_token_id, "BUY", no_price, size)
        except Exception as exc:
            failed = _record_attempt(
                settings,
                ts=ts,
                signal=signal,
                market=market,
                mode=mode,
                decision="ONE_LEG_FAILED",
                reason="NO leg submit failed after YES leg submit",
                adjusted_edge=adjusted_edge,
                effective_depth_usdc=effective_depth,
                notional_usdc=notional,
                size=size,
                expected_pnl_usdc=expected_pnl,
                realized_pnl_usdc=0.0,
                yes_payload=yes_payload,
                error=str(exc),
            )
            failures.append(failed)
            attempted += 1
            continue
        opened.append(
            _record_attempt(
                settings,
                ts=ts,
                signal=signal,
                market=market,
                mode=mode,
                decision="SUBMITTED",
                reason="sequential paired orders submitted; verify fills externally",
                adjusted_edge=adjusted_edge,
                effective_depth_usdc=effective_depth,
                notional_usdc=notional,
                size=size,
                expected_pnl_usdc=expected_pnl,
                yes_payload=yes_payload,
                no_payload=no_payload,
            )
        )
        reserved_notional_usdc = round(reserved_notional_usdc + notional, 6)
        attempted += 1

    return {
        "enabled": enabled,
        "mode": mode,
        "armed": armed,
        "submitted_count": sum(1 for item in opened if str(item.get("decision") or "") == "SUBMITTED"),
        "dry_run_count": sum(1 for item in opened if str(item.get("decision") or "") == "DRY_RUN"),
        "blocked_count": len(blocks),
        "failed_count": len(failures),
        "submitted": opened,
        "blocks": blocks,
        "failures": failures,
        "notes": [
            "Live complete-set arb pilot is isolated from paper/live-paper strategy metrics.",
            "Real order submission requires explicit live mode, confirmation string, official CLOB credentials, and paired FOK batch support.",
        ],
    }
