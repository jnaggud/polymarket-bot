from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bot.accounting import as_float, rebuild_closed_trades
from bot.backtest import _parse_ts, _replay_closed_trade, _replay_open_position
from bot.core import _extract_intraday_question_window

STATE = ROOT / "state"
OUT = STATE / "backtests" / "strategy_variants.json"


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _copy_trade(trade: dict[str, Any]) -> dict[str, Any]:
    return dict(trade)


def _copy_position(position: dict[str, Any]) -> dict[str, Any]:
    return dict(position)


def _mark_index(marks: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(item.get("position_id", "")): item
        for item in marks.get("positions", [])
        if item.get("position_id")
    }


def _summarize(
    name: str,
    trades: list[dict[str, Any]],
    positions: list[dict[str, Any]],
    marks: dict[str, Any],
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rebuilt = rebuild_closed_trades(trades)
    replay_closes = [_replay_closed_trade(trade) for trade in rebuilt]
    marks_by_position = _mark_index(marks)
    replay_positions = [_replay_open_position(position, marks_by_position) for position in positions]
    realized = round(sum(as_float(trade.get("pnl_usdc")) for trade in replay_closes), 2)
    unrealized = round(sum(as_float(position.get("replay_unrealized_pnl_usdc")) for position in replay_positions), 2)
    recent_cutoff = _now() - timedelta(hours=24)
    recent_replay_closes = [
        trade
        for trade in replay_closes
        if (_parse_ts(str(trade.get("closed_at"))) or datetime.min.replace(tzinfo=timezone.utc)) >= recent_cutoff
    ]
    recent_realized = round(sum(as_float(trade.get("pnl_usdc")) for trade in recent_replay_closes), 2)
    return {
        "name": name,
        "trade_events": len(trades),
        "closed_trades": len(replay_closes),
        "open_positions": len(positions),
        "realized_pnl_usdc": realized,
        "unrealized_pnl_usdc": unrealized,
        "net_total_pnl_usdc": round(realized + unrealized, 2),
        "last_24h_realized_pnl_usdc": recent_realized,
        "gross_profit_usdc": round(sum(as_float(t.get("pnl_usdc")) for t in replay_closes if as_float(t.get("pnl_usdc")) > 0), 2),
        "gross_loss_usdc": round(sum(as_float(t.get("pnl_usdc")) for t in replay_closes if as_float(t.get("pnl_usdc")) < 0), 2),
        **(extra or {}),
    }


def _scale_scheduled_trade(trade: dict[str, Any], ratio: float) -> dict[str, Any]:
    scaled = _copy_trade(trade)
    if trade.get("type") in {"OPEN", "CLOSE"}:
        if "shares" in scaled:
            scaled["shares"] = round(as_float(scaled.get("shares")) * ratio, 6)
        if "notional_usdc" in scaled:
            scaled["notional_usdc"] = round(as_float(scaled.get("notional_usdc")) * ratio, 2)
        if "requested_notional_usdc" in scaled:
            scaled["requested_notional_usdc"] = round(as_float(scaled.get("requested_notional_usdc")) * ratio, 2)
        if "pnl_usdc" in scaled:
            scaled["pnl_usdc"] = round(as_float(scaled.get("pnl_usdc")) * ratio, 2)
    return scaled


def _scale_scheduled_position(position: dict[str, Any], ratio: float) -> dict[str, Any]:
    scaled = _copy_position(position)
    if "notional_usdc" in scaled:
        scaled["notional_usdc"] = round(as_float(scaled.get("notional_usdc")) * ratio, 2)
    if "shares" in scaled:
        scaled["shares"] = round(as_float(scaled.get("shares")) * ratio, 6)
    return scaled


def _scheduled_allowed_open(open_trade: dict[str, Any], max_lead_minutes: int) -> tuple[bool, str]:
    question = str(open_trade.get("question", ""))
    opened_at = _parse_ts(str(open_trade.get("opened_at")))
    window = _extract_intraday_question_window(question, now=opened_at or _now())
    if opened_at is None or window is None:
        return False, "missing explicit window"
    start, _end = window
    lead_minutes = (start - opened_at).total_seconds() / 60.0
    if lead_minutes > max_lead_minutes:
        return False, f"lead>{max_lead_minutes}m"
    if lead_minutes < -5:
        return False, "opened after window start"
    return True, "allowed"


def simulate_scheduled(max_lead_minutes: int, size_ratio: float) -> dict[str, Any]:
    raw_trades = _load_json(STATE / "ab_crypto_intraday_scheduled_trades.json", [])
    raw_positions = _load_json(STATE / "ab_crypto_intraday_scheduled_positions.json", [])
    raw_marks = _load_json(STATE / "ab_crypto_intraday_scheduled_marks.json", {"positions": [], "summary": {}})
    allowed_ids: set[str] = set()
    rejection_counts: Counter[str] = Counter()
    filtered_trades: list[dict[str, Any]] = []
    for trade in raw_trades:
        kind = str(trade.get("type", "")).upper()
        position_id = str(trade.get("position_id", ""))
        if kind == "OPEN":
            allowed, reason = _scheduled_allowed_open(trade, max_lead_minutes)
            if allowed:
                allowed_ids.add(position_id)
                filtered_trades.append(_scale_scheduled_trade(trade, size_ratio))
            else:
                rejection_counts[reason] += 1
        elif position_id and position_id in allowed_ids:
            filtered_trades.append(_scale_scheduled_trade(trade, size_ratio))
    filtered_positions = [
        _scale_scheduled_position(position, size_ratio)
        for position in raw_positions
        if str(position.get("position_id", "")) in allowed_ids
    ]
    filtered_marks = {
        **raw_marks,
        "positions": [
            item
            for item in raw_marks.get("positions", [])
            if str(item.get("position_id", "")) in allowed_ids
        ],
    }
    return _summarize(
        name=f"scheduled_lead_{max_lead_minutes}m",
        trades=filtered_trades,
        positions=filtered_positions,
        marks=filtered_marks,
        extra={
            "allowed_position_ids": len(allowed_ids),
            "size_ratio": size_ratio,
            "rejections": dict(rejection_counts),
        },
    )


@dataclass(frozen=True)
class AggressiveScenario:
    name: str
    block_politics: bool
    block_conference_finals: bool
    block_nomination: bool
    block_mvp: bool
    max_tranches_per_market: int


def _aggressive_reason(question: str, category: str, scenario: AggressiveScenario) -> str | None:
    q = question.lower()
    cat = category.lower()
    if scenario.block_politics and cat == "politics":
        return "politics"
    if scenario.block_conference_finals and "conference finals" in q:
        return "conference finals"
    if scenario.block_nomination and "nomination" in q:
        return "nomination"
    if scenario.block_mvp and "mvp" in q:
        return "mvp"
    return None


def simulate_aggressive(scenario: AggressiveScenario) -> dict[str, Any]:
    raw_trades = _load_json(STATE / "ab_wallet_copy_aggressive_trades.json", [])
    raw_positions = _load_json(STATE / "ab_wallet_copy_aggressive_positions.json", [])
    raw_marks = _load_json(STATE / "ab_wallet_copy_aggressive_marks.json", {"positions": [], "summary": {}})
    market_open_counts: defaultdict[str, int] = defaultdict(int)
    allowed_ids: set[str] = set()
    filtered_trades: list[dict[str, Any]] = []
    rejection_counts: Counter[str] = Counter()

    def _ts(trade: dict[str, Any]) -> datetime:
        return (
            _parse_ts(str(trade.get("opened_at") or trade.get("scaled_at") or trade.get("closed_at")))
            or datetime.min.replace(tzinfo=timezone.utc)
        )

    for trade in sorted(raw_trades, key=_ts):
        kind = str(trade.get("type", "")).upper()
        position_id = str(trade.get("position_id", ""))
        market_id = str(trade.get("market_id", ""))
        question = str(trade.get("question", ""))
        category = str(trade.get("category", "unknown"))
        if kind == "OPEN":
            reason = _aggressive_reason(question, category, scenario)
            if reason:
                rejection_counts[reason] += 1
                continue
            if market_open_counts[market_id] >= scenario.max_tranches_per_market:
                rejection_counts["max_tranches"] += 1
                continue
            market_open_counts[market_id] += 1
            allowed_ids.add(position_id)
            filtered_trades.append(_copy_trade(trade))
        elif position_id and position_id in allowed_ids:
            filtered_trades.append(_copy_trade(trade))
            if kind == "CLOSE" and market_id:
                market_open_counts[market_id] = max(0, market_open_counts[market_id] - 1)

    filtered_positions = [position for position in raw_positions if str(position.get("position_id", "")) in allowed_ids]
    filtered_marks = {
        **raw_marks,
        "positions": [
            item
            for item in raw_marks.get("positions", [])
            if str(item.get("position_id", "")) in allowed_ids
        ],
    }
    return _summarize(
        name=scenario.name,
        trades=filtered_trades,
        positions=filtered_positions,
        marks=filtered_marks,
        extra={
            "allowed_position_ids": len(allowed_ids),
            "rejections": dict(rejection_counts),
        },
    )


def main() -> None:
    scheduled = {
        "current_live_policy": simulate_scheduled(max_lead_minutes=240, size_ratio=0.4),
        "looser_12h": simulate_scheduled(max_lead_minutes=720, size_ratio=0.4),
        "looser_24h": simulate_scheduled(max_lead_minutes=1440, size_ratio=0.4),
        "baseline_current_ledgers": _summarize(
            "scheduled_baseline",
            _load_json(STATE / "ab_crypto_intraday_scheduled_trades.json", []),
            _load_json(STATE / "ab_crypto_intraday_scheduled_positions.json", []),
            _load_json(STATE / "ab_crypto_intraday_scheduled_marks.json", {"positions": [], "summary": {}}),
        ),
    }
    aggressive = {
        "current_live_policy": simulate_aggressive(
            AggressiveScenario(
                name="aggressive_current_live_policy",
                block_politics=True,
                block_conference_finals=True,
                block_nomination=True,
                block_mvp=True,
                max_tranches_per_market=2,
            )
        ),
        "keep_mvp": simulate_aggressive(
            AggressiveScenario(
                name="aggressive_keep_mvp",
                block_politics=True,
                block_conference_finals=True,
                block_nomination=True,
                block_mvp=False,
                max_tranches_per_market=2,
            )
        ),
        "baseline_current_ledgers": _summarize(
            "aggressive_baseline",
            _load_json(STATE / "ab_wallet_copy_aggressive_trades.json", []),
            _load_json(STATE / "ab_wallet_copy_aggressive_positions.json", []),
            _load_json(STATE / "ab_wallet_copy_aggressive_marks.json", {"positions": [], "summary": {}}),
        ),
    }
    result = {
        "generated_at": _now().isoformat(),
        "caveat": (
            "Counterfactual replay of recorded paper trades under newer filters. "
            "This is not a full historical candidate-stream rebuild."
        ),
        "scheduled_intraday": scheduled,
        "wallet_copy_aggressive": aggressive,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
