from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from bot.accounting import dedupe_closed_trades, effective_position_shares, position_mark, realized_pnl_from_trades, side_contract_price
from bot.config import Settings
from bot.dashboard import serve_dashboard
from bot.models import MarketCandidate, Position, Thesis, Vote, utc_now_iso


class GeoblockedError(RuntimeError):
    pass


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
    return json.loads(path.read_text(encoding="utf-8"))


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

    token_ids = _first(market, "clobTokenIds", "tokenIds")
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
        "politics": ("president", "senate", "house", "election", "trump", "biden", "minister", "prime minister", "government", "court", "sentence"),
        "macro": ("fed", "inflation", "cpi", "gdp", "treasury", "interest rate", "recession", "tariff", "oil", "gold", "stocks"),
        "sports": ("nba", "nfl", "mlb", "nhl", "championship", "final", "goal", "touchdown", "world cup"),
        "tech": ("openai", "chatgpt", "ai", "tesla", "apple", "google", "meta"),
        "culture": ("movie", "oscar", "grammy", "celebrity", "festival"),
    }
    for category, keywords in keyword_groups.items():
        if any(keyword in haystack for keyword in keywords):
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


def _market_is_tradeable(market: dict[str, Any]) -> bool:
    if not bool(_first(market, "active", default=True)):
        return False
    if bool(_first(market, "closed", default=False)):
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
    return keys


def _candidate_match_keys(candidate: MarketCandidate) -> set[str]:
    keys = {candidate.market_id, candidate.token_id, candidate.slug}
    normalized_question = _normalize_market_text(candidate.question)
    if normalized_question:
        keys.add(f"q:{normalized_question}")
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
    def __init__(self, binary: str) -> None:
        self.binary = binary

    def _run_json(self, args: list[str]) -> Any:
        cmd = [self.binary, "-o", "json", *args]
        last_error: RuntimeError | None = None
        for attempt in range(1, 4):
            try:
                proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
            except FileNotFoundError as exc:
                raise RuntimeError(f"{self.binary} is not installed or not on PATH") from exc
            if proc.returncode == 0:
                return json.loads(proc.stdout)

            message = proc.stderr.strip() or proc.stdout.strip() or f"command failed: {' '.join(cmd)}"
            last_error = RuntimeError(message)
            if attempt < 3 and _is_transient_cli_error(message):
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


def discover_targets_from_leaderboard(settings: Settings, cli: PolymarketCLI) -> list[dict[str, Any]]:
    ranked = []
    entries = cli.leaderboard(
        period=settings.target_discovery_period,
        order_by=settings.target_discovery_order_by,
        limit=settings.target_discovery_limit,
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
                "period": settings.target_discovery_period,
                "order_by": settings.target_discovery_order_by,
            }
        )
    _json_dump(settings.targets_path, ranked)
    return ranked


def ensure_targets(settings: Settings, cli: PolymarketCLI) -> list[dict[str, Any]]:
    targets = _json_load(settings.targets_path, [])
    if targets:
        return targets
    return discover_targets_from_leaderboard(settings, cli)


def refresh_target_activity(settings: Settings, cli: PolymarketCLI, limit: int = 25) -> dict[str, Any]:
    cached = _json_load(settings.target_activity_path, {"updated_at": None, "wallets": [], "by_match_key": {}, "errors": []})
    if (
        settings.target_activity_refresh_interval_seconds > 0
        and isinstance(cached, dict)
        and cached.get("wallets")
        and _thesis_cache_entry_is_fresh(
            {"generated_at": cached.get("updated_at")},
            settings.target_activity_refresh_interval_seconds,
        )
    ):
        return cached

    targets = _json_load(settings.targets_path, [])
    cached_wallets = {
        str(item.get("wallet", "")).lower(): item
        for item in cached.get("wallets", [])
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
    _json_dump(settings.target_activity_path, activity)
    return activity


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

        book = cli.book(token_id)
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


def _wallet_copy_candidate_pool(settings: Settings, cli: PolymarketCLI, activity: dict[str, Any]) -> list[MarketCandidate]:
    matched_candidates: list[MarketCandidate] = []
    effective_depth_threshold = max(settings.min_book_depth_usdc * 0.5, 100.0)

    for market in cli.list_markets(settings.markets_limit):
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

        midpoint = cli.midpoint(token_id)
        if midpoint <= 0.0:
            continue

        book = cli.book(token_id)
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
        trade_flow_score = sum(max(_as_float(trade.get("size"), 0.0) * max(_as_float(trade.get("price"), midpoint), 0.01), 1.0) for trade in matched_trades)
        recency_score = max((_wallet_trade_timestamp_value(trade) for trade in matched_trades), default=0.0)
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
    return matched_candidates[: settings.queue_max_candidates]


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
            except json.JSONDecodeError as exc:
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

    def open_position(self, candidate: MarketCandidate, side: str, notional_usdc: float, confidence: float, expected_gap: float) -> dict[str, Any]:
        shares = round(notional_usdc / max(side_contract_price(side, candidate.midpoint), 0.01), 6)
        position = Position(
            position_id=uuid4().hex,
            market_id=candidate.market_id,
            token_id=candidate.token_id,
            question=candidate.question,
            side=side,
            shares=shares,
            notional_usdc=notional_usdc,
            entry_price=candidate.midpoint,
            expected_gap=expected_gap,
            opened_at=utc_now_iso(),
            thesis_confidence=confidence,
            mode=self.settings.bot_mode,
            category=candidate.category,
        )
        positions = self.load_positions()
        positions.append(position.to_dict())
        self.save_positions(positions)
        trade = {"type": "OPEN", **position.to_dict()}
        self.append_trade(trade)
        return trade

    def scale_position(self, position: dict[str, Any], candidate: MarketCandidate, add_notional_usdc: float, reason: str) -> dict[str, Any]:
        positions = self.load_positions()
        add_shares = round(add_notional_usdc / max(side_contract_price(position.get("side", "BUY"), candidate.midpoint), 0.01), 6)
        scaled_position: dict[str, Any] | None = None
        for item in positions:
            if item["position_id"] != position["position_id"]:
                continue
            old_shares = effective_position_shares(item)
            old_entry = _as_float(item.get("entry_price"), candidate.midpoint)
            total_shares = old_shares + add_shares
            if total_shares <= 0:
                continue
            item["entry_price"] = round(((old_entry * old_shares) + (candidate.midpoint * add_shares)) / total_shares, 6)
            item["shares"] = round(total_shares, 6)
            item["notional_usdc"] = round(_as_float(item.get("notional_usdc"), 0.0) + add_notional_usdc, 2)
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
            "add_notional_usdc": add_notional_usdc,
            "notional_usdc": scaled_position["notional_usdc"],
            "entry_price": candidate.midpoint,
            "new_avg_entry_price": scaled_position["entry_price"],
            "reason": reason,
            "scaled_at": utc_now_iso(),
            "mode": self.settings.bot_mode,
        }
        self.append_trade(trade)
        return trade

    def close_position(self, position: dict[str, Any], exit_price: float, reason: str) -> dict[str, Any]:
        positions = [item for item in self.load_positions() if item["position_id"] != position["position_id"]]
        self.save_positions(positions)
        shares = effective_position_shares(position)
        pnl = round((side_contract_price(position["side"], exit_price) - side_contract_price(position["side"], position["entry_price"])) * shares, 2)
        trade = {
            "type": "CLOSE",
            "position_id": position["position_id"],
            "market_id": position["market_id"],
            "token_id": position["token_id"],
            "question": position["question"],
            "category": position.get("category", "unknown"),
            "entry_price": position["entry_price"],
            "exit_price": exit_price,
            "shares": shares,
            "pnl_usdc": pnl,
            "reason": reason,
            "closed_at": utc_now_iso(),
            "mode": self.settings.bot_mode,
        }
        self.append_trade(trade)
        return trade


class LiveExecutor(PaperExecutor):
    def __init__(self, settings: Settings, cli: PolymarketCLI) -> None:
        super().__init__(settings)
        self.cli = cli
        ensure_live_allowed(settings)

    def open_position(self, candidate: MarketCandidate, side: str, notional_usdc: float, confidence: float, expected_gap: float) -> dict[str, Any]:
        shares = round(notional_usdc / max(candidate.midpoint, 0.01), 6)
        self.cli.create_limit_order(candidate.token_id, side, candidate.midpoint, shares)
        return super().open_position(candidate, side, notional_usdc, confidence, expected_gap)

    def close_position(self, position: dict[str, Any], exit_price: float, reason: str) -> dict[str, Any]:
        self.cli.create_limit_order(position["token_id"], "SELL" if position["side"] == "BUY" else "BUY", exit_price, position["shares"])
        return super().close_position(position, exit_price, reason)


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


def _remaining_bankroll_usdc(settings: Settings, executor: PaperExecutor) -> float:
    portfolio_cap = _current_bankroll_usdc(settings) * max(settings.max_portfolio_fraction, 0.0)
    remaining = round(portfolio_cap - executor.open_notional_usdc(), 2)
    return max(remaining, 0.0)


def _position_cap_usdc(settings: Settings) -> float:
    return round(_current_bankroll_usdc(settings) * max(settings.max_position_fraction, 0.0), 2)


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
        try:
            current_midpoint = cli.midpoint(position["token_id"])
        except Exception as exc:  # noqa: BLE001
            mark_error = str(exc)

        shares, mark_value, unrealized_pnl = position_mark(position, current_midpoint)
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
                "shares": shares,
                "mark_value_usdc": mark_value,
                "unrealized_pnl_usdc": unrealized_pnl,
                "opened_at": position["opened_at"],
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
    open_token_ids = {str(item.get("token_id", "")) for item in existing_positions}
    open_market_ids = {str(item.get("market_id", "")) for item in existing_positions}
    position_by_market = {str(item.get("market_id", "")): item for item in existing_positions}
    position_by_token = {str(item.get("token_id", "")): item for item in existing_positions}
    decisions: list[dict[str, Any]] = []
    scale_ins_this_cycle = 0

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
        cooldown_active, cooldown_reason = _within_market_cooldown(settings, candidate, last_trade_map.get(candidate.market_id))
        if cooldown_active and existing_position is None:
            decisions.append({**decision_base, "action": "SKIP", "reason": cooldown_reason})
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
            rotation_pnl = round((side_contract_price(rotation_target["side"], exit_price) - side_contract_price(rotation_target["side"], rotation_target["entry_price"])) * effective_position_shares(rotation_target), 2)
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
            rotated = executor.close_position(rotation_target, exit_price, "ROTATED_OUT")
            existing_positions = [item for item in existing_positions if item["position_id"] != rotation_target["position_id"]]
            position_by_market.pop(str(rotation_target.get("market_id", "")), None)
            position_by_token.pop(str(rotation_target.get("token_id", "")), None)
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
            rotation_pnl = round((side_contract_price(rotation_target["side"], exit_price) - side_contract_price(rotation_target["side"], rotation_target["entry_price"])) * effective_position_shares(rotation_target), 2)
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
            rotated = executor.close_position(rotation_target, exit_price, "ROTATED_OUT")
            existing_positions = [item for item in existing_positions if item["position_id"] != rotation_target["position_id"]]
            position_by_market.pop(str(rotation_target.get("market_id", "")), None)
            position_by_token.pop(str(rotation_target.get("token_id", "")), None)
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
            scaled = executor.scale_position(existing_position, candidate, add_notional, "SCALE_TO_TARGET")
            existing_position.update(scaled)
            scale_ins_this_cycle += 1
            decisions.append({**decision_base, "action": "SCALE", "trade": scaled, "votes": [vote.to_dict() for vote in votes]})
            continue

        expected_gap = abs(thesis.estimated_probability - candidate.midpoint)
        opened = executor.open_position(candidate, side, size, avg_confidence, expected_gap)
        existing_positions.append(opened)
        open_token_ids.add(candidate.token_id)
        open_market_ids.add(candidate.market_id)
        position_by_market[candidate.market_id] = opened
        position_by_token[candidate.token_id] = opened
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
) -> list[dict[str, Any]]:
    shadow_settings = _shadow_settings(settings, suffix)
    activity = _json_load(settings.target_activity_path, {"by_match_key": {}})
    queue = _wallet_copy_candidate_pool(settings, cli, activity)
    executor = PaperExecutor(shadow_settings)
    existing_positions = executor.load_positions()
    trades = _json_load(shadow_settings.trades_path, [])
    last_trade_map = _last_trade_by_market(trades)
    position_by_market = {str(item.get("market_id", "")): item for item in existing_positions}
    position_by_token = {str(item.get("token_id", "")): item for item in existing_positions}
    decisions: list[dict[str, Any]] = []
    scale_ins_this_cycle = 0

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
        cooldown_active, cooldown_reason = _within_market_cooldown(settings, candidate, last_trade_map.get(candidate.market_id))
        if cooldown_active and existing_position is None:
            decisions.append({**decision_base, "action": "SKIP", "reason": cooldown_reason})
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
            scaled = executor.scale_position(existing_position, candidate, add_notional, "AB_SCALE_TO_TARGET")
            existing_position.update(scaled)
            scale_ins_this_cycle += 1
            decisions.append({**decision_base, "action": "SCALE", "trade": scaled, "votes": [vote.to_dict() for vote in votes]})
            continue

        expected_gap = abs(whale_vote.estimated_probability - candidate.midpoint)
        opened = executor.open_position(candidate, side, size, whale_vote.confidence, expected_gap)
        existing_positions.append(opened)
        position_by_market[candidate.market_id] = opened
        position_by_token[candidate.token_id] = opened
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
) -> dict[str, Any]:
    shadow_settings = _shadow_settings(settings, suffix)
    pre_trade_exits = monitor_exits(shadow_settings, cli)
    decisions = trade_candidates_wallet_copy_variant(
        settings,
        cli,
        suffix=suffix,
        min_confidence=min_confidence,
        require_microstructure_alignment=require_microstructure_alignment,
        fixed_position_usdc=fixed_position_usdc,
        agent_name=agent_name,
    )
    exits = pre_trade_exits + monitor_exits(shadow_settings, cli)
    marks = mark_open_positions(shadow_settings, cli)
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
    }


def monitor_exits(settings: Settings, cli: PolymarketCLI) -> list[dict[str, Any]]:
    executor = _executor(settings, cli)
    positions = executor.load_positions()
    exits: list[dict[str, Any]] = []

    for position in positions:
        midpoint = cli.midpoint(position["token_id"])
        book = cli.book(position["token_id"])
        bids = book.get("bids", []) if isinstance(book, dict) else []
        asks = book.get("asks", []) if isinstance(book, dict) else []
        depth = min(_sum_book_depth(bids), _sum_book_depth(asks))
        opened_at = _normalize_iso(position["opened_at"])
        age_hours = 0.0
        if opened_at is not None:
            age_hours = max((datetime.now(timezone.utc) - opened_at).total_seconds() / 3600.0, 0.0)

        target_price = position["entry_price"] + (position["expected_gap"] * 0.85 * (1 if position["side"] == "BUY" else -1))
        if position["side"] == "BUY" and midpoint >= target_price:
            exits.append(executor.close_position(position, midpoint, "TARGET_HIT"))
            continue
        if position["side"] == "SELL" and midpoint <= target_price:
            exits.append(executor.close_position(position, midpoint, "TARGET_HIT"))
            continue
        if depth < settings.min_book_depth_usdc * 0.5:
            exits.append(executor.close_position(position, midpoint, "ORDER_FLOW_SPIKE_PROXY"))
            continue
        if age_hours > 24 and abs(midpoint - position["entry_price"]) < 0.02:
            exits.append(executor.close_position(position, midpoint, "STALE_THESIS"))

    return exits


def run_cycle(settings: Settings, cli: PolymarketCLI) -> dict[str, Any]:
    starting_bankroll = settings.bankroll_usdc
    realized_pnl = _realized_pnl_usdc(settings)
    current_bankroll = _current_bankroll_usdc(settings)
    ensure_targets(settings, cli)
    refresh_target_activity(settings, cli)
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
        ab_tests["wallet_copy_aggressive"] = run_ab_wallet_copy_cycle(
            settings,
            cli,
            suffix="ab_wallet_copy_aggressive",
            strategy_name="wallet_copy_aggressive",
            min_confidence=settings.ab_test_aggressive_min_whale_confidence,
            require_microstructure_alignment=settings.ab_test_aggressive_require_microstructure_alignment,
            fixed_position_usdc=settings.ab_test_aggressive_fixed_position_usdc,
            agent_name="wallet_copy_aggressive_ab",
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


def run_daemon(settings: Settings, cli: PolymarketCLI, interval_seconds: int | None = None) -> None:
    interval = interval_seconds or settings.daemon_interval_seconds
    current_bankroll = _current_bankroll_usdc(settings)
    _log(
        "daemon starting "
        f"mode={settings.bot_mode} starting_bankroll={settings.bankroll_usdc} "
        f"current_bankroll={current_bankroll} "
        f"markets_limit={settings.markets_limit} queue_max={settings.queue_max_candidates} "
        f"max_open_positions={settings.max_open_positions} interval={interval}s"
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
        started = utc_now_iso()
        _log("cycle started")
        _write_status(settings, {"last_cycle_started_at": started, "status": "running", "last_error": None})
        try:
            result = run_cycle(settings, cli)
            _log(
                "cycle completed "
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
                },
            )
        _log(f"sleeping {interval}s")
        time.sleep(interval)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Polymarket bot scaffold")
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
    subparsers.add_parser("reset-paper-book")
    subparsers.add_parser("dedupe-open-positions")
    daemon = subparsers.add_parser("daemon")
    daemon.add_argument("--interval", type=int)
    dashboard = subparsers.add_parser("serve-dashboard")
    dashboard.add_argument("--host")
    dashboard.add_argument("--port", type=int)
    return parser


def main() -> int:
    _parse_env_file(Path(".env"))
    settings = Settings.from_env()
    settings.ensure_dirs()
    cli = PolymarketCLI(settings.polymarket_cli_bin)
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
        elif args.command == "reset-paper-book":
            print(json.dumps(reset_paper_book(settings), indent=2))
        elif args.command == "dedupe-open-positions":
            print(json.dumps(dedupe_open_positions(settings), indent=2))
        elif args.command == "daemon":
            run_daemon(settings, cli, args.interval)
        elif args.command == "serve-dashboard":
            serve_dashboard(settings, host=args.host or settings.dashboard_host, port=args.port or settings.dashboard_port)
        else:
            parser.error(f"unknown command {args.command}")
    except GeoblockedError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0
