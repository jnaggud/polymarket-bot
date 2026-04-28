from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw is not None else default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw is not None else default


def _env_csv(name: str) -> tuple[str, ...]:
    raw = os.getenv(name, "")
    return tuple(item.strip().lower() for item in raw.split(",") if item.strip())


@dataclass(slots=True)
class Settings:
    bot_mode: str
    live_trading_enabled: bool
    polymarket_cli_bin: str
    openai_api_key: str | None
    openai_model: str
    bankroll_usdc: float
    markets_limit: int
    queue_max_candidates: int
    scan_pool_multiplier: int
    edge_threshold: float
    min_book_depth_usdc: float
    min_hours_to_resolution: int
    max_hours_to_resolution: int
    min_thesis_confidence: float
    max_kelly_fraction: float
    single_vote_size_multiplier: float
    max_position_fraction: float
    max_portfolio_fraction: float
    min_position_usdc: float
    conviction_min_notional_usdc: float
    conviction_confidence_threshold: float
    conviction_depth_threshold_usdc: float
    consensus_votes_required: int
    max_open_positions: int
    scale_in_enabled: bool
    scale_in_threshold_fraction: float
    max_scale_ins_per_cycle: int
    market_cooldown_minutes: int
    winner_cooldown_minutes: int
    loser_cooldown_minutes: int
    rotation_enabled: bool
    rotation_min_holding_minutes: int
    rotation_min_priority_score_delta: float
    category_include: tuple[str, ...]
    category_exclude: tuple[str, ...]
    category_rotation: tuple[str, ...]
    category_rotation_days: int
    target_discovery_period: str
    target_discovery_order_by: str
    target_discovery_limit: int
    target_activity_refresh_interval_seconds: int
    target_activity_wallets_per_refresh: int
    thesis_max_candidates_per_cycle: int
    thesis_reuse_ttl_seconds: int
    ab_test_enabled: bool
    ab_test_strategy: str
    ab_test_min_whale_confidence: float
    ab_test_require_microstructure_alignment: bool
    ab_test_aggressive_min_whale_confidence: float
    ab_test_aggressive_require_microstructure_alignment: bool
    ab_test_aggressive_fixed_position_usdc: float
    daemon_interval_seconds: int
    dashboard_host: str
    dashboard_port: int
    targets_path: Path
    target_activity_path: Path
    queue_path: Path
    theses_path: Path
    positions_path: Path
    trades_path: Path
    status_path: Path
    marks_path: Path
    last_nonempty_queue_path: Path
    last_nonempty_theses_path: Path
    missed_opportunities_path: Path
    thesis_cache_path: Path

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            bot_mode=os.getenv("BOT_MODE", "paper").strip().lower(),
            live_trading_enabled=_env_bool("LIVE_TRADING_ENABLED", False),
            polymarket_cli_bin=os.getenv("POLYMARKET_CLI_BIN", "polymarket"),
            openai_api_key=os.getenv("OPENAI_API_KEY") or None,
            openai_model=os.getenv("OPENAI_MODEL", "gpt-5-mini"),
            bankroll_usdc=_env_float("BANKROLL_USDC", 500.0),
            markets_limit=_env_int("MARKETS_LIMIT", 100),
            queue_max_candidates=_env_int("QUEUE_MAX_CANDIDATES", 25),
            scan_pool_multiplier=_env_int("SCAN_POOL_MULTIPLIER", 5),
            edge_threshold=_env_float("EDGE_THRESHOLD", 0.07),
            min_book_depth_usdc=_env_float("MIN_BOOK_DEPTH_USDC", 500.0),
            min_hours_to_resolution=_env_int("MIN_HOURS_TO_RESOLUTION", 4),
            max_hours_to_resolution=_env_int("MAX_HOURS_TO_RESOLUTION", 168),
            min_thesis_confidence=_env_float("MIN_THESIS_CONFIDENCE", 0.75),
            max_kelly_fraction=_env_float("MAX_KELLY_FRACTION", 0.25),
            single_vote_size_multiplier=_env_float("SINGLE_VOTE_SIZE_MULTIPLIER", 0.5),
            max_position_fraction=_env_float("MAX_POSITION_FRACTION", 0.15),
            max_portfolio_fraction=_env_float("MAX_PORTFOLIO_FRACTION", 1.0),
            min_position_usdc=_env_float("MIN_POSITION_USDC", 0.0),
            conviction_min_notional_usdc=_env_float("CONVICTION_MIN_NOTIONAL_USDC", 0.0),
            conviction_confidence_threshold=_env_float("CONVICTION_CONFIDENCE_THRESHOLD", 0.65),
            conviction_depth_threshold_usdc=_env_float("CONVICTION_DEPTH_THRESHOLD_USDC", 10_000.0),
            consensus_votes_required=_env_int("CONSENSUS_VOTES_REQUIRED", 2),
            max_open_positions=_env_int("MAX_OPEN_POSITIONS", 8),
            scale_in_enabled=_env_bool("SCALE_IN_ENABLED", False),
            scale_in_threshold_fraction=_env_float("SCALE_IN_THRESHOLD_FRACTION", 0.6),
            max_scale_ins_per_cycle=_env_int("MAX_SCALE_INS_PER_CYCLE", 2),
            market_cooldown_minutes=_env_int("MARKET_COOLDOWN_MINUTES", 180),
            winner_cooldown_minutes=_env_int("WINNER_COOLDOWN_MINUTES", _env_int("MARKET_COOLDOWN_MINUTES", 180)),
            loser_cooldown_minutes=_env_int("LOSER_COOLDOWN_MINUTES", _env_int("MARKET_COOLDOWN_MINUTES", 180) * 2),
            rotation_enabled=_env_bool("ROTATION_ENABLED", True),
            rotation_min_holding_minutes=_env_int("ROTATION_MIN_HOLDING_MINUTES", 30),
            rotation_min_priority_score_delta=_env_float("ROTATION_MIN_PRIORITY_SCORE_DELTA", 0.75),
            category_include=_env_csv("CATEGORY_INCLUDE"),
            category_exclude=_env_csv("CATEGORY_EXCLUDE"),
            category_rotation=_env_csv("CATEGORY_ROTATION"),
            category_rotation_days=_env_int("CATEGORY_ROTATION_DAYS", 7),
            target_discovery_period=os.getenv("TARGET_DISCOVERY_PERIOD", "month"),
            target_discovery_order_by=os.getenv("TARGET_DISCOVERY_ORDER_BY", "pnl"),
            target_discovery_limit=_env_int("TARGET_DISCOVERY_LIMIT", 50),
            target_activity_refresh_interval_seconds=_env_int("TARGET_ACTIVITY_REFRESH_INTERVAL_SECONDS", 300),
            target_activity_wallets_per_refresh=_env_int("TARGET_ACTIVITY_WALLETS_PER_REFRESH", 10),
            thesis_max_candidates_per_cycle=_env_int("THESIS_MAX_CANDIDATES_PER_CYCLE", 12),
            thesis_reuse_ttl_seconds=_env_int("THESIS_REUSE_TTL_SECONDS", 900),
            ab_test_enabled=_env_bool("AB_TEST_ENABLED", True),
            ab_test_strategy=os.getenv("AB_TEST_STRATEGY", "wallet_copy").strip().lower(),
            ab_test_min_whale_confidence=_env_float("AB_TEST_MIN_WHALE_CONFIDENCE", 0.60),
            ab_test_require_microstructure_alignment=_env_bool("AB_TEST_REQUIRE_MICROSTRUCTURE_ALIGNMENT", True),
            ab_test_aggressive_min_whale_confidence=_env_float("AB_TEST_AGGRESSIVE_MIN_WHALE_CONFIDENCE", 0.50),
            ab_test_aggressive_require_microstructure_alignment=_env_bool("AB_TEST_AGGRESSIVE_REQUIRE_MICROSTRUCTURE_ALIGNMENT", False),
            ab_test_aggressive_fixed_position_usdc=_env_float("AB_TEST_AGGRESSIVE_FIXED_POSITION_USDC", 250.0),
            daemon_interval_seconds=_env_int("DAEMON_INTERVAL_SECONDS", 300),
            dashboard_host=os.getenv("DASHBOARD_HOST", "127.0.0.1"),
            dashboard_port=_env_int("DASHBOARD_PORT", 8080),
            targets_path=Path(os.getenv("TARGETS_PATH", "state/targets.json")),
            target_activity_path=Path(os.getenv("TARGET_ACTIVITY_PATH", "state/target_activity.json")),
            queue_path=Path(os.getenv("QUEUE_PATH", "state/queue.json")),
            theses_path=Path(os.getenv("THESES_PATH", "state/theses.json")),
            positions_path=Path(os.getenv("POSITIONS_PATH", "state/positions.json")),
            trades_path=Path(os.getenv("TRADES_PATH", "state/trades.json")),
            status_path=Path(os.getenv("STATUS_PATH", "state/status.json")),
            marks_path=Path(os.getenv("MARKS_PATH", "state/marks.json")),
            last_nonempty_queue_path=Path(os.getenv("LAST_NONEMPTY_QUEUE_PATH", "state/last_nonempty_queue.json")),
            last_nonempty_theses_path=Path(os.getenv("LAST_NONEMPTY_THESES_PATH", "state/last_nonempty_theses.json")),
            missed_opportunities_path=Path(os.getenv("MISSED_OPPORTUNITIES_PATH", "state/missed_opportunities.json")),
            thesis_cache_path=Path(os.getenv("THESIS_CACHE_PATH", "state/thesis_cache.json")),
        )

    def ensure_dirs(self) -> None:
        for path in [
            self.targets_path,
            self.target_activity_path,
            self.queue_path,
            self.theses_path,
            self.positions_path,
            self.trades_path,
            self.status_path,
            self.marks_path,
            self.last_nonempty_queue_path,
            self.last_nonempty_theses_path,
            self.missed_opportunities_path,
            self.thesis_cache_path,
        ]:
            path.parent.mkdir(parents=True, exist_ok=True)
