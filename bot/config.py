from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parent.parent


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


def _default_polymarket_cli_bin() -> str:
    configured = os.getenv("POLYMARKET_CLI_BIN")
    if configured:
        return configured
    bundled = _REPO_ROOT / "bin" / "polymarket"
    if bundled.exists():
        return str(bundled)
    return "polymarket"


@dataclass(slots=True)
class Settings:
    bot_mode: str
    live_trading_enabled: bool
    polymarket_cli_bin: str
    polymarket_cli_timeout_seconds: int
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
    use_equity_for_risk_caps: bool
    min_position_usdc: float
    max_sell_midpoint: float
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
    stale_reentry_block_minutes: int
    stale_reentry_lookback_hours: int
    stale_reentry_max_closes: int
    stale_reentry_min_cumulative_loss_usdc: float
    rotation_enabled: bool
    rotation_min_holding_minutes: int
    rotation_min_priority_score_delta: float
    forced_derisk_enabled: bool
    forced_derisk_min_holding_minutes: int
    forced_derisk_max_drawdown_fraction: float
    forced_derisk_max_contract_loss: float
    primary_book_block_politics: bool
    strategy_health_gate_enabled: bool
    strategy_health_lookback_hours: int
    strategy_health_min_closed_trades: int
    strategy_health_min_realized_pnl_usdc: float
    strategy_health_block_degraded: bool
    strategy_warmup_max_open_positions: int
    strategy_warmup_max_unrealized_drawdown_usdc: float
    strategy_probation_max_open_positions: int
    strategy_probation_size_multiplier: float
    crypto_wallet_copy_quarantined: bool
    dashboard_main_stale_after_minutes: int
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
    ab_test_aggressive_markets_limit: int
    ab_test_aggressive_queue_max_candidates: int
    ab_test_aggressive_prebook_candidate_limit: int
    ab_test_aggressive_allow_multi_tranche: bool
    ab_test_aggressive_max_tranches_per_market: int
    ab_test_aggressive_max_theme_positions: int
    ab_test_aggressive_max_theme_notional_usdc: float
    ab_test_aggressive_max_new_positions_per_theme_per_cycle: int
    ab_test_aggressive_reentry_requires_profit: bool
    ab_test_aggressive_reentry_min_profit_usdc: float
    ab_test_aggressive_separate_daemon: bool
    ab_test_aggressive_full_sync_every_cycles: int
    ab_test_crypto_enabled: bool
    ab_test_crypto_min_whale_confidence: float
    ab_test_crypto_require_microstructure_alignment: bool
    ab_test_crypto_fixed_position_usdc: float
    ab_test_crypto_top_wallets: int
    ab_test_crypto_source_wallets_limit: int
    ab_test_crypto_wallets_per_refresh: int
    ab_test_crypto_trade_limit: int
    ab_test_crypto_markets_limit: int
    ab_test_crypto_queue_max_candidates: int
    ab_test_crypto_prebook_candidate_limit: int
    ab_test_crypto_min_book_depth_usdc: float
    ab_test_crypto_min_hours_to_resolution: int
    ab_test_crypto_separate_daemon: bool
    ab_test_crypto_5m_sniper_enabled: bool
    ab_test_crypto_5m_sniper_fixed_position_usdc: float
    ab_test_crypto_5m_sniper_markets_limit: int
    ab_test_crypto_5m_sniper_max_minutes_to_resolution: int
    ab_test_crypto_5m_sniper_min_book_depth_usdc: float
    ab_test_crypto_5m_sniper_min_book_imbalance: float
    ab_test_crypto_5m_sniper_max_side_price: float
    ab_test_crypto_5m_sniper_hold_minutes: float
    ab_test_crypto_5m_sniper_take_profit_contract_price: float
    ab_test_crypto_5m_sniper_separate_daemon: bool
    ab_test_crypto_next_window_sniper_enabled: bool
    ab_test_crypto_next_window_sniper_fixed_position_usdc: float
    ab_test_crypto_next_window_sniper_markets_limit: int
    ab_test_crypto_next_window_sniper_min_minutes_to_resolution: int
    ab_test_crypto_next_window_sniper_max_minutes_to_resolution: int
    ab_test_crypto_next_window_sniper_min_book_depth_usdc: float
    ab_test_crypto_next_window_sniper_min_book_imbalance: float
    ab_test_crypto_next_window_sniper_min_midpoint_edge: float
    ab_test_crypto_next_window_sniper_max_side_price: float
    ab_test_crypto_next_window_sniper_hold_minutes: float
    ab_test_crypto_next_window_sniper_take_profit_contract_price: float
    ab_test_crypto_next_window_sniper_separate_daemon: bool
    ab_test_crypto_5m_box_enabled: bool
    ab_test_crypto_5m_box_pair_budget_usdc: float
    ab_test_crypto_5m_box_markets_limit: int
    ab_test_crypto_5m_box_max_minutes_to_resolution: int
    ab_test_crypto_5m_box_min_book_depth_usdc: float
    ab_test_crypto_5m_box_min_edge_per_share: float
    ab_test_crypto_5m_box_min_net_edge_per_share: float
    ab_test_crypto_5m_box_estimated_fee_per_share: float
    ab_test_crypto_5m_box_estimated_slippage_per_share: float
    ab_test_crypto_5m_box_aggressive_queue_fill_probability: float
    ab_test_crypto_5m_box_aggressive_maker_fee_per_share: float
    ab_test_crypto_5m_box_aggressive_estimated_slippage_per_share: float
    ab_test_crypto_5m_box_aggressive_tick_size: float
    ab_test_crypto_5m_box_aggressive_price_concession_ticks: int
    ab_test_crypto_5m_box_hold_minutes: float
    ab_test_crypto_5m_box_capture_ratio: float
    ab_test_crypto_5m_box_separate_daemon: bool
    ab_test_crypto_5m_box_assets: tuple[str, ...]
    ab_test_crypto_next_window_box_enabled: bool
    ab_test_crypto_next_window_box_pair_budget_usdc: float
    ab_test_crypto_next_window_box_markets_limit: int
    ab_test_crypto_next_window_box_min_minutes_to_resolution: int
    ab_test_crypto_next_window_box_max_minutes_to_resolution: int
    ab_test_crypto_next_window_box_min_book_depth_usdc: float
    ab_test_crypto_next_window_box_min_edge_per_share: float
    ab_test_crypto_next_window_box_min_net_edge_per_share: float
    ab_test_crypto_next_window_box_estimated_fee_per_share: float
    ab_test_crypto_next_window_box_estimated_slippage_per_share: float
    ab_test_crypto_next_window_box_hold_minutes: float
    ab_test_crypto_next_window_box_capture_ratio: float
    ab_test_crypto_next_window_box_separate_daemon: bool
    ab_test_crypto_intraday_scheduled_enabled: bool
    ab_test_crypto_intraday_scheduled_fixed_position_usdc: float
    ab_test_crypto_intraday_scheduled_markets_limit: int
    ab_test_crypto_intraday_scheduled_max_minutes_to_resolution: int
    ab_test_crypto_intraday_scheduled_min_book_depth_usdc: float
    ab_test_crypto_intraday_scheduled_min_book_imbalance: float
    ab_test_crypto_intraday_scheduled_min_midpoint_edge: float
    ab_test_crypto_intraday_scheduled_max_side_price: float
    ab_test_crypto_intraday_scheduled_hold_minutes: float
    ab_test_crypto_intraday_scheduled_take_profit_contract_price: float
    ab_test_crypto_intraday_scheduled_separate_daemon: bool
    ab_test_crypto_threshold_snapshot_enabled: bool
    ab_test_crypto_threshold_snapshot_fixed_position_usdc: float
    ab_test_crypto_threshold_snapshot_markets_limit: int
    ab_test_crypto_threshold_snapshot_max_minutes_to_resolution: int
    ab_test_crypto_threshold_snapshot_min_book_depth_usdc: float
    ab_test_crypto_threshold_snapshot_min_book_imbalance: float
    ab_test_crypto_threshold_snapshot_min_midpoint_edge: float
    ab_test_crypto_threshold_snapshot_soft_max_spread: float
    ab_test_crypto_threshold_snapshot_hard_max_spread: float
    ab_test_crypto_threshold_snapshot_min_spread_size_multiplier: float
    ab_test_crypto_threshold_snapshot_max_side_price: float
    ab_test_crypto_threshold_snapshot_hold_minutes: float
    ab_test_crypto_threshold_snapshot_take_profit_contract_price: float
    ab_test_crypto_threshold_snapshot_separate_daemon: bool
    ab_test_sports_early_entry_enabled: bool
    ab_test_sports_early_entry_fixed_position_usdc: float
    ab_test_sports_early_entry_markets_limit: int
    ab_test_sports_early_entry_min_hours_to_resolution: int
    ab_test_sports_early_entry_max_hours_to_resolution: int
    ab_test_sports_early_entry_min_book_depth_usdc: float
    ab_test_sports_early_entry_min_midpoint: float
    ab_test_sports_early_entry_max_midpoint: float
    ab_test_sports_early_entry_hold_minutes: float
    ab_test_sports_early_entry_take_profit_contract_price: float
    ab_test_sports_early_entry_separate_daemon: bool
    ab_test_penny_longshot_enabled: bool
    ab_test_penny_longshot_fixed_position_usdc: float
    ab_test_penny_longshot_markets_limit: int
    ab_test_penny_longshot_min_hours_to_resolution: int
    ab_test_penny_longshot_max_hours_to_resolution: int
    ab_test_penny_longshot_min_book_depth_usdc: float
    ab_test_penny_longshot_max_midpoint: float
    ab_test_penny_longshot_max_open_positions: int
    ab_test_penny_longshot_hold_minutes: float
    ab_test_penny_longshot_take_profit_contract_price: float
    ab_test_penny_longshot_separate_daemon: bool
    ab_test_crypto_latency_5m_enabled: bool
    ab_test_crypto_latency_5m_fixed_position_usdc: float
    ab_test_crypto_latency_5m_markets_limit: int
    ab_test_crypto_latency_5m_max_minutes_to_resolution: int
    ab_test_crypto_latency_5m_min_book_depth_usdc: float
    ab_test_crypto_latency_5m_min_book_imbalance: float
    ab_test_crypto_latency_5m_max_side_price: float
    ab_test_crypto_latency_5m_hold_minutes: float
    ab_test_crypto_latency_5m_take_profit_contract_price: float
    ab_test_crypto_latency_5m_separate_daemon: bool
    ab_test_verified_public_enabled: bool
    ab_test_verified_public_min_whale_confidence: float
    ab_test_verified_public_require_microstructure_alignment: bool
    ab_test_verified_public_fixed_position_usdc: float
    intraday_registry_enabled: bool
    intraday_registry_bootstrap_limit: int
    intraday_registry_gamma_refresh_limit: int
    intraday_registry_gamma_refresh_max_queries: int
    intraday_registry_gamma_search_timeout_seconds: int
    intraday_registry_max_minutes_to_resolution: int
    intraday_registry_stale_after_seconds: int
    intraday_registry_ws_enabled: bool
    intraday_registry_ws_endpoint: str
    intraday_registry_prefer_registry: bool
    intraday_registry_rejections_limit: int
    intraday_registry_raw_updown_limit: int
    intraday_registry_imminent_updown_limit: int
    intraday_registry_imminent_seen_age_minutes: int
    intraday_registry_imminent_max_minutes_to_resolution: int
    intraday_registry_watchlist_max_minutes_to_resolution: int
    intraday_registry_book_tape_limit: int
    intraday_registry_book_snapshot_topn: int
    intraday_registry_threshold_live_limit: int
    intraday_registry_threshold_live_max_minutes_to_resolution: int
    intraday_registry_ws_batch_asset_limit: int
    intraday_registry_ws_max_message_bytes: int
    main_daemon_full_sync_every_cycles: int
    daemon_cycle_timeout_seconds: int
    daemon_interval_seconds: int
    dashboard_host: str
    dashboard_port: int
    targets_path: Path
    target_activity_path: Path
    crypto_targets_path: Path
    crypto_activity_path: Path
    crypto_source_targets_path: Path
    crypto_source_activity_path: Path
    verified_public_traders_path: Path
    verified_public_activity_path: Path
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
    intraday_registry_path: Path
    intraday_registry_rejections_path: Path
    intraday_registry_raw_updown_path: Path
    intraday_registry_imminent_updown_path: Path
    intraday_registry_book_tape_path: Path
    intraday_registry_threshold_live_path: Path
    intraday_registry_transition_tape_path: Path
    intraday_registry_imminent_box_arb_tape_path: Path
    intraday_registry_edge_alerts_path: Path
    intraday_registry_audit_sqlite_path: Path

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            bot_mode=os.getenv("BOT_MODE", "paper").strip().lower(),
            live_trading_enabled=_env_bool("LIVE_TRADING_ENABLED", False),
            polymarket_cli_bin=_default_polymarket_cli_bin(),
            polymarket_cli_timeout_seconds=_env_int("POLYMARKET_CLI_TIMEOUT_SECONDS", 12),
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
            use_equity_for_risk_caps=_env_bool("USE_EQUITY_FOR_RISK_CAPS", True),
            min_position_usdc=_env_float("MIN_POSITION_USDC", 0.0),
            max_sell_midpoint=_env_float("MAX_SELL_MIDPOINT", 0.85),
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
            stale_reentry_block_minutes=_env_int("STALE_REENTRY_BLOCK_MINUTES", 2880),
            stale_reentry_lookback_hours=_env_int("STALE_REENTRY_LOOKBACK_HOURS", 168),
            stale_reentry_max_closes=_env_int("STALE_REENTRY_MAX_CLOSES", 2),
            stale_reentry_min_cumulative_loss_usdc=_env_float("STALE_REENTRY_MIN_CUMULATIVE_LOSS_USDC", 25.0),
            rotation_enabled=_env_bool("ROTATION_ENABLED", True),
            rotation_min_holding_minutes=_env_int("ROTATION_MIN_HOLDING_MINUTES", 30),
            rotation_min_priority_score_delta=_env_float("ROTATION_MIN_PRIORITY_SCORE_DELTA", 0.75),
            forced_derisk_enabled=_env_bool("FORCED_DERISK_ENABLED", True),
            forced_derisk_min_holding_minutes=_env_int("FORCED_DERISK_MIN_HOLDING_MINUTES", 240),
            forced_derisk_max_drawdown_fraction=_env_float("FORCED_DERISK_MAX_DRAWDOWN_FRACTION", 0.25),
            forced_derisk_max_contract_loss=_env_float("FORCED_DERISK_MAX_CONTRACT_LOSS", 0.18),
            primary_book_block_politics=_env_bool("PRIMARY_BOOK_BLOCK_POLITICS", True),
            strategy_health_gate_enabled=_env_bool("STRATEGY_HEALTH_GATE_ENABLED", True),
            strategy_health_lookback_hours=_env_int("STRATEGY_HEALTH_LOOKBACK_HOURS", 24),
            strategy_health_min_closed_trades=_env_int("STRATEGY_HEALTH_MIN_CLOSED_TRADES", 3),
            strategy_health_min_realized_pnl_usdc=_env_float("STRATEGY_HEALTH_MIN_REALIZED_PNL_USDC", 1.0),
            strategy_health_block_degraded=_env_bool("STRATEGY_HEALTH_BLOCK_DEGRADED", True),
            strategy_warmup_max_open_positions=_env_int("STRATEGY_WARMUP_MAX_OPEN_POSITIONS", 10),
            strategy_warmup_max_unrealized_drawdown_usdc=_env_float("STRATEGY_WARMUP_MAX_UNREALIZED_DRAWDOWN_USDC", 150.0),
            strategy_probation_max_open_positions=_env_int("STRATEGY_PROBATION_MAX_OPEN_POSITIONS", 4),
            strategy_probation_size_multiplier=_env_float("STRATEGY_PROBATION_SIZE_MULTIPLIER", 0.35),
            crypto_wallet_copy_quarantined=_env_bool("CRYPTO_WALLET_COPY_QUARANTINED", True),
            dashboard_main_stale_after_minutes=_env_int("DASHBOARD_MAIN_STALE_AFTER_MINUTES", 15),
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
            ab_test_aggressive_min_whale_confidence=_env_float("AB_TEST_AGGRESSIVE_MIN_WHALE_CONFIDENCE", 0.60),
            ab_test_aggressive_require_microstructure_alignment=_env_bool("AB_TEST_AGGRESSIVE_REQUIRE_MICROSTRUCTURE_ALIGNMENT", True),
            ab_test_aggressive_fixed_position_usdc=_env_float("AB_TEST_AGGRESSIVE_FIXED_POSITION_USDC", 250.0),
            ab_test_aggressive_markets_limit=_env_int("AB_TEST_AGGRESSIVE_MARKETS_LIMIT", 400),
            ab_test_aggressive_queue_max_candidates=_env_int("AB_TEST_AGGRESSIVE_QUEUE_MAX_CANDIDATES", 60),
            ab_test_aggressive_prebook_candidate_limit=_env_int("AB_TEST_AGGRESSIVE_PREBOOK_CANDIDATE_LIMIT", 90),
            ab_test_aggressive_allow_multi_tranche=_env_bool("AB_TEST_AGGRESSIVE_ALLOW_MULTI_TRANCHE", True),
            ab_test_aggressive_max_tranches_per_market=_env_int("AB_TEST_AGGRESSIVE_MAX_TRANCHES_PER_MARKET", 2),
            ab_test_aggressive_max_theme_positions=_env_int("AB_TEST_AGGRESSIVE_MAX_THEME_POSITIONS", 6),
            ab_test_aggressive_max_theme_notional_usdc=_env_float("AB_TEST_AGGRESSIVE_MAX_THEME_NOTIONAL_USDC", 1500.0),
            ab_test_aggressive_max_new_positions_per_theme_per_cycle=_env_int("AB_TEST_AGGRESSIVE_MAX_NEW_POSITIONS_PER_THEME_PER_CYCLE", 1),
            ab_test_aggressive_reentry_requires_profit=_env_bool("AB_TEST_AGGRESSIVE_REENTRY_REQUIRES_PROFIT", True),
            ab_test_aggressive_reentry_min_profit_usdc=_env_float("AB_TEST_AGGRESSIVE_REENTRY_MIN_PROFIT_USDC", 25.0),
            ab_test_aggressive_separate_daemon=_env_bool("AB_TEST_AGGRESSIVE_SEPARATE_DAEMON", True),
            ab_test_aggressive_full_sync_every_cycles=_env_int("AB_TEST_AGGRESSIVE_FULL_SYNC_EVERY_CYCLES", 6),
            ab_test_crypto_enabled=_env_bool("AB_TEST_CRYPTO_ENABLED", True),
            ab_test_crypto_min_whale_confidence=_env_float("AB_TEST_CRYPTO_MIN_WHALE_CONFIDENCE", 0.55),
            ab_test_crypto_require_microstructure_alignment=_env_bool("AB_TEST_CRYPTO_REQUIRE_MICROSTRUCTURE_ALIGNMENT", False),
            ab_test_crypto_fixed_position_usdc=_env_float("AB_TEST_CRYPTO_FIXED_POSITION_USDC", 300.0),
            ab_test_crypto_top_wallets=_env_int("AB_TEST_CRYPTO_TOP_WALLETS", 8),
            ab_test_crypto_source_wallets_limit=_env_int("AB_TEST_CRYPTO_SOURCE_WALLETS_LIMIT", 150),
            ab_test_crypto_wallets_per_refresh=_env_int("AB_TEST_CRYPTO_WALLETS_PER_REFRESH", 40),
            ab_test_crypto_trade_limit=_env_int("AB_TEST_CRYPTO_TRADE_LIMIT", 50),
            ab_test_crypto_markets_limit=_env_int("AB_TEST_CRYPTO_MARKETS_LIMIT", 1000),
            ab_test_crypto_queue_max_candidates=_env_int("AB_TEST_CRYPTO_QUEUE_MAX_CANDIDATES", 40),
            ab_test_crypto_prebook_candidate_limit=_env_int("AB_TEST_CRYPTO_PREBOOK_CANDIDATE_LIMIT", 120),
            ab_test_crypto_min_book_depth_usdc=_env_float("AB_TEST_CRYPTO_MIN_BOOK_DEPTH_USDC", 50.0),
            ab_test_crypto_min_hours_to_resolution=_env_int("AB_TEST_CRYPTO_MIN_HOURS_TO_RESOLUTION", 0),
            ab_test_crypto_separate_daemon=_env_bool("AB_TEST_CRYPTO_SEPARATE_DAEMON", True),
            ab_test_crypto_5m_sniper_enabled=_env_bool("AB_TEST_CRYPTO_5M_SNIPER_ENABLED", True),
            ab_test_crypto_5m_sniper_fixed_position_usdc=_env_float("AB_TEST_CRYPTO_5M_SNIPER_FIXED_POSITION_USDC", 250.0),
            ab_test_crypto_5m_sniper_markets_limit=_env_int("AB_TEST_CRYPTO_5M_SNIPER_MARKETS_LIMIT", 1000),
            ab_test_crypto_5m_sniper_max_minutes_to_resolution=_env_int("AB_TEST_CRYPTO_5M_SNIPER_MAX_MINUTES_TO_RESOLUTION", 12),
            ab_test_crypto_5m_sniper_min_book_depth_usdc=_env_float("AB_TEST_CRYPTO_5M_SNIPER_MIN_BOOK_DEPTH_USDC", 150.0),
            ab_test_crypto_5m_sniper_min_book_imbalance=_env_float("AB_TEST_CRYPTO_5M_SNIPER_MIN_BOOK_IMBALANCE", 0.22),
            ab_test_crypto_5m_sniper_max_side_price=_env_float("AB_TEST_CRYPTO_5M_SNIPER_MAX_SIDE_PRICE", 0.62),
            ab_test_crypto_5m_sniper_hold_minutes=_env_float("AB_TEST_CRYPTO_5M_SNIPER_HOLD_MINUTES", 4.0),
            ab_test_crypto_5m_sniper_take_profit_contract_price=_env_float("AB_TEST_CRYPTO_5M_SNIPER_TAKE_PROFIT_CONTRACT_PRICE", 0.9),
            ab_test_crypto_5m_sniper_separate_daemon=_env_bool("AB_TEST_CRYPTO_5M_SNIPER_SEPARATE_DAEMON", True),
            ab_test_crypto_next_window_sniper_enabled=_env_bool("AB_TEST_CRYPTO_NEXT_WINDOW_SNIPER_ENABLED", True),
            ab_test_crypto_next_window_sniper_fixed_position_usdc=_env_float("AB_TEST_CRYPTO_NEXT_WINDOW_SNIPER_FIXED_POSITION_USDC", 100.0),
            ab_test_crypto_next_window_sniper_markets_limit=_env_int("AB_TEST_CRYPTO_NEXT_WINDOW_SNIPER_MARKETS_LIMIT", 4000),
            ab_test_crypto_next_window_sniper_min_minutes_to_resolution=_env_int("AB_TEST_CRYPTO_NEXT_WINDOW_SNIPER_MIN_MINUTES_TO_RESOLUTION", 60),
            ab_test_crypto_next_window_sniper_max_minutes_to_resolution=_env_int("AB_TEST_CRYPTO_NEXT_WINDOW_SNIPER_MAX_MINUTES_TO_RESOLUTION", 1800),
            ab_test_crypto_next_window_sniper_min_book_depth_usdc=_env_float("AB_TEST_CRYPTO_NEXT_WINDOW_SNIPER_MIN_BOOK_DEPTH_USDC", 60.0),
            ab_test_crypto_next_window_sniper_min_book_imbalance=_env_float("AB_TEST_CRYPTO_NEXT_WINDOW_SNIPER_MIN_BOOK_IMBALANCE", 0.10),
            ab_test_crypto_next_window_sniper_min_midpoint_edge=_env_float("AB_TEST_CRYPTO_NEXT_WINDOW_SNIPER_MIN_MIDPOINT_EDGE", 0.01),
            ab_test_crypto_next_window_sniper_max_side_price=_env_float("AB_TEST_CRYPTO_NEXT_WINDOW_SNIPER_MAX_SIDE_PRICE", 0.72),
            ab_test_crypto_next_window_sniper_hold_minutes=_env_float("AB_TEST_CRYPTO_NEXT_WINDOW_SNIPER_HOLD_MINUTES", 240.0),
            ab_test_crypto_next_window_sniper_take_profit_contract_price=_env_float("AB_TEST_CRYPTO_NEXT_WINDOW_SNIPER_TAKE_PROFIT_CONTRACT_PRICE", 0.72),
            ab_test_crypto_next_window_sniper_separate_daemon=_env_bool("AB_TEST_CRYPTO_NEXT_WINDOW_SNIPER_SEPARATE_DAEMON", True),
            ab_test_crypto_5m_box_enabled=_env_bool("AB_TEST_CRYPTO_5M_BOX_ENABLED", True),
            ab_test_crypto_5m_box_pair_budget_usdc=_env_float("AB_TEST_CRYPTO_5M_BOX_PAIR_BUDGET_USDC", 300.0),
            ab_test_crypto_5m_box_markets_limit=_env_int("AB_TEST_CRYPTO_5M_BOX_MARKETS_LIMIT", 1000),
            ab_test_crypto_5m_box_max_minutes_to_resolution=_env_int("AB_TEST_CRYPTO_5M_BOX_MAX_MINUTES_TO_RESOLUTION", 12),
            ab_test_crypto_5m_box_min_book_depth_usdc=_env_float("AB_TEST_CRYPTO_5M_BOX_MIN_BOOK_DEPTH_USDC", 250.0),
            ab_test_crypto_5m_box_min_edge_per_share=_env_float("AB_TEST_CRYPTO_5M_BOX_MIN_EDGE_PER_SHARE", 0.001),
            ab_test_crypto_5m_box_min_net_edge_per_share=_env_float("AB_TEST_CRYPTO_5M_BOX_MIN_NET_EDGE_PER_SHARE", 0.0002),
            ab_test_crypto_5m_box_estimated_fee_per_share=_env_float("AB_TEST_CRYPTO_5M_BOX_ESTIMATED_FEE_PER_SHARE", 0.0005),
            ab_test_crypto_5m_box_estimated_slippage_per_share=_env_float("AB_TEST_CRYPTO_5M_BOX_ESTIMATED_SLIPPAGE_PER_SHARE", 0.0005),
            ab_test_crypto_5m_box_aggressive_queue_fill_probability=_env_float("AB_TEST_CRYPTO_5M_BOX_AGGRESSIVE_QUEUE_FILL_PROBABILITY", 0.5),
            ab_test_crypto_5m_box_aggressive_maker_fee_per_share=_env_float("AB_TEST_CRYPTO_5M_BOX_AGGRESSIVE_MAKER_FEE_PER_SHARE", 0.0),
            ab_test_crypto_5m_box_aggressive_estimated_slippage_per_share=_env_float("AB_TEST_CRYPTO_5M_BOX_AGGRESSIVE_ESTIMATED_SLIPPAGE_PER_SHARE", 0.0),
            ab_test_crypto_5m_box_aggressive_tick_size=_env_float("AB_TEST_CRYPTO_5M_BOX_AGGRESSIVE_TICK_SIZE", 0.01),
            ab_test_crypto_5m_box_aggressive_price_concession_ticks=_env_int("AB_TEST_CRYPTO_5M_BOX_AGGRESSIVE_PRICE_CONCESSION_TICKS", 0),
            ab_test_crypto_5m_box_hold_minutes=_env_float("AB_TEST_CRYPTO_5M_BOX_HOLD_MINUTES", 4.0),
            ab_test_crypto_5m_box_capture_ratio=_env_float("AB_TEST_CRYPTO_5M_BOX_CAPTURE_RATIO", 0.7),
            ab_test_crypto_5m_box_separate_daemon=_env_bool("AB_TEST_CRYPTO_5M_BOX_SEPARATE_DAEMON", True),
            ab_test_crypto_5m_box_assets=_env_csv("AB_TEST_CRYPTO_5M_BOX_ASSETS") or ("btc",),
            ab_test_crypto_next_window_box_enabled=_env_bool("AB_TEST_CRYPTO_NEXT_WINDOW_BOX_ENABLED", True),
            ab_test_crypto_next_window_box_pair_budget_usdc=_env_float("AB_TEST_CRYPTO_NEXT_WINDOW_BOX_PAIR_BUDGET_USDC", 150.0),
            ab_test_crypto_next_window_box_markets_limit=_env_int("AB_TEST_CRYPTO_NEXT_WINDOW_BOX_MARKETS_LIMIT", 4000),
            ab_test_crypto_next_window_box_min_minutes_to_resolution=_env_int("AB_TEST_CRYPTO_NEXT_WINDOW_BOX_MIN_MINUTES_TO_RESOLUTION", 60),
            ab_test_crypto_next_window_box_max_minutes_to_resolution=_env_int("AB_TEST_CRYPTO_NEXT_WINDOW_BOX_MAX_MINUTES_TO_RESOLUTION", 1800),
            ab_test_crypto_next_window_box_min_book_depth_usdc=_env_float("AB_TEST_CRYPTO_NEXT_WINDOW_BOX_MIN_BOOK_DEPTH_USDC", 60.0),
            ab_test_crypto_next_window_box_min_edge_per_share=_env_float("AB_TEST_CRYPTO_NEXT_WINDOW_BOX_MIN_EDGE_PER_SHARE", 0.001),
            ab_test_crypto_next_window_box_min_net_edge_per_share=_env_float("AB_TEST_CRYPTO_NEXT_WINDOW_BOX_MIN_NET_EDGE_PER_SHARE", 0.0002),
            ab_test_crypto_next_window_box_estimated_fee_per_share=_env_float("AB_TEST_CRYPTO_NEXT_WINDOW_BOX_ESTIMATED_FEE_PER_SHARE", 0.001),
            ab_test_crypto_next_window_box_estimated_slippage_per_share=_env_float("AB_TEST_CRYPTO_NEXT_WINDOW_BOX_ESTIMATED_SLIPPAGE_PER_SHARE", 0.001),
            ab_test_crypto_next_window_box_hold_minutes=_env_float("AB_TEST_CRYPTO_NEXT_WINDOW_BOX_HOLD_MINUTES", 360.0),
            ab_test_crypto_next_window_box_capture_ratio=_env_float("AB_TEST_CRYPTO_NEXT_WINDOW_BOX_CAPTURE_RATIO", 0.5),
            ab_test_crypto_next_window_box_separate_daemon=_env_bool("AB_TEST_CRYPTO_NEXT_WINDOW_BOX_SEPARATE_DAEMON", True),
            ab_test_crypto_intraday_scheduled_enabled=_env_bool("AB_TEST_CRYPTO_INTRADAY_SCHEDULED_ENABLED", True),
            ab_test_crypto_intraday_scheduled_fixed_position_usdc=_env_float("AB_TEST_CRYPTO_INTRADAY_SCHEDULED_FIXED_POSITION_USDC", 100.0),
            ab_test_crypto_intraday_scheduled_markets_limit=_env_int("AB_TEST_CRYPTO_INTRADAY_SCHEDULED_MARKETS_LIMIT", 2000),
            ab_test_crypto_intraday_scheduled_max_minutes_to_resolution=_env_int("AB_TEST_CRYPTO_INTRADAY_SCHEDULED_MAX_MINUTES_TO_RESOLUTION", 1800),
            ab_test_crypto_intraday_scheduled_min_book_depth_usdc=_env_float("AB_TEST_CRYPTO_INTRADAY_SCHEDULED_MIN_BOOK_DEPTH_USDC", 120.0),
            ab_test_crypto_intraday_scheduled_min_book_imbalance=_env_float("AB_TEST_CRYPTO_INTRADAY_SCHEDULED_MIN_BOOK_IMBALANCE", 0.12),
            ab_test_crypto_intraday_scheduled_min_midpoint_edge=_env_float("AB_TEST_CRYPTO_INTRADAY_SCHEDULED_MIN_MIDPOINT_EDGE", 0.01),
            ab_test_crypto_intraday_scheduled_max_side_price=_env_float("AB_TEST_CRYPTO_INTRADAY_SCHEDULED_MAX_SIDE_PRICE", 0.66),
            ab_test_crypto_intraday_scheduled_hold_minutes=_env_float("AB_TEST_CRYPTO_INTRADAY_SCHEDULED_HOLD_MINUTES", 45.0),
            ab_test_crypto_intraday_scheduled_take_profit_contract_price=_env_float("AB_TEST_CRYPTO_INTRADAY_SCHEDULED_TAKE_PROFIT_CONTRACT_PRICE", 0.60),
            ab_test_crypto_intraday_scheduled_separate_daemon=_env_bool("AB_TEST_CRYPTO_INTRADAY_SCHEDULED_SEPARATE_DAEMON", True),
            ab_test_crypto_threshold_snapshot_enabled=_env_bool("AB_TEST_CRYPTO_THRESHOLD_SNAPSHOT_ENABLED", True),
            ab_test_crypto_threshold_snapshot_fixed_position_usdc=_env_float("AB_TEST_CRYPTO_THRESHOLD_SNAPSHOT_FIXED_POSITION_USDC", 100.0),
            ab_test_crypto_threshold_snapshot_markets_limit=_env_int("AB_TEST_CRYPTO_THRESHOLD_SNAPSHOT_MARKETS_LIMIT", 2000),
            ab_test_crypto_threshold_snapshot_max_minutes_to_resolution=_env_int("AB_TEST_CRYPTO_THRESHOLD_SNAPSHOT_MAX_MINUTES_TO_RESOLUTION", 45),
            ab_test_crypto_threshold_snapshot_min_book_depth_usdc=_env_float("AB_TEST_CRYPTO_THRESHOLD_SNAPSHOT_MIN_BOOK_DEPTH_USDC", 40.0),
            ab_test_crypto_threshold_snapshot_min_book_imbalance=_env_float("AB_TEST_CRYPTO_THRESHOLD_SNAPSHOT_MIN_BOOK_IMBALANCE", 0.08),
            ab_test_crypto_threshold_snapshot_min_midpoint_edge=_env_float("AB_TEST_CRYPTO_THRESHOLD_SNAPSHOT_MIN_MIDPOINT_EDGE", 0.02),
            ab_test_crypto_threshold_snapshot_soft_max_spread=_env_float("AB_TEST_CRYPTO_THRESHOLD_SNAPSHOT_SOFT_MAX_SPREAD", 0.08),
            ab_test_crypto_threshold_snapshot_hard_max_spread=_env_float("AB_TEST_CRYPTO_THRESHOLD_SNAPSHOT_HARD_MAX_SPREAD", 0.14),
            ab_test_crypto_threshold_snapshot_min_spread_size_multiplier=_env_float("AB_TEST_CRYPTO_THRESHOLD_SNAPSHOT_MIN_SPREAD_SIZE_MULTIPLIER", 0.35),
            ab_test_crypto_threshold_snapshot_max_side_price=_env_float("AB_TEST_CRYPTO_THRESHOLD_SNAPSHOT_MAX_SIDE_PRICE", 0.85),
            ab_test_crypto_threshold_snapshot_hold_minutes=_env_float("AB_TEST_CRYPTO_THRESHOLD_SNAPSHOT_HOLD_MINUTES", 15.0),
            ab_test_crypto_threshold_snapshot_take_profit_contract_price=_env_float("AB_TEST_CRYPTO_THRESHOLD_SNAPSHOT_TAKE_PROFIT_CONTRACT_PRICE", 0.90),
            ab_test_crypto_threshold_snapshot_separate_daemon=_env_bool("AB_TEST_CRYPTO_THRESHOLD_SNAPSHOT_SEPARATE_DAEMON", True),
            ab_test_sports_early_entry_enabled=_env_bool("AB_TEST_SPORTS_EARLY_ENTRY_ENABLED", False),
            ab_test_sports_early_entry_fixed_position_usdc=_env_float("AB_TEST_SPORTS_EARLY_ENTRY_FIXED_POSITION_USDC", 100.0),
            ab_test_sports_early_entry_markets_limit=_env_int("AB_TEST_SPORTS_EARLY_ENTRY_MARKETS_LIMIT", 800),
            ab_test_sports_early_entry_min_hours_to_resolution=_env_int("AB_TEST_SPORTS_EARLY_ENTRY_MIN_HOURS_TO_RESOLUTION", 4),
            ab_test_sports_early_entry_max_hours_to_resolution=_env_int("AB_TEST_SPORTS_EARLY_ENTRY_MAX_HOURS_TO_RESOLUTION", 96),
            ab_test_sports_early_entry_min_book_depth_usdc=_env_float("AB_TEST_SPORTS_EARLY_ENTRY_MIN_BOOK_DEPTH_USDC", 100.0),
            ab_test_sports_early_entry_min_midpoint=_env_float("AB_TEST_SPORTS_EARLY_ENTRY_MIN_MIDPOINT", 0.08),
            ab_test_sports_early_entry_max_midpoint=_env_float("AB_TEST_SPORTS_EARLY_ENTRY_MAX_MIDPOINT", 0.50),
            ab_test_sports_early_entry_hold_minutes=_env_float("AB_TEST_SPORTS_EARLY_ENTRY_HOLD_MINUTES", 360.0),
            ab_test_sports_early_entry_take_profit_contract_price=_env_float("AB_TEST_SPORTS_EARLY_ENTRY_TAKE_PROFIT_CONTRACT_PRICE", 0.72),
            ab_test_sports_early_entry_separate_daemon=_env_bool("AB_TEST_SPORTS_EARLY_ENTRY_SEPARATE_DAEMON", True),
            ab_test_penny_longshot_enabled=_env_bool("AB_TEST_PENNY_LONGSHOT_ENABLED", False),
            ab_test_penny_longshot_fixed_position_usdc=_env_float("AB_TEST_PENNY_LONGSHOT_FIXED_POSITION_USDC", 25.0),
            ab_test_penny_longshot_markets_limit=_env_int("AB_TEST_PENNY_LONGSHOT_MARKETS_LIMIT", 1200),
            ab_test_penny_longshot_min_hours_to_resolution=_env_int("AB_TEST_PENNY_LONGSHOT_MIN_HOURS_TO_RESOLUTION", 6),
            ab_test_penny_longshot_max_hours_to_resolution=_env_int("AB_TEST_PENNY_LONGSHOT_MAX_HOURS_TO_RESOLUTION", 720),
            ab_test_penny_longshot_min_book_depth_usdc=_env_float("AB_TEST_PENNY_LONGSHOT_MIN_BOOK_DEPTH_USDC", 50.0),
            ab_test_penny_longshot_max_midpoint=_env_float("AB_TEST_PENNY_LONGSHOT_MAX_MIDPOINT", 0.05),
            ab_test_penny_longshot_max_open_positions=_env_int("AB_TEST_PENNY_LONGSHOT_MAX_OPEN_POSITIONS", 20),
            ab_test_penny_longshot_hold_minutes=_env_float("AB_TEST_PENNY_LONGSHOT_HOLD_MINUTES", 1440.0),
            ab_test_penny_longshot_take_profit_contract_price=_env_float("AB_TEST_PENNY_LONGSHOT_TAKE_PROFIT_CONTRACT_PRICE", 0.12),
            ab_test_penny_longshot_separate_daemon=_env_bool("AB_TEST_PENNY_LONGSHOT_SEPARATE_DAEMON", True),
            ab_test_crypto_latency_5m_enabled=_env_bool("AB_TEST_CRYPTO_LATENCY_5M_ENABLED", False),
            ab_test_crypto_latency_5m_fixed_position_usdc=_env_float("AB_TEST_CRYPTO_LATENCY_5M_FIXED_POSITION_USDC", 100.0),
            ab_test_crypto_latency_5m_markets_limit=_env_int("AB_TEST_CRYPTO_LATENCY_5M_MARKETS_LIMIT", 2000),
            ab_test_crypto_latency_5m_max_minutes_to_resolution=_env_int("AB_TEST_CRYPTO_LATENCY_5M_MAX_MINUTES_TO_RESOLUTION", 12),
            ab_test_crypto_latency_5m_min_book_depth_usdc=_env_float("AB_TEST_CRYPTO_LATENCY_5M_MIN_BOOK_DEPTH_USDC", 150.0),
            ab_test_crypto_latency_5m_min_book_imbalance=_env_float("AB_TEST_CRYPTO_LATENCY_5M_MIN_BOOK_IMBALANCE", 0.25),
            ab_test_crypto_latency_5m_max_side_price=_env_float("AB_TEST_CRYPTO_LATENCY_5M_MAX_SIDE_PRICE", 0.60),
            ab_test_crypto_latency_5m_hold_minutes=_env_float("AB_TEST_CRYPTO_LATENCY_5M_HOLD_MINUTES", 4.0),
            ab_test_crypto_latency_5m_take_profit_contract_price=_env_float("AB_TEST_CRYPTO_LATENCY_5M_TAKE_PROFIT_CONTRACT_PRICE", 0.90),
            ab_test_crypto_latency_5m_separate_daemon=_env_bool("AB_TEST_CRYPTO_LATENCY_5M_SEPARATE_DAEMON", True),
            ab_test_verified_public_enabled=_env_bool("AB_TEST_VERIFIED_PUBLIC_ENABLED", True),
            ab_test_verified_public_min_whale_confidence=_env_float("AB_TEST_VERIFIED_PUBLIC_MIN_WHALE_CONFIDENCE", 0.55),
            ab_test_verified_public_require_microstructure_alignment=_env_bool("AB_TEST_VERIFIED_PUBLIC_REQUIRE_MICROSTRUCTURE_ALIGNMENT", False),
            ab_test_verified_public_fixed_position_usdc=_env_float("AB_TEST_VERIFIED_PUBLIC_FIXED_POSITION_USDC", 300.0),
            intraday_registry_enabled=_env_bool("INTRADAY_REGISTRY_ENABLED", True),
            intraday_registry_bootstrap_limit=_env_int("INTRADAY_REGISTRY_BOOTSTRAP_LIMIT", 4000),
            intraday_registry_gamma_refresh_limit=_env_int("INTRADAY_REGISTRY_GAMMA_REFRESH_LIMIT", 1000),
            intraday_registry_gamma_refresh_max_queries=_env_int("INTRADAY_REGISTRY_GAMMA_REFRESH_MAX_QUERIES", 4),
            intraday_registry_gamma_search_timeout_seconds=_env_int("INTRADAY_REGISTRY_GAMMA_SEARCH_TIMEOUT_SECONDS", 3),
            intraday_registry_max_minutes_to_resolution=_env_int("INTRADAY_REGISTRY_MAX_MINUTES_TO_RESOLUTION", 120),
            intraday_registry_stale_after_seconds=_env_int("INTRADAY_REGISTRY_STALE_AFTER_SECONDS", 90),
            intraday_registry_ws_enabled=_env_bool("INTRADAY_REGISTRY_WS_ENABLED", True),
            intraday_registry_ws_endpoint=os.getenv("INTRADAY_REGISTRY_WS_ENDPOINT", "wss://ws-subscriptions-clob.polymarket.com/ws/market"),
            intraday_registry_prefer_registry=_env_bool("INTRADAY_REGISTRY_PREFER_REGISTRY", True),
            intraday_registry_rejections_limit=_env_int("INTRADAY_REGISTRY_REJECTIONS_LIMIT", 200),
            intraday_registry_raw_updown_limit=_env_int("INTRADAY_REGISTRY_RAW_UPDOWN_LIMIT", 200),
            intraday_registry_imminent_updown_limit=_env_int("INTRADAY_REGISTRY_IMMINENT_UPDOWN_LIMIT", 200),
            intraday_registry_imminent_seen_age_minutes=_env_int("INTRADAY_REGISTRY_IMMINENT_SEEN_AGE_MINUTES", 45),
            intraday_registry_imminent_max_minutes_to_resolution=_env_int("INTRADAY_REGISTRY_IMMINENT_MAX_MINUTES_TO_RESOLUTION", 45),
            intraday_registry_watchlist_max_minutes_to_resolution=_env_int("INTRADAY_REGISTRY_WATCHLIST_MAX_MINUTES_TO_RESOLUTION", 360),
            intraday_registry_book_tape_limit=_env_int("INTRADAY_REGISTRY_BOOK_TAPE_LIMIT", 400),
            intraday_registry_book_snapshot_topn=_env_int("INTRADAY_REGISTRY_BOOK_SNAPSHOT_TOPN", 40),
            intraday_registry_threshold_live_limit=_env_int("INTRADAY_REGISTRY_THRESHOLD_LIVE_LIMIT", 120),
            intraday_registry_threshold_live_max_minutes_to_resolution=_env_int("INTRADAY_REGISTRY_THRESHOLD_LIVE_MAX_MINUTES_TO_RESOLUTION", 45),
            intraday_registry_ws_batch_asset_limit=_env_int("INTRADAY_REGISTRY_WS_BATCH_ASSET_LIMIT", 120),
            intraday_registry_ws_max_message_bytes=_env_int("INTRADAY_REGISTRY_WS_MAX_MESSAGE_BYTES", 8388608),
            main_daemon_full_sync_every_cycles=_env_int("MAIN_DAEMON_FULL_SYNC_EVERY_CYCLES", 6),
            daemon_cycle_timeout_seconds=_env_int("DAEMON_CYCLE_TIMEOUT_SECONDS", 45),
            daemon_interval_seconds=_env_int("DAEMON_INTERVAL_SECONDS", 300),
            dashboard_host=os.getenv("DASHBOARD_HOST", "127.0.0.1"),
            dashboard_port=_env_int("DASHBOARD_PORT", 8080),
            targets_path=Path(os.getenv("TARGETS_PATH", "state/targets.json")),
            target_activity_path=Path(os.getenv("TARGET_ACTIVITY_PATH", "state/target_activity.json")),
            crypto_targets_path=Path(os.getenv("CRYPTO_TARGETS_PATH", "state/crypto_targets.json")),
            crypto_activity_path=Path(os.getenv("CRYPTO_ACTIVITY_PATH", "state/crypto_activity.json")),
            crypto_source_targets_path=Path(os.getenv("CRYPTO_SOURCE_TARGETS_PATH", "state/crypto_source_targets.json")),
            crypto_source_activity_path=Path(os.getenv("CRYPTO_SOURCE_ACTIVITY_PATH", "state/crypto_source_activity.json")),
            verified_public_traders_path=Path(os.getenv("VERIFIED_PUBLIC_TRADERS_PATH", "config/verified_public_traders.json")),
            verified_public_activity_path=Path(os.getenv("VERIFIED_PUBLIC_ACTIVITY_PATH", "state/verified_public_activity.json")),
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
            intraday_registry_path=Path(os.getenv("INTRADAY_REGISTRY_PATH", "state/intraday_registry.json")),
            intraday_registry_rejections_path=Path(os.getenv("INTRADAY_REGISTRY_REJECTIONS_PATH", "state/intraday_registry_rejections.json")),
            intraday_registry_raw_updown_path=Path(os.getenv("INTRADAY_REGISTRY_RAW_UPDOWN_PATH", "state/intraday_updown_raw.json")),
            intraday_registry_imminent_updown_path=Path(os.getenv("INTRADAY_REGISTRY_IMMINENT_UPDOWN_PATH", "state/intraday_updown_imminent.json")),
            intraday_registry_book_tape_path=Path(os.getenv("INTRADAY_REGISTRY_BOOK_TAPE_PATH", "state/intraday_book_tape.json")),
            intraday_registry_threshold_live_path=Path(os.getenv("INTRADAY_REGISTRY_THRESHOLD_LIVE_PATH", "state/intraday_threshold_live.json")),
            intraday_registry_transition_tape_path=Path(os.getenv("INTRADAY_REGISTRY_TRANSITION_TAPE_PATH", "state/intraday_transition_tape.json")),
            intraday_registry_imminent_box_arb_tape_path=Path(os.getenv("INTRADAY_REGISTRY_IMMINENT_BOX_ARB_TAPE_PATH", "state/intraday_imminent_box_arb_tape.json")),
            intraday_registry_edge_alerts_path=Path(os.getenv("INTRADAY_REGISTRY_EDGE_ALERTS_PATH", "state/intraday_edge_alerts.json")),
            intraday_registry_audit_sqlite_path=Path(os.getenv("INTRADAY_REGISTRY_AUDIT_SQLITE_PATH", "state/intraday_audit.sqlite3")),
        )

    def ensure_dirs(self) -> None:
        for path in [
            self.targets_path,
            self.target_activity_path,
            self.crypto_targets_path,
            self.crypto_activity_path,
            self.crypto_source_targets_path,
            self.crypto_source_activity_path,
            self.verified_public_traders_path,
            self.verified_public_activity_path,
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
            self.intraday_registry_path,
            self.intraday_registry_rejections_path,
            self.intraday_registry_raw_updown_path,
            self.intraday_registry_imminent_updown_path,
            self.intraday_registry_book_tape_path,
            self.intraday_registry_threshold_live_path,
            self.intraday_registry_transition_tape_path,
            self.intraday_registry_imminent_box_arb_tape_path,
            self.intraday_registry_edge_alerts_path,
            self.intraday_registry_audit_sqlite_path,
        ]:
            path.parent.mkdir(parents=True, exist_ok=True)
