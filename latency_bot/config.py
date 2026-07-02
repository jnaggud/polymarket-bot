from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _load_env_file(path: Path = Path(".env")) -> None:
    if not path.exists():
        return
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip().strip('"').strip("'")
        os.environ[key] = value


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw is not None else default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw is not None else default


def _env_csv_int(name: str, default: tuple[int, ...]) -> tuple[int, ...]:
    raw = os.getenv(name)
    if not raw:
        return default
    values: list[int] = []
    for item in raw.split(","):
        item = item.strip()
        if item:
            values.append(int(item))
    return tuple(values) if values else default


def _env_csv_float(name: str, default: tuple[float, ...]) -> tuple[float, ...]:
    raw = os.getenv(name)
    if not raw:
        return default
    values: list[float] = []
    for item in raw.split(","):
        item = item.strip()
        if item:
            values.append(float(item))
    return tuple(values) if values else default


def _env_csv_str(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.getenv(name)
    if not raw:
        return default
    values = tuple(item.strip().lower() for item in raw.split(",") if item.strip())
    return values or default


@dataclass(slots=True)
class LatencyBotSettings:
    db_path: Path
    status_path: Path
    markets_path: Path
    polymarket_cache_path: Path
    binance_cache_path: Path
    bootstrap_intraday_book_tape_path: Path
    dashboard_host: str
    dashboard_port: int
    bankroll_usdc: float
    assets: tuple[str, ...]
    tenors_minutes: tuple[int, ...]
    taker_min_edge_5m: float
    taker_min_edge_15m: float
    maker_min_edge_5m: float
    maker_min_edge_15m: float
    maker_quote_life_5m_seconds: int
    maker_quote_life_15m_seconds: int
    max_reprices: int
    max_total_open_notional_fraction: float
    max_btc_open_notional_fraction: float
    max_eth_open_notional_fraction: float
    max_simultaneous_positions: int
    max_same_direction_positions_per_asset: int
    feed_max_binance_staleness_seconds: int
    feed_max_polymarket_staleness_seconds: int
    daemon_interval_seconds: int
    discovery_refresh_seconds: int
    discovery_limit: int
    discovery_lookahead_minutes: int
    book_fetch_horizon_minutes: int
    paper_position_notional_usdc: float
    taker_fee_per_share: float
    taker_slippage_per_share: float
    min_trade_price: float
    max_trade_price: float
    min_book_depth_usdc: float
    max_book_age_ms: float
    max_reference_sample_age_seconds: int
    min_volatility_floor: float
    min_binance_observations: int
    min_taker_entry_seconds_left_5m: int
    min_taker_entry_seconds_left_15m: int
    taker_min_fair_yes_5m: float
    taker_min_fair_yes_15m: float
    exit_edge_floor: float
    min_hold_seconds_before_edge_close: int
    force_exit_seconds_5m: int
    force_exit_seconds_15m: int
    stop_loss_fraction: float
    take_profit_fraction: float
    same_market_cooldown_seconds: int
    stop_loss_streak_pause_count: int
    stop_loss_pause_seconds: int
    recent_loss_window_trades: int
    recent_loss_pause_threshold_usdc: float
    max_btc_5m_eligible_rate: float
    allow_btc_yes_taker: bool
    allow_eth_yes_taker: bool
    allow_15m_taker: bool
    allow_taker_no: bool
    allow_eth_no_taker: bool
    shadow_allow_btc_no_taker: bool
    shadow_taker_min_edge_5m: float
    shadow_taker_min_fair_no_5m: float
    shadow_yes_fair_floors_5m: tuple[float, ...]
    shadow_yes_model_variants_5m: tuple[str, ...]
    shadow_variant_assets: tuple[str, ...]
    shadow_variant_sides: tuple[str, ...]
    shadow_variant_tenors_minutes: tuple[int, ...]
    shadow_variant_fair_floors: tuple[float, ...]
    shadow_variant_models: tuple[str, ...]
    shadow_variant_edge_thresholds_5m: tuple[float, ...]
    shadow_variant_edge_thresholds_15m: tuple[float, ...]
    shadow_variant_entry_seconds_5m: tuple[int, ...]
    shadow_variant_entry_seconds_15m: tuple[int, ...]
    shadow_variant_watchlist: tuple[str, ...]
    promoted_variant_ids: tuple[str, ...]
    promoted_variant_max_open_positions: int
    complete_set_arb_enabled: bool
    complete_set_arb_min_profit_per_share: float
    complete_set_arb_slippage_per_share: float
    complete_set_arb_min_depth_usdc: float
    complete_set_arb_max_book_age_ms: float
    complete_set_arb_min_seconds_left: int
    complete_set_arb_max_seconds_left: int
    complete_set_arb_notional_usdc: float
    complete_set_arb_max_sets_per_cycle: int
    complete_set_arb_same_market_cooldown_seconds: int
    complete_set_arb_execution_policy: str
    live_complete_set_arb_pilot_enabled: bool
    live_complete_set_arb_pilot_mode: str
    live_complete_set_arb_pilot_confirm: str
    live_complete_set_arb_pilot_capital_usdc: float
    live_complete_set_arb_pilot_notional_usdc: float
    live_complete_set_arb_pilot_min_edge_per_share: float
    live_complete_set_arb_pilot_min_depth_usdc: float
    live_complete_set_arb_pilot_depth_haircut: float
    live_complete_set_arb_pilot_extra_slippage_per_share: float
    live_complete_set_arb_pilot_min_seconds_left: int
    live_complete_set_arb_pilot_min_leg_amount_usdc: float
    live_complete_set_arb_pilot_max_sets_per_cycle: int
    live_complete_set_arb_pilot_max_open_sets: int
    live_complete_set_arb_pilot_daily_loss_limit_usdc: float
    live_complete_set_arb_pilot_allow_sequential_orders: bool
    live_complete_set_arb_pilot_require_fok: bool
    live_complete_set_arb_pilot_enable_rescue: bool
    live_complete_set_arb_pilot_same_market_cooldown_seconds: int
    live_complete_set_arb_pilot_private_key: str
    live_complete_set_arb_pilot_api_key: str
    live_complete_set_arb_pilot_api_secret: str
    live_complete_set_arb_pilot_api_passphrase: str
    live_complete_set_arb_pilot_funder_address: str
    live_complete_set_arb_pilot_signature_type: int
    live_complete_set_arb_pilot_host: str
    live_complete_set_arb_pilot_chain_id: int
    live_complete_set_arb_pilot_tick_size: str
    live_complete_set_arb_pilot_neg_risk: bool
    shadow_variant_top_raw_pnl_count: int
    shadow_variant_dashboard_grid_limit: int
    shadow_variant_dashboard_reason_limit: int
    shadow_variant_dashboard_family_limit: int
    allow_maker_join: bool
    allow_maker_improve: bool
    binance_rest_endpoint: str
    binance_sample_history: int
    polymarket_cli_timeout_seconds: int
    polymarket_clob_book_endpoint: str
    realistic_complete_set_arb_enabled: bool = True
    realistic_complete_set_arb_capital_usdc: float = 3000.0
    realistic_complete_set_arb_lookback_hours: int = 24
    realistic_complete_set_arb_depth_haircut: float = 0.50
    realistic_complete_set_arb_latency_ms: float = 500.0
    realistic_complete_set_arb_latency_edge_decay_per_second: float = 0.0030
    realistic_complete_set_arb_extra_slippage_per_share: float = 0.0030
    realistic_complete_set_arb_partial_fill_fraction: float = 0.50
    realistic_complete_set_arb_failed_leg_loss_fraction: float = 0.0100
    realistic_complete_set_arb_operational_failure_rate: float = 0.02
    realistic_complete_set_arb_redeem_lag_seconds: int = 60
    preowned_inventory_arb_capital_usdc: float = 50.0
    preowned_inventory_arb_notional_usdc: float = 15.66
    preowned_inventory_arb_seed_side_notional_usdc: float = 7.83
    preowned_inventory_arb_min_edge_per_share: float = 0.0200
    preowned_inventory_arb_min_depth_usdc: float = 2.0
    preowned_inventory_arb_min_seconds_left: int = 60
    preowned_inventory_arb_seed_min_seconds_left: int = 180
    preowned_inventory_arb_seed_max_seconds_left: int = 900
    preowned_inventory_arb_seed_max_complete_set_cost: float = 1.0
    preowned_inventory_arb_seed_min_depth_usdc: float = 0.0
    preowned_inventory_arb_same_market_cooldown_seconds: int = 300
    preowned_inventory_arb_max_open_seeded_markets: int = 3
    preowned_inventory_arb_per_asset_time_bucket_cap: int = 1
    preowned_inventory_arb_time_bucket_seconds: int = 300
    preowned_inventory_arb_seed_require_original_eligible: bool = False
    cex_latency_paper_enabled: bool = True
    cex_latency_paper_capital_usdc: float = 1000.0
    cex_latency_paper_notional_usdc: float = 50.0
    cex_latency_paper_max_open_positions: int = 3
    cex_latency_paper_assets: tuple[str, ...] = ("btc", "eth")
    cex_latency_paper_min_edge_per_share: float = 0.0200
    cex_latency_paper_min_depth_usdc: float = 750.0
    cex_latency_paper_max_book_age_ms: float = 15000.0
    cex_latency_paper_min_seconds_left_5m: int = 90
    cex_latency_paper_min_seconds_left_15m: int = 180
    cex_latency_paper_max_seconds_left: int = 900
    cex_latency_paper_min_trade_price: float = 0.25
    cex_latency_paper_max_trade_price: float = 0.85
    cex_latency_paper_model: str = "quant_poc"
    cex_latency_paper_stop_loss_fraction: float = 0.35
    cex_latency_paper_take_profit_fraction: float = 0.35
    cex_latency_paper_exit_edge_floor: float = 0.0100
    cex_latency_paper_force_exit_seconds: int = 30
    cex_latency_paper_same_market_cooldown_seconds: int = 300
    btc_fair_value_paper_enabled: bool = True
    btc_fair_value_paper_capital_usdc: float = 1000.0
    btc_fair_value_paper_notional_usdc: float = 50.0
    btc_fair_value_paper_max_open_positions: int = 3
    btc_fair_value_paper_assets: tuple[str, ...] = ("btc",)
    btc_fair_value_paper_min_edge_per_share: float = 0.0500
    btc_fair_value_paper_min_depth_usdc: float = 500.0
    btc_fair_value_paper_max_book_age_ms: float = 5000.0
    btc_fair_value_paper_min_seconds_left: int = 90
    btc_fair_value_paper_max_seconds_left: int = 360
    btc_fair_value_paper_min_trade_price: float = 0.08
    btc_fair_value_paper_max_trade_price: float = 0.92
    btc_fair_value_paper_market_weight: float = 0.45
    btc_fair_value_paper_microprice_weight: float = 0.25
    btc_fair_value_paper_binance_weight: float = 0.30
    btc_fair_value_paper_min_model_confidence: float = 0.035
    btc_fair_value_paper_stop_loss_fraction: float = 0.40
    btc_fair_value_paper_take_profit_fraction: float = 0.60
    btc_fair_value_paper_exit_edge_floor: float = 0.0000
    btc_fair_value_paper_force_exit_seconds: int = 10
    btc_fair_value_paper_same_market_cooldown_seconds: int = 300
    temporal_inventory_maker_paper_enabled: bool = True
    temporal_inventory_maker_paper_capital_usdc: float = 1000.0
    temporal_inventory_maker_paper_max_market_exposure_usdc: float = 150.0
    temporal_inventory_maker_paper_max_total_exposure_usdc: float = 500.0
    temporal_inventory_maker_paper_base_order_usdc: float = 25.0
    temporal_inventory_maker_paper_min_net_edge: float = 0.0200
    temporal_inventory_maker_paper_max_pair_cost: float = 0.9900
    temporal_inventory_maker_paper_quote_ttl_seconds: int = 12
    temporal_inventory_maker_paper_force_exit_seconds: int = 20
    temporal_inventory_maker_paper_daily_loss_limit_usdc: float = 25.0
    live_temporal_inventory_maker_enabled: bool = False
    live_temporal_inventory_maker_mode: str = "dry_run"
    live_temporal_inventory_maker_confirm: str = ""
    live_temporal_inventory_maker_capital_usdc: float = 50.0
    live_temporal_inventory_maker_base_order_usdc: float = 5.0
    live_temporal_inventory_maker_max_open_orders: int = 1
    live_temporal_inventory_maker_max_orders_per_cycle: int = 1
    live_temporal_inventory_maker_min_edge: float = 0.0300
    live_temporal_inventory_maker_min_seconds_left: int = 90
    live_temporal_inventory_maker_max_order_age_seconds: int = 20
    live_temporal_inventory_maker_heartbeat_timeout_seconds: int = 30
    live_temporal_inventory_maker_daily_loss_limit_usdc: float = 5.0
    live_temporal_inventory_maker_require_positive_paper_pnl: bool = True
    live_temporal_inventory_maker_require_reconciliation: bool = True
    late_resolution_capture_paper_enabled: bool = True
    late_resolution_capture_paper_capital_usdc: float = 1000.0
    late_resolution_capture_paper_notional_usdc: float = 25.0
    late_resolution_capture_paper_max_market_exposure_usdc: float = 50.0
    late_resolution_capture_paper_max_total_exposure_usdc: float = 150.0
    late_resolution_capture_paper_min_seconds_left: int = 3
    late_resolution_capture_paper_max_seconds_left: int = 45
    late_resolution_capture_paper_min_official_confidence: float = 0.97
    late_resolution_capture_paper_min_boundary_distance_bps: float = 8.0
    late_resolution_capture_paper_min_edge: float = 0.0100
    late_resolution_capture_paper_daily_loss_limit_usdc: float = 25.0
    wallet_teacher_sniper_enabled: bool = True
    wallet_teacher_sniper_wallet: str = "0xb27bc932bf8110d8f78e55da7d5f0497a18b5b82"
    wallet_teacher_sniper_capital_usdc: float = 1000.0
    wallet_teacher_sniper_notional_usdc: float = 50.0
    wallet_teacher_sniper_max_open_positions: int = 3
    wallet_teacher_sniper_assets: tuple[str, ...] = ("btc",)
    wallet_teacher_sniper_trade_lookback_seconds: int = 120
    wallet_teacher_sniper_fetch_limit: int = 100
    wallet_teacher_sniper_min_teacher_notional_usdc: float = 5.0
    wallet_teacher_sniper_min_depth_usdc: float = 50.0
    wallet_teacher_sniper_max_book_age_ms: float = 15000.0
    wallet_teacher_sniper_min_seconds_left: int = 30
    wallet_teacher_sniper_max_seconds_left: int = 360
    wallet_teacher_sniper_min_trade_price: float = 0.05
    wallet_teacher_sniper_max_trade_price: float = 0.95
    wallet_teacher_sniper_stop_loss_fraction: float = 0.50
    wallet_teacher_sniper_take_profit_fraction: float = 0.75
    wallet_teacher_sniper_exit_edge_floor: float = -1.0
    wallet_teacher_sniper_force_exit_seconds: int = 8
    wallet_teacher_sniper_same_market_cooldown_seconds: int = 60
    polymarket_us_arb_enabled: bool = True
    polymarket_us_arb_symbols: tuple[str, ...] = ()
    polymarket_us_arb_orderbook_endpoint: str = "https://api.prod.polymarketexchange.com/v1/orderbook"
    polymarket_us_arb_api_key: str = ""
    polymarket_us_key_id: str = ""
    polymarket_us_secret_key: str = ""
    polymarket_us_arb_lookback_hours: int = 24
    polymarket_us_arb_capital_usdc: float = 3000.0
    polymarket_us_arb_notional_usdc: float = 1000.0
    polymarket_us_arb_min_edge_per_share: float = 0.0025
    polymarket_us_arb_min_depth_usdc: float = 100.0
    polymarket_us_arb_depth_haircut: float = 0.50
    polymarket_us_arb_latency_ms: float = 500.0
    polymarket_us_arb_latency_edge_decay_per_second: float = 0.0030
    polymarket_us_arb_extra_slippage_per_share: float = 0.0030
    polymarket_us_arb_fee_per_share: float = 0.0
    polymarket_us_arb_operational_failure_rate: float = 0.02
    polymarket_us_arb_failed_leg_loss_fraction: float = 0.0100
    polymarket_us_arb_same_symbol_cooldown_seconds: int = 10
    kalshi_arb_enabled: bool = True
    kalshi_arb_api_base: str = "https://external-api.kalshi.com/trade-api/v2"
    kalshi_arb_tickers: tuple[str, ...] = ()
    kalshi_arb_auto_discover: bool = True
    kalshi_arb_assets: tuple[str, ...] = ("btc", "eth", "sol")
    kalshi_arb_market_limit: int = 1000
    kalshi_arb_lookback_hours: int = 24
    kalshi_arb_capital_usdc: float = 3000.0
    kalshi_arb_notional_usdc: float = 1000.0
    kalshi_arb_min_edge_per_share: float = 0.0025
    kalshi_arb_min_depth_usdc: float = 100.0
    kalshi_arb_depth_haircut: float = 0.50
    kalshi_arb_latency_ms: float = 500.0
    kalshi_arb_latency_edge_decay_per_second: float = 0.0030
    kalshi_arb_extra_slippage_per_share: float = 0.0030
    kalshi_arb_fee_per_share: float = 0.0
    kalshi_arb_operational_failure_rate: float = 0.02
    kalshi_arb_failed_leg_loss_fraction: float = 0.0100
    kalshi_arb_same_ticker_cooldown_seconds: int = 10
    kalshi_arb_settlement_lag_seconds: int = 60
    kalshi_api_key_id: str = ""
    kalshi_private_key_path: str = ""
    kalshi_private_key_pem: str = ""

    @classmethod
    def from_env(cls) -> "LatencyBotSettings":
        _load_env_file()
        return cls(
            db_path=Path(os.getenv("LATENCY_BOT_DB_PATH", "state/latency_bot.sqlite3")),
            status_path=Path(os.getenv("LATENCY_BOT_STATUS_PATH", "state/latency_bot_status.json")),
            markets_path=Path(os.getenv("LATENCY_BOT_MARKETS_PATH", "state/latency_bot_markets.json")),
            polymarket_cache_path=Path(os.getenv("LATENCY_BOT_POLYMARKET_CACHE_PATH", "state/latency_bot_polymarket_cache.json")),
            binance_cache_path=Path(os.getenv("LATENCY_BOT_BINANCE_CACHE_PATH", "state/latency_bot_binance_cache.json")),
            bootstrap_intraday_book_tape_path=Path(os.getenv("LATENCY_BOT_BOOTSTRAP_INTRADAY_BOOK_TAPE_PATH", "state/intraday_book_tape.json")),
            dashboard_host=os.getenv("LATENCY_BOT_DASHBOARD_HOST", "127.0.0.1"),
            dashboard_port=_env_int("LATENCY_BOT_DASHBOARD_PORT", 8090),
            bankroll_usdc=_env_float("LATENCY_BOT_BANKROLL_USDC", 10000.0),
            assets=_env_csv_str("LATENCY_BOT_ASSETS", ("btc", "eth")),
            tenors_minutes=_env_csv_int("LATENCY_BOT_TENORS_MINUTES", (5, 15)),
            taker_min_edge_5m=_env_float("LATENCY_BOT_TAKER_MIN_EDGE_5M", 0.03),
            taker_min_edge_15m=_env_float("LATENCY_BOT_TAKER_MIN_EDGE_15M", 0.06),
            maker_min_edge_5m=_env_float("LATENCY_BOT_MAKER_MIN_EDGE_5M", 0.015),
            maker_min_edge_15m=_env_float("LATENCY_BOT_MAKER_MIN_EDGE_15M", 0.012),
            maker_quote_life_5m_seconds=_env_int("LATENCY_BOT_MAKER_QUOTE_LIFE_5M_SECONDS", 8),
            maker_quote_life_15m_seconds=_env_int("LATENCY_BOT_MAKER_QUOTE_LIFE_15M_SECONDS", 12),
            max_reprices=_env_int("LATENCY_BOT_MAX_REPRICES", 2),
            max_total_open_notional_fraction=_env_float("LATENCY_BOT_MAX_TOTAL_OPEN_NOTIONAL_FRACTION", 0.08),
            max_btc_open_notional_fraction=_env_float("LATENCY_BOT_MAX_BTC_OPEN_NOTIONAL_FRACTION", 0.04),
            max_eth_open_notional_fraction=_env_float("LATENCY_BOT_MAX_ETH_OPEN_NOTIONAL_FRACTION", 0.04),
            max_simultaneous_positions=_env_int("LATENCY_BOT_MAX_SIMULTANEOUS_POSITIONS", 4),
            max_same_direction_positions_per_asset=_env_int("LATENCY_BOT_MAX_SAME_DIRECTION_POSITIONS_PER_ASSET", 1),
            feed_max_binance_staleness_seconds=_env_int("LATENCY_BOT_FEED_MAX_BINANCE_STALENESS_SECONDS", 1),
            feed_max_polymarket_staleness_seconds=_env_int("LATENCY_BOT_FEED_MAX_POLYMARKET_STALENESS_SECONDS", 2),
            daemon_interval_seconds=_env_int("LATENCY_BOT_DAEMON_INTERVAL_SECONDS", 15),
            discovery_refresh_seconds=_env_int("LATENCY_BOT_DISCOVERY_REFRESH_SECONDS", 60),
            discovery_limit=_env_int("LATENCY_BOT_DISCOVERY_LIMIT", 500),
            discovery_lookahead_minutes=_env_int("LATENCY_BOT_DISCOVERY_LOOKAHEAD_MINUTES", 360),
            book_fetch_horizon_minutes=_env_int("LATENCY_BOT_BOOK_FETCH_HORIZON_MINUTES", 60),
            paper_position_notional_usdc=_env_float("LATENCY_BOT_PAPER_POSITION_NOTIONAL_USDC", 100.0),
            taker_fee_per_share=_env_float("LATENCY_BOT_TAKER_FEE_PER_SHARE", 0.0),
            taker_slippage_per_share=_env_float("LATENCY_BOT_TAKER_SLIPPAGE_PER_SHARE", 0.0025),
            min_trade_price=_env_float("LATENCY_BOT_MIN_TRADE_PRICE", 0.35),
            max_trade_price=_env_float("LATENCY_BOT_MAX_TRADE_PRICE", 0.97),
            min_book_depth_usdc=_env_float("LATENCY_BOT_MIN_BOOK_DEPTH_USDC", 750.0),
            max_book_age_ms=_env_float("LATENCY_BOT_MAX_BOOK_AGE_MS", 15000.0),
            max_reference_sample_age_seconds=_env_int("LATENCY_BOT_MAX_REFERENCE_SAMPLE_AGE_SECONDS", 45),
            min_volatility_floor=_env_float("LATENCY_BOT_MIN_VOLATILITY_FLOOR", 0.0005),
            min_binance_observations=_env_int("LATENCY_BOT_MIN_BINANCE_OBSERVATIONS", 3),
            min_taker_entry_seconds_left_5m=_env_int("LATENCY_BOT_MIN_TAKER_ENTRY_SECONDS_LEFT_5M", 180),
            min_taker_entry_seconds_left_15m=_env_int("LATENCY_BOT_MIN_TAKER_ENTRY_SECONDS_LEFT_15M", 300),
            taker_min_fair_yes_5m=_env_float("LATENCY_BOT_TAKER_MIN_FAIR_YES_5M", 0.60),
            taker_min_fair_yes_15m=_env_float("LATENCY_BOT_TAKER_MIN_FAIR_YES_15M", 0.58),
            exit_edge_floor=_env_float("LATENCY_BOT_EXIT_EDGE_FLOOR", 0.005),
            min_hold_seconds_before_edge_close=_env_int("LATENCY_BOT_MIN_HOLD_SECONDS_BEFORE_EDGE_CLOSE", 120),
            force_exit_seconds_5m=_env_int("LATENCY_BOT_FORCE_EXIT_SECONDS_5M", 45),
            force_exit_seconds_15m=_env_int("LATENCY_BOT_FORCE_EXIT_SECONDS_15M", 90),
            stop_loss_fraction=_env_float("LATENCY_BOT_STOP_LOSS_FRACTION", 0.25),
            take_profit_fraction=_env_float("LATENCY_BOT_TAKE_PROFIT_FRACTION", 0.20),
            same_market_cooldown_seconds=_env_int("LATENCY_BOT_SAME_MARKET_COOLDOWN_SECONDS", 900),
            stop_loss_streak_pause_count=_env_int("LATENCY_BOT_STOP_LOSS_STREAK_PAUSE_COUNT", 3),
            stop_loss_pause_seconds=_env_int("LATENCY_BOT_STOP_LOSS_PAUSE_SECONDS", 1800),
            recent_loss_window_trades=_env_int("LATENCY_BOT_RECENT_LOSS_WINDOW_TRADES", 5),
            recent_loss_pause_threshold_usdc=_env_float("LATENCY_BOT_RECENT_LOSS_PAUSE_THRESHOLD_USDC", -100.0),
            max_btc_5m_eligible_rate=_env_float("LATENCY_BOT_MAX_BTC_5M_ELIGIBLE_RATE", 0.35),
            allow_btc_yes_taker=os.getenv("LATENCY_BOT_ALLOW_BTC_YES_TAKER", "1").strip().lower() in {"1", "true", "yes", "on"},
            allow_eth_yes_taker=os.getenv("LATENCY_BOT_ALLOW_ETH_YES_TAKER", "0").strip().lower() in {"1", "true", "yes", "on"},
            allow_15m_taker=os.getenv("LATENCY_BOT_ALLOW_15M_TAKER", "0").strip().lower() in {"1", "true", "yes", "on"},
            allow_taker_no=os.getenv("LATENCY_BOT_ALLOW_TAKER_NO", "0").strip().lower() in {"1", "true", "yes", "on"},
            allow_eth_no_taker=os.getenv("LATENCY_BOT_ALLOW_ETH_NO_TAKER", "0").strip().lower() in {"1", "true", "yes", "on"},
            shadow_allow_btc_no_taker=os.getenv("LATENCY_BOT_SHADOW_ALLOW_BTC_NO_TAKER", "1").strip().lower() in {"1", "true", "yes", "on"},
            shadow_taker_min_edge_5m=_env_float("LATENCY_BOT_SHADOW_TAKER_MIN_EDGE_5M", 0.03),
            shadow_taker_min_fair_no_5m=_env_float("LATENCY_BOT_SHADOW_TAKER_MIN_FAIR_NO_5M", 0.55),
            shadow_yes_fair_floors_5m=_env_csv_float("LATENCY_BOT_SHADOW_YES_FAIR_FLOORS_5M", (0.50, 0.52, 0.55, 0.60)),
            shadow_yes_model_variants_5m=_env_csv_str(
                "LATENCY_BOT_SHADOW_YES_MODEL_VARIANTS_5M",
                ("calibrated_digital", "logit_proxy", "gbt_proxy", "market_blend"),
            ),
            shadow_variant_assets=_env_csv_str("LATENCY_BOT_SHADOW_VARIANT_ASSETS", ("btc", "eth")),
            shadow_variant_sides=_env_csv_str("LATENCY_BOT_SHADOW_VARIANT_SIDES", ("yes", "no")),
            shadow_variant_tenors_minutes=_env_csv_int("LATENCY_BOT_SHADOW_VARIANT_TENORS_MINUTES", (5, 15)),
            shadow_variant_fair_floors=_env_csv_float("LATENCY_BOT_SHADOW_VARIANT_FAIR_FLOORS", (0.50, 0.52)),
            shadow_variant_models=_env_csv_str(
                "LATENCY_BOT_SHADOW_VARIANT_MODELS",
                ("fair", "calibrated_digital", "logit_proxy", "gbt_proxy", "market_blend"),
            ),
            shadow_variant_edge_thresholds_5m=_env_csv_float("LATENCY_BOT_SHADOW_VARIANT_EDGE_THRESHOLDS_5M", (0.03, 0.05)),
            shadow_variant_edge_thresholds_15m=_env_csv_float("LATENCY_BOT_SHADOW_VARIANT_EDGE_THRESHOLDS_15M", (0.05,)),
            shadow_variant_entry_seconds_5m=_env_csv_int("LATENCY_BOT_SHADOW_VARIANT_ENTRY_SECONDS_5M", (180,)),
            shadow_variant_entry_seconds_15m=_env_csv_int("LATENCY_BOT_SHADOW_VARIANT_ENTRY_SECONDS_15M", (300,)),
            shadow_variant_watchlist=_env_csv_str(
                "LATENCY_BOT_SHADOW_VARIANT_WATCHLIST",
                (
                    "btc5_no_fair_f0.50_e0.05_t180",
                    "btc5_no_fair_f0.50_e0.03_t180",
                    "btc5_no_calibrated_digital_f0.50_e0.03_t180",
                    "btc5_no_calibrated_digital_f0.50_e0.05_t180",
                    "btc5_yes_fair_f0.50_e0.03_t180",
                    "btc5_yes_calibrated_digital_f0.50_e0.03_t180",
                    "eth5_no_fair_f0.50_e0.03_t180",
                    "eth5_no_calibrated_digital_f0.50_e0.03_t180",
                    "eth5_yes_calibrated_digital_f0.50_e0.03_t180",
                    "eth5_yes_fair_f0.50_e0.03_t180",
                ),
            ),
            promoted_variant_ids=_env_csv_str(
                "LATENCY_BOT_PROMOTED_VARIANT_IDS",
                (
                    "btc5_no_fair_f0.50_e0.05_t180",
                    "btc5_no_fair_f0.50_e0.03_t180",
                    "btc5_no_calibrated_digital_f0.50_e0.03_t180",
                    "btc5_no_calibrated_digital_f0.50_e0.05_t180",
                    "btc5_yes_fair_f0.50_e0.03_t180",
                    "btc5_yes_calibrated_digital_f0.50_e0.03_t180",
                    "eth5_no_fair_f0.50_e0.03_t180",
                    "eth5_no_calibrated_digital_f0.50_e0.03_t180",
                    "eth5_yes_calibrated_digital_f0.50_e0.03_t180",
                    "eth5_yes_fair_f0.50_e0.03_t180",
                ),
            ),
            promoted_variant_max_open_positions=_env_int("LATENCY_BOT_PROMOTED_VARIANT_MAX_OPEN_POSITIONS", 10),
            complete_set_arb_enabled=os.getenv("LATENCY_BOT_COMPLETE_SET_ARB_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"},
            complete_set_arb_min_profit_per_share=_env_float("LATENCY_BOT_COMPLETE_SET_ARB_MIN_PROFIT_PER_SHARE", 0.0025),
            complete_set_arb_slippage_per_share=_env_float("LATENCY_BOT_COMPLETE_SET_ARB_SLIPPAGE_PER_SHARE", 0.0010),
            complete_set_arb_min_depth_usdc=_env_float("LATENCY_BOT_COMPLETE_SET_ARB_MIN_DEPTH_USDC", 100.0),
            complete_set_arb_max_book_age_ms=_env_float("LATENCY_BOT_COMPLETE_SET_ARB_MAX_BOOK_AGE_MS", 2500.0),
            complete_set_arb_min_seconds_left=_env_int("LATENCY_BOT_COMPLETE_SET_ARB_MIN_SECONDS_LEFT", 20),
            complete_set_arb_max_seconds_left=_env_int("LATENCY_BOT_COMPLETE_SET_ARB_MAX_SECONDS_LEFT", 900),
            complete_set_arb_notional_usdc=_env_float("LATENCY_BOT_COMPLETE_SET_ARB_NOTIONAL_USDC", 1000.0),
            complete_set_arb_max_sets_per_cycle=_env_int("LATENCY_BOT_COMPLETE_SET_ARB_MAX_SETS_PER_CYCLE", 3),
            complete_set_arb_same_market_cooldown_seconds=_env_int("LATENCY_BOT_COMPLETE_SET_ARB_SAME_MARKET_COOLDOWN_SECONDS", 300),
            complete_set_arb_execution_policy=os.getenv("LATENCY_BOT_COMPLETE_SET_ARB_EXECUTION_POLICY", "paired_fok_batch"),
            live_complete_set_arb_pilot_enabled=os.getenv("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"},
            live_complete_set_arb_pilot_mode=os.getenv("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_MODE", "dry_run").strip().lower(),
            live_complete_set_arb_pilot_confirm=os.getenv("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_CONFIRM", ""),
            live_complete_set_arb_pilot_capital_usdc=_env_float("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_CAPITAL_USDC", 50.0),
            live_complete_set_arb_pilot_notional_usdc=_env_float("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_NOTIONAL_USDC", 5.0),
            live_complete_set_arb_pilot_min_edge_per_share=_env_float("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_MIN_EDGE_PER_SHARE", 0.0100),
            live_complete_set_arb_pilot_min_depth_usdc=_env_float("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_MIN_DEPTH_USDC", 10.0),
            live_complete_set_arb_pilot_depth_haircut=_env_float("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_DEPTH_HAIRCUT", 0.50),
            live_complete_set_arb_pilot_extra_slippage_per_share=_env_float("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_EXTRA_SLIPPAGE_PER_SHARE", 0.0030),
            live_complete_set_arb_pilot_min_seconds_left=_env_int("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_MIN_SECONDS_LEFT", 180),
            live_complete_set_arb_pilot_min_leg_amount_usdc=_env_float("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_MIN_LEG_AMOUNT_USDC", 1.0),
            live_complete_set_arb_pilot_max_sets_per_cycle=_env_int("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_MAX_SETS_PER_CYCLE", 1),
            live_complete_set_arb_pilot_max_open_sets=_env_int("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_MAX_OPEN_SETS", 1),
            live_complete_set_arb_pilot_daily_loss_limit_usdc=_env_float("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_DAILY_LOSS_LIMIT_USDC", 5.0),
            live_complete_set_arb_pilot_allow_sequential_orders=os.getenv("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_ALLOW_SEQUENTIAL_ORDERS", "0").strip().lower() in {"1", "true", "yes", "on"},
            live_complete_set_arb_pilot_require_fok=os.getenv("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_REQUIRE_FOK", "1").strip().lower() in {"1", "true", "yes", "on"},
            live_complete_set_arb_pilot_enable_rescue=os.getenv("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_ENABLE_RESCUE", "1").strip().lower() in {"1", "true", "yes", "on"},
            live_complete_set_arb_pilot_same_market_cooldown_seconds=_env_int("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_SAME_MARKET_COOLDOWN_SECONDS", 300),
            live_complete_set_arb_pilot_private_key=os.getenv("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_PRIVATE_KEY", ""),
            live_complete_set_arb_pilot_api_key=os.getenv("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_API_KEY", ""),
            live_complete_set_arb_pilot_api_secret=os.getenv("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_API_SECRET", ""),
            live_complete_set_arb_pilot_api_passphrase=os.getenv("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_API_PASSPHRASE", ""),
            live_complete_set_arb_pilot_funder_address=os.getenv("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_FUNDER_ADDRESS", ""),
            live_complete_set_arb_pilot_signature_type=_env_int("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_SIGNATURE_TYPE", 3),
            live_complete_set_arb_pilot_host=os.getenv("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_HOST", "https://clob.polymarket.com"),
            live_complete_set_arb_pilot_chain_id=_env_int("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_CHAIN_ID", 137),
            live_complete_set_arb_pilot_tick_size=os.getenv("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_TICK_SIZE", "0.01"),
            live_complete_set_arb_pilot_neg_risk=os.getenv("LATENCY_BOT_LIVE_COMPLETE_SET_ARB_PILOT_NEG_RISK", "0").strip().lower() in {"1", "true", "yes", "on"},
            realistic_complete_set_arb_enabled=os.getenv("LATENCY_BOT_REALISTIC_COMPLETE_SET_ARB_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"},
            realistic_complete_set_arb_capital_usdc=_env_float("LATENCY_BOT_REALISTIC_COMPLETE_SET_ARB_CAPITAL_USDC", 3000.0),
            realistic_complete_set_arb_lookback_hours=_env_int("LATENCY_BOT_REALISTIC_COMPLETE_SET_ARB_LOOKBACK_HOURS", 24),
            realistic_complete_set_arb_depth_haircut=_env_float("LATENCY_BOT_REALISTIC_COMPLETE_SET_ARB_DEPTH_HAIRCUT", 0.50),
            realistic_complete_set_arb_latency_ms=_env_float("LATENCY_BOT_REALISTIC_COMPLETE_SET_ARB_LATENCY_MS", 500.0),
            realistic_complete_set_arb_latency_edge_decay_per_second=_env_float("LATENCY_BOT_REALISTIC_COMPLETE_SET_ARB_LATENCY_EDGE_DECAY_PER_SECOND", 0.0030),
            realistic_complete_set_arb_extra_slippage_per_share=_env_float("LATENCY_BOT_REALISTIC_COMPLETE_SET_ARB_EXTRA_SLIPPAGE_PER_SHARE", 0.0030),
            realistic_complete_set_arb_partial_fill_fraction=_env_float("LATENCY_BOT_REALISTIC_COMPLETE_SET_ARB_PARTIAL_FILL_FRACTION", 0.50),
            realistic_complete_set_arb_failed_leg_loss_fraction=_env_float("LATENCY_BOT_REALISTIC_COMPLETE_SET_ARB_FAILED_LEG_LOSS_FRACTION", 0.0100),
            realistic_complete_set_arb_operational_failure_rate=_env_float("LATENCY_BOT_REALISTIC_COMPLETE_SET_ARB_OPERATIONAL_FAILURE_RATE", 0.02),
            realistic_complete_set_arb_redeem_lag_seconds=_env_int("LATENCY_BOT_REALISTIC_COMPLETE_SET_ARB_REDEEM_LAG_SECONDS", 60),
            preowned_inventory_arb_capital_usdc=_env_float("LATENCY_BOT_PREOWNED_INVENTORY_ARB_CAPITAL_USDC", 50.0),
            preowned_inventory_arb_notional_usdc=_env_float("LATENCY_BOT_PREOWNED_INVENTORY_ARB_NOTIONAL_USDC", 15.66),
            preowned_inventory_arb_seed_side_notional_usdc=_env_float("LATENCY_BOT_PREOWNED_INVENTORY_ARB_SEED_SIDE_NOTIONAL_USDC", 7.83),
            preowned_inventory_arb_min_edge_per_share=_env_float("LATENCY_BOT_PREOWNED_INVENTORY_ARB_MIN_EDGE_PER_SHARE", 0.0200),
            preowned_inventory_arb_min_depth_usdc=_env_float("LATENCY_BOT_PREOWNED_INVENTORY_ARB_MIN_DEPTH_USDC", 2.0),
            preowned_inventory_arb_min_seconds_left=_env_int("LATENCY_BOT_PREOWNED_INVENTORY_ARB_MIN_SECONDS_LEFT", 60),
            preowned_inventory_arb_seed_min_seconds_left=_env_int("LATENCY_BOT_PREOWNED_INVENTORY_ARB_SEED_MIN_SECONDS_LEFT", 180),
            preowned_inventory_arb_seed_max_seconds_left=_env_int("LATENCY_BOT_PREOWNED_INVENTORY_ARB_SEED_MAX_SECONDS_LEFT", 900),
            preowned_inventory_arb_seed_max_complete_set_cost=_env_float("LATENCY_BOT_PREOWNED_INVENTORY_ARB_SEED_MAX_COMPLETE_SET_COST", 1.0),
            preowned_inventory_arb_seed_min_depth_usdc=_env_float("LATENCY_BOT_PREOWNED_INVENTORY_ARB_SEED_MIN_DEPTH_USDC", 0.0),
            preowned_inventory_arb_same_market_cooldown_seconds=_env_int("LATENCY_BOT_PREOWNED_INVENTORY_ARB_SAME_MARKET_COOLDOWN_SECONDS", 300),
            preowned_inventory_arb_max_open_seeded_markets=_env_int("LATENCY_BOT_PREOWNED_INVENTORY_ARB_MAX_OPEN_SEEDED_MARKETS", 3),
            preowned_inventory_arb_per_asset_time_bucket_cap=_env_int("LATENCY_BOT_PREOWNED_INVENTORY_ARB_PER_ASSET_TIME_BUCKET_CAP", 1),
            preowned_inventory_arb_time_bucket_seconds=_env_int("LATENCY_BOT_PREOWNED_INVENTORY_ARB_TIME_BUCKET_SECONDS", 300),
            preowned_inventory_arb_seed_require_original_eligible=os.getenv("LATENCY_BOT_PREOWNED_INVENTORY_ARB_SEED_REQUIRE_ORIGINAL_ELIGIBLE", "0").strip().lower() in {"1", "true", "yes", "on"},
            cex_latency_paper_enabled=os.getenv("LATENCY_BOT_CEX_LATENCY_PAPER_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"},
            cex_latency_paper_capital_usdc=_env_float("LATENCY_BOT_CEX_LATENCY_PAPER_CAPITAL_USDC", 1000.0),
            cex_latency_paper_notional_usdc=_env_float("LATENCY_BOT_CEX_LATENCY_PAPER_NOTIONAL_USDC", 50.0),
            cex_latency_paper_max_open_positions=_env_int("LATENCY_BOT_CEX_LATENCY_PAPER_MAX_OPEN_POSITIONS", 3),
            cex_latency_paper_assets=_env_csv_str("LATENCY_BOT_CEX_LATENCY_PAPER_ASSETS", ("btc", "eth")),
            cex_latency_paper_min_edge_per_share=_env_float("LATENCY_BOT_CEX_LATENCY_PAPER_MIN_EDGE_PER_SHARE", 0.0200),
            cex_latency_paper_min_depth_usdc=_env_float("LATENCY_BOT_CEX_LATENCY_PAPER_MIN_DEPTH_USDC", 750.0),
            cex_latency_paper_max_book_age_ms=_env_float("LATENCY_BOT_CEX_LATENCY_PAPER_MAX_BOOK_AGE_MS", 15000.0),
            cex_latency_paper_min_seconds_left_5m=_env_int("LATENCY_BOT_CEX_LATENCY_PAPER_MIN_SECONDS_LEFT_5M", 90),
            cex_latency_paper_min_seconds_left_15m=_env_int("LATENCY_BOT_CEX_LATENCY_PAPER_MIN_SECONDS_LEFT_15M", 180),
            cex_latency_paper_max_seconds_left=_env_int("LATENCY_BOT_CEX_LATENCY_PAPER_MAX_SECONDS_LEFT", 900),
            cex_latency_paper_min_trade_price=_env_float("LATENCY_BOT_CEX_LATENCY_PAPER_MIN_TRADE_PRICE", 0.25),
            cex_latency_paper_max_trade_price=_env_float("LATENCY_BOT_CEX_LATENCY_PAPER_MAX_TRADE_PRICE", 0.85),
            cex_latency_paper_model=os.getenv("LATENCY_BOT_CEX_LATENCY_PAPER_MODEL", "quant_poc").strip().lower(),
            cex_latency_paper_stop_loss_fraction=_env_float("LATENCY_BOT_CEX_LATENCY_PAPER_STOP_LOSS_FRACTION", 0.35),
            cex_latency_paper_take_profit_fraction=_env_float("LATENCY_BOT_CEX_LATENCY_PAPER_TAKE_PROFIT_FRACTION", 0.35),
            cex_latency_paper_exit_edge_floor=_env_float("LATENCY_BOT_CEX_LATENCY_PAPER_EXIT_EDGE_FLOOR", 0.0100),
            cex_latency_paper_force_exit_seconds=_env_int("LATENCY_BOT_CEX_LATENCY_PAPER_FORCE_EXIT_SECONDS", 30),
            cex_latency_paper_same_market_cooldown_seconds=_env_int("LATENCY_BOT_CEX_LATENCY_PAPER_SAME_MARKET_COOLDOWN_SECONDS", 300),
            btc_fair_value_paper_enabled=os.getenv("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"},
            btc_fair_value_paper_capital_usdc=_env_float("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_CAPITAL_USDC", 1000.0),
            btc_fair_value_paper_notional_usdc=_env_float("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_NOTIONAL_USDC", 50.0),
            btc_fair_value_paper_max_open_positions=_env_int("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_MAX_OPEN_POSITIONS", 3),
            btc_fair_value_paper_assets=_env_csv_str("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_ASSETS", ("btc",)),
            btc_fair_value_paper_min_edge_per_share=_env_float("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_MIN_EDGE_PER_SHARE", 0.0500),
            btc_fair_value_paper_min_depth_usdc=_env_float("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_MIN_DEPTH_USDC", 500.0),
            btc_fair_value_paper_max_book_age_ms=_env_float("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_MAX_BOOK_AGE_MS", 5000.0),
            btc_fair_value_paper_min_seconds_left=_env_int("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_MIN_SECONDS_LEFT", 90),
            btc_fair_value_paper_max_seconds_left=_env_int("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_MAX_SECONDS_LEFT", 360),
            btc_fair_value_paper_min_trade_price=_env_float("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_MIN_TRADE_PRICE", 0.08),
            btc_fair_value_paper_max_trade_price=_env_float("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_MAX_TRADE_PRICE", 0.92),
            btc_fair_value_paper_market_weight=_env_float("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_MARKET_WEIGHT", 0.45),
            btc_fair_value_paper_microprice_weight=_env_float("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_MICROPRICE_WEIGHT", 0.25),
            btc_fair_value_paper_binance_weight=_env_float("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_BINANCE_WEIGHT", 0.30),
            btc_fair_value_paper_min_model_confidence=_env_float("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_MIN_MODEL_CONFIDENCE", 0.035),
            btc_fair_value_paper_stop_loss_fraction=_env_float("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_STOP_LOSS_FRACTION", 0.40),
            btc_fair_value_paper_take_profit_fraction=_env_float("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_TAKE_PROFIT_FRACTION", 0.60),
            btc_fair_value_paper_exit_edge_floor=_env_float("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_EXIT_EDGE_FLOOR", 0.0000),
            btc_fair_value_paper_force_exit_seconds=_env_int("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_FORCE_EXIT_SECONDS", 10),
            btc_fair_value_paper_same_market_cooldown_seconds=_env_int("LATENCY_BOT_BTC_FAIR_VALUE_PAPER_SAME_MARKET_COOLDOWN_SECONDS", 300),
            temporal_inventory_maker_paper_enabled=os.getenv("LATENCY_BOT_TEMPORAL_INVENTORY_MAKER_PAPER_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"},
            temporal_inventory_maker_paper_capital_usdc=_env_float("LATENCY_BOT_TEMPORAL_INVENTORY_MAKER_PAPER_CAPITAL_USDC", 1000.0),
            temporal_inventory_maker_paper_max_market_exposure_usdc=_env_float("LATENCY_BOT_TEMPORAL_INVENTORY_MAKER_PAPER_MAX_MARKET_EXPOSURE_USDC", 150.0),
            temporal_inventory_maker_paper_max_total_exposure_usdc=_env_float("LATENCY_BOT_TEMPORAL_INVENTORY_MAKER_PAPER_MAX_TOTAL_EXPOSURE_USDC", 500.0),
            temporal_inventory_maker_paper_base_order_usdc=_env_float("LATENCY_BOT_TEMPORAL_INVENTORY_MAKER_PAPER_BASE_ORDER_USDC", 25.0),
            temporal_inventory_maker_paper_min_net_edge=_env_float("LATENCY_BOT_TEMPORAL_INVENTORY_MAKER_PAPER_MIN_NET_EDGE", 0.0200),
            temporal_inventory_maker_paper_max_pair_cost=_env_float("LATENCY_BOT_TEMPORAL_INVENTORY_MAKER_PAPER_MAX_PAIR_COST", 0.9900),
            temporal_inventory_maker_paper_quote_ttl_seconds=_env_int("LATENCY_BOT_TEMPORAL_INVENTORY_MAKER_PAPER_QUOTE_TTL_SECONDS", 12),
            temporal_inventory_maker_paper_force_exit_seconds=_env_int("LATENCY_BOT_TEMPORAL_INVENTORY_MAKER_PAPER_FORCE_EXIT_SECONDS", 20),
            temporal_inventory_maker_paper_daily_loss_limit_usdc=_env_float("LATENCY_BOT_TEMPORAL_INVENTORY_MAKER_PAPER_DAILY_LOSS_LIMIT_USDC", 25.0),
            live_temporal_inventory_maker_enabled=os.getenv("LATENCY_BOT_LIVE_TEMPORAL_INVENTORY_MAKER_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"},
            live_temporal_inventory_maker_mode=os.getenv("LATENCY_BOT_LIVE_TEMPORAL_INVENTORY_MAKER_MODE", "dry_run").strip().lower(),
            live_temporal_inventory_maker_confirm=os.getenv("LATENCY_BOT_LIVE_TEMPORAL_INVENTORY_MAKER_CONFIRM", ""),
            live_temporal_inventory_maker_capital_usdc=_env_float("LATENCY_BOT_LIVE_TEMPORAL_INVENTORY_MAKER_CAPITAL_USDC", 50.0),
            live_temporal_inventory_maker_base_order_usdc=_env_float("LATENCY_BOT_LIVE_TEMPORAL_INVENTORY_MAKER_BASE_ORDER_USDC", 5.0),
            live_temporal_inventory_maker_max_open_orders=_env_int("LATENCY_BOT_LIVE_TEMPORAL_INVENTORY_MAKER_MAX_OPEN_ORDERS", 1),
            live_temporal_inventory_maker_max_orders_per_cycle=_env_int("LATENCY_BOT_LIVE_TEMPORAL_INVENTORY_MAKER_MAX_ORDERS_PER_CYCLE", 1),
            live_temporal_inventory_maker_min_edge=_env_float("LATENCY_BOT_LIVE_TEMPORAL_INVENTORY_MAKER_MIN_EDGE", 0.0300),
            live_temporal_inventory_maker_min_seconds_left=_env_int("LATENCY_BOT_LIVE_TEMPORAL_INVENTORY_MAKER_MIN_SECONDS_LEFT", 90),
            live_temporal_inventory_maker_max_order_age_seconds=_env_int("LATENCY_BOT_LIVE_TEMPORAL_INVENTORY_MAKER_MAX_ORDER_AGE_SECONDS", 20),
            live_temporal_inventory_maker_heartbeat_timeout_seconds=_env_int("LATENCY_BOT_LIVE_TEMPORAL_INVENTORY_MAKER_HEARTBEAT_TIMEOUT_SECONDS", 30),
            live_temporal_inventory_maker_daily_loss_limit_usdc=_env_float("LATENCY_BOT_LIVE_TEMPORAL_INVENTORY_MAKER_DAILY_LOSS_LIMIT_USDC", 5.0),
            live_temporal_inventory_maker_require_positive_paper_pnl=os.getenv("LATENCY_BOT_LIVE_TEMPORAL_INVENTORY_MAKER_REQUIRE_POSITIVE_PAPER_PNL", "1").strip().lower() in {"1", "true", "yes", "on"},
            live_temporal_inventory_maker_require_reconciliation=os.getenv("LATENCY_BOT_LIVE_TEMPORAL_INVENTORY_MAKER_REQUIRE_RECONCILIATION", "1").strip().lower() in {"1", "true", "yes", "on"},
            late_resolution_capture_paper_enabled=os.getenv("LATENCY_BOT_LATE_RESOLUTION_CAPTURE_PAPER_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"},
            late_resolution_capture_paper_capital_usdc=_env_float("LATENCY_BOT_LATE_RESOLUTION_CAPTURE_PAPER_CAPITAL_USDC", 1000.0),
            late_resolution_capture_paper_notional_usdc=_env_float("LATENCY_BOT_LATE_RESOLUTION_CAPTURE_PAPER_NOTIONAL_USDC", 25.0),
            late_resolution_capture_paper_max_market_exposure_usdc=_env_float("LATENCY_BOT_LATE_RESOLUTION_CAPTURE_PAPER_MAX_MARKET_EXPOSURE_USDC", 50.0),
            late_resolution_capture_paper_max_total_exposure_usdc=_env_float("LATENCY_BOT_LATE_RESOLUTION_CAPTURE_PAPER_MAX_TOTAL_EXPOSURE_USDC", 150.0),
            late_resolution_capture_paper_min_seconds_left=_env_int("LATENCY_BOT_LATE_RESOLUTION_CAPTURE_PAPER_MIN_SECONDS_LEFT", 3),
            late_resolution_capture_paper_max_seconds_left=_env_int("LATENCY_BOT_LATE_RESOLUTION_CAPTURE_PAPER_MAX_SECONDS_LEFT", 45),
            late_resolution_capture_paper_min_official_confidence=_env_float("LATENCY_BOT_LATE_RESOLUTION_CAPTURE_PAPER_MIN_OFFICIAL_CONFIDENCE", 0.97),
            late_resolution_capture_paper_min_boundary_distance_bps=_env_float("LATENCY_BOT_LATE_RESOLUTION_CAPTURE_PAPER_MIN_BOUNDARY_DISTANCE_BPS", 8.0),
            late_resolution_capture_paper_min_edge=_env_float("LATENCY_BOT_LATE_RESOLUTION_CAPTURE_PAPER_MIN_EDGE", 0.0100),
            late_resolution_capture_paper_daily_loss_limit_usdc=_env_float("LATENCY_BOT_LATE_RESOLUTION_CAPTURE_PAPER_DAILY_LOSS_LIMIT_USDC", 25.0),
            wallet_teacher_sniper_enabled=os.getenv("LATENCY_BOT_WALLET_TEACHER_SNIPER_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"},
            wallet_teacher_sniper_wallet=os.getenv(
                "LATENCY_BOT_WALLET_TEACHER_SNIPER_WALLET",
                "0xb27bc932bf8110d8f78e55da7d5f0497a18b5b82",
            ).strip(),
            wallet_teacher_sniper_capital_usdc=_env_float("LATENCY_BOT_WALLET_TEACHER_SNIPER_CAPITAL_USDC", 1000.0),
            wallet_teacher_sniper_notional_usdc=_env_float("LATENCY_BOT_WALLET_TEACHER_SNIPER_NOTIONAL_USDC", 50.0),
            wallet_teacher_sniper_max_open_positions=_env_int("LATENCY_BOT_WALLET_TEACHER_SNIPER_MAX_OPEN_POSITIONS", 3),
            wallet_teacher_sniper_assets=_env_csv_str("LATENCY_BOT_WALLET_TEACHER_SNIPER_ASSETS", ("btc",)),
            wallet_teacher_sniper_trade_lookback_seconds=_env_int("LATENCY_BOT_WALLET_TEACHER_SNIPER_TRADE_LOOKBACK_SECONDS", 120),
            wallet_teacher_sniper_fetch_limit=_env_int("LATENCY_BOT_WALLET_TEACHER_SNIPER_FETCH_LIMIT", 100),
            wallet_teacher_sniper_min_teacher_notional_usdc=_env_float("LATENCY_BOT_WALLET_TEACHER_SNIPER_MIN_TEACHER_NOTIONAL_USDC", 5.0),
            wallet_teacher_sniper_min_depth_usdc=_env_float("LATENCY_BOT_WALLET_TEACHER_SNIPER_MIN_DEPTH_USDC", 50.0),
            wallet_teacher_sniper_max_book_age_ms=_env_float("LATENCY_BOT_WALLET_TEACHER_SNIPER_MAX_BOOK_AGE_MS", 15000.0),
            wallet_teacher_sniper_min_seconds_left=_env_int("LATENCY_BOT_WALLET_TEACHER_SNIPER_MIN_SECONDS_LEFT", 30),
            wallet_teacher_sniper_max_seconds_left=_env_int("LATENCY_BOT_WALLET_TEACHER_SNIPER_MAX_SECONDS_LEFT", 360),
            wallet_teacher_sniper_min_trade_price=_env_float("LATENCY_BOT_WALLET_TEACHER_SNIPER_MIN_TRADE_PRICE", 0.05),
            wallet_teacher_sniper_max_trade_price=_env_float("LATENCY_BOT_WALLET_TEACHER_SNIPER_MAX_TRADE_PRICE", 0.95),
            wallet_teacher_sniper_stop_loss_fraction=_env_float("LATENCY_BOT_WALLET_TEACHER_SNIPER_STOP_LOSS_FRACTION", 0.50),
            wallet_teacher_sniper_take_profit_fraction=_env_float("LATENCY_BOT_WALLET_TEACHER_SNIPER_TAKE_PROFIT_FRACTION", 0.75),
            wallet_teacher_sniper_exit_edge_floor=_env_float("LATENCY_BOT_WALLET_TEACHER_SNIPER_EXIT_EDGE_FLOOR", -1.0),
            wallet_teacher_sniper_force_exit_seconds=_env_int("LATENCY_BOT_WALLET_TEACHER_SNIPER_FORCE_EXIT_SECONDS", 8),
            wallet_teacher_sniper_same_market_cooldown_seconds=_env_int("LATENCY_BOT_WALLET_TEACHER_SNIPER_SAME_MARKET_COOLDOWN_SECONDS", 60),
            polymarket_us_arb_enabled=os.getenv("LATENCY_BOT_POLYMARKET_US_ARB_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"},
            polymarket_us_arb_symbols=_env_csv_str("LATENCY_BOT_POLYMARKET_US_ARB_SYMBOLS", ()),
            polymarket_us_arb_orderbook_endpoint=os.getenv(
                "LATENCY_BOT_POLYMARKET_US_ARB_ORDERBOOK_ENDPOINT",
                "https://api.prod.polymarketexchange.com/v1/orderbook",
            ),
            polymarket_us_arb_api_key=os.getenv("LATENCY_BOT_POLYMARKET_US_API_KEY", ""),
            polymarket_us_key_id=os.getenv("LATENCY_BOT_POLYMARKET_US_KEY_ID", ""),
            polymarket_us_secret_key=os.getenv("LATENCY_BOT_POLYMARKET_US_SECRET_KEY", ""),
            polymarket_us_arb_lookback_hours=_env_int("LATENCY_BOT_POLYMARKET_US_ARB_LOOKBACK_HOURS", 24),
            polymarket_us_arb_capital_usdc=_env_float("LATENCY_BOT_POLYMARKET_US_ARB_CAPITAL_USDC", 3000.0),
            polymarket_us_arb_notional_usdc=_env_float("LATENCY_BOT_POLYMARKET_US_ARB_NOTIONAL_USDC", 1000.0),
            polymarket_us_arb_min_edge_per_share=_env_float("LATENCY_BOT_POLYMARKET_US_ARB_MIN_EDGE_PER_SHARE", 0.0025),
            polymarket_us_arb_min_depth_usdc=_env_float("LATENCY_BOT_POLYMARKET_US_ARB_MIN_DEPTH_USDC", 100.0),
            polymarket_us_arb_depth_haircut=_env_float("LATENCY_BOT_POLYMARKET_US_ARB_DEPTH_HAIRCUT", 0.50),
            polymarket_us_arb_latency_ms=_env_float("LATENCY_BOT_POLYMARKET_US_ARB_LATENCY_MS", 500.0),
            polymarket_us_arb_latency_edge_decay_per_second=_env_float("LATENCY_BOT_POLYMARKET_US_ARB_LATENCY_EDGE_DECAY_PER_SECOND", 0.0030),
            polymarket_us_arb_extra_slippage_per_share=_env_float("LATENCY_BOT_POLYMARKET_US_ARB_EXTRA_SLIPPAGE_PER_SHARE", 0.0030),
            polymarket_us_arb_fee_per_share=_env_float("LATENCY_BOT_POLYMARKET_US_ARB_FEE_PER_SHARE", 0.0),
            polymarket_us_arb_operational_failure_rate=_env_float("LATENCY_BOT_POLYMARKET_US_ARB_OPERATIONAL_FAILURE_RATE", 0.02),
            polymarket_us_arb_failed_leg_loss_fraction=_env_float("LATENCY_BOT_POLYMARKET_US_ARB_FAILED_LEG_LOSS_FRACTION", 0.0100),
            polymarket_us_arb_same_symbol_cooldown_seconds=_env_int("LATENCY_BOT_POLYMARKET_US_ARB_SAME_SYMBOL_COOLDOWN_SECONDS", 10),
            kalshi_arb_enabled=os.getenv("LATENCY_BOT_KALSHI_ARB_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"},
            kalshi_arb_api_base=os.getenv("LATENCY_BOT_KALSHI_ARB_API_BASE", "https://external-api.kalshi.com/trade-api/v2"),
            kalshi_arb_tickers=_env_csv_str("LATENCY_BOT_KALSHI_ARB_TICKERS", ()),
            kalshi_arb_auto_discover=os.getenv("LATENCY_BOT_KALSHI_ARB_AUTO_DISCOVER", "1").strip().lower() in {"1", "true", "yes", "on"},
            kalshi_arb_assets=_env_csv_str("LATENCY_BOT_KALSHI_ARB_ASSETS", ("btc", "eth", "sol")),
            kalshi_arb_market_limit=_env_int("LATENCY_BOT_KALSHI_ARB_MARKET_LIMIT", 1000),
            kalshi_arb_lookback_hours=_env_int("LATENCY_BOT_KALSHI_ARB_LOOKBACK_HOURS", 24),
            kalshi_arb_capital_usdc=_env_float("LATENCY_BOT_KALSHI_ARB_CAPITAL_USDC", 3000.0),
            kalshi_arb_notional_usdc=_env_float("LATENCY_BOT_KALSHI_ARB_NOTIONAL_USDC", 1000.0),
            kalshi_arb_min_edge_per_share=_env_float("LATENCY_BOT_KALSHI_ARB_MIN_EDGE_PER_SHARE", 0.0025),
            kalshi_arb_min_depth_usdc=_env_float("LATENCY_BOT_KALSHI_ARB_MIN_DEPTH_USDC", 100.0),
            kalshi_arb_depth_haircut=_env_float("LATENCY_BOT_KALSHI_ARB_DEPTH_HAIRCUT", 0.50),
            kalshi_arb_latency_ms=_env_float("LATENCY_BOT_KALSHI_ARB_LATENCY_MS", 500.0),
            kalshi_arb_latency_edge_decay_per_second=_env_float("LATENCY_BOT_KALSHI_ARB_LATENCY_EDGE_DECAY_PER_SECOND", 0.0030),
            kalshi_arb_extra_slippage_per_share=_env_float("LATENCY_BOT_KALSHI_ARB_EXTRA_SLIPPAGE_PER_SHARE", 0.0030),
            kalshi_arb_fee_per_share=_env_float("LATENCY_BOT_KALSHI_ARB_FEE_PER_SHARE", 0.0),
            kalshi_arb_operational_failure_rate=_env_float("LATENCY_BOT_KALSHI_ARB_OPERATIONAL_FAILURE_RATE", 0.02),
            kalshi_arb_failed_leg_loss_fraction=_env_float("LATENCY_BOT_KALSHI_ARB_FAILED_LEG_LOSS_FRACTION", 0.0100),
            kalshi_arb_same_ticker_cooldown_seconds=_env_int("LATENCY_BOT_KALSHI_ARB_SAME_TICKER_COOLDOWN_SECONDS", 10),
            kalshi_arb_settlement_lag_seconds=_env_int("LATENCY_BOT_KALSHI_ARB_SETTLEMENT_LAG_SECONDS", 60),
            kalshi_api_key_id=os.getenv("LATENCY_BOT_KALSHI_API_KEY_ID", ""),
            kalshi_private_key_path=os.getenv("LATENCY_BOT_KALSHI_PRIVATE_KEY_PATH", ""),
            kalshi_private_key_pem=os.getenv("LATENCY_BOT_KALSHI_PRIVATE_KEY_PEM", ""),
            shadow_variant_top_raw_pnl_count=_env_int("LATENCY_BOT_SHADOW_VARIANT_TOP_RAW_PNL_COUNT", 10),
            shadow_variant_dashboard_grid_limit=_env_int("LATENCY_BOT_SHADOW_VARIANT_DASHBOARD_GRID_LIMIT", 40),
            shadow_variant_dashboard_reason_limit=_env_int("LATENCY_BOT_SHADOW_VARIANT_DASHBOARD_REASON_LIMIT", 120),
            shadow_variant_dashboard_family_limit=_env_int("LATENCY_BOT_SHADOW_VARIANT_DASHBOARD_FAMILY_LIMIT", 20),
            allow_maker_join=os.getenv("LATENCY_BOT_ALLOW_MAKER_JOIN", "0").strip().lower() in {"1", "true", "yes", "on"},
            allow_maker_improve=os.getenv("LATENCY_BOT_ALLOW_MAKER_IMPROVE", "0").strip().lower() in {"1", "true", "yes", "on"},
            binance_rest_endpoint=os.getenv("LATENCY_BOT_BINANCE_REST_ENDPOINT", "https://api.binance.com/api/v3/ticker/bookTicker"),
            binance_sample_history=_env_int("LATENCY_BOT_BINANCE_SAMPLE_HISTORY", 120),
            polymarket_cli_timeout_seconds=_env_int("LATENCY_BOT_POLYMARKET_CLI_TIMEOUT_SECONDS", 12),
            polymarket_clob_book_endpoint=os.getenv("LATENCY_BOT_POLYMARKET_CLOB_BOOK_ENDPOINT", "https://clob.polymarket.com/book"),
        )

    def ensure_dirs(self) -> None:
        for path in (
            self.db_path,
            self.status_path,
            self.markets_path,
            self.polymarket_cache_path,
            self.binance_cache_path,
        ):
            path.parent.mkdir(parents=True, exist_ok=True)
