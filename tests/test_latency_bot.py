import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from latency_bot.config import LatencyBotSettings
from latency_bot.core import (
    latency_bot_discovery_cycle,
    latency_bot_engine_cycle,
    latency_bot_init,
    latency_bot_summarize,
)
from latency_bot.dashboard import build_latency_bot_dashboard_state, render_latency_bot_dashboard_html
from latency_bot.execution.live_complete_set_arb import run_live_complete_set_arb_pilot_cycle
from latency_bot.execution.live_temporal_inventory_maker import run_live_temporal_inventory_maker_cycle
from latency_bot.execution.paper import (
    run_cex_latency_paper_cycle,
    run_complete_set_arb_paper_cycle,
    run_late_resolution_capture_paper_cycle,
    run_paper_execution_cycle,
    run_promoted_variant_paper_cycle,
    run_temporal_inventory_maker_paper_cycle,
)
from latency_bot.feeds.binance import refresh_binance_cache
from latency_bot.feeds.discovery import _normalize_latency_discovery_payload
from latency_bot.feeds.polymarket import refresh_polymarket_cache
from latency_bot.strategy.fair_value import build_fair_values, compute_updown_fair_yes
from latency_bot.strategy.complete_set_arb import build_complete_set_arb_signals
from latency_bot.strategy.signals import (
    build_cex_latency_paper_signals,
    build_late_resolution_capture_paper_signals,
    build_shadow_btc_no_signals,
    build_shadow_btc_yes_variant_signals,
    build_signals,
    build_temporal_inventory_maker_paper_signals,
)
from latency_bot.storage import (
    append_cex_latency_paper_signals,
    append_complete_set_arb_signals,
    close_position,
    connect_latency_bot_db,
    create_position,
    latency_bot_cex_latency_paper_stats,
    latency_bot_complete_set_arb_stats,
    latency_bot_live_complete_set_arb_pilot_stats,
    latency_bot_live_temporal_inventory_maker_stats,
    latency_bot_late_resolution_capture_paper_stats,
    latency_bot_performance_stats,
    latency_bot_temporal_inventory_maker_paper_stats,
    load_cex_latency_paper_open_positions,
    load_open_orders,
    load_open_positions,
)
from unittest.mock import patch
import json


class LatencyBotScaffoldTest(unittest.TestCase):
    def make_settings(self, tmpdir: str) -> LatencyBotSettings:
        root = Path(tmpdir)
        return LatencyBotSettings(
            db_path=root / "state" / "latency_bot.sqlite3",
            status_path=root / "state" / "latency_bot_status.json",
            markets_path=root / "state" / "latency_bot_markets.json",
            polymarket_cache_path=root / "state" / "latency_bot_polymarket_cache.json",
            binance_cache_path=root / "state" / "latency_bot_binance_cache.json",
            bootstrap_intraday_book_tape_path=root / "state" / "intraday_book_tape.json",
            dashboard_host="127.0.0.1",
            dashboard_port=8090,
            bankroll_usdc=10000.0,
            assets=("btc", "eth"),
            tenors_minutes=(5, 15),
            taker_min_edge_5m=0.035,
            taker_min_edge_15m=0.025,
            maker_min_edge_5m=0.015,
            maker_min_edge_15m=0.012,
            maker_quote_life_5m_seconds=8,
            maker_quote_life_15m_seconds=12,
            max_reprices=2,
            max_total_open_notional_fraction=0.15,
            max_btc_open_notional_fraction=0.08,
            max_eth_open_notional_fraction=0.06,
            max_simultaneous_positions=8,
            max_same_direction_positions_per_asset=3,
            feed_max_binance_staleness_seconds=1,
            feed_max_polymarket_staleness_seconds=2,
            daemon_interval_seconds=15,
            discovery_refresh_seconds=60,
            discovery_limit=500,
            discovery_lookahead_minutes=60 * 24 * 365 * 100,
            book_fetch_horizon_minutes=60,
            paper_position_notional_usdc=100.0,
            taker_fee_per_share=0.0,
            taker_slippage_per_share=0.0025,
            min_trade_price=0.03,
            max_trade_price=0.97,
            min_book_depth_usdc=250.0,
            max_book_age_ms=15000.0,
            max_reference_sample_age_seconds=45,
            min_volatility_floor=0.0005,
            min_binance_observations=3,
            min_taker_entry_seconds_left_5m=0,
            min_taker_entry_seconds_left_15m=0,
            taker_min_fair_yes_5m=0.50,
            taker_min_fair_yes_15m=0.50,
            exit_edge_floor=0.005,
            min_hold_seconds_before_edge_close=90,
            force_exit_seconds_5m=45,
            force_exit_seconds_15m=90,
            stop_loss_fraction=0.25,
            take_profit_fraction=0.20,
            same_market_cooldown_seconds=0,
            stop_loss_streak_pause_count=0,
            stop_loss_pause_seconds=1800,
            recent_loss_window_trades=5,
            recent_loss_pause_threshold_usdc=-100.0,
            max_btc_5m_eligible_rate=0.0,
            allow_btc_yes_taker=True,
            allow_eth_yes_taker=True,
            allow_15m_taker=True,
            allow_taker_no=False,
            allow_eth_no_taker=False,
            shadow_allow_btc_no_taker=True,
            shadow_taker_min_edge_5m=0.03,
            shadow_taker_min_fair_no_5m=0.55,
            shadow_yes_fair_floors_5m=(0.50, 0.52, 0.55, 0.60),
            shadow_yes_model_variants_5m=("calibrated_digital", "logit_proxy", "gbt_proxy", "market_blend"),
            shadow_variant_assets=("btc", "eth"),
            shadow_variant_sides=("yes", "no"),
            shadow_variant_tenors_minutes=(5, 15),
            shadow_variant_fair_floors=(0.50, 0.52),
            shadow_variant_models=("fair", "logit_proxy", "gbt_proxy"),
            shadow_variant_edge_thresholds_5m=(0.03, 0.05),
            shadow_variant_edge_thresholds_15m=(0.05,),
            shadow_variant_entry_seconds_5m=(0,),
            shadow_variant_entry_seconds_15m=(0,),
            shadow_variant_watchlist=(
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
            promoted_variant_ids=(
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
            promoted_variant_max_open_positions=10,
            complete_set_arb_enabled=True,
            complete_set_arb_min_profit_per_share=0.0025,
            complete_set_arb_slippage_per_share=0.0010,
            complete_set_arb_min_depth_usdc=100.0,
            complete_set_arb_max_book_age_ms=15000.0,
            complete_set_arb_min_seconds_left=0,
            complete_set_arb_max_seconds_left=900,
            complete_set_arb_notional_usdc=1000.0,
            complete_set_arb_max_sets_per_cycle=3,
            complete_set_arb_same_market_cooldown_seconds=300,
            complete_set_arb_execution_policy="paired_fok_batch",
            live_complete_set_arb_pilot_enabled=False,
            live_complete_set_arb_pilot_mode="dry_run",
            live_complete_set_arb_pilot_confirm="",
            live_complete_set_arb_pilot_capital_usdc=50.0,
            live_complete_set_arb_pilot_notional_usdc=5.0,
            live_complete_set_arb_pilot_min_edge_per_share=0.0100,
            live_complete_set_arb_pilot_min_depth_usdc=10.0,
            live_complete_set_arb_pilot_depth_haircut=0.50,
            live_complete_set_arb_pilot_extra_slippage_per_share=0.0030,
            live_complete_set_arb_pilot_min_seconds_left=180,
            live_complete_set_arb_pilot_min_leg_amount_usdc=1.0,
            live_complete_set_arb_pilot_max_sets_per_cycle=1,
            live_complete_set_arb_pilot_max_open_sets=1,
            live_complete_set_arb_pilot_daily_loss_limit_usdc=5.0,
            live_complete_set_arb_pilot_allow_sequential_orders=False,
            live_complete_set_arb_pilot_require_fok=True,
            live_complete_set_arb_pilot_enable_rescue=True,
            live_complete_set_arb_pilot_same_market_cooldown_seconds=300,
            live_complete_set_arb_pilot_private_key="",
            live_complete_set_arb_pilot_api_key="",
            live_complete_set_arb_pilot_api_secret="",
            live_complete_set_arb_pilot_api_passphrase="",
            live_complete_set_arb_pilot_funder_address="",
            live_complete_set_arb_pilot_signature_type=3,
            live_complete_set_arb_pilot_host="https://clob.polymarket.com",
            live_complete_set_arb_pilot_chain_id=137,
            live_complete_set_arb_pilot_tick_size="0.01",
            live_complete_set_arb_pilot_neg_risk=False,
            preowned_inventory_arb_capital_usdc=50.0,
            preowned_inventory_arb_notional_usdc=15.66,
            preowned_inventory_arb_seed_side_notional_usdc=7.83,
            preowned_inventory_arb_min_edge_per_share=0.0200,
            preowned_inventory_arb_min_depth_usdc=2.0,
            preowned_inventory_arb_min_seconds_left=60,
            preowned_inventory_arb_seed_min_seconds_left=180,
            preowned_inventory_arb_seed_max_seconds_left=900,
            preowned_inventory_arb_seed_max_complete_set_cost=1.0,
            preowned_inventory_arb_seed_min_depth_usdc=0.0,
            preowned_inventory_arb_same_market_cooldown_seconds=300,
            preowned_inventory_arb_max_open_seeded_markets=3,
            preowned_inventory_arb_per_asset_time_bucket_cap=1,
            preowned_inventory_arb_time_bucket_seconds=300,
            preowned_inventory_arb_seed_require_original_eligible=False,
            shadow_variant_top_raw_pnl_count=10,
            shadow_variant_dashboard_grid_limit=40,
            shadow_variant_dashboard_reason_limit=120,
            shadow_variant_dashboard_family_limit=20,
            allow_maker_join=False,
            allow_maker_improve=False,
            binance_rest_endpoint="https://api.binance.com/api/v3/ticker/bookTicker",
            binance_sample_history=120,
            polymarket_cli_timeout_seconds=12,
            polymarket_clob_book_endpoint="https://clob.polymarket.com/book",
        )

    def test_init_creates_schema_and_state_files(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            payload = latency_bot_init(settings)
            self.assertTrue(payload["ok"])
            self.assertTrue(settings.db_path.exists())
            self.assertTrue(settings.status_path.exists())
            self.assertTrue(settings.markets_path.exists())
            with connect_latency_bot_db(settings) as conn:
                tables = {
                    row["name"]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    ).fetchall()
                }
            self.assertIn("markets", tables)
            self.assertIn("engine_cycles", tables)
            self.assertIn("equity_snapshots", tables)

    def test_engine_cycle_records_status_and_db_cycle(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            latency_bot_init(settings)
            result = latency_bot_engine_cycle(settings)
            self.assertEqual(result["phase"], "engine")
            summary = latency_bot_summarize(settings, 60)
            self.assertEqual(summary["db"]["recent_counts"]["engine_cycles"], 1)
            self.assertEqual(summary["status"]["phase"], "engine")
            self.assertEqual(summary["db"]["totals"]["equity_snapshots_total"], 1)

    def test_engine_cycle_bootstraps_discovery_when_registry_empty(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            latency_bot_init(settings)
            discovered = {
                "generated_at": "2026-05-07T14:00:00Z",
                "count": 1,
                "items": [
                    {
                        "market_id": "btc5",
                        "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                        "asset": "btc",
                        "tenor_minutes": 5,
                        "hours_to_expiry": 0.25,
                        "expiry_ts": "2099-05-07T15:20:00Z",
                        "yes_token_id": "yes1",
                        "no_token_id": "no1",
                        "created_at": "2099-05-07T15:00:00Z",
                        "first_seen_at": "2099-05-07T15:00:00Z",
                        "status": "tracked",
                    }
                ],
                "assets": ["btc", "eth"],
                "tenors_minutes": [5, 15],
                "source": {"name": "gamma_events_by_tag", "tag_candidate_count": 4, "fetched_count": 1},
            }
            empty_cache = {"updated_at": "2099-05-07T15:00:00Z", "count": 0, "source": "test", "errors": [], "items": []}
            with patch("latency_bot.core.discover_latency_markets", return_value=discovered), patch(
                "latency_bot.core.refresh_polymarket_cache", return_value=empty_cache
            ), patch("latency_bot.core.refresh_binance_cache", return_value=empty_cache):
                result = latency_bot_engine_cycle(settings)
            self.assertEqual(result["tracked_markets_count"], 1)

    def test_engine_cycle_reuses_existing_markets_when_discovery_refresh_fails(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            latency_bot_init(settings)
            settings.markets_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2099-05-07T15:00:00Z",
                        "count": 1,
                        "items": [
                            {
                                "market_id": "btc5",
                                "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                                "asset": "btc",
                                "tenor_minutes": 5,
                                "hours_to_expiry": 0.25,
                                "expiry_ts": "2099-05-07T15:20:00Z",
                                "yes_token_id": "yes1",
                                "no_token_id": "no1",
                                "created_at": "2099-05-07T15:00:00Z",
                                "first_seen_at": "2099-05-07T15:00:00Z",
                                "status": "tracked",
                            }
                        ],
                        "source": {"name": "gamma_events_by_tag"},
                    }
                )
            )
            empty_cache = {"updated_at": "2099-05-07T15:00:00Z", "count": 0, "source": "test", "errors": [], "items": []}
            with patch("latency_bot.core.discover_latency_markets", side_effect=RuntimeError("gamma down")), patch(
                "latency_bot.core.refresh_polymarket_cache", return_value=empty_cache
            ), patch("latency_bot.core.refresh_binance_cache", return_value=empty_cache):
                result = latency_bot_engine_cycle(settings)
            self.assertEqual(result["tracked_markets_count"], 1)

    def test_discovery_cycle_updates_status(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            latency_bot_init(settings)
            result = latency_bot_discovery_cycle(settings)
            self.assertEqual(result["phase"], "discovery")
            summary = latency_bot_summarize(settings, 60)
            self.assertEqual(summary["status"]["phase"], "discovery")

    def test_dashboard_shell_renders(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            latency_bot_init(settings)
            latency_bot_engine_cycle(settings)
            state = build_latency_bot_dashboard_state(settings)
            html = render_latency_bot_dashboard_html(state)
            self.assertIn("Latency Bot", html)
            self.assertIn("Tracked Markets", html)
            self.assertIn("Database", html)
            self.assertIn("Shadow NO Summary", html)
            self.assertIn("Live Signal Slices", html)
            self.assertIn("Shadow NO Price Bands", html)
            self.assertIn("Live Performance", html)
            self.assertIn("Live Recent Closes", html)
            self.assertIn("Live Opportunity", html)
            self.assertIn("Shadow NO Opportunity", html)
            self.assertIn("Live Streak Diagnostics", html)
            self.assertIn("Live Close Price Bands", html)
            self.assertIn("Live Integrity", html)
            self.assertIn("Shadow Taker Variant Grid", html)
            self.assertIn("Shadow Taker Top Raw PnL", html)
            self.assertIn("Shadow Taker Watchlist", html)
            self.assertIn("Shadow Taker Strategy Families", html)
            self.assertIn("Shadow Taker Unique Market PnL", html)
            self.assertIn("Shadow Taker Fair Calibration", html)
            self.assertIn("Live Paper PnL Curve", html)
            self.assertIn("Live Paper Strategy PnL Curves", html)
            self.assertIn("Shadow Taker Research Rankings", html)
            self.assertIn("Shadow Taker Variant Skip Reasons", html)
            self.assertIn("Top Promoted Strategy Revenue", html)
            self.assertIn("Complete-Set Arb Prototype", html)
            self.assertIn("Consensus Meta-Strategy", html)
            self.assertIn("Confidence Sizing Ladder", html)

    def test_shadow_btc_no_signals_can_be_eligible(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = replace(
                self.make_settings(tmpdir),
                shadow_variant_assets=("btc",),
                shadow_variant_sides=("yes",),
                shadow_variant_tenors_minutes=(5,),
                shadow_variant_fair_floors=(0.50, 0.55),
                shadow_variant_models=("fair", "logit_proxy", "gbt_proxy", "market_blend"),
                shadow_variant_edge_thresholds_5m=(0.03,),
                shadow_variant_entry_seconds_5m=(0,),
            )
            markets_payload = {
                "items": [
                    {
                        "market_id": "btc5",
                        "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                        "asset": "btc",
                        "tenor_minutes": 5,
                        "expiry_ts": "2099-05-07T15:20:00Z",
                    }
                ]
            }
            polymarket_cache = {
                "items": [
                    {
                        "market_id": "btc5",
                        "best_bid": 0.40,
                        "best_ask": 0.42,
                        "min_depth_usdc": 1000.0,
                        "book_age_ms": 1000.0,
                    }
                ]
            }
            fair_values = [
                {
                    "market_id": "btc5",
                    "asset": "btc",
                    "fair_yes": 0.35,
                    "fair_no": 0.65,
                    "time_to_expiry_sec": 300.0,
                }
            ]
            signals = build_shadow_btc_no_signals(
                settings,
                markets_payload=markets_payload,
                polymarket_cache=polymarket_cache,
                fair_values=fair_values,
            )
            self.assertEqual(len(signals), 1)
            self.assertEqual(signals[0]["signal_type"], "SHADOW_TAKE_NO")

    def test_promoted_variants_open_one_live_paper_position_per_market(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            latency_bot_init(settings)
            markets_payload = {
                "items": [
                    {
                        "market_id": "btc5",
                        "asset": "btc",
                        "tenor_minutes": 5,
                        "expiry_ts": "2099-05-07T15:20:00Z",
                    }
                ]
            }
            polymarket_cache = {
                "items": [
                    {
                        "market_id": "btc5",
                        "best_bid": 0.41,
                        "best_ask": 0.42,
                    }
                ]
            }
            signals = [
                {
                    "variant_id": "btc5_no_fair_f0.50_e0.03_t180",
                    "market_id": "btc5",
                    "asset": "btc",
                    "tenor_minutes": 5,
                    "signal_type": "SHADOW_VARIANT_TAKE_NO",
                    "side": "NO",
                    "edge": 0.08,
                    "fair_yes": 0.53,
                    "fair_no": 0.47,
                    "yes_ask": 0.42,
                    "no_ask": 0.58,
                    "order_price": 0.42,
                    "seconds_left": 300.0,
                    "eligible": True,
                },
                {
                    "variant_id": "btc5_no_fair_f0.50_e0.05_t180",
                    "market_id": "btc5",
                    "asset": "btc",
                    "tenor_minutes": 5,
                    "signal_type": "SHADOW_VARIANT_TAKE_NO",
                    "side": "NO",
                    "edge": 0.09,
                    "fair_yes": 0.53,
                    "fair_no": 0.47,
                    "yes_ask": 0.42,
                    "no_ask": 0.58,
                    "order_price": 0.42,
                    "seconds_left": 300.0,
                    "eligible": True,
                },
            ]
            result = run_promoted_variant_paper_cycle(
                settings,
                markets_payload=markets_payload,
                polymarket_cache=polymarket_cache,
                signals=signals,
                ts="2099-05-07T15:15:00Z",
            )
            positions = load_open_positions(settings)
            self.assertEqual(result["opened_positions_count"], 1)
            self.assertEqual(len(positions), 1)
            self.assertEqual(positions[0]["mode"], "promoted_variant:btc5_no_fair_f0.50_e0.05_t180")

    def test_complete_set_arb_locks_positive_paired_edge(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = replace(self.make_settings(tmpdir), complete_set_arb_max_seconds_left=0)
            latency_bot_init(settings)
            markets_payload = {
                "items": [
                    {
                        "market_id": "btc5",
                        "asset": "btc",
                        "tenor_minutes": 5,
                        "expiry_ts": "2099-05-07T15:20:00Z",
                    }
                ]
            }
            polymarket_cache = {
                "items": [
                    {
                        "market_id": "btc5",
                        "asset": "btc",
                        "best_ask": 0.48,
                        "no_best_ask": 0.515,
                        "asks_depth_usdc": 2000.0,
                        "no_asks_depth_usdc": 1500.0,
                        "book_age_ms": 100.0,
                    }
                ]
            }
            signals = build_complete_set_arb_signals(
                settings,
                markets_payload=markets_payload,
                polymarket_cache=polymarket_cache,
            )
            self.assertEqual(len(signals), 1)
            self.assertTrue(signals[0]["eligible"])
            append_complete_set_arb_signals(settings, signals, ts="2099-05-07T15:15:00Z")
            result = run_complete_set_arb_paper_cycle(settings, signals=signals, ts="2099-05-07T15:15:00Z")
            stats = latency_bot_complete_set_arb_stats(settings)
            self.assertEqual(result["opened_positions_count"], 1)
            self.assertEqual(result["closed_positions_count"], 1)
            self.assertEqual(stats["summary"]["closed"], 1)
            self.assertGreater(stats["summary"]["net_pnl"], 0.0)

    def test_cex_latency_paper_bot_dashboard_pane(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = replace(
                self.make_settings(tmpdir),
                cex_latency_paper_enabled=True,
                cex_latency_paper_capital_usdc=1000.0,
                cex_latency_paper_notional_usdc=100.0,
                cex_latency_paper_max_open_positions=2,
                cex_latency_paper_assets=("btc",),
                cex_latency_paper_min_edge_per_share=0.03,
                cex_latency_paper_min_depth_usdc=100.0,
                cex_latency_paper_max_book_age_ms=15000.0,
                cex_latency_paper_min_seconds_left_5m=0,
                cex_latency_paper_max_seconds_left=999999,
                cex_latency_paper_min_trade_price=0.01,
                cex_latency_paper_max_trade_price=0.99,
                cex_latency_paper_model="fair",
                cex_latency_paper_stop_loss_fraction=0.50,
                cex_latency_paper_take_profit_fraction=0.20,
                cex_latency_paper_exit_edge_floor=0.01,
                cex_latency_paper_force_exit_seconds=0,
                cex_latency_paper_same_market_cooldown_seconds=0,
                taker_slippage_per_share=0.0,
            )
            latency_bot_init(settings)
            markets_payload = {
                "items": [
                    {
                        "market_id": "btc-cex-5m",
                        "asset": "btc",
                        "tenor_minutes": 5,
                        "expiry_ts": "2099-05-07T15:20:00Z",
                        "question": "Bitcoin Up or Down - test",
                    }
                ]
            }
            polymarket_cache = {
                "items": [
                    {
                        "market_id": "btc-cex-5m",
                        "asset": "btc",
                        "best_bid": 0.39,
                        "best_ask": 0.40,
                        "no_best_bid": 0.59,
                        "no_best_ask": 0.60,
                        "min_depth_usdc": 1000.0,
                        "book_age_ms": 100.0,
                    }
                ]
            }
            fair_values = [
                {
                    "market_id": "btc-cex-5m",
                    "asset": "btc",
                    "fair_yes": 0.55,
                    "fair_no": 0.45,
                    "reference_price": 100.0,
                    "volatility": 0.001,
                    "time_to_expiry_sec": 300.0,
                }
            ]
            signals = build_cex_latency_paper_signals(
                settings,
                markets_payload=markets_payload,
                polymarket_cache=polymarket_cache,
                fair_values=fair_values,
            )
            self.assertEqual(len(signals), 1)
            self.assertTrue(signals[0]["eligible"])
            self.assertEqual(signals[0]["side"], "YES")
            append_cex_latency_paper_signals(settings, signals, ts="2099-05-07T15:00:00Z")
            open_result = run_cex_latency_paper_cycle(
                settings,
                markets_payload=markets_payload,
                polymarket_cache=polymarket_cache,
                signals=signals,
                ts="2099-05-07T15:00:00Z",
            )
            self.assertEqual(open_result["opened_positions_count"], 1)
            self.assertEqual(len(load_cex_latency_paper_open_positions(settings)), 1)

            exit_cache = {
                "items": [
                    {
                        "market_id": "btc-cex-5m",
                        "asset": "btc",
                        "best_bid": 0.50,
                        "best_ask": 0.51,
                        "no_best_bid": 0.49,
                        "no_best_ask": 0.50,
                        "min_depth_usdc": 1000.0,
                        "book_age_ms": 100.0,
                    }
                ]
            }
            exit_signals = build_cex_latency_paper_signals(
                settings,
                markets_payload=markets_payload,
                polymarket_cache=exit_cache,
                fair_values=fair_values,
            )
            append_cex_latency_paper_signals(settings, exit_signals, ts="2099-05-07T15:02:00Z")
            close_result = run_cex_latency_paper_cycle(
                settings,
                markets_payload=markets_payload,
                polymarket_cache=exit_cache,
                signals=exit_signals,
                ts="2099-05-07T15:02:00Z",
            )
            stats = latency_bot_cex_latency_paper_stats(settings)
            self.assertEqual(close_result["closed_positions_count"], 1)
            self.assertEqual(stats["summary"]["closed"], 1)
            self.assertGreater(stats["summary"]["net_pnl"], 0.0)
            self.assertGreater(stats["summary"]["equity_usdc"], 1000.0)

            state = build_latency_bot_dashboard_state(settings, fast=True)
            html = render_latency_bot_dashboard_html(state)
            self.assertIn("CEX Latency Paper Bot", html)
            self.assertIn("Research-only unless recent realized PnL", html)

    def test_temporal_inventory_maker_quotes_seed_and_lock_owned_pair(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = replace(
                self.make_settings(tmpdir),
                temporal_inventory_maker_paper_enabled=True,
                temporal_inventory_maker_paper_capital_usdc=1000.0,
                temporal_inventory_maker_paper_base_order_usdc=50.0,
                temporal_inventory_maker_paper_min_net_edge=0.01,
                temporal_inventory_maker_paper_max_pair_cost=0.99,
                temporal_inventory_maker_paper_max_market_exposure_usdc=200.0,
                temporal_inventory_maker_paper_max_total_exposure_usdc=500.0,
                temporal_inventory_maker_paper_quote_ttl_seconds=60,
                temporal_inventory_maker_paper_force_exit_seconds=5,
                taker_fee_per_share=0.0,
            )
            latency_bot_init(settings)
            markets_payload = {
                "items": [
                    {
                        "market_id": "btc-temporal-5m",
                        "asset": "btc",
                        "tenor_minutes": 5,
                        "expiry_ts": "2099-05-07T15:20:00Z",
                    }
                ]
            }

            def signals_for(cache: dict, fair_yes: float = 0.65) -> list[dict]:
                return build_temporal_inventory_maker_paper_signals(
                    settings,
                    markets_payload=markets_payload,
                    polymarket_cache=cache,
                    fair_values=[
                        {
                            "market_id": "btc-temporal-5m",
                            "asset": "btc",
                            "fair_yes": fair_yes,
                            "fair_no": 1.0 - fair_yes,
                            "reference_price": 100.0,
                            "volatility": 0.001,
                            "time_to_expiry_sec": 300.0,
                        }
                    ],
                )

            seed_quote_cache = {
                "items": [
                    {
                        "market_id": "btc-temporal-5m",
                        "asset": "btc",
                        "best_bid": 0.40,
                        "best_ask": 0.50,
                        "no_best_bid": 0.49,
                        "no_best_ask": 0.60,
                        "bids_depth_usdc": 1000.0,
                        "asks_depth_usdc": 1000.0,
                        "book_age_ms": 100.0,
                    }
                ]
            }
            seed_signals = signals_for(seed_quote_cache)
            self.assertTrue(seed_signals[0]["eligible"])
            quote_result = run_temporal_inventory_maker_paper_cycle(
                settings,
                markets_payload=markets_payload,
                polymarket_cache=seed_quote_cache,
                signals=seed_signals,
                ts="2099-05-07T15:00:00Z",
            )
            self.assertEqual(quote_result["opened_quotes_count"], 1)

            seed_fill_cache = {
                "items": [
                    {
                        "market_id": "btc-temporal-5m",
                        "asset": "btc",
                        "best_bid": 0.39,
                        "best_ask": 0.40,
                        "no_best_bid": 0.59,
                        "no_best_ask": 0.61,
                        "bids_depth_usdc": 1000.0,
                        "asks_depth_usdc": 1000.0,
                        "book_age_ms": 100.0,
                    }
                ]
            }
            fill_seed_result = run_temporal_inventory_maker_paper_cycle(
                settings,
                markets_payload=markets_payload,
                polymarket_cache=seed_fill_cache,
                signals=signals_for(seed_fill_cache),
                ts="2099-05-07T15:00:10Z",
            )
            self.assertEqual(fill_seed_result["filled_quotes_count"], 1)
            stats_after_seed = latency_bot_temporal_inventory_maker_paper_stats(settings)
            self.assertEqual(stats_after_seed["markets"][0]["state"], "SEEDED")
            self.assertGreater(float(stats_after_seed["markets"][0]["yes_shares"]), 0.0)
            self.assertEqual(float(stats_after_seed["markets"][0]["no_shares"]), 0.0)

            hedge_quote_cache = {
                "items": [
                    {
                        "market_id": "btc-temporal-5m",
                        "asset": "btc",
                        "best_bid": 0.40,
                        "best_ask": 0.50,
                        "no_best_bid": 0.39,
                        "no_best_ask": 0.50,
                        "bids_depth_usdc": 1000.0,
                        "asks_depth_usdc": 1000.0,
                        "book_age_ms": 100.0,
                    }
                ]
            }
            hedge_quote_result = run_temporal_inventory_maker_paper_cycle(
                settings,
                markets_payload=markets_payload,
                polymarket_cache=hedge_quote_cache,
                signals=signals_for(hedge_quote_cache),
                ts="2099-05-07T15:00:20Z",
            )
            self.assertEqual(hedge_quote_result["opened_quotes_count"], 1)
            self.assertEqual(hedge_quote_result["opened_quotes"][0]["side"], "NO")

            hedge_fill_cache = {
                "items": [
                    {
                        "market_id": "btc-temporal-5m",
                        "asset": "btc",
                        "best_bid": 0.40,
                        "best_ask": 0.50,
                        "no_best_bid": 0.38,
                        "no_best_ask": 0.39,
                        "bids_depth_usdc": 1000.0,
                        "asks_depth_usdc": 1000.0,
                        "book_age_ms": 100.0,
                    }
                ]
            }
            hedge_fill_result = run_temporal_inventory_maker_paper_cycle(
                settings,
                markets_payload=markets_payload,
                polymarket_cache=hedge_fill_cache,
                signals=signals_for(hedge_fill_cache),
                ts="2099-05-07T15:00:30Z",
            )
            self.assertEqual(hedge_fill_result["filled_quotes_count"], 1)
            stats = latency_bot_temporal_inventory_maker_paper_stats(settings)
            summary = stats["summary"]
            market = stats["markets"][0]
            self.assertEqual(market["state"], "LOCKED_PAIR")
            self.assertAlmostEqual(float(market["yes_shares"]), float(market["no_shares"]), places=6)
            self.assertGreater(summary["locked_pair_shares"], 0.0)
            self.assertLess(summary["average_pair_cost"], 0.99)
            event_types = {item["event_type"] for item in stats["recent_events"]}
            self.assertIn("SEED", event_types)
            self.assertIn("HEDGE", event_types)
            self.assertIn("LOCKED_PAIR", event_types)

            state = build_latency_bot_dashboard_state(settings, fast=True)
            html = render_latency_bot_dashboard_html(state)
            self.assertIn("Temporal Inventory Maker Paper Bot", html)
            self.assertIn("MAKER_FILL", html)

    def test_temporal_inventory_maker_cancels_stale_quote(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = replace(
                self.make_settings(tmpdir),
                temporal_inventory_maker_paper_enabled=True,
                temporal_inventory_maker_paper_base_order_usdc=50.0,
                temporal_inventory_maker_paper_min_net_edge=0.01,
                temporal_inventory_maker_paper_quote_ttl_seconds=1,
                temporal_inventory_maker_paper_force_exit_seconds=5,
                taker_fee_per_share=0.0,
            )
            latency_bot_init(settings)
            markets_payload = {
                "items": [
                    {
                        "market_id": "btc-temporal-stale",
                        "asset": "btc",
                        "tenor_minutes": 5,
                        "expiry_ts": "2099-05-07T15:20:00Z",
                    }
                ]
            }
            cache = {
                "items": [
                    {
                        "market_id": "btc-temporal-stale",
                        "asset": "btc",
                        "best_bid": 0.40,
                        "best_ask": 0.50,
                        "no_best_bid": 0.49,
                        "no_best_ask": 0.60,
                        "bids_depth_usdc": 1000.0,
                        "asks_depth_usdc": 1000.0,
                        "book_age_ms": 100.0,
                    }
                ]
            }
            signals = build_temporal_inventory_maker_paper_signals(
                settings,
                markets_payload=markets_payload,
                polymarket_cache=cache,
                fair_values=[
                    {
                        "market_id": "btc-temporal-stale",
                        "asset": "btc",
                        "fair_yes": 0.65,
                        "fair_no": 0.35,
                        "time_to_expiry_sec": 300.0,
                    }
                ],
            )
            self.assertTrue(signals[0]["eligible"])
            run_temporal_inventory_maker_paper_cycle(
                settings,
                markets_payload=markets_payload,
                polymarket_cache=cache,
                signals=signals,
                ts="2099-05-07T15:00:00Z",
            )
            result = run_temporal_inventory_maker_paper_cycle(
                settings,
                markets_payload=markets_payload,
                polymarket_cache=cache,
                signals=signals,
                ts="2099-05-07T15:00:05Z",
            )
            self.assertEqual(result["cancelled_quotes_count"], 1)
            stats = latency_bot_temporal_inventory_maker_paper_stats(settings)
            self.assertGreaterEqual(stats["summary"]["quote_cancelled"], 1)

    def test_live_temporal_inventory_maker_dry_run_records_candidate(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = replace(
                self.make_settings(tmpdir),
                live_temporal_inventory_maker_enabled=True,
                live_temporal_inventory_maker_mode="dry_run",
                live_temporal_inventory_maker_require_positive_paper_pnl=False,
                live_temporal_inventory_maker_require_reconciliation=False,
                live_temporal_inventory_maker_min_edge=0.01,
                live_temporal_inventory_maker_min_seconds_left=0,
                live_temporal_inventory_maker_base_order_usdc=5.0,
                live_temporal_inventory_maker_max_open_orders=2,
            )
            latency_bot_init(settings)
            markets_payload = {
                "items": [
                    {
                        "market_id": "btc-live-maker-dry-run",
                        "asset": "btc",
                        "yes_token_id": "yes-token",
                        "no_token_id": "no-token",
                        "expiry_ts": "2099-05-07T15:20:00Z",
                    }
                ]
            }
            signals = [
                {
                    "market_id": "btc-live-maker-dry-run",
                    "asset": "btc",
                    "side": "YES",
                    "eligible": True,
                    "edge": 0.05,
                    "order_price": 0.40,
                    "yes_ask": 0.50,
                    "no_ask": 0.60,
                    "seconds_left": 300.0,
                }
            ]
            result = run_live_temporal_inventory_maker_cycle(
                settings,
                markets_payload=markets_payload,
                signals=signals,
                ts="2099-05-07T15:00:00Z",
            )
            self.assertEqual(result["dry_run_count"], 1)
            self.assertEqual(result["blocked_count"], 0)
            stats = latency_bot_live_temporal_inventory_maker_stats(settings)
            self.assertEqual(stats["summary"]["dry_run_24h"], 1)
            self.assertEqual(stats["recent_orders"][0]["decision"], "DRY_RUN")

    def test_live_temporal_inventory_maker_live_requires_confirmation(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = replace(
                self.make_settings(tmpdir),
                live_temporal_inventory_maker_enabled=True,
                live_temporal_inventory_maker_mode="live",
                live_temporal_inventory_maker_confirm="",
                live_temporal_inventory_maker_require_positive_paper_pnl=False,
                live_temporal_inventory_maker_require_reconciliation=False,
                live_temporal_inventory_maker_min_edge=0.01,
                live_temporal_inventory_maker_min_seconds_left=0,
            )
            latency_bot_init(settings)
            markets_payload = {
                "items": [
                    {
                        "market_id": "btc-live-maker-unarmed",
                        "asset": "btc",
                        "yes_token_id": "yes-token",
                        "no_token_id": "no-token",
                        "expiry_ts": "2099-05-07T15:20:00Z",
                    }
                ]
            }
            signals = [
                {
                    "market_id": "btc-live-maker-unarmed",
                    "asset": "btc",
                    "side": "NO",
                    "eligible": True,
                    "edge": 0.05,
                    "order_price": 0.40,
                    "yes_ask": 0.60,
                    "no_ask": 0.50,
                    "seconds_left": 300.0,
                }
            ]
            result = run_live_temporal_inventory_maker_cycle(
                settings,
                markets_payload=markets_payload,
                signals=signals,
                ts="2099-05-07T15:00:00Z",
            )
            self.assertEqual(result["submitted_count"], 0)
            self.assertEqual(result["blocked_count"], 1)
            self.assertIn("not armed", result["blocks"][0]["reason"])

    def test_late_resolution_capture_paper_opens_and_closes(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = replace(
                self.make_settings(tmpdir),
                late_resolution_capture_paper_enabled=True,
                late_resolution_capture_paper_notional_usdc=25.0,
                late_resolution_capture_paper_max_market_exposure_usdc=50.0,
                late_resolution_capture_paper_max_total_exposure_usdc=100.0,
                late_resolution_capture_paper_min_seconds_left=1,
                late_resolution_capture_paper_max_seconds_left=45,
                late_resolution_capture_paper_min_official_confidence=0.97,
                late_resolution_capture_paper_min_boundary_distance_bps=1.0,
                late_resolution_capture_paper_min_edge=0.01,
                taker_fee_per_share=0.0,
                taker_slippage_per_share=0.0,
            )
            latency_bot_init(settings)
            open_expiry = (datetime.now(timezone.utc) + timedelta(seconds=20)).isoformat().replace("+00:00", "Z")
            markets_payload = {
                "items": [
                    {
                        "market_id": "btc-late-resolution",
                        "asset": "btc",
                        "tenor_minutes": 5,
                        "expiry_ts": open_expiry,
                    }
                ]
            }
            cache = {
                "items": [
                    {
                        "market_id": "btc-late-resolution",
                        "asset": "btc",
                        "best_bid": 0.88,
                        "best_ask": 0.90,
                        "no_best_bid": 0.09,
                        "no_best_ask": 0.11,
                        "asks_depth_usdc": 1000.0,
                        "book_age_ms": 100.0,
                    }
                ]
            }
            fair_values = [
                {
                    "market_id": "btc-late-resolution",
                    "asset": "btc",
                    "fair_yes": 0.997,
                    "fair_no": 0.003,
                    "reference_price": 100.0,
                    "current_price": 101.0,
                    "time_to_expiry_sec": 20.0,
                }
            ]
            signals = build_late_resolution_capture_paper_signals(
                settings,
                markets_payload=markets_payload,
                polymarket_cache=cache,
                fair_values=fair_values,
            )
            self.assertTrue(signals[0]["eligible"])
            open_result = run_late_resolution_capture_paper_cycle(
                settings,
                markets_payload=markets_payload,
                polymarket_cache=cache,
                signals=signals,
                ts=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            )
            self.assertEqual(open_result["opened_positions_count"], 1)

            expired_payload = {
                "items": [
                    {
                        "market_id": "btc-late-resolution",
                        "asset": "btc",
                        "tenor_minutes": 5,
                        "expiry_ts": (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
                    }
                ]
            }
            close_result = run_late_resolution_capture_paper_cycle(
                settings,
                markets_payload=expired_payload,
                polymarket_cache=cache,
                signals=signals,
                ts=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            )
            self.assertEqual(close_result["closed_positions_count"], 1)
            stats = latency_bot_late_resolution_capture_paper_stats(settings)
            self.assertEqual(stats["summary"]["closed"], 1)
            self.assertGreater(stats["summary"]["net_pnl"], 0.0)

            state = build_latency_bot_dashboard_state(settings, fast=True)
            html = render_latency_bot_dashboard_html(state)
            self.assertIn("Late Resolution Capture Paper", html)
            self.assertIn("Live Temporal Inventory Maker", html)

    def test_cex_latency_quant_poc_filters_weak_signals(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = replace(
                self.make_settings(tmpdir),
                cex_latency_paper_enabled=True,
                cex_latency_paper_assets=("btc",),
                cex_latency_paper_model="quant_poc",
                cex_latency_paper_min_edge_per_share=0.02,
                cex_latency_paper_min_depth_usdc=100.0,
                cex_latency_paper_min_seconds_left_5m=90,
                cex_latency_paper_max_seconds_left=900,
                cex_latency_paper_min_trade_price=0.25,
                cex_latency_paper_max_trade_price=0.85,
                taker_slippage_per_share=0.0,
            )
            markets_payload = {
                "items": [
                    {
                        "market_id": "btc-quant-poc",
                        "asset": "btc",
                        "tenor_minutes": 5,
                        "expiry_ts": "2099-05-07T15:20:00Z",
                    }
                ]
            }
            polymarket_cache = {
                "items": [
                    {
                        "market_id": "btc-quant-poc",
                        "asset": "btc",
                        "best_bid": 0.39,
                        "best_ask": 0.40,
                        "no_best_bid": 0.59,
                        "no_best_ask": 0.60,
                        "min_depth_usdc": 5000.0,
                        "book_age_ms": 100.0,
                    }
                ]
            }

            weak_signals = build_cex_latency_paper_signals(
                settings,
                markets_payload=markets_payload,
                polymarket_cache=polymarket_cache,
                fair_values=[
                    {
                        "market_id": "btc-quant-poc",
                        "asset": "btc",
                        "fair_yes": 0.505,
                        "fair_no": 0.495,
                        "time_to_expiry_sec": 300.0,
                    }
                ],
            )
            self.assertFalse(weak_signals[0]["eligible"])

            strong_signals = build_cex_latency_paper_signals(
                settings,
                markets_payload=markets_payload,
                polymarket_cache=polymarket_cache,
                fair_values=[
                    {
                        "market_id": "btc-quant-poc",
                        "asset": "btc",
                        "fair_yes": 0.65,
                        "fair_no": 0.35,
                        "time_to_expiry_sec": 300.0,
                    }
                ],
            )
            self.assertTrue(strong_signals[0]["eligible"])
            self.assertEqual(strong_signals[0]["side"], "YES")

    def test_live_complete_set_pilot_blocks_tiny_leg_amount(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = replace(
                self.make_settings(tmpdir),
                live_complete_set_arb_pilot_enabled=True,
                live_complete_set_arb_pilot_mode="dry_run",
                live_complete_set_arb_pilot_min_edge_per_share=0.0,
                live_complete_set_arb_pilot_min_depth_usdc=5.0,
                live_complete_set_arb_pilot_depth_haircut=1.0,
                live_complete_set_arb_pilot_extra_slippage_per_share=0.0,
                live_complete_set_arb_pilot_notional_usdc=15.66,
                live_complete_set_arb_pilot_min_seconds_left=180,
                live_complete_set_arb_pilot_min_leg_amount_usdc=1.0,
            )
            latency_bot_init(settings)
            markets_payload = {
                "items": [
                    {
                        "market_id": "eth5",
                        "asset": "eth",
                        "tenor_minutes": 5,
                        "expiry_ts": "2099-05-07T15:20:00Z",
                        "yes_token_id": "yes-token",
                        "no_token_id": "no-token",
                    }
                ]
            }
            signals = [
                {
                    "market_id": "eth5",
                    "asset": "eth",
                    "tenor_minutes": 5,
                    "yes_ask": 0.97,
                    "no_ask": 0.03,
                    "gross_edge": 0.0,
                    "net_edge": 0.01,
                    "executable_depth_usdc": 1000.0,
                    "book_age_ms": 100.0,
                    "seconds_left": 240.0,
                    "eligible": True,
                    "reason": "complete set net edge clears threshold",
                }
            ]
            result = run_live_complete_set_arb_pilot_cycle(
                settings,
                signals=signals,
                markets_payload=markets_payload,
                ts="2099-05-07T15:16:00Z",
            )
            stats = latency_bot_live_complete_set_arb_pilot_stats(settings)
            self.assertEqual(len(result["blocks"]), 1)
            self.assertEqual(result["blocks"][0]["reason"], "live pilot per-leg amount below CLOB minimum")
            self.assertEqual(stats["summary"]["dry_run_candidates"], 0)

    def test_shadow_btc_yes_variant_signals_follow_fair_floors(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = replace(
                self.make_settings(tmpdir),
                shadow_variant_assets=("btc",),
                shadow_variant_sides=("yes",),
                shadow_variant_tenors_minutes=(5,),
                shadow_variant_fair_floors=(0.50, 0.55),
                shadow_variant_models=("fair", "logit_proxy", "gbt_proxy", "market_blend"),
                shadow_variant_edge_thresholds_5m=(0.03,),
                shadow_variant_entry_seconds_5m=(0,),
            )
            markets_payload = {
                "items": [
                    {
                        "market_id": "btc5",
                        "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                        "asset": "btc",
                        "tenor_minutes": 5,
                        "expiry_ts": "2099-05-07T15:20:00Z",
                    }
                ]
            }
            polymarket_cache = {
                "items": [
                    {
                        "market_id": "btc5",
                        "best_bid": 0.40,
                        "best_ask": 0.42,
                        "min_depth_usdc": 1000.0,
                        "book_age_ms": 1000.0,
                    }
                ]
            }
            fair_values = [
                {
                    "market_id": "btc5",
                    "asset": "btc",
                    "fair_yes": 0.53,
                    "fair_no": 0.47,
                    "time_to_expiry_sec": 300.0,
                    "reference_price": 100.0,
                    "volatility": 0.001,
                }
            ]
            signals = build_shadow_btc_yes_variant_signals(
                settings,
                markets_payload=markets_payload,
                polymarket_cache=polymarket_cache,
                fair_values=fair_values,
            )
            by_variant = {signal["variant_id"]: signal for signal in signals}
            self.assertTrue(by_variant["btc5_yes_fair_f0.50_e0.03_t0"]["eligible"])
            self.assertFalse(by_variant["btc5_yes_fair_f0.55_e0.03_t0"]["eligible"])
            self.assertEqual(by_variant["btc5_yes_fair_f0.55_e0.03_t0"]["reason"], "fair yes below variant floor")
            self.assertIn("btc5_yes_logit_proxy_f0.50_e0.03_t0", by_variant)
            self.assertIn("btc5_yes_gbt_proxy_f0.50_e0.03_t0", by_variant)
            self.assertIn("btc5_yes_market_blend_f0.50_e0.03_t0", by_variant)
            self.assertTrue(signals[0]["eligible"])

    def test_shadow_variant_signals_cover_no_eth_15m(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = replace(
                self.make_settings(tmpdir),
                shadow_variant_assets=("eth",),
                shadow_variant_sides=("no",),
                shadow_variant_tenors_minutes=(15,),
                shadow_variant_fair_floors=(0.52,),
                shadow_variant_models=("fair",),
                shadow_variant_edge_thresholds_15m=(0.05,),
                shadow_variant_entry_seconds_15m=(0,),
            )
            markets_payload = {
                "items": [
                    {
                        "market_id": "eth15",
                        "question": "Ethereum Up or Down - May 7, 10:15AM-10:30AM ET",
                        "asset": "eth",
                        "tenor_minutes": 15,
                        "expiry_ts": "2099-05-07T15:30:00Z",
                    }
                ]
            }
            polymarket_cache = {
                "items": [
                    {
                        "market_id": "eth15",
                        "best_bid": 0.30,
                        "best_ask": 0.32,
                        "min_depth_usdc": 1000.0,
                        "book_age_ms": 1000.0,
                    }
                ]
            }
            fair_values = [
                {
                    "market_id": "eth15",
                    "asset": "eth",
                    "fair_yes": 0.22,
                    "fair_no": 0.78,
                    "time_to_expiry_sec": 900.0,
                    "reference_price": 100.0,
                    "volatility": 0.001,
                }
            ]
            signals = build_shadow_btc_yes_variant_signals(
                settings,
                markets_payload=markets_payload,
                polymarket_cache=polymarket_cache,
                fair_values=fair_values,
            )
            self.assertEqual(len(signals), 1)
            signal = signals[0]
            self.assertEqual(signal["variant_id"], "eth15_no_fair_f0.52_e0.05_t0")
            self.assertEqual(signal["side"], "NO")
            self.assertTrue(signal["eligible"])
            self.assertAlmostEqual(signal["order_price"], 0.70)

    def test_discovery_normalization_keeps_only_btc_eth_5m_15m(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            payload = {
                "markets": [
                    {
                        "id": "btc5",
                        "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                        "slug": "bitcoin-up-or-down-may-7-1015am-1020am-et",
                        "clobTokenIds": '["yes1","no1"]',
                    },
                    {
                        "id": "eth15",
                        "question": "Ethereum Up or Down - May 7, 10:15AM-10:30AM ET",
                        "slug": "ethereum-up-or-down-may-7-1015am-1030am-et",
                        "clobTokenIds": '["yes2","no2"]',
                    },
                    {
                        "id": "sol5",
                        "question": "Solana Up or Down - May 7, 10:15AM-10:20AM ET",
                        "slug": "solana-up-or-down-may-7-1015am-1020am-et",
                        "clobTokenIds": '["yes3","no3"]',
                    },
                    {
                        "id": "btc30",
                        "question": "Bitcoin Up or Down - May 7, 10:00AM-10:30AM ET",
                        "slug": "bitcoin-up-or-down-may-7-1000am-1030am-et",
                        "clobTokenIds": '["yes4","no4"]',
                    },
                ]
            }
            with patch("latency_bot.feeds.discovery._extract_short_market_hours_to_resolution", return_value=0.25):
                items = _normalize_latency_discovery_payload(payload, settings, seen_at="2026-05-07T14:00:00Z")
            self.assertEqual(len(items), 2)
            self.assertEqual({item["market_id"] for item in items}, {"btc5", "eth15"})

    def test_discovery_cycle_writes_market_registry(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            latency_bot_init(settings)
            fake_payload = {
                "generated_at": "2026-05-07T14:00:00Z",
                "count": 1,
                "items": [
                    {
                        "market_id": "btc5",
                        "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                        "slug": "bitcoin-up-or-down",
                        "asset": "btc",
                        "tenor_minutes": 5,
                        "hours_to_expiry": 0.25,
                        "expiry_ts": "2099-05-09T15:20:00Z",
                        "yes_token_id": "yes1",
                        "no_token_id": "no1",
                        "created_at": "2026-05-07T14:00:00Z",
                        "first_seen_at": "2026-05-07T14:00:00Z",
                        "status": "tracked",
                    }
                ],
                "assets": ["btc", "eth"],
                "tenors_minutes": [5, 15],
                "source": {"name": "gamma_events_by_tag", "tag_candidate_count": 4, "fetched_count": 12},
            }
            with patch("latency_bot.core.discover_latency_markets", return_value=fake_payload):
                result = latency_bot_discovery_cycle(settings)
            self.assertEqual(result["tracked_markets_count"], 1)
            state = build_latency_bot_dashboard_state(settings)
            self.assertEqual(state["markets"]["count"], 1)

    def test_polymarket_refresh_prefers_direct_clob_book(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = replace(self.make_settings(tmpdir), cex_latency_paper_notional_usdc=100.0)
            markets_payload = {
                "items": [
                    {
                        "market_id": "btc5",
                        "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                        "asset": "btc",
                        "hours_to_expiry": 0.25,
                        "yes_token_id": "yes1",
                    }
                ]
            }
            with patch(
                "latency_bot.feeds.polymarket._fetch_clob_book",
                return_value={
                    "bids": [{"price": "0.50", "size": "1000"}],
                    "asks": [{"price": "0.52", "size": "50"}, {"price": "0.54", "size": "1200"}],
                },
            ):
                cache = refresh_polymarket_cache(settings, markets_payload)
            self.assertEqual(cache["count"], 1)
            self.assertEqual(cache["items"][0]["market_id"], "btc5")
            self.assertEqual(cache["items"][0]["source"], "clob_rest_book")
            self.assertGreater(cache["items"][0]["ask_vwap"], 0.52)
            self.assertGreater(cache["items"][0]["ask_fillable_usdc"], 100.0)

    def test_polymarket_refresh_falls_back_to_shared_tape(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            tape = {
                "updated_at": "2026-05-07T14:00:00Z",
                "entries": [
                    {
                        "seen_at": "2026-05-07T14:00:01Z",
                        "market_id": "btc5",
                        "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                        "asset": "btc",
                        "hours_to_resolution": 0.25,
                        "midpoint": 0.51,
                        "best_bid": 0.50,
                        "best_ask": 0.52,
                        "spread": 0.02,
                        "bids_depth_usdc": 1000.0,
                        "asks_depth_usdc": 1200.0,
                        "min_depth_usdc": 1000.0,
                    }
                ],
            }
            settings.bootstrap_intraday_book_tape_path.parent.mkdir(parents=True, exist_ok=True)
            settings.bootstrap_intraday_book_tape_path.write_text(json.dumps(tape))
            markets_payload = {
                "items": [
                    {
                        "market_id": "btc5",
                        "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                        "asset": "btc",
                        "hours_to_expiry": 0.25,
                        "yes_token_id": "yes1",
                    }
                ]
            }
            class FailingCLI:
                def book(self, token_id: str) -> dict:
                    raise RuntimeError("boom")

            with patch("latency_bot.feeds.polymarket.shutil.which", return_value="/usr/local/bin/polymarket"):
                cache = refresh_polymarket_cache(settings, markets_payload, cli=FailingCLI())
            self.assertEqual(cache["count"], 1)
            self.assertEqual(cache["items"][0]["source"], "shared_intraday_book_tape_fallback")

    def test_binance_refresh_uses_direct_snapshot(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            fake_payload = {"bidPrice": "100000.0", "askPrice": "100010.0"}
            with patch("latency_bot.feeds.binance._fetch_book_ticker", return_value=fake_payload):
                cache = refresh_binance_cache(settings)
            self.assertEqual(cache["count"], 2)
            assets = {item["asset"] for item in cache["items"]}
            self.assertEqual(assets, {"btc", "eth"})
            self.assertTrue(str(cache.get("source_endpoint") or "").startswith("https://"))

    def test_binance_refresh_reuses_previous_on_error(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            settings.binance_cache_path.parent.mkdir(parents=True, exist_ok=True)
            settings.binance_cache_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2099-05-07T15:00:00Z",
                        "count": 1,
                        "items": [
                            {"asset": "btc", "mid": 100000, "bid": 99990, "ask": 100010, "last": 100005, "source_latency_ms": 12, "seen_at": "2026-05-07T14:00:00Z"},
                            {"asset": "sol", "mid": 150, "bid": 149, "ask": 151, "last": 150, "source_latency_ms": 12, "seen_at": "2026-05-07T14:00:00Z"},
                        ]
                    }
                )
            )
            with patch("latency_bot.feeds.binance._fetch_book_ticker", side_effect=RuntimeError("offline")):
                cache = refresh_binance_cache(settings)
            self.assertEqual(cache["count"], 1)
            self.assertEqual(cache["items"][0]["asset"], "btc")
            self.assertTrue(cache["items"][0]["stale"])

    def test_binance_refresh_falls_through_to_alternate_endpoint(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            calls: list[str] = []

            def fake_fetch(endpoint: str, symbol: str) -> dict:
                calls.append(endpoint)
                if endpoint == settings.binance_rest_endpoint:
                    raise RuntimeError("primary down")
                return {"bidPrice": "100000.0", "askPrice": "100010.0"}

            with patch("latency_bot.feeds.binance._fetch_book_ticker", side_effect=fake_fetch):
                cache = refresh_binance_cache(settings)
            self.assertEqual(cache["count"], 2)
            self.assertGreaterEqual(len(calls), 2)
            self.assertNotEqual(cache.get("source_endpoint"), settings.binance_rest_endpoint)

    def test_compute_updown_fair_yes_moves_with_price_advantage(self) -> None:
        fair_yes = compute_updown_fair_yes(
            reference_price=100.0,
            current_price=101.0,
            return_volatility=0.001,
            seconds_remaining=300.0,
            floor=0.0005,
        )
        self.assertGreater(fair_yes, 0.5)

    def test_build_fair_values_neutral_before_window_start(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)

            class FrozenDateTime(datetime):
                @classmethod
                def now(cls, tz=None):
                    value = cls(2026, 5, 11, 12, 29, 0, tzinfo=timezone.utc)
                    return value if tz is None else value.astimezone(tz)

            markets_payload = {
                "items": [
                    {
                        "market_id": "btc5",
                        "asset": "btc",
                        "question": "Bitcoin Up or Down - May 11, 2026, 8:30AM-8:35AM ET",
                        "tenor_minutes": 5,
                    }
                ]
            }
            polymarket_cache = {"items": [{"market_id": "btc5", "asset": "btc"}]}
            binance_cache = {
                "items": [
                    {
                        "asset": "btc",
                        "mid": 111.0,
                        "samples": [
                            {"seen_at": "2026-05-11T12:23:00Z", "mid": 99.0},
                            {"seen_at": "2026-05-11T12:24:00Z", "mid": 100.0},
                            {"seen_at": "2026-05-11T12:29:00Z", "mid": 111.0},
                        ],
                    }
                ]
            }
            with patch("latency_bot.strategy.fair_value.datetime", FrozenDateTime):
                fair_values = build_fair_values(
                    settings,
                    markets_payload=markets_payload,
                    polymarket_cache=polymarket_cache,
                    binance_cache=binance_cache,
                )
            self.assertEqual(len(fair_values), 1)
            self.assertEqual(fair_values[0]["fair_yes"], 0.5)
            self.assertEqual(fair_values[0]["reference_price"], 111.0)

    def test_build_fair_values_uses_window_start_reference(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)

            class FrozenDateTime(datetime):
                @classmethod
                def now(cls, tz=None):
                    value = cls(2026, 5, 11, 12, 31, 0, tzinfo=timezone.utc)
                    return value if tz is None else value.astimezone(tz)

            markets_payload = {
                "items": [
                    {
                        "market_id": "btc5",
                        "asset": "btc",
                        "question": "Bitcoin Up or Down - May 11, 2026, 8:30AM-8:35AM ET",
                        "tenor_minutes": 5,
                    }
                ]
            }
            polymarket_cache = {"items": [{"market_id": "btc5", "asset": "btc"}]}
            binance_cache = {
                "items": [
                    {
                        "asset": "btc",
                        "mid": 111.0,
                        "samples": [
                            {"seen_at": "2026-05-11T12:24:00Z", "mid": 100.0},
                            {"seen_at": "2026-05-11T12:30:03Z", "mid": 110.0},
                            {"seen_at": "2026-05-11T12:31:00Z", "mid": 111.0},
                        ],
                    }
                ]
            }
            with patch("latency_bot.strategy.fair_value.datetime", FrozenDateTime):
                fair_values = build_fair_values(
                    settings,
                    markets_payload=markets_payload,
                    polymarket_cache=polymarket_cache,
                    binance_cache=binance_cache,
                )
            self.assertEqual(len(fair_values), 1)
            self.assertEqual(fair_values[0]["reference_price"], 110.0)
            self.assertGreater(fair_values[0]["fair_yes"], 0.5)

    def test_build_signals_emits_taker_yes_on_large_edge(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            markets_payload = {
                "items": [
                    {
                        "market_id": "btc5",
                        "asset": "btc",
                        "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                        "tenor_minutes": 5,
                    }
                ]
            }
            polymarket_cache = {
                "items": [
                    {
                        "market_id": "btc5",
                        "best_bid": 0.50,
                        "best_ask": 0.52,
                        "min_depth_usdc": 1000.0,
                        "book_age_ms": 0.0,
                    }
                ]
            }
            fair_values = [
                {
                    "market_id": "btc5",
                    "asset": "btc",
                    "fair_yes": 0.60,
                    "fair_no": 0.40,
                    "time_to_expiry_sec": 120.0,
                }
            ]
            signals = build_signals(settings, markets_payload=markets_payload, polymarket_cache=polymarket_cache, fair_values=fair_values)
            self.assertEqual(len(signals), 1)
            self.assertEqual(signals[0]["signal_type"], "TAKE_YES")

    def test_build_signals_emits_maker_improve_when_taker_not_ready(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            settings.allow_maker_improve = True
            markets_payload = {
                "items": [
                    {
                        "market_id": "btc5",
                        "asset": "btc",
                        "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                        "tenor_minutes": 5,
                    }
                ]
            }
            polymarket_cache = {
                "items": [
                    {
                        "market_id": "btc5",
                        "best_bid": 0.50,
                        "best_ask": 0.52,
                        "min_depth_usdc": 1000.0,
                        "book_age_ms": 0.0,
                    }
                ]
            }
            fair_values = [
                {
                    "market_id": "btc5",
                    "asset": "btc",
                    "fair_yes": 0.525,
                    "fair_no": 0.475,
                    "time_to_expiry_sec": 120.0,
                }
            ]
            signals = build_signals(settings, markets_payload=markets_payload, polymarket_cache=polymarket_cache, fair_values=fair_values)
            self.assertEqual(len(signals), 1)
            self.assertEqual(signals[0]["signal_type"], "MAKE_YES_IMPROVE")
            self.assertTrue(signals[0]["eligible"])
            self.assertGreater(float(signals[0]["order_price"]), 0.50)

    def test_build_signals_blocks_no_taker_by_default(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            markets_payload = {
                "items": [
                    {
                        "market_id": "btc5",
                        "asset": "btc",
                        "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                        "tenor_minutes": 5,
                    }
                ]
            }
            polymarket_cache = {
                "items": [
                    {
                        "market_id": "btc5",
                        "best_bid": 0.10,
                        "best_ask": 0.12,
                        "min_depth_usdc": 1000.0,
                        "book_age_ms": 0.0,
                    }
                ]
            }
            fair_values = [
                {
                    "market_id": "btc5",
                    "asset": "btc",
                    "fair_yes": 0.10,
                    "fair_no": 0.90,
                    "time_to_expiry_sec": 120.0,
                }
            ]
            signals = build_signals(settings, markets_payload=markets_payload, polymarket_cache=polymarket_cache, fair_values=fair_values)
            self.assertEqual(len(signals), 1)
            self.assertEqual(signals[0]["signal_type"], "SKIP")

    def test_build_signals_reports_fair_yes_threshold_blocker(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            settings.taker_min_fair_yes_5m = 0.60
            markets_payload = {
                "items": [
                    {
                        "market_id": "btc5",
                        "asset": "btc",
                        "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                        "tenor_minutes": 5,
                    }
                ]
            }
            polymarket_cache = {
                "items": [
                    {
                        "market_id": "btc5",
                        "best_bid": 0.31,
                        "best_ask": 0.34,
                        "min_depth_usdc": 1000.0,
                        "book_age_ms": 0.0,
                    }
                ]
            }
            fair_values = [
                {
                    "market_id": "btc5",
                    "asset": "btc",
                    "fair_yes": 0.484,
                    "fair_no": 0.516,
                    "time_to_expiry_sec": 240.0,
                }
            ]
            signals = build_signals(settings, markets_payload=markets_payload, polymarket_cache=polymarket_cache, fair_values=fair_values)
            self.assertEqual(len(signals), 1)
            self.assertEqual(signals[0]["signal_type"], "SKIP")
            self.assertEqual(signals[0]["reason"], "fair yes below threshold")
            self.assertEqual(signals[0]["blocked_reason"], "min_fair_yes")
            self.assertGreater(float(signals[0]["edge"]), 0.0)

    def test_build_signals_blocks_eth_yes_by_default(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            settings.allow_eth_yes_taker = False
            markets_payload = {
                "items": [
                    {
                        "market_id": "eth5",
                        "asset": "eth",
                        "question": "Ethereum Up or Down - May 7, 10:15AM-10:20AM ET",
                        "tenor_minutes": 5,
                    }
                ]
            }
            polymarket_cache = {
                "items": [
                    {
                        "market_id": "eth5",
                        "best_bid": 0.50,
                        "best_ask": 0.52,
                        "min_depth_usdc": 1000.0,
                        "book_age_ms": 0.0,
                    }
                ]
            }
            fair_values = [
                {
                    "market_id": "eth5",
                    "asset": "eth",
                    "fair_yes": 0.70,
                    "fair_no": 0.30,
                    "time_to_expiry_sec": 120.0,
                }
            ]
            signals = build_signals(settings, markets_payload=markets_payload, polymarket_cache=polymarket_cache, fair_values=fair_values)
            self.assertEqual(len(signals), 1)
            self.assertEqual(signals[0]["signal_type"], "SKIP")
            self.assertEqual(signals[0]["reason"], "live slice disabled")

    def test_engine_cycle_closes_position_on_stop_loss(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            settings.stop_loss_fraction = 0.20
            latency_bot_init(settings)
            expiry_ts = "2099-05-07T15:20:00Z"
            settings.markets_path.write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "market_id": "btc5",
                                "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                                "asset": "btc",
                                "tenor_minutes": 5,
                                "hours_to_expiry": 0.25,
                                "expiry_ts": expiry_ts,
                                "yes_token_id": "yes1",
                                "no_token_id": "no1",
                                "created_at": "2099-05-07T15:00:00Z",
                                "first_seen_at": "2099-05-07T15:00:00Z",
                                "status": "tracked",
                            }
                        ]
                    }
                )
            )
            create_position(
                settings,
                ts="2099-05-07T15:00:00Z",
                market_id="btc5",
                asset="btc",
                side="YES",
                entry_price=0.50,
                size=200.0,
                mode="taker",
            )
            polymarket_cache = {
                "updated_at": "2099-05-07T15:01:00Z",
                "count": 1,
                "source": "test",
                "errors": [],
                "items": [
                    {
                        "market_id": "btc5",
                        "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                        "asset": "btc",
                        "hours_to_expiry": 0.23,
                        "best_bid": 0.39,
                        "best_ask": 0.40,
                        "spread": 0.01,
                        "bids_depth_usdc": 2000.0,
                        "asks_depth_usdc": 2000.0,
                        "min_depth_usdc": 2000.0,
                        "book_age_ms": 0.0,
                    }
                ],
            }
            binance_cache = {
                "updated_at": "2099-05-07T15:01:00Z",
                "count": 1,
                "source": "test",
                "errors": [],
                "items": [
                    {
                        "asset": "btc",
                        "symbol": "BTCUSDT",
                        "mid": 100.1,
                        "bid": 100.0,
                        "ask": 100.2,
                        "last": 100.1,
                        "source_latency_ms": 0.0,
                        "seen_at": "2099-05-07T15:01:00Z",
                        "samples": [
                            {"seen_at": "2099-05-07T14:59:00Z", "mid": 100.0, "bid": 99.9, "ask": 100.1},
                            {"seen_at": "2099-05-07T14:59:30Z", "mid": 100.05, "bid": 99.95, "ask": 100.15},
                            {"seen_at": "2099-05-07T15:01:00Z", "mid": 100.1, "bid": 100.0, "ask": 100.2},
                        ],
                    }
                ],
            }
            fair_values = [{"market_id": "btc5", "asset": "btc", "fair_yes": 0.62, "fair_no": 0.38, "time_to_expiry_sec": 240.0}]
            with patch("latency_bot.core.refresh_polymarket_cache", return_value=polymarket_cache), patch(
                "latency_bot.core.refresh_binance_cache", return_value=binance_cache
            ), patch("latency_bot.core.build_fair_values", return_value=fair_values):
                latency_bot_engine_cycle(settings)
            self.assertEqual(len(load_open_positions(settings)), 0)

    def test_close_position_is_idempotent(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            latency_bot_init(settings)
            position = create_position(
                settings,
                ts="2099-05-07T15:00:00Z",
                market_id="btc5",
                asset="btc",
                side="YES",
                entry_price=0.50,
                size=200.0,
                mode="taker",
            )

            close_position(
                settings,
                position_id=position["position_id"],
                ts="2099-05-07T15:01:00Z",
                exit_price=0.40,
                pnl=-20.0,
                reason="STOP_LOSS",
            )
            close_position(
                settings,
                position_id=position["position_id"],
                ts="2099-05-07T15:01:01Z",
                exit_price=0.39,
                pnl=-22.0,
                reason="STOP_LOSS",
            )

            with connect_latency_bot_db(settings) as conn:
                close_count = conn.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM position_events
                    WHERE position_id = ? AND event_type = 'close'
                    """,
                    (position["position_id"],),
                ).fetchone()["count"]
                exit_fills = conn.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM fills
                    WHERE fill_type = 'exit'
                    """
                ).fetchone()["count"]
                status = conn.execute(
                    "SELECT status FROM positions WHERE position_id = ?",
                    (position["position_id"],),
                ).fetchone()["status"]
            self.assertEqual(close_count, 1)
            self.assertEqual(exit_fills, 1)
            self.assertEqual(status, "closed")

    def test_position_entry_features_feed_close_analytics(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            latency_bot_init(settings)
            position = create_position(
                settings,
                ts="2099-05-07T15:00:00Z",
                market_id="btc5",
                asset="btc",
                side="YES",
                entry_price=0.50,
                size=200.0,
                mode="taker",
                signal={
                    "signal_type": "TAKE_YES",
                    "edge": 0.091,
                    "fair_yes": 0.62,
                    "fair_no": 0.38,
                    "seconds_left": 420.0,
                    "min_depth_usdc": 1500.0,
                    "book_age_ms": 0.0,
                    "reference_price": 80000.0,
                    "volatility": 0.0005,
                    "tenor_minutes": 5,
                    "reason": "yes taker edge clears threshold",
                },
            )
            close_position(
                settings,
                position_id=position["position_id"],
                ts="2099-05-07T15:01:00Z",
                exit_price=0.62,
                pnl=24.0,
                reason="TAKE_PROFIT",
            )

            stats = latency_bot_performance_stats(settings)
            self.assertEqual(stats["edge_band_breakdown"][0]["band"], ">=0.08")
            self.assertAlmostEqual(stats["edge_band_breakdown"][0]["avg_edge"], 0.091)
            self.assertEqual(stats["seconds_band_breakdown"][0]["band"], "181-420s")

    def test_same_market_cooldown_blocks_reentry(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            settings.same_market_cooldown_seconds = 900
            latency_bot_init(settings)
            position = create_position(
                settings,
                ts="2099-05-07T15:00:00Z",
                market_id="btc5",
                asset="btc",
                side="YES",
                entry_price=0.50,
                size=200.0,
                mode="taker",
            )
            close_position(
                settings,
                position_id=position["position_id"],
                ts="2099-05-07T15:01:00Z",
                exit_price=0.40,
                pnl=-20.0,
                reason="STOP_LOSS",
            )
            result = run_paper_execution_cycle(
                settings,
                markets_payload={
                    "items": [
                        {
                            "market_id": "btc5",
                            "asset": "btc",
                            "tenor_minutes": 5,
                            "expiry_ts": "2099-05-07T15:20:00Z",
                        }
                    ]
                },
                polymarket_cache={"items": []},
                signals=[
                    {
                        "market_id": "btc5",
                        "asset": "btc",
                        "tenor_minutes": 5,
                        "signal_type": "TAKE_YES",
                        "mode": "taker",
                        "eligible": True,
                        "yes_ask": 0.50,
                        "edge": 0.10,
                        "seconds_left": 1000.0,
                    }
                ],
                ts="2099-05-07T15:02:00Z",
            )

            self.assertEqual(result["opened_positions_count"], 0)
            self.assertEqual(result["entry_blocks_count"], 1)
            self.assertEqual(result["entry_blocks"][0]["reason"], "same market cooldown")

    def test_init_dedupes_legacy_duplicate_live_closes(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            latency_bot_init(settings)
            position = create_position(
                settings,
                ts="2099-05-07T15:00:00Z",
                market_id="btc5",
                asset="btc",
                side="YES",
                entry_price=0.50,
                size=200.0,
                mode="taker",
            )
            position_id = position["position_id"]
            with connect_latency_bot_db(settings) as conn:
                conn.execute("DROP INDEX IF EXISTS idx_position_events_one_close")
                conn.execute("UPDATE positions SET status = 'closed' WHERE position_id = ?", (position_id,))
                for ts in ("2099-05-07T15:01:00Z", "2099-05-07T15:01:01Z"):
                    order_id = f"paper-exit-{position_id}-{ts}"
                    fill_id = f"paper-exit-fill-{position_id}-{ts}"
                    conn.execute(
                        """
                        INSERT INTO orders (
                            order_id, ts_created, market_id, mode, side, price, size, status, reprices, cancel_reason
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (order_id, ts, "btc5", "taker", "YES", 0.40, 200.0, "filled", 0, ""),
                    )
                    conn.execute(
                        """
                        INSERT INTO fills (
                            fill_id, order_id, ts, market_id, side, price, size, fill_type
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (fill_id, order_id, ts, "btc5", "YES", 0.40, 200.0, "exit"),
                    )
                    conn.execute(
                        """
                        INSERT INTO position_events (
                            ts, position_id, event_type, mark, edge, pnl, reason
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (ts, position_id, "close", 0.40, None, -20.0, "STOP_LOSS"),
                    )
                conn.commit()

            payload = latency_bot_init(settings)
            with connect_latency_bot_db(settings) as conn:
                close_count = conn.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM position_events
                    WHERE position_id = ? AND event_type = 'close'
                    """,
                    (position_id,),
                ).fetchone()["count"]
                exit_fills = conn.execute(
                    "SELECT COUNT(*) AS count FROM fills WHERE fill_type = 'exit'"
                ).fetchone()["count"]
            self.assertEqual(payload["db"]["deduped_live_closes"], 1)
            self.assertEqual(close_count, 1)
            self.assertEqual(exit_fills, 1)

    def test_engine_cycle_opens_maker_order_when_signal_is_maker(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            settings.allow_maker_join = True
            settings.allow_maker_improve = True
            latency_bot_init(settings)
            expiry_ts = "2099-05-07T15:20:00Z"
            settings.markets_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2099-05-07T15:00:00Z",
                        "count": 1,
                        "items": [
                            {
                                "market_id": "btc5",
                                "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                                "asset": "btc",
                                "tenor_minutes": 5,
                                "hours_to_expiry": 0.25,
                                "expiry_ts": expiry_ts,
                                "yes_token_id": "yes1",
                                "no_token_id": "no1",
                                "created_at": "2099-05-07T15:00:00Z",
                                "first_seen_at": "2099-05-07T15:00:00Z",
                                "status": "tracked",
                            }
                        ]
                    }
                )
            )
            polymarket_cache = {
                "updated_at": "2099-05-07T15:00:00Z",
                "count": 1,
                "source": "test",
                "errors": [],
                "items": [
                    {
                        "market_id": "btc5",
                        "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                        "asset": "btc",
                        "hours_to_expiry": 0.25,
                        "best_bid": 0.50,
                        "best_ask": 0.52,
                        "spread": 0.02,
                        "bids_depth_usdc": 1000.0,
                        "asks_depth_usdc": 1200.0,
                        "min_depth_usdc": 1000.0,
                        "book_age_ms": 0.0,
                    }
                ],
            }
            binance_cache = {
                "updated_at": "2099-05-07T15:00:00Z",
                "count": 1,
                "source": "test",
                "errors": [],
                "items": [
                    {
                        "asset": "btc",
                        "symbol": "BTCUSDT",
                        "mid": 100.1,
                        "bid": 100.0,
                        "ask": 100.2,
                        "last": 100.1,
                        "source_latency_ms": 0.0,
                        "seen_at": "2099-05-07T15:00:00Z",
                        "samples": [
                            {"seen_at": "2099-05-07T14:59:00Z", "mid": 100.0, "bid": 99.9, "ask": 100.1},
                            {"seen_at": "2099-05-07T14:59:30Z", "mid": 100.05, "bid": 99.95, "ask": 100.15},
                            {"seen_at": "2099-05-07T15:00:00Z", "mid": 100.1, "bid": 100.0, "ask": 100.2},
                        ],
                    }
                ],
            }
            fair_values = [{"market_id": "btc5", "asset": "btc", "fair_yes": 0.525, "fair_no": 0.475, "time_to_expiry_sec": 120.0}]
            with patch("latency_bot.core.refresh_polymarket_cache", return_value=polymarket_cache), patch(
                "latency_bot.core.refresh_binance_cache", return_value=binance_cache
            ), patch("latency_bot.core.build_fair_values", return_value=fair_values):
                result = latency_bot_engine_cycle(settings)
            self.assertEqual(len(load_open_orders(settings)), 1)
            self.assertEqual(len(load_open_positions(settings)), 0)

    def test_engine_cycle_fills_existing_maker_order_when_book_trades_through(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            latency_bot_init(settings)
            expiry_ts = "2099-05-07T15:20:00Z"
            settings.markets_path.write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "market_id": "btc5",
                                "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                                "asset": "btc",
                                "tenor_minutes": 5,
                                "hours_to_expiry": 0.25,
                                "expiry_ts": expiry_ts,
                                "yes_token_id": "yes1",
                                "no_token_id": "no1",
                                "created_at": "2099-05-07T15:00:00Z",
                                "first_seen_at": "2099-05-07T15:00:00Z",
                                "status": "tracked",
                            }
                        ]
                    }
                )
            )
            first_polymarket_cache = {
                "updated_at": "2099-05-07T15:00:00Z",
                "count": 1,
                "source": "test",
                "errors": [],
                "items": [
                    {
                        "market_id": "btc5",
                        "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                        "asset": "btc",
                        "hours_to_expiry": 0.25,
                        "best_bid": 0.50,
                        "best_ask": 0.52,
                        "spread": 0.02,
                        "bids_depth_usdc": 1000.0,
                        "asks_depth_usdc": 1200.0,
                        "min_depth_usdc": 1000.0,
                        "book_age_ms": 0.0,
                    }
                ],
            }
            second_polymarket_cache = {
                "updated_at": "2099-05-07T15:00:05Z",
                "count": 1,
                "source": "test",
                "errors": [],
                "items": [
                    {
                        "market_id": "btc5",
                        "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                        "asset": "btc",
                        "hours_to_expiry": 0.24,
                        "best_bid": 0.50,
                        "best_ask": 0.50,
                        "spread": 0.00,
                        "bids_depth_usdc": 1000.0,
                        "asks_depth_usdc": 1200.0,
                        "min_depth_usdc": 1000.0,
                        "book_age_ms": 0.0,
                    }
                ],
            }
            binance_cache = {
                "updated_at": "2099-05-07T15:00:00Z",
                "count": 1,
                "source": "test",
                "errors": [],
                "items": [
                    {
                        "asset": "btc",
                        "symbol": "BTCUSDT",
                        "mid": 100.1,
                        "bid": 100.0,
                        "ask": 100.2,
                        "last": 100.1,
                        "source_latency_ms": 0.0,
                        "seen_at": "2099-05-07T15:00:00Z",
                        "samples": [
                            {"seen_at": "2099-05-07T14:59:00Z", "mid": 100.0, "bid": 99.9, "ask": 100.1},
                            {"seen_at": "2099-05-07T14:59:30Z", "mid": 100.05, "bid": 99.95, "ask": 100.15},
                            {"seen_at": "2099-05-07T15:00:00Z", "mid": 100.1, "bid": 100.0, "ask": 100.2},
                        ],
                    }
                ],
            }
            fair_values = [{"market_id": "btc5", "asset": "btc", "fair_yes": 0.525, "fair_no": 0.475, "time_to_expiry_sec": 120.0}]
            with patch("latency_bot.core.refresh_polymarket_cache", return_value=first_polymarket_cache), patch(
                "latency_bot.core.refresh_binance_cache", return_value=binance_cache
            ), patch("latency_bot.core.build_fair_values", return_value=fair_values):
                latency_bot_engine_cycle(settings)
            with patch("latency_bot.core.refresh_polymarket_cache", return_value=second_polymarket_cache), patch(
                "latency_bot.core.refresh_binance_cache", return_value=binance_cache
            ), patch("latency_bot.core.build_fair_values", return_value=fair_values):
                result = latency_bot_engine_cycle(settings)
            self.assertEqual(len(load_open_orders(settings)), 0)
            self.assertIn(len(load_open_positions(settings)), {0, 1})

    def test_engine_cycle_refreshes_stale_expired_market_registry(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            latency_bot_init(settings)
            settings.markets_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-05-07T17:00:00Z",
                        "count": 1,
                        "items": [
                            {
                                "market_id": "expired-btc5",
                                "question": "Bitcoin Up or Down - May 7, 1:25PM-1:30PM ET",
                                "asset": "btc",
                                "tenor_minutes": 5,
                                "hours_to_expiry": 0.01,
                                "expiry_ts": "2026-05-07T17:30:00Z",
                                "yes_token_id": "yes-old",
                                "no_token_id": "no-old",
                                "created_at": "2026-05-07T17:00:00Z",
                                "first_seen_at": "2026-05-07T17:00:00Z",
                                "status": "tracked",
                            }
                        ],
                    }
                )
            )
            discovered = {
                "generated_at": "2099-05-07T15:00:00Z",
                "count": 1,
                "items": [
                    {
                        "market_id": "live-btc5",
                        "question": "Bitcoin Up or Down - May 7, 10:15AM-10:20AM ET",
                        "asset": "btc",
                        "tenor_minutes": 5,
                        "hours_to_expiry": 0.25,
                        "expiry_ts": "2099-05-07T15:20:00Z",
                        "yes_token_id": "yes-new",
                        "no_token_id": "no-new",
                        "created_at": "2099-05-07T15:00:00Z",
                        "first_seen_at": "2099-05-07T15:00:00Z",
                        "status": "tracked",
                    }
                ],
                "assets": ["btc", "eth"],
                "tenors_minutes": [5, 15],
                "source": {"name": "gamma_events_by_tag", "tag_candidate_count": 4, "fetched_count": 1},
            }
            empty_cache = {"updated_at": "2099-05-07T15:00:00Z", "count": 0, "source": "test", "errors": [], "items": []}
            with patch("latency_bot.core.discover_latency_markets", return_value=discovered), patch(
                "latency_bot.core.refresh_polymarket_cache", return_value=empty_cache
            ), patch("latency_bot.core.refresh_binance_cache", return_value=empty_cache):
                result = latency_bot_engine_cycle(settings)
            self.assertEqual(result["tracked_markets_count"], 1)
            state = build_latency_bot_dashboard_state(settings)
            self.assertEqual(state["markets"]["items"][0]["market_id"], "live-btc5")

    def test_dashboard_hides_stale_historical_rows(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            latency_bot_init(settings)
            with connect_latency_bot_db(settings) as conn:
                conn.execute(
                    """
                    INSERT INTO fair_values (
                        ts, market_id, asset, fair_yes, fair_no, reference_price, volatility, time_to_expiry_sec
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("2026-05-07T00:00:00Z", "old-market", "btc", 0.5, 0.5, 80000.0, 0.001, 120.0),
                )
                conn.execute(
                    """
                    INSERT INTO signals (
                        ts, market_id, signal_type, edge, mode, reason, eligible, blocked_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("2026-05-07T00:00:00Z", "old-market", "TAKE_YES", 0.1, "taker", "old row", 1, ""),
                )
                conn.execute(
                    """
                    INSERT INTO fills (
                        fill_id, order_id, ts, market_id, side, price, size, fill_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("fill-old", "order-old", "2026-05-07T00:00:00Z", "old-market", "YES", 0.5, 10.0, "entry"),
                )
                conn.execute(
                    """
                    INSERT INTO orders (
                        order_id, ts_created, market_id, mode, side, price, size, status, reprices, cancel_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("order-old", "2026-05-07T00:00:00Z", "old-market", "taker", "YES", 0.5, 10.0, "filled", 0, ""),
                )
                conn.commit()
            state = build_latency_bot_dashboard_state(settings)
            html = render_latency_bot_dashboard_html(state)
            self.assertIn("No data", html)
            self.assertNotIn("old-market", html)

    def test_signal_slice_analytics_ignore_legacy_rows_without_features(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            latency_bot_init(settings)
            with connect_latency_bot_db(settings) as conn:
                conn.execute(
                    """
                    INSERT INTO signals (
                        ts, market_id, signal_type, edge, mode, reason, eligible, blocked_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("2099-05-07T00:00:00Z", "legacy-market", "TAKE_YES", 0.1, "taker", "legacy row", 1, ""),
                )
                conn.commit()
            state = build_latency_bot_dashboard_state(settings)
            html = render_latency_bot_dashboard_html(state)
            self.assertNotIn("? 0m", html)

    def test_signal_dimension_backfill_repairs_legacy_rows(self) -> None:
        with TemporaryDirectory() as tmpdir:
            settings = self.make_settings(tmpdir)
            latency_bot_init(settings)
            with connect_latency_bot_db(settings) as conn:
                conn.execute(
                    """
                    INSERT INTO markets (
                        market_id, question, asset, tenor_minutes, expiry_ts,
                        yes_token_id, no_token_id, created_at, first_seen_at, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "btc5",
                        "Bitcoin Up or Down - Test",
                        "btc",
                        5,
                        "2099-05-07T15:20:00Z",
                        "yes1",
                        "no1",
                        "2099-05-07T15:00:00Z",
                        "2099-05-07T15:00:00Z",
                        "tracked",
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO shadow_signals (
                        ts, market_id, signal_type, edge, mode, reason, eligible, blocked_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("2099-05-07T00:00:00Z", "btc5", "SHADOW_SKIP", 0.0, "shadow", "legacy row", 0, ""),
                )
                conn.commit()
            latency_bot_init(settings)
            with connect_latency_bot_db(settings) as conn:
                row = conn.execute(
                    """
                    SELECT asset, tenor_minutes
                    FROM shadow_signals
                    WHERE market_id = 'btc5'
                    ORDER BY id DESC
                    LIMIT 1
                    """
                ).fetchone()
            self.assertEqual(str(row["asset"] or ""), "btc")
            self.assertEqual(int(row["tenor_minutes"] or 0), 5)
