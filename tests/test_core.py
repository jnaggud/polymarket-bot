import json
import sqlite3
import unittest
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from zoneinfo import ZoneInfo

from bot.accounting import dedupe_closed_trades, effective_position_shares, rebuild_closed_trades
from bot.config import Settings
from bot.dashboard import _build_lifetime_stats
from bot.core import (
    _aggressive_market_guard,
    _find_target_trades,
    _current_bankroll_usdc,
    _current_equity_usdc,
    _current_cycle_result_from_files,
    _aggressive_reentry_guard,
    _extract_openai_output_text,
    _extract_market_category,
    _extract_market_token_ids,
    _extract_short_market_hours_to_resolution,
    _forced_derisk_reason,
    _is_five_minute_updown_market,
    _load_intraday_book_tape_markets,
    _load_intraday_imminent_updown_markets,
    _load_intraday_raw_updown_markets,
    _load_intraday_registry_markets,
    _load_intraday_registry_watchlist_markets,
    _load_intraday_threshold_live_markets,
    _intraday_ws_asset_batches,
    _market_theme_key,
    _high_midpoint_sell_guard,
    _intraday_registry_merge_market,
    _intraday_registry_market_record,
    _intraday_registry_is_fresh,
    _intraday_box_execution_state,
    _intraday_registry_rejection_reason,
    _intraday_registry_records,
    _intraday_registry_population_metrics,
    _append_intraday_edge_alerts,
    _index_wallet_trades,
    _is_transient_cli_error,
    _market_prefilter,
    _market_from_intraday_ws_event,
    _parse_intraday_ws_payloads,
    _record_intraday_audit_arb_opportunities,
    _record_intraday_audit_book_snapshots,
    _record_intraday_audit_cycle,
    _record_intraday_audit_imminent_candidates,
    _record_intraday_audit_aggressive_box_simulations,
    _record_intraday_audit_maker_box_simulations,
    _record_intraday_audit_market_lifecycle,
    _record_intraday_audit_watchlist_transitions,
    _record_intraday_audit_ws_events,
    _record_intraday_discovery_comparator_batch,
    _refresh_gamma_watchlist_records,
    _remaining_bankroll_usdc,
    _risk_bankroll_usdc,
    _scan_short_crypto_updown_candidates,
    _scheduled_intraday_exit_deadline,
    _select_rotation_position,
    _side_market_price,
    _side_probability,
    _primary_book_entry_guard,
    _scheduled_intraday_crypto_vote,
    _short_crypto_sniper_vote,
    _short_crypto_market_asset,
    _short_crypto_updown_asset,
    _simulate_box_pair_open_maker_proxy,
    _simulate_box_pair_open_aggressive_proxy,
    _threshold_snapshot_crypto_vote,
    _threshold_snapshot_spread_size_multiplier,
    _box_arb_edge_metrics,
    _simulate_box_pair_open,
    _stale_reentry_guard,
    _strategy_health_gate,
    _force_close_position_zero_proceeds,
    _within_market_cooldown,
    _wallet_copy_candidate_pool,
    build_theses,
    compare_intraday_discovery_sources,
    derive_crypto_target_activity,
    MarketCandidate,
    summarize_intraday_discovery_comparator,
    PolymarketCLI,
    PaperExecutor,
    kelly_size,
    refresh_target_activity,
    refresh_target_activity_for_paths,
    trade_candidates,
    trade_candidates_wallet_copy_variant,
    whale_copy_vote,
    wallet_copy_ab_vote,
    wallet_copy_variant_vote,
    _write_intraday_registry,
    _subscription_seed_records_from_markets,
    summarize_intraday_audit,
    summarize_intraday_lifecycle,
)


class KellySizeTest(unittest.TestCase):
    def test_negative_edge_returns_zero(self) -> None:
        self.assertEqual(kelly_size(0.40, 0.65, 800, 0.25), 0.0)

    def test_positive_edge_is_capped(self) -> None:
        size = kelly_size(0.82, 0.65, 800, 0.25)
        self.assertGreater(size, 0.0)
        self.assertLessEqual(size, 200.0)

    def test_extract_openai_output_text(self) -> None:
        payload = {
            "output": [
                {
                    "type": "message",
                    "content": [
                        {
                            "type": "output_text",
                            "text": '{"market_id":"1","token_id":"2","estimated_probability":0.6,"confidence":0.8,"thesis":"x","catalysts":[],"crowd_error":"y"}',
                        }
                    ],
                }
            ]
        }
        self.assertIn('"market_id":"1"', _extract_openai_output_text(payload))

    def test_market_prefilter_uses_relaxed_when_strict_empty(self) -> None:
        settings = Settings.from_env()
        settings.markets_limit = 10
        settings.queue_max_candidates = 5
        settings.min_hours_to_resolution = 4
        settings.max_hours_to_resolution = 168
        now = datetime.now(timezone.utc)
        markets = [
            {
                "id": "1",
                "question": "A",
                "slug": "a",
                "active": True,
                "closed": False,
                "acceptingOrders": True,
                "endDate": (now + timedelta(hours=200)).isoformat().replace("+00:00", "Z"),
                "clobTokenIds": '["tok1","tok2"]',
                "volume": "1000",
            },
            {
                "id": "2",
                "question": "B",
                "slug": "b",
                "active": True,
                "closed": False,
                "acceptingOrders": True,
                "endDate": (now + timedelta(hours=220)).isoformat().replace("+00:00", "Z"),
                "clobTokenIds": '["tok3","tok4"]',
                "volume": "1000",
            },
        ]
        selected, mode = _market_prefilter(settings, markets)
        self.assertTrue(mode.startswith("relaxed"))
        self.assertGreaterEqual(len(selected), 1)

    def test_side_probability_for_sell_uses_no_side(self) -> None:
        self.assertAlmostEqual(_side_probability("SELL", 0.2), 0.8)
        self.assertAlmostEqual(_side_market_price("SELL", 0.3), 0.7)

    def test_settings_has_open_position_cap(self) -> None:
        settings = Settings.from_env()
        self.assertGreaterEqual(settings.max_open_positions, 1)

    def test_stale_reentry_guard_blocks_repeat_stale_losers(self) -> None:
        settings = Settings.from_env()
        settings.stale_reentry_block_minutes = 2880
        settings.stale_reentry_lookback_hours = 168
        settings.stale_reentry_max_closes = 2
        settings.stale_reentry_min_cumulative_loss_usdc = 25.0
        candidate = MarketCandidate(
            market_id="m1",
            question="Repeat loser market",
            slug="repeat-loser",
            token_id="tok1",
            midpoint=0.4,
            best_bid=0.39,
            best_ask=0.41,
            bids_depth=1000.0,
            asks_depth=1000.0,
            spread=0.02,
            hours_to_resolution=24.0,
            total_volume=10000.0,
            book_imbalance=0.0,
            category="politics",
            priority_score=5.0,
        )
        now = datetime.now(timezone.utc)
        history = {
            "m1": [
                {
                    "type": "CLOSE",
                    "market_id": "m1",
                    "reason": "STALE_THESIS",
                    "pnl_usdc": -20.0,
                    "closed_at": (now - timedelta(hours=5)).isoformat().replace("+00:00", "Z"),
                },
                {
                    "type": "CLOSE",
                    "market_id": "m1",
                    "reason": "STALE_THESIS",
                    "pnl_usdc": -15.0,
                    "closed_at": (now - timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
                },
            ]
        }

        blocked, reason = _stale_reentry_guard(settings, candidate, history)

        self.assertTrue(blocked)
        self.assertIn("stale re-entry blocked", reason)

    def test_high_midpoint_sell_guard_blocks_rich_sell_entries(self) -> None:
        settings = Settings.from_env()
        settings.max_sell_midpoint = 0.85
        candidate = MarketCandidate(
            market_id="m2",
            question="Will the Democrats win the Minnesota governor race in 2026?",
            slug="mn-governor-2026",
            token_id="tok2",
            midpoint=0.92,
            best_bid=0.91,
            best_ask=0.93,
            bids_depth=1000.0,
            asks_depth=1000.0,
            spread=0.02,
            hours_to_resolution=24.0,
            total_volume=10000.0,
            book_imbalance=0.0,
            category="unknown",
            priority_score=5.0,
        )

        blocked, reason = _high_midpoint_sell_guard(settings, "SELL", candidate)

        self.assertTrue(blocked)
        self.assertIn("sell midpoint too high", reason)

    def test_aggressive_reentry_guard_requires_recent_profit(self) -> None:
        candidate = MarketCandidate(
            market_id="m3",
            question="Aggressive re-entry market",
            slug="aggressive-reentry-market",
            token_id="tok3",
            midpoint=0.25,
            best_bid=0.24,
            best_ask=0.26,
            bids_depth=1000.0,
            asks_depth=1000.0,
            spread=0.02,
            hours_to_resolution=24.0,
            total_volume=10000.0,
            book_imbalance=0.0,
            category="sports",
            priority_score=5.0,
        )
        blocked, reason = _aggressive_reentry_guard(
            candidate,
            {"m3": [{"type": "CLOSE", "market_id": "m3", "pnl_usdc": -1.0}]},
            require_profit=True,
            min_profit_usdc=5.0,
        )
        self.assertTrue(blocked)
        self.assertIn("re-entry requires prior profit", reason)

        allowed, _ = _aggressive_reentry_guard(
            candidate,
            {"m3": [{"type": "CLOSE", "market_id": "m3", "pnl_usdc": 12.0}]},
            require_profit=True,
            min_profit_usdc=5.0,
        )
        self.assertFalse(allowed)

    def test_market_theme_key_groups_outrights(self) -> None:
        self.assertEqual(
            _market_theme_key("Will Canada win the 2026 FIFA World Cup?", "sports"),
            "2026 fifa world cup",
        )
        self.assertEqual(
            _market_theme_key("Will the Los Angeles Lakers win the 2026 NBA Finals?", "sports"),
            "2026 nba finals",
        )

    def test_wallet_copy_variant_respects_theme_position_cap(self) -> None:
        settings = Settings.from_env()
        candidate = MarketCandidate(
            market_id="theme-1",
            question="Will Morocco win the 2026 FIFA World Cup?",
            slug="morocco-2026-world-cup",
            token_id="tok-theme-1",
            midpoint=0.02,
            best_bid=0.019,
            best_ask=0.021,
            bids_depth=10_000.0,
            asks_depth=10_000.0,
            spread=0.002,
            hours_to_resolution=24.0,
            total_volume=100_000.0,
            book_imbalance=0.0,
            category="sports",
            priority_score=8.0,
        )
        with TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            shadow_settings = replace(
                settings,
                positions_path=tmp / "positions.json",
                trades_path=tmp / "trades.json",
                marks_path=tmp / "marks.json",
                missed_opportunities_path=tmp / "missed.json",
            )
            shadow_settings.ensure_dirs()
            positions = [
                {
                    "position_id": f"p{i}",
                    "market_id": f"m{i}",
                    "token_id": f"tok{i}",
                    "question": f"Will Team {i} win the 2026 FIFA World Cup?",
                    "side": "BUY",
                    "shares": 100.0,
                    "notional_usdc": 250.0,
                    "entry_price": 0.02,
                    "expected_gap": 0.05,
                    "opened_at": "2026-05-01T00:00:00Z",
                    "thesis_confidence": 1.0,
                    "mode": "paper",
                    "category": "sports",
                }
                for i in range(10)
            ]
            shadow_settings.positions_path.write_text(json.dumps(positions), encoding="utf-8")
            shadow_settings.trades_path.write_text("[]", encoding="utf-8")
            activity = {
                "by_match_key": {
                    "theme-1": [{"market_id": "theme-1", "token_id": "tok-theme-1", "side": "BUY", "size": 1000}]
                }
            }
            activity_path = tmp / "activity.json"
            activity_path.write_text(json.dumps(activity), encoding="utf-8")

            with patch("bot.core._shadow_settings", return_value=shadow_settings), patch(
                "bot.core._wallet_copy_candidate_pool", return_value=[candidate]
            ):
                decisions = trade_candidates_wallet_copy_variant(
                    settings,
                    cli=object(),
                    suffix="ab_wallet_copy_aggressive",
                    min_confidence=0.5,
                    require_microstructure_alignment=False,
                    fixed_position_usdc=250.0,
                    activity_path=activity_path,
                    allow_multi_tranche=True,
                    max_tranches_per_market=4,
                    max_theme_positions=10,
                    max_theme_notional_usdc=2500.0,
                    max_new_positions_per_theme_per_cycle=2,
                )

        self.assertEqual(decisions[0]["action"], "SKIP")
        self.assertIn("theme position cap reached", decisions[0]["reason"])

    def test_extract_market_category_maps_governor_races_to_politics(self) -> None:
        market = {
            "question": "Will the Democrats win the Minnesota governor race in 2026?",
            "slug": "democrats-minnesota-governor-race-2026",
        }
        self.assertEqual(_extract_market_category(market), "politics")

    def test_extract_market_category_does_not_false_match_eth_substrings(self) -> None:
        self.assertEqual(_extract_market_category({"question": "Will Netherlands win the 2026 FIFA World Cup?", "slug": "will-netherlands-win-the-2026-fifa-world-cup"}), "sports")
        self.assertEqual(_extract_market_category({"question": "Will Pete Hegseth win the 2028 US Presidential Election?", "slug": "will-pete-hegseth-win-the-2028-us-presidential-election"}), "politics")

    def test_kelly_size_can_be_capped_by_remaining_bankroll(self) -> None:
        size = kelly_size(0.8, 0.4, 50, 0.25)
        self.assertLessEqual(size, 50)

    def test_refresh_target_activity_tolerates_wallet_fetch_errors(self) -> None:
        class FakeCLI:
            def wallet_trades(self, wallet: str, limit: int) -> list[dict]:
                if wallet == "bad-wallet":
                    raise RuntimeError("boom")
                return [{"market_id": "1", "token_id": "tok1", "side": "BUY", "size": "10", "price": "0.4"}]

        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings.targets_path = root / "targets.json"
            settings.target_activity_path = root / "activity.json"
            settings.targets_path.write_text(
                '[{"wallet":"good-wallet"},{"wallet":"bad-wallet"}]',
                encoding="utf-8",
            )
            payload = refresh_target_activity(settings, FakeCLI(), limit=5)

        self.assertEqual(len(payload["wallets"]), 2)
        self.assertEqual(len(payload["errors"]), 1)
        self.assertEqual(payload["errors"][0]["wallet"], "bad-wallet")
        good_wallet = next(item for item in payload["wallets"] if item["wallet"] == "good-wallet")
        bad_wallet = next(item for item in payload["wallets"] if item["wallet"] == "bad-wallet")
        self.assertEqual(len(good_wallet["trades"]), 1)
        self.assertEqual(bad_wallet["trades"], [])
        self.assertEqual(bad_wallet["error"], "boom")

    def test_refresh_target_activity_for_paths_uses_override_files(self) -> None:
        class FakeCLI:
            def wallet_trades(self, wallet: str, limit: int) -> list[dict]:
                return [{"market_id": wallet, "token_id": f"{wallet}-tok", "side": "BUY", "size": "5", "price": "0.2"}]

        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings.targets_path = root / "default_targets.json"
            settings.target_activity_path = root / "default_activity.json"
            settings.targets_path.write_text('[{"wallet":"default-wallet"}]', encoding="utf-8")
            override_targets = root / "override_targets.json"
            override_activity = root / "override_activity.json"
            override_targets.write_text('[{"wallet":"override-wallet"}]', encoding="utf-8")

            payload = refresh_target_activity_for_paths(
                settings,
                FakeCLI(),
                targets_path=override_targets,
                activity_path=override_activity,
                limit=10,
            )

        self.assertEqual(payload["wallets"][0]["wallet"], "override-wallet")
        self.assertEqual(payload["wallets"][0]["trades"][0]["market_id"], "override-wallet")
        self.assertFalse(settings.target_activity_path.exists())

    def test_refresh_target_activity_preserves_cached_wallet_trades_on_error(self) -> None:
        settings = Settings.from_env()
        settings.target_activity_refresh_interval_seconds = 0
        settings.target_activity_wallets_per_refresh = 10
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings.targets_path = root / "targets.json"
            settings.target_activity_path = root / "activity.json"
            settings.targets_path.write_text('[{"wallet":"cached-wallet"}]', encoding="utf-8")
            settings.target_activity_path.write_text(
                json.dumps(
                    {
                        "updated_at": "2026-04-27T00:00:00Z",
                        "wallets": [
                            {
                                "wallet": "cached-wallet",
                                "trades": [
                                    {
                                        "wallet": "cached-wallet",
                                        "market_id": "1",
                                        "token_id": "tok1",
                                        "slug": "market-a",
                                        "question": "Market A?",
                                        "side": "BUY",
                                        "size": 10,
                                    }
                                ],
                            }
                        ],
                        "by_match_key": {"1": [{"market_id": "1", "token_id": "tok1", "side": "BUY", "size": 10}]},
                        "errors": [],
                    }
                ),
                encoding="utf-8",
            )

            class FakeCLI:
                def wallet_trades(self, wallet: str, limit: int) -> list[dict]:
                    raise RuntimeError("HTTP Error 503: Service Unavailable")

            payload = refresh_target_activity(settings, FakeCLI(), limit=5)

        cached_wallet = payload["wallets"][0]
        self.assertTrue(cached_wallet["preserved_from_cache"])
        self.assertEqual(len(cached_wallet["trades"]), 1)
        self.assertIn("1", payload["by_match_key"])

    def test_refresh_target_activity_refreshes_wallet_subset_each_cycle(self) -> None:
        settings = Settings.from_env()
        settings.target_activity_refresh_interval_seconds = 0
        settings.target_activity_wallets_per_refresh = 2
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings.targets_path = root / "targets.json"
            settings.target_activity_path = root / "activity.json"
            settings.targets_path.write_text(
                json.dumps([{"wallet": "w1"}, {"wallet": "w2"}, {"wallet": "w3"}]),
                encoding="utf-8",
            )

            seen: list[str] = []

            class FakeCLI:
                def wallet_trades(self, wallet: str, limit: int) -> list[dict]:
                    seen.append(wallet)
                    return []

            first = refresh_target_activity(settings, FakeCLI(), limit=5)
            second = refresh_target_activity(settings, FakeCLI(), limit=5)

        self.assertEqual(seen[:2], ["w1", "w2"])
        self.assertEqual(seen[2:4], ["w3", "w1"])
        self.assertEqual(first["refresh_cursor"], 2)
        self.assertEqual(second["refresh_cursor"], 1)

    def test_transient_cli_error_detection(self) -> None:
        self.assertTrue(_is_transient_cli_error("HTTP Error 503: Service Unavailable"))
        self.assertTrue(_is_transient_cli_error('{"error":"Internal: error sending request for url (...)" }'))
        self.assertFalse(_is_transient_cli_error("invalid api key"))

    def test_cli_retries_transient_failure(self) -> None:
        cli = PolymarketCLI("fake-polymarket")

        class Proc:
            def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
                self.returncode = returncode
                self.stdout = stdout
                self.stderr = stderr

        with patch("bot.core.subprocess.run", side_effect=[Proc(1, stderr="HTTP Error 503: Service Unavailable"), Proc(0, stdout="[]")]) as run_mock:
            with patch("bot.core.time.sleep") as sleep_mock:
                payload = cli._run_json(["markets", "list"])

        self.assertEqual(payload, [])
        self.assertEqual(run_mock.call_count, 2)
        sleep_mock.assert_called_once()

    def test_current_cycle_result_uses_preserved_files(self) -> None:
        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings.queue_path = root / "queue.json"
            settings.theses_path = root / "theses.json"
            settings.marks_path = root / "marks.json"
            settings.trades_path = root / "trades.json"
            settings.missed_opportunities_path = root / "missed.json"
            settings.queue_path.write_text('[{"market_id":"1"},{"market_id":"2"}]', encoding="utf-8")
            settings.theses_path.write_text('[{"market_id":"1"}]', encoding="utf-8")
            settings.marks_path.write_text(
                '{"summary":{"open_positions":3,"total_unrealized_pnl_usdc":1.25}}',
                encoding="utf-8",
            )
            settings.trades_path.write_text("[]", encoding="utf-8")
            settings.missed_opportunities_path.write_text('{"items":[{"market_id":"1","reason":"cooldown"}]}', encoding="utf-8")
            payload = _current_cycle_result_from_files(settings, error="HTTP Error 503: Service Unavailable")

        self.assertEqual(payload["queue_count"], 2)
        self.assertEqual(payload["thesis_count"], 1)
        self.assertEqual(payload["marked_positions_count"], 3)
        self.assertEqual(payload["unrealized_pnl_usdc"], 1.25)
        self.assertEqual(len(payload["missed_opportunities"]), 1)
        self.assertEqual(payload["error"], "HTTP Error 503: Service Unavailable")

    def test_market_cooldown_blocks_recent_market(self) -> None:
        settings = Settings.from_env()
        settings.market_cooldown_minutes = 180
        candidate = MarketCandidate(
            market_id="1",
            question="Will BTC rally?",
            slug="btc-rally",
            token_id="tok1",
            midpoint=0.4,
            best_bid=0.39,
            best_ask=0.41,
            bids_depth=1000,
            asks_depth=1000,
            spread=0.02,
            hours_to_resolution=12,
            total_volume=10000,
            book_imbalance=0.1,
        )
        blocked, reason = _within_market_cooldown(
            settings,
            candidate,
            {"market_id": "1", "opened_at": (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat().replace("+00:00", "Z")},
        )
        self.assertTrue(blocked)
        self.assertIn("cooldown active", reason)

    def test_lifetime_stats_dedupe_duplicate_closes(self) -> None:
        trades = [
            {"type": "OPEN", "position_id": "p1", "opened_at": "2026-04-22T10:00:00Z", "question": "A"},
            {"type": "CLOSE", "position_id": "p1", "closed_at": "2026-04-22T11:00:00Z", "pnl_usdc": 4.5, "question": "A"},
            {"type": "CLOSE", "position_id": "p1", "closed_at": "2026-04-22T11:01:00Z", "pnl_usdc": 4.5, "question": "A"},
            {"type": "OPEN", "position_id": "p2", "opened_at": "2026-04-22T12:00:00Z", "question": "B"},
            {"type": "CLOSE", "position_id": "p2", "closed_at": "2026-04-22T13:30:00Z", "pnl_usdc": -2.0, "question": "B"},
        ]
        closes = dedupe_closed_trades(trades)
        stats = _build_lifetime_stats(trades, bankroll_usdc=1000.0, unrealized_pnl_usdc=1.25)

        self.assertEqual(len(closes), 2)
        self.assertEqual(stats["closed_count"], 2)
        self.assertEqual(stats["realized_pnl_usdc"], 2.5)
        self.assertEqual(stats["gross_profit_usdc"], 4.5)
        self.assertEqual(stats["gross_loss_usdc"], 2.0)
        self.assertEqual(stats["win_rate_pct"], 50.0)
        self.assertEqual(stats["net_total_pnl_usdc"], 3.75)
        self.assertEqual(len(stats["equity_curve"]), 2)

    def test_sell_position_uses_no_side_price_for_shares_and_pnl(self) -> None:
        position = {
            "side": "SELL",
            "notional_usdc": 100.0,
            "entry_price": 0.2,
        }
        self.assertEqual(effective_position_shares(position), 125.0)

        trades = [
            {
                "type": "OPEN",
                "position_id": "p1",
                "market_id": "m1",
                "token_id": "t1",
                "question": "Sell side market",
                "side": "SELL",
                "notional_usdc": 100.0,
                "entry_price": 0.2,
                "opened_at": "2026-04-22T10:00:00Z",
                "category": "crypto",
            },
            {
                "type": "CLOSE",
                "position_id": "p1",
                "market_id": "m1",
                "token_id": "t1",
                "question": "Sell side market",
                "exit_price": 0.1,
                "closed_at": "2026-04-22T11:00:00Z",
            },
        ]
        closes = rebuild_closed_trades(trades)
        self.assertEqual(len(closes), 1)
        self.assertEqual(closes[0]["shares"], 125.0)
        self.assertEqual(closes[0]["pnl_usdc"], 12.5)

    def test_current_bankroll_compounds_deduped_realized_pnl(self) -> None:
        settings = Settings.from_env()
        settings.bankroll_usdc = 1000.0
        with TemporaryDirectory() as tmpdir:
            settings.trades_path = Path(tmpdir) / "trades.json"
            settings.trades_path.write_text(
                """
                [
                  {"type":"CLOSE","position_id":"p1","pnl_usdc":10.0},
                  {"type":"CLOSE","position_id":"p1","pnl_usdc":10.0},
                  {"type":"CLOSE","position_id":"p2","pnl_usdc":-3.5},
                  {"type":"OPEN","position_id":"p3","notional_usdc":50.0}
                ]
                """,
                encoding="utf-8",
            )

            self.assertEqual(_current_bankroll_usdc(settings), 1006.5)

    def test_build_theses_reuses_fresh_cache_and_hard_caps_work(self) -> None:
        settings = Settings.from_env()
        settings.openai_api_key = "test-key"
        settings.thesis_max_candidates_per_cycle = 1
        settings.thesis_reuse_ttl_seconds = 900
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings.queue_path = root / "queue.json"
            settings.target_activity_path = root / "target_activity.json"
            settings.theses_path = root / "theses.json"
            settings.thesis_cache_path = root / "thesis_cache.json"

            settings.queue_path.write_text(
                """
                [
                  {"market_id":"m1","token_id":"t1","slug":"s1","midpoint":0.4,"question":"A"},
                  {"market_id":"m2","token_id":"t2","slug":"s2","midpoint":0.5,"question":"B"}
                ]
                """,
                encoding="utf-8",
            )
            settings.target_activity_path.write_text('{"by_match_key":{}}', encoding="utf-8")
            settings.thesis_cache_path.write_text(
                """
                {
                  "updated_at":"2026-04-23T00:00:00Z",
                  "items":[
                    {
                      "market_id":"m1",
                      "token_id":"t1",
                      "estimated_probability":0.61,
                      "confidence":0.7,
                      "thesis":"cached",
                      "catalysts":[],
                      "crowd_error":"0.21",
                      "source":"openai",
                      "generated_at":"2999-04-23T00:00:00Z"
                    }
                  ]
                }
                """,
                encoding="utf-8",
            )

            with patch("bot.core._openai_request") as openai_mock:
                theses = build_theses(settings)

            self.assertEqual(len(theses), 1)
            self.assertEqual(theses[0]["market_id"], "m1")
            self.assertEqual(theses[0]["thesis"], "cached")
            openai_mock.assert_not_called()

    def test_refresh_target_activity_reuses_fresh_cache(self) -> None:
        settings = Settings.from_env()
        settings.target_activity_refresh_interval_seconds = 300
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings.target_activity_path = root / "activity.json"
            settings.targets_path = root / "targets.json"
            settings.target_activity_path.write_text(
                """
                {
                  "updated_at":"2999-04-23T00:00:00Z",
                  "wallets":[{"wallet":"cached-wallet","trades":[]}],
                  "by_match_key":{},
                  "errors":[]
                }
                """,
                encoding="utf-8",
            )
            settings.targets_path.write_text('[{"wallet":"cached-wallet"}]', encoding="utf-8")

            class FakeCLI:
                def wallet_trades(self, wallet: str, limit: int) -> list[dict]:
                    raise AssertionError("wallet_trades should not be called when cache is fresh")

            payload = refresh_target_activity(settings, FakeCLI(), limit=5)

        self.assertEqual(len(payload["wallets"]), 1)
        self.assertEqual(payload["wallets"][0]["wallet"], "cached-wallet")

    def test_derive_crypto_target_activity_builds_crypto_only_subset(self) -> None:
        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings = replace(
                settings,
                targets_path=root / "targets.json",
                target_activity_path=root / "target_activity.json",
                crypto_targets_path=root / "crypto_targets.json",
                crypto_activity_path=root / "crypto_activity.json",
            )
            settings.ensure_dirs()
            settings.targets_path.write_text(
                json.dumps(
                    [
                        {"wallet": "w1", "username": "crypto1", "total_pnl": 1000.0},
                        {"wallet": "w2", "username": "sports1", "total_pnl": 900.0},
                    ]
                ),
                encoding="utf-8",
            )
            settings.target_activity_path.write_text(
                json.dumps(
                    {
                        "wallets": [
                            {
                                "wallet": "w1",
                                "meta": {"wallet": "w1", "username": "crypto1", "total_pnl": 1000.0},
                                "trades": [
                                    {"wallet": "w1", "side": "BUY", "size": 100, "price": 0.5, "slug": "btc-above-120k-by-june", "question": "Will BTC be above $120k by June?"},
                                    {"wallet": "w1", "side": "BUY", "size": 50, "price": 0.4, "slug": "eth-above-10k", "question": "Will ETH be above $10k in 2026?"},
                                ],
                            },
                            {
                                "wallet": "w2",
                                "meta": {"wallet": "w2", "username": "sports1", "total_pnl": 900.0},
                                "trades": [
                                    {"wallet": "w2", "side": "BUY", "size": 100, "price": 0.5, "slug": "nba-lal-bos-2026-05-01", "question": "Lakers vs. Celtics"}
                                ],
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )

            payload = derive_crypto_target_activity(settings)
            crypto_targets = json.loads(settings.crypto_targets_path.read_text(encoding="utf-8"))
            crypto_activity = json.loads(settings.crypto_activity_path.read_text(encoding="utf-8"))

        self.assertEqual(payload["wallets_selected"], 1)
        self.assertEqual(crypto_targets[0]["wallet"], "w1")
        self.assertEqual(len(crypto_activity["wallets"]), 1)
        self.assertEqual(crypto_activity["wallets"][0]["wallet"], "w1")
        self.assertEqual(len(crypto_activity["wallets"][0]["trades"]), 2)

    def test_wallet_copy_ab_vote_respects_confidence_threshold(self) -> None:
        settings = Settings.from_env()
        settings.ab_test_min_whale_confidence = 0.7
        candidate = MarketCandidate(
            market_id="1",
            question="A",
            slug="a",
            token_id="tok1",
            midpoint=0.5,
            best_bid=0.49,
            best_ask=0.51,
            bids_depth=1000,
            asks_depth=1000,
            spread=0.02,
            hours_to_resolution=24,
            total_volume=10000,
            book_imbalance=0.0,
        )
        activity = {
            "by_match_key": {
                "1": [
                    {"market_id": "1", "token_id": "tok1", "side": "BUY", "size": 60},
                    {"market_id": "1", "token_id": "tok1", "side": "SELL", "size": 40},
                ]
            }
        }
        vote = wallet_copy_ab_vote(candidate, activity, settings)
        self.assertEqual(vote.action, "HOLD")

    def test_wallet_copy_variant_vote_allows_equal_split_at_half_threshold(self) -> None:
        candidate = MarketCandidate(
            market_id="1",
            question="A",
            slug="a",
            token_id="tok1",
            midpoint=0.5,
            best_bid=0.49,
            best_ask=0.51,
            bids_depth=1000,
            asks_depth=1000,
            spread=0.02,
            hours_to_resolution=24,
            total_volume=10000,
            book_imbalance=0.0,
        )
        activity = {
            "by_match_key": {
                "1": [
                    {"market_id": "1", "token_id": "tok1", "side": "BUY", "size": 51},
                    {"market_id": "1", "token_id": "tok1", "side": "SELL", "size": 49},
                ]
            }
        }
        vote = wallet_copy_variant_vote(candidate, activity, min_confidence=0.50, agent_name="wallet_copy_aggressive_ab")
        self.assertEqual(vote.action, "BUY")

    def test_find_target_trades_matches_normalized_question_fallback(self) -> None:
        candidate = MarketCandidate(
            market_id="1",
            question="Will MegaETH perform an airdrop by June 30?",
            slug="different-slug",
            token_id="different-token",
            midpoint=0.5,
            best_bid=0.49,
            best_ask=0.51,
            bids_depth=1000,
            asks_depth=1000,
            spread=0.02,
            hours_to_resolution=24,
            total_volume=10000,
            book_imbalance=0.0,
        )
        activity = {
            "by_match_key": {
                "q:will megaeth perform an airdrop by june 30": [
                    {"question": "Will MegaETH perform an airdrop by June 30?", "side": "BUY", "size": 12}
                ]
            }
        }

        matched = _find_target_trades(activity, candidate)
        vote = whale_copy_vote(candidate, activity)

        self.assertEqual(len(matched), 1)
        self.assertEqual(vote.action, "BUY")

    def test_find_target_trades_matches_crypto_series_family(self) -> None:
        candidate = MarketCandidate(
            market_id="1",
            question="Bitcoin Up or Down - May 1, 4PM ET",
            slug="bitcoin-up-or-down-may-1-2026-4pm-et",
            token_id="different-token",
            midpoint=0.5,
            best_bid=0.49,
            best_ask=0.51,
            bids_depth=1000,
            asks_depth=1000,
            spread=0.02,
            hours_to_resolution=1,
            total_volume=10000,
            book_imbalance=0.0,
            category="crypto",
            priority_score=5.0,
        )
        activity = {
            "by_match_key": _index_wallet_trades(
                [
                    {
                        "question": "Bitcoin Up or Down - May 1, 3PM ET",
                        "slug": "bitcoin-up-or-down-may-1-2026-3pm-et",
                        "side": "BUY",
                        "size": 12,
                    }
                ]
            )
        }

        matched = _find_target_trades(activity, candidate)

        self.assertEqual(len(matched), 1)

    def test_find_target_trades_matches_crypto_asset_theme_fallback(self) -> None:
        candidate = MarketCandidate(
            market_id="2",
            question="Will Bitcoin hit $150k by June 30, 2026?",
            slug="will-bitcoin-hit-150k-by-june-30-2026",
            token_id="tok-btc-150",
            midpoint=0.5,
            best_bid=0.49,
            best_ask=0.51,
            bids_depth=1000,
            asks_depth=1000,
            spread=0.02,
            hours_to_resolution=24,
            total_volume=10000,
            book_imbalance=0.0,
            category="crypto",
            priority_score=5.0,
        )
        activity = {
            "by_match_key": _index_wallet_trades(
                [
                    {
                        "question": "Bitcoin Up or Down - May 1, 3PM ET",
                        "slug": "bitcoin-up-or-down-may-1-2026-3pm-et",
                        "side": "BUY",
                        "size": 12,
                    }
                ]
            )
        }

        matched = _find_target_trades(activity, candidate)

        self.assertEqual(len(matched), 1)

    def test_short_crypto_market_detection(self) -> None:
        question = "Bitcoin Up or Down - April 14, 11:00AM-11:05AM ET"
        slug = "bitcoin-up-or-down-april-14-11am-et"
        self.assertEqual(_short_crypto_updown_asset(question, slug), "btc")
        self.assertTrue(_is_five_minute_updown_market(question, slug))

    def test_short_crypto_market_detection_supports_xrp(self) -> None:
        question = "XRP Up or Down - May 5, 11:25AM-11:30AM ET"
        slug = "xrp-up-or-down-may-5-1125am-1130am-et"
        self.assertEqual(_short_crypto_updown_asset(question, slug), "xrp")

    def test_short_crypto_market_detection_avoids_ethan_false_positive(self) -> None:
        question = "Ethan Mbappe: Anytime Goalscorer"
        slug = "ethan-mbappe-anytime-goalscorer"
        self.assertIsNone(_short_crypto_market_asset(question, slug))

    def test_short_crypto_hours_uses_intraday_window(self) -> None:
        now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
        start_local = now + timedelta(minutes=2)
        end_local = start_local + timedelta(minutes=5)
        question = (
            f"Bitcoin Up or Down - {start_local.strftime('%B')} {start_local.day}, "
            f"{start_local.strftime('%I:%M%p').lstrip('0')}-{end_local.strftime('%I:%M%p').lstrip('0')} ET"
        )
        hours = _extract_short_market_hours_to_resolution(
            {
                "question": question,
                "slug": "bitcoin-up-or-down-test",
                "active": True,
                "closed": False,
            }
        )
        self.assertGreater(hours, 0.0)
        self.assertLess(hours, 0.2)

    def test_short_crypto_scan_accepts_unknown_category_intraday_market(self) -> None:
        now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
        start_local = now + timedelta(minutes=2)
        end_local = start_local + timedelta(minutes=5)
        question = (
            f"Bitcoin Up or Down - {start_local.strftime('%B')} {start_local.day}, "
            f"{start_local.strftime('%I:%M%p').lstrip('0')}-{end_local.strftime('%I:%M%p').lstrip('0')} ET"
        )

        class FakeCLI:
            def midpoint(self, token_id: str) -> float:
                return 0.51

            def book(self, token_id: str) -> dict[str, list[dict[str, str]]]:
                return {
                    "bids": [{"price": "0.50", "size": "400"}],
                    "asks": [{"price": "0.52", "size": "400"}],
                }

        candidates = _scan_short_crypto_updown_candidates(
            FakeCLI(),
            markets_limit=500,
            max_minutes_to_resolution=30,
            min_book_depth_usdc=25,
            require_updown=False,
            market_fetcher=lambda limit, offset: [
                {
                    "id": "short-btc-1",
                    "question": question,
                    "slug": "bitcoin-up-or-down-test",
                    "active": True,
                    "closed": False,
                    "acceptingOrders": True,
                    "clobTokenIds": '["tok-short-1","tok-short-2"]',
                    "volume": "1200",
                }
            ]
            if offset == 0
            else [],
        )
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].category, "crypto")

    def test_short_crypto_scan_skips_midpoint_errors(self) -> None:
        now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
        start_local = now + timedelta(minutes=2)
        end_local = start_local + timedelta(minutes=5)
        question = (
            f"Bitcoin Up or Down - {start_local.strftime('%B')} {start_local.day}, "
            f"{start_local.strftime('%I:%M%p').lstrip('0')}-{end_local.strftime('%I:%M%p').lstrip('0')} ET"
        )

        class FakeCLI:
            def midpoint(self, token_id: str) -> float:
                raise RuntimeError("midpoint unavailable")

            def book(self, token_id: str) -> dict[str, list[dict[str, str]]]:
                return {
                    "bids": [{"price": "0.50", "size": "400"}],
                    "asks": [{"price": "0.52", "size": "400"}],
                }

        candidates = _scan_short_crypto_updown_candidates(
            FakeCLI(),
            markets_limit=500,
            max_minutes_to_resolution=30,
            min_book_depth_usdc=25,
            require_updown=False,
            market_fetcher=lambda limit, offset: [
                {
                    "id": "short-btc-err",
                    "question": question,
                    "slug": "bitcoin-up-or-down-test",
                    "active": True,
                    "closed": False,
                    "acceptingOrders": True,
                    "clobTokenIds": '["tok-short-1","tok-short-2"]',
                    "volume": "1200",
                }
            ]
            if offset == 0
            else [],
        )
        self.assertEqual(len(candidates), 1)
        self.assertAlmostEqual(candidates[0].midpoint, 0.51, places=2)

    def test_short_crypto_scan_respects_min_minutes_to_resolution(self) -> None:
        now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
        near_start = now + timedelta(minutes=5)
        near_end = near_start + timedelta(minutes=5)
        far_start = now + timedelta(hours=23, minutes=55)
        far_end = far_start + timedelta(minutes=5)
        near_question = (
            f"Bitcoin Up or Down - {near_start.strftime('%B')} {near_start.day}, "
            f"{near_start.strftime('%I:%M%p').lstrip('0')}-{near_end.strftime('%I:%M%p').lstrip('0')} ET"
        )
        far_question = (
            f"Bitcoin Up or Down - {far_start.strftime('%B')} {far_start.day}, "
            f"{far_start.strftime('%I:%M%p').lstrip('0')}-{far_end.strftime('%I:%M%p').lstrip('0')} ET"
        )

        class FakeCLI:
            def midpoint(self, token_id: str) -> float:
                return 0.51

            def book(self, token_id: str) -> dict[str, list[dict[str, str]]]:
                return {
                    "bids": [{"price": "0.50", "size": "400"}],
                    "asks": [{"price": "0.52", "size": "400"}],
                }

        candidates = _scan_short_crypto_updown_candidates(
            FakeCLI(),
            markets_limit=500,
            min_minutes_to_resolution=60,
            max_minutes_to_resolution=1800,
            min_book_depth_usdc=25,
            require_updown=True,
            market_fetcher=lambda limit, offset: [
                {
                    "id": "short-btc-near",
                    "question": near_question,
                    "slug": "btc-updown-near",
                    "active": True,
                    "closed": False,
                    "acceptingOrders": True,
                    "clobTokenIds": '["tok-near-1","tok-near-2"]',
                    "volume": "1200",
                },
                {
                    "id": "short-btc-far",
                    "question": far_question,
                    "slug": "btc-updown-far",
                    "active": True,
                    "closed": False,
                    "acceptingOrders": True,
                    "clobTokenIds": '["tok-far-1","tok-far-2"]',
                    "volume": "1200",
                },
            ]
            if offset == 0
            else [],
        )
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].market_id, "short-btc-far")

    def test_intraday_registry_record_preserves_short_crypto_market(self) -> None:
        now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
        start_local = now + timedelta(minutes=1)
        end_local = start_local + timedelta(minutes=5)
        question = (
            f"Bitcoin Up or Down - {start_local.strftime('%B')} {start_local.day}, "
            f"{start_local.strftime('%I:%M%p').lstrip('0')}-{end_local.strftime('%I:%M%p').lstrip('0')} ET"
        )
        record = _intraday_registry_market_record(
            {
                "id": "registry-btc-1",
                "question": question,
                "slug": "bitcoin-up-or-down-test",
                "active": True,
                "closed": False,
                "acceptingOrders": True,
                "clobTokenIds": '["tok-short-1","tok-short-2"]',
                "volume": "1200",
            },
            source="gamma",
        )
        self.assertIsNotNone(record)
        self.assertEqual(record["registry_asset"], "btc")
        self.assertTrue(record["registry_updown"])
        self.assertGreater(record["registry_hours_to_resolution"], 0.0)

    def test_intraday_registry_loader_uses_fresh_registry(self) -> None:
        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            registry_path = Path(tmpdir) / "intraday_registry.json"
            settings = replace(settings, intraday_registry_path=registry_path)
            now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
            start_local = now + timedelta(minutes=1)
            end_local = start_local + timedelta(minutes=5)
            question = (
                f"Bitcoin Up or Down - {start_local.strftime('%B')} {start_local.day}, "
                f"{start_local.strftime('%I:%M%p').lstrip('0')}-{end_local.strftime('%I:%M%p').lstrip('0')} ET"
            )
            registry_path.write_text(
                json.dumps(
                    {
                        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                        "markets": [
                            {
                                "id": "registry-btc-1",
                                "question": question,
                                "slug": "bitcoin-up-or-down-test",
                                "active": True,
                                "closed": False,
                                "acceptingOrders": True,
                                "clobTokenIds": ["tok-short-1", "tok-short-2"],
                                "volume": "1200",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            payload = json.loads(registry_path.read_text(encoding="utf-8"))
            self.assertTrue(_intraday_registry_is_fresh(settings, payload))
            markets = _load_intraday_registry_markets(settings, max_minutes_to_resolution=30, require_updown=False)
            self.assertEqual(len(markets), 1)
            self.assertEqual(markets[0]["id"], "registry-btc-1")

    def test_intraday_raw_updown_loader_uses_recent_entries(self) -> None:
        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            raw_path = Path(tmpdir) / "intraday_updown_raw.json"
            settings = replace(settings, intraday_registry_raw_updown_path=raw_path)
            now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
            start_local = now + timedelta(minutes=10)
            end_local = start_local + timedelta(minutes=5)
            question = (
                f"Bitcoin Up or Down - {start_local.strftime('%B')} {start_local.day}, "
                f"{start_local.strftime('%I:%M%p').lstrip('0')}-{end_local.strftime('%I:%M%p').lstrip('0')} ET"
            )
            raw_path.write_text(
                json.dumps(
                    {
                        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                        "entries": [
                            {
                                "market_id": "scheduled-btc-1",
                                "question": question,
                                "slug": "scheduled-bitcoin-updown",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with patch(
                "bot.core._resolve_intraday_market_tokens",
                return_value={
                    "id": "scheduled-btc-1",
                    "question": question,
                    "slug": "scheduled-bitcoin-updown",
                    "active": True,
                    "closed": False,
                    "acceptingOrders": True,
                    "clobTokenIds": ["tok-short-1", "tok-short-2"],
                    "volume": "1200",
                },
            ):
                markets = _load_intraday_raw_updown_markets(
                    settings,
                    max_minutes_to_resolution=1440,
                    require_updown=True,
                )
            self.assertEqual(len(markets), 1)
            self.assertEqual(markets[0]["id"], "scheduled-btc-1")

    def test_intraday_imminent_loader_filters_by_seen_age(self) -> None:
        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            imminent_path = Path(tmpdir) / "intraday_updown_imminent.json"
            settings = replace(
                settings,
                intraday_registry_imminent_updown_path=imminent_path,
                intraday_registry_imminent_seen_age_minutes=30,
            )
            now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
            start_local = now + timedelta(minutes=10)
            end_local = start_local + timedelta(minutes=5)
            question = (
                f"Bitcoin Up or Down - {start_local.strftime('%B')} {start_local.day}, "
                f"{start_local.strftime('%I:%M%p').lstrip('0')}-{end_local.strftime('%I:%M%p').lstrip('0')} ET"
            )
            imminent_path.write_text(
                json.dumps(
                    {
                        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                        "entries": [
                            {
                                "seen_at": (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
                                "market_id": "scheduled-btc-fresh",
                                "question": question,
                                "slug": "scheduled-bitcoin-updown-fresh",
                            },
                            {
                                "seen_at": (datetime.now(timezone.utc) - timedelta(minutes=90)).isoformat().replace("+00:00", "Z"),
                                "market_id": "scheduled-btc-stale",
                                "question": question,
                                "slug": "scheduled-bitcoin-updown-stale",
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with patch(
                "bot.core._resolve_intraday_market_tokens",
                side_effect=[
                    {
                        "id": "scheduled-btc-fresh",
                        "question": question,
                        "slug": "scheduled-bitcoin-updown-fresh",
                        "active": True,
                        "closed": False,
                        "acceptingOrders": True,
                        "clobTokenIds": ["tok-short-1", "tok-short-2"],
                        "volume": "1200",
                    }
                ],
            ):
                markets = _load_intraday_imminent_updown_markets(
                    settings,
                    max_minutes_to_resolution=45,
                    require_updown=True,
                )
            self.assertEqual(len(markets), 1)
            self.assertEqual(markets[0]["id"], "scheduled-btc-fresh")

    def test_intraday_threshold_live_loader_keeps_executable_threshold_markets(self) -> None:
        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            threshold_path = Path(tmpdir) / "intraday_threshold_live.json"
            settings = replace(
                settings,
                intraday_registry_threshold_live_path=threshold_path,
            )
            now_local = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
            start_local = now_local + timedelta(minutes=10)
            question = (
                f"Bitcoin above 79,800 on {start_local.strftime('%B')} {start_local.day}, "
                f"{start_local.strftime('%I:%M%p').lstrip('0')} ET?"
            )
            future_end = (datetime.now(timezone.utc) + timedelta(minutes=20)).isoformat().replace("+00:00", "Z")
            threshold_path.write_text(
                json.dumps(
                    {
                        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                        "entries": [
                            {
                                "seen_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                                "market_id": "btc-threshold-1",
                                "question": question,
                                "slug": "bitcoin-above-79800-threshold-test",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with patch(
                "bot.core._resolve_intraday_market_tokens",
                side_effect=[
                    {
                        "id": "btc-threshold-1",
                        "question": question,
                        "slug": "bitcoin-above-79800-threshold-test",
                        "active": True,
                        "closed": False,
                        "acceptingOrders": True,
                        "clobTokenIds": ["tok-yes", "tok-no"],
                        "volume": "1200",
                        "endDate": future_end,
                    }
                ],
            ):
                markets = _load_intraday_threshold_live_markets(
                    settings,
                    max_minutes_to_resolution=45,
                )
            self.assertEqual(len(markets), 1)
            self.assertEqual(markets[0]["id"], "btc-threshold-1")

    def test_intraday_book_tape_loader_keeps_fresh_cached_updown_markets(self) -> None:
        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            tape_path = Path(tmpdir) / "intraday_book_tape.json"
            settings = replace(
                settings,
                intraday_registry_book_tape_path=tape_path,
                intraday_registry_imminent_seen_age_minutes=30,
            )
            now_local = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
            end_local = now_local + timedelta(minutes=10)
            question = (
                f"Bitcoin Up or Down - {end_local.strftime('%B')} {end_local.day}, "
                f"{end_local.strftime('%I:%M%p').lstrip('0')}-{(end_local + timedelta(minutes=5)).strftime('%I:%M%p').lstrip('0')} ET"
            )
            tape_path.write_text(
                json.dumps(
                    {
                        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                        "entries": [
                            {
                                "seen_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                                "market_id": "updown-fresh",
                                "question": question,
                                "slug": "bitcoin-up-or-down-fresh",
                                "token_id": "tok-fresh",
                                "active": True,
                                "acceptingOrders": True,
                                "hours_to_resolution": 0.16,
                                "midpoint": 0.51,
                                "best_bid": 0.50,
                                "best_ask": 0.52,
                                "cached_bids": [{"price": "0.50", "size": "100"}],
                                "cached_asks": [{"price": "0.52", "size": "100"}],
                            },
                            {
                                "seen_at": (datetime.now(timezone.utc) - timedelta(minutes=45)).isoformat().replace("+00:00", "Z"),
                                "market_id": "updown-stale",
                                "question": question,
                                "slug": "bitcoin-up-or-down-stale",
                                "token_id": "tok-stale",
                                "active": True,
                                "acceptingOrders": True,
                                "hours_to_resolution": 0.16,
                                "midpoint": 0.51,
                                "best_bid": 0.50,
                                "best_ask": 0.52,
                                "cached_bids": [{"price": "0.50", "size": "100"}],
                                "cached_asks": [{"price": "0.52", "size": "100"}],
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            markets = _load_intraday_book_tape_markets(
                settings,
                max_minutes_to_resolution=45,
                require_updown=True,
            )
            self.assertEqual(len(markets), 1)
            self.assertEqual(markets[0]["market_id"], "updown-fresh")

    def test_intraday_registry_rejection_reason_reports_expired_market(self) -> None:
        settings = Settings.from_env()
        now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
        start_local = now - timedelta(minutes=10)
        end_local = start_local + timedelta(minutes=5)
        question = (
            f"XRP Up or Down - {start_local.strftime('%B')} {start_local.day}, "
            f"{start_local.strftime('%I:%M%p').lstrip('0')}-{end_local.strftime('%I:%M%p').lstrip('0')} ET"
        )
        reason, details = _intraday_registry_rejection_reason(
            settings,
            {
                "id": "xrp-old-1",
                "question": question,
                "slug": "xrp-up-or-down-old",
                "active": True,
                "closed": False,
                "acceptingOrders": True,
                "clobTokenIds": ["tok-short-1", "tok-short-2"],
            },
        )
        self.assertEqual(reason, "expired_or_zero_hours")
        self.assertEqual(details["asset_guess"], "xrp")

    def test_intraday_registry_rejection_reason_reports_missing_tokens(self) -> None:
        settings = Settings.from_env()
        now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
        start_local = now + timedelta(minutes=2)
        end_local = start_local + timedelta(minutes=5)
        question = (
            f"Bitcoin Up or Down - {start_local.strftime('%B')} {start_local.day}, "
            f"{start_local.strftime('%I:%M%p').lstrip('0')}-{end_local.strftime('%I:%M%p').lstrip('0')} ET"
        )
        reason, _details = _intraday_registry_rejection_reason(
            settings,
            {
                "id": "btc-missing-tokens",
                "question": question,
                "slug": "btc-up-or-down-missing-tokens",
                "active": True,
                "closed": False,
                "acceptingOrders": True,
            },
        )
        self.assertEqual(reason, "missing_token_ids")

    def test_intraday_registry_merge_market_resolves_missing_tokens(self) -> None:
        settings = Settings.from_env()
        now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
        start_local = now + timedelta(minutes=2)
        end_local = start_local + timedelta(minutes=5)
        question = (
            f"Bitcoin Up or Down - {start_local.strftime('%B')} {start_local.day}, "
            f"{start_local.strftime('%I:%M%p').lstrip('0')}-{end_local.strftime('%I:%M%p').lstrip('0')} ET"
        )
        market = {
            "id": "btc-live-1",
            "question": question,
            "slug": "btc-up-or-down-live",
            "active": True,
            "closed": False,
            "acceptingOrders": True,
        }
        resolved = {
            **market,
            "clobTokenIds": ["tok-short-1", "tok-short-2"],
        }
        records: dict[str, dict[str, object]] = {}
        with patch("bot.core._fetch_gamma_market_detail", return_value=resolved):
            result = _intraday_registry_merge_market(settings, records, market, source="ws:new_market")
        self.assertTrue(result["accepted"])
        self.assertTrue(result["was_enriched"])
        self.assertEqual(len(records), 1)
        self.assertEqual(next(iter(records.values()))["clobTokenIds"], ["tok-short-1", "tok-short-2"])

    def test_intraday_registry_merge_market_prefers_resolved_tradeability_fields(self) -> None:
        settings = Settings.from_env()
        now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
        start_local = now + timedelta(minutes=2)
        end_local = start_local + timedelta(minutes=5)
        question = (
            f"Bitcoin Up or Down - {start_local.strftime('%B')} {start_local.day}, "
            f"{start_local.strftime('%I:%M%p').lstrip('0')}-{end_local.strftime('%I:%M%p').lstrip('0')} ET"
        )
        market = {
            "id": "btc-live-2",
            "question": question,
            "slug": "btc-up-or-down-live-2",
            "active": False,
            "closed": False,
            "acceptingOrders": True,
        }
        resolved = {
            **market,
            "active": True,
            "clobTokenIds": ["tok-short-3", "tok-short-4"],
        }
        records: dict[str, dict[str, object]] = {}
        with patch("bot.core._fetch_gamma_market_detail", return_value=resolved):
            result = _intraday_registry_merge_market(settings, records, market, source="ws:new_market")
        self.assertTrue(result["accepted"])
        stored = next(iter(records.values()))
        self.assertTrue(stored["active"])

    def test_intraday_registry_merge_market_keeps_relevant_ws_markets_as_watch_only(self) -> None:
        settings = Settings.from_env()
        now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
        start_local = now + timedelta(hours=5, minutes=30)
        end_local = start_local + timedelta(minutes=5)
        question = (
            f"Bitcoin Up or Down - {start_local.strftime('%B')} {start_local.day}, "
            f"{start_local.strftime('%I:%M%p').lstrip('0')}-{end_local.strftime('%I:%M%p').lstrip('0')} ET"
        )
        market = {
            "id": "btc-watch-only-1",
            "question": question,
            "slug": "btc-up-or-down-watch-only",
            "active": False,
            "closed": False,
            "acceptingOrders": True,
            "clobTokenIds": ["tok-watch-1", "tok-watch-2"],
        }
        records: dict[str, dict[str, object]] = {}
        with patch("bot.core._fetch_gamma_market_detail", return_value=None):
            result = _intraday_registry_merge_market(settings, records, market, source="ws:new_market")
        self.assertTrue(result["accepted"])
        self.assertEqual(result["reason"], "inactive")
        stored = records["btc-watch-only-1"]
        self.assertTrue(stored["registry_watch_only"])

    def test_intraday_registry_persists_watch_only_market_for_realtime_cache(self) -> None:
        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            registry_path = Path(tmpdir) / "intraday_registry.json"
            settings = replace(settings, intraday_registry_path=registry_path)
            now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
            start_local = now + timedelta(hours=5, minutes=30)
            end_local = start_local + timedelta(minutes=5)
            market = {
                "id": "btc-watch-persist-1",
                "question": (
                    f"Bitcoin Up or Down - {start_local.strftime('%B')} {start_local.day}, "
                    f"{start_local.strftime('%I:%M%p').lstrip('0')}-{end_local.strftime('%I:%M%p').lstrip('0')} ET"
                ),
                "slug": "btc-up-or-down-watch-persist",
                "active": False,
                "closed": False,
                "acceptingOrders": True,
                "clobTokenIds": ["tok-persist-1", "tok-persist-2"],
                "registry_watch_only": True,
                "registry_rejection_reason": "inactive",
            }
            payload = _write_intraday_registry(settings, [market], metadata={"source": "test"})
            self.assertEqual(payload["market_count"], 1)
            records = _intraday_registry_records(settings)
            self.assertIn("btc-watch-persist-1", records)

    def test_intraday_registry_watchlist_loader_returns_watch_only_markets_in_window(self) -> None:
        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            registry_path = Path(tmpdir) / "intraday_registry.json"
            settings = replace(settings, intraday_registry_path=registry_path)
            now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
            start_local = now + timedelta(hours=5, minutes=30)
            end_local = start_local + timedelta(minutes=5)
            market = {
                "id": "btc-watch-load-1",
                "question": (
                    f"Bitcoin Up or Down - {start_local.strftime('%B')} {start_local.day}, "
                    f"{start_local.strftime('%I:%M%p').lstrip('0')}-{end_local.strftime('%I:%M%p').lstrip('0')} ET"
                ),
                "slug": "btc-up-or-down-watch-load",
                "active": True,
                "closed": False,
                "acceptingOrders": True,
                "clobTokenIds": ["tok-load-1", "tok-load-2"],
                "registry_watch_only": True,
                "registry_rejection_reason": "outside_max_minutes_window",
            }
            _write_intraday_registry(settings, [market], metadata={"source": "test"})
            loaded = _load_intraday_registry_watchlist_markets(
                settings,
                min_minutes_to_resolution=60,
                max_minutes_to_resolution=24 * 60,
                require_updown=True,
            )
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0]["id"], "btc-watch-load-1")

    def test_subscription_seed_records_from_markets_keeps_gamma_watchlist_market(self) -> None:
        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings = replace(
                settings,
                intraday_registry_raw_updown_path=root / "intraday_updown_raw.json",
                intraday_registry_rejections_path=root / "intraday_registry_rejections.json",
                intraday_registry_audit_sqlite_path=root / "intraday_audit.sqlite3",
            )
            now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
            start_local = now + timedelta(hours=5, minutes=30)
            end_local = start_local + timedelta(minutes=5)
            question = (
                f"Bitcoin Up or Down - {start_local.strftime('%B')} {start_local.day}, "
                f"{start_local.strftime('%I:%M%p').lstrip('0')}-{end_local.strftime('%I:%M%p').lstrip('0')} ET"
            )
            seeded = _subscription_seed_records_from_markets(
                settings,
                [
                    {
                        "id": "btc-gamma-seed-watch-1",
                        "question": question,
                        "slug": "btc-up-or-down-gamma-seed-watch",
                        "active": True,
                        "closed": False,
                        "acceptingOrders": True,
                        "clobTokenIds": ["tok-gamma-watch-1", "tok-gamma-watch-2"],
                    }
                ],
                source="gamma-seed",
            )
            self.assertIn("btc-gamma-seed-watch-1", seeded)
            self.assertTrue(seeded["btc-gamma-seed-watch-1"]["registry_watch_only"])
            self.assertEqual(seeded["btc-gamma-seed-watch-1"]["registry_rejection_reason"], "outside_max_minutes_window")

    def test_append_intraday_raw_updown_records_gamma_discovery_lifecycle(self) -> None:
        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings = replace(
                settings,
                intraday_registry_raw_updown_path=root / "intraday_updown_raw.json",
                intraday_registry_audit_sqlite_path=root / "intraday_audit.sqlite3",
            )
            now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
            start_local = now + timedelta(hours=2)
            end_local = start_local + timedelta(minutes=5)
            question = (
                f"Ethereum Up or Down - {start_local.strftime('%B')} {start_local.day}, "
                f"{start_local.strftime('%I:%M%p').lstrip('0')}-{end_local.strftime('%I:%M%p').lstrip('0')} ET"
            )
            market = {
                "id": "eth-gamma-discovery-1",
                "question": question,
                "slug": "eth-up-or-down-gamma-discovery",
                "active": True,
                "closed": False,
                "acceptingOrders": True,
                "clobTokenIds": ["tok-eth-discovery-1", "tok-eth-discovery-2"],
            }
            from bot.core import _append_intraday_registry_raw_updown

            _append_intraday_registry_raw_updown(settings, market, source="gamma", event_type="refresh")
            with closing(sqlite3.connect(settings.intraday_registry_audit_sqlite_path)) as conn, conn:
                row = conn.execute(
                    "SELECT first_seen_source FROM market_lifecycle WHERE market_id = ?",
                    ("eth-gamma-discovery-1",),
                ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], "gamma_discovery")

    def test_refresh_gamma_watchlist_records_adds_new_markets(self) -> None:
        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings = replace(
                settings,
                intraday_registry_gamma_refresh_limit=20,
                intraday_registry_raw_updown_path=root / "intraday_updown_raw.json",
                intraday_registry_rejections_path=root / "intraday_registry_rejections.json",
                intraday_registry_audit_sqlite_path=root / "intraday_audit.sqlite3",
            )
            now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
            start_local = now + timedelta(hours=5, minutes=20)
            end_local = start_local + timedelta(minutes=5)
            question = (
                f"Solana Up or Down - {start_local.strftime('%B')} {start_local.day}, "
                f"{start_local.strftime('%I:%M%p').lstrip('0')}-{end_local.strftime('%I:%M%p').lstrip('0')} ET"
            )
            market = {
                "id": "sol-gamma-refresh-1",
                "question": question,
                "slug": "sol-up-or-down-gamma-refresh",
                "active": True,
                "closed": False,
                "acceptingOrders": True,
                "clobTokenIds": ["tok-sol-refresh-1", "tok-sol-refresh-2"],
            }
            records: dict[str, dict[str, object]] = {}
            with patch(
                "bot.core._gamma_tag_updown_market_payload",
                return_value={
                    "markets": [market],
                    "tag_candidate_count": 2,
                    "tag_candidates": [{"id": "21", "slug": "crypto", "label": "crypto"}],
                    "fetched_count": 1,
                },
            ):
                stats = _refresh_gamma_watchlist_records(settings, records, source="gamma-refresh-daemon")
            self.assertEqual(stats["tag_candidate_count"], 2)
            self.assertEqual(stats["tag_live_upcoming_count"], 1)
            self.assertEqual(stats["raw_fetched_markets"], 1)
            self.assertEqual(stats["fetched_markets"], 1)
            self.assertEqual(stats["new_markets"], 1)
            self.assertEqual(stats["watch_only_markets"], 1)
            self.assertIn("sol-gamma-refresh-1", records)

    def test_extract_market_token_ids_supports_websocket_field_names(self) -> None:
        self.assertEqual(
            _extract_market_token_ids({"clob_token_ids": ["tok-a", "tok-b"]}),
            ["tok-a", "tok-b"],
        )
        self.assertEqual(
            _extract_market_token_ids({"assets_ids": ["tok-c", "tok-d"]}),
            ["tok-c", "tok-d"],
        )

    def test_market_from_intraday_ws_event_materializes_book_update_from_asset_lookup(self) -> None:
        base_market = {
            "id": "btc-live-book-1",
            "question": "Bitcoin above 100,000 on May 6, 4PM ET?",
            "slug": "btc-above-100000-on-may-6-4pm-et",
            "active": True,
            "closed": False,
            "acceptingOrders": True,
            "clobTokenIds": ["tok-book-1", "tok-book-2"],
        }
        materialized = _market_from_intraday_ws_event(
            {
                "market": "0xabc",
                "asset_id": "tok-book-1",
                "event_type": "book",
                "bids": [{"price": "0.48", "size": "100"}],
                "asks": [{"price": "0.51", "size": "120"}],
            },
            asset_lookup={"tok-book-1": base_market},
            market_lookup={},
        )
        self.assertEqual(len(materialized), 1)
        market, event_type = materialized[0]
        self.assertEqual(event_type, "book")
        self.assertEqual(market["id"], "btc-live-book-1")
        self.assertEqual(market["cached_bids"][0]["price"], "0.48")
        self.assertEqual(market["cached_asks"][0]["price"], "0.51")

    def test_parse_intraday_ws_payloads_splits_concatenated_json(self) -> None:
        payloads = _parse_intraday_ws_payloads('{"event_type":"best_bid_ask"}{"event_type":"price_change"}')
        self.assertEqual(len(payloads), 2)
        self.assertEqual(payloads[0]["event_type"], "best_bid_ask")
        self.assertEqual(payloads[1]["event_type"], "price_change")

    def test_primary_book_entry_guard_blocks_new_politics_entries(self) -> None:
        settings = Settings.from_env()
        candidate = MarketCandidate(
            market_id="pol-1",
            question="Will the Democrats win the Maine Senate race in 2026?",
            slug="maine-senate-2026",
            token_id="tok-pol-1",
            midpoint=0.61,
            best_bid=0.60,
            best_ask=0.62,
            bids_depth=1000.0,
            asks_depth=1000.0,
            spread=0.02,
            hours_to_resolution=24.0,
            total_volume=5000.0,
            book_imbalance=0.0,
            category="politics",
            priority_score=5.0,
            raw_market={},
        )
        blocked, reason = _primary_book_entry_guard(settings, candidate, None)
        self.assertTrue(blocked)
        self.assertEqual(reason, "primary book politics entries disabled")

    def test_short_crypto_sniper_vote_prefers_side_with_supported_imbalance(self) -> None:
        candidate = MarketCandidate(
            market_id="sniper-1",
            question="Bitcoin Up or Down - April 14, 11:00AM-11:05AM ET",
            slug="bitcoin-up-or-down-april-14-11am-et",
            token_id="tok-sniper-1",
            midpoint=0.41,
            best_bid=0.40,
            best_ask=0.42,
            bids_depth=1800.0,
            asks_depth=700.0,
            spread=0.02,
            hours_to_resolution=0.08,
            total_volume=15000.0,
            book_imbalance=0.44,
            category="crypto",
            priority_score=9.0,
        )
        vote = _short_crypto_sniper_vote(candidate, min_book_imbalance=0.22, max_side_price=0.62)
        self.assertEqual(vote.action, "BUY")

    def test_short_crypto_sniper_vote_uses_custom_strategy_name(self) -> None:
        candidate = MarketCandidate(
            market_id="sniper-next-1",
            question="Bitcoin Up or Down - May 6, 11:30AM-11:35AM ET",
            slug="btc-updown-next-window",
            token_id="tok-sniper-next-1",
            midpoint=0.41,
            best_bid=0.40,
            best_ask=0.42,
            bids_depth=1800.0,
            asks_depth=700.0,
            spread=0.02,
            hours_to_resolution=24.0,
            total_volume=15000.0,
            book_imbalance=0.44,
            category="crypto",
            priority_score=9.0,
        )
        vote = _short_crypto_sniper_vote(
            candidate,
            strategy_name="crypto_next_window_sniper",
            min_book_imbalance=0.18,
            max_side_price=0.68,
        )
        self.assertEqual(vote.to_dict()["agent"], "crypto_next_window_sniper")
        self.assertEqual(vote.action, "BUY")

    def test_scheduled_intraday_vote_supports_broader_window_entry(self) -> None:
        candidate = MarketCandidate(
            market_id="sched-1",
            question="Bitcoin Up or Down - April 14, 1:00PM-1:15PM ET",
            slug="bitcoin-up-or-down-april-14-1pm-et",
            token_id="tok-sched-1",
            midpoint=0.44,
            best_bid=0.43,
            best_ask=0.45,
            bids_depth=1400.0,
            asks_depth=800.0,
            spread=0.02,
            hours_to_resolution=4.0,
            total_volume=9000.0,
            book_imbalance=0.27,
            category="crypto",
            priority_score=2.0,
            raw_market={},
        )
        vote = _scheduled_intraday_crypto_vote(
            candidate,
            min_book_imbalance=0.12,
            min_midpoint_edge=0.03,
            max_side_price=0.72,
            max_hours_to_resolution=30.0,
        )
        self.assertEqual(vote.action, "BUY")

    def test_scheduled_intraday_vote_rejects_broad_daily_market(self) -> None:
        candidate = MarketCandidate(
            market_id="sched-2",
            question="XRP Up or Down on May 6?",
            slug="xrp-up-or-down-may-6",
            token_id="tok-sched-2",
            midpoint=0.41,
            best_bid=0.40,
            best_ask=0.42,
            bids_depth=1800.0,
            asks_depth=900.0,
            spread=0.02,
            hours_to_resolution=5.0,
            total_volume=9000.0,
            book_imbalance=0.32,
            category="crypto",
            priority_score=2.0,
            raw_market={},
        )
        vote = _scheduled_intraday_crypto_vote(
            candidate,
            min_book_imbalance=0.22,
            min_midpoint_edge=0.03,
            max_side_price=0.60,
            max_hours_to_resolution=30.0,
        )
        self.assertEqual(vote.action, "HOLD")
        self.assertIn("intraday window", vote.rationale)

    def test_scheduled_intraday_vote_holds_when_midpoint_near_fair_value(self) -> None:
        candidate = MarketCandidate(
            market_id="sched-3",
            question="Bitcoin Up or Down - May 6, 11:30AM-11:35AM ET",
            slug="bitcoin-up-or-down-may-6-1130am-1135am-et",
            token_id="tok-sched-3",
            midpoint=0.505,
            best_bid=0.495,
            best_ask=0.515,
            bids_depth=1800.0,
            asks_depth=900.0,
            spread=0.02,
            hours_to_resolution=20.0,
            total_volume=9000.0,
            book_imbalance=-0.30,
            category="crypto",
            priority_score=2.0,
            raw_market={},
        )
        vote = _scheduled_intraday_crypto_vote(
            candidate,
            min_book_imbalance=0.18,
            min_midpoint_edge=0.03,
            max_side_price=0.68,
            max_hours_to_resolution=30.0,
        )
        self.assertEqual(vote.action, "HOLD")
        self.assertIn("fair value", vote.rationale)

    def test_threshold_snapshot_vote_prefers_threshold_side_with_supported_imbalance(self) -> None:
        candidate = MarketCandidate(
            market_id="threshold-1",
            question="Bitcoin above 79,800 on May 5, 4PM ET?",
            slug="bitcoin-above-79800-on-may-5-4pm-et",
            token_id="tok-threshold-1",
            midpoint=0.43,
            best_bid=0.42,
            best_ask=0.44,
            bids_depth=1500.0,
            asks_depth=700.0,
            spread=0.02,
            hours_to_resolution=0.5,
            total_volume=12000.0,
            book_imbalance=0.22,
            category="crypto",
            priority_score=7.0,
            raw_market={},
        )
        vote = _threshold_snapshot_crypto_vote(
            candidate,
            min_book_imbalance=0.08,
            min_midpoint_edge=0.02,
            max_spread=0.14,
            max_side_price=0.85,
        )
        self.assertEqual(vote.action, "BUY")

    def test_threshold_snapshot_spread_size_multiplier_scales_wide_spreads(self) -> None:
        settings = Settings.from_env()
        settings = replace(
            settings,
            ab_test_crypto_threshold_snapshot_soft_max_spread=0.08,
            ab_test_crypto_threshold_snapshot_hard_max_spread=0.14,
            ab_test_crypto_threshold_snapshot_min_spread_size_multiplier=0.35,
        )
        self.assertEqual(_threshold_snapshot_spread_size_multiplier(settings, 0.05), 1.0)
        self.assertAlmostEqual(_threshold_snapshot_spread_size_multiplier(settings, 0.11), 0.675, places=3)
        self.assertEqual(_threshold_snapshot_spread_size_multiplier(settings, 0.14), 0.0)

    def test_scheduled_intraday_exit_deadline_uses_window_end(self) -> None:
        position = {
            "question": "Bitcoin Up or Down - May 5, 2026, 12:30PM-12:45PM ET",
            "opened_at": "2026-05-05T15:00:00Z",
        }
        deadline = _scheduled_intraday_exit_deadline(position, grace_minutes=5.0)
        self.assertIsNotNone(deadline)
        self.assertEqual(deadline, datetime(2026, 5, 5, 16, 50, tzinfo=timezone.utc))

    def test_scheduled_intraday_exit_deadline_missing_for_broad_market(self) -> None:
        position = {
            "question": "XRP Up or Down on May 6?",
            "opened_at": "2026-05-05T15:00:00Z",
        }
        self.assertIsNone(_scheduled_intraday_exit_deadline(position, grace_minutes=5.0))

    def test_aggressive_market_guard_rejects_weak_family(self) -> None:
        candidate = MarketCandidate(
            market_id="aggr-1",
            question="Will the Toronto Raptors win the NBA Eastern Conference Finals?",
            slug="raptors-east-finals",
            token_id="tok-aggr-1",
            midpoint=0.22,
            best_bid=0.21,
            best_ask=0.23,
            bids_depth=1000.0,
            asks_depth=1000.0,
            spread=0.02,
            hours_to_resolution=120.0,
            total_volume=5000.0,
            book_imbalance=0.1,
            category="sports",
            priority_score=4.0,
            raw_market={},
        )
        blocked, reason = _aggressive_market_guard(candidate)
        self.assertTrue(blocked)
        self.assertIn("weak market family", reason)

    def test_simulate_box_pair_open_requires_positive_edge(self) -> None:
        execution = _simulate_box_pair_open(
            yes_levels=[(0.46, 1000.0)],
            no_levels=[(0.48, 1000.0)],
            budget_usdc=300.0,
        )
        self.assertGreater(execution["filled_shares"], 0.0)
        self.assertAlmostEqual(execution["combined_contract_price"], 0.94, places=2)
        self.assertGreater(execution["edge_per_share"], 0.05)

    def test_box_arb_edge_metrics_accounts_for_costs(self) -> None:
        metrics = _box_arb_edge_metrics(
            {"edge_per_share": 0.0065},
            estimated_fee_per_share=0.001,
            estimated_slippage_per_share=0.002,
        )
        self.assertAlmostEqual(metrics["gross_edge_per_share"], 0.0065, places=6)
        self.assertAlmostEqual(metrics["net_edge_per_share"], 0.0035, places=6)

    def test_simulate_box_pair_open_maker_proxy_can_improve_over_taker(self) -> None:
        book = {
            "bids": [{"price": 0.50, "size": 1000}],
            "asks": [{"price": 0.51, "size": 1000}],
        }
        maker = _simulate_box_pair_open_maker_proxy(
            book,
            300.0,
            estimated_fee_per_share=0.0,
            estimated_slippage_per_share=0.0,
        )
        self.assertGreater(maker["maker_filled_shares"], 0.0)
        self.assertAlmostEqual(maker["maker_combined_contract_price"], 1.0, places=2)
        self.assertAlmostEqual(maker["maker_gross_edge_per_share"], 0.0, places=6)

    def test_simulate_box_pair_open_aggressive_proxy_models_two_passive_legs(self) -> None:
        aggressive = _simulate_box_pair_open_aggressive_proxy(
            300.0,
            best_bid=0.50,
            best_ask=0.51,
            maker_fee_per_share=0.0,
            estimated_slippage_per_share=0.0,
            queue_fill_probability=0.5,
            tick_size=0.01,
            price_concession_ticks=0,
        )
        self.assertGreater(aggressive["aggressive_filled_shares"], 0.0)
        self.assertAlmostEqual(aggressive["aggressive_combined_contract_price"], 0.99, places=6)
        self.assertAlmostEqual(aggressive["aggressive_gross_edge_per_share"], 0.01, places=6)
        self.assertAlmostEqual(aggressive["aggressive_expected_net_edge_per_share"], 0.005, places=6)

    def test_wallet_copy_candidate_pool_sources_markets_from_target_activity(self) -> None:
        settings = Settings.from_env()
        settings.markets_limit = 10
        settings.queue_max_candidates = 5
        settings.min_book_depth_usdc = 100
        activity = {
            "by_match_key": {
                "nba-lal-hou-2026-04-26": [
                    {
                        "wallet": "w1",
                        "slug": "nba-lal-hou-2026-04-26",
                        "question": "Lakers vs. Rockets",
                        "side": "BUY",
                        "size": 1000,
                        "price": 0.59,
                        "timestamp": "1777251862",
                    }
                ]
            }
        }

        class FakeCLI:
            def list_markets(self, limit: int) -> list[dict]:
                return [
                    {
                        "id": "101",
                        "question": "Lakers vs. Rockets",
                        "slug": "nba-lal-hou-2026-04-26",
                        "active": True,
                        "closed": False,
                        "acceptingOrders": True,
                        "gameStartTime": (datetime.now(timezone.utc) + timedelta(hours=6)).isoformat().replace("+00:00", "Z"),
                        "clobTokenIds": '["tok1","tok2"]',
                        "volume": "50000",
                    }
                ]

            def midpoint(self, token_id: str) -> float:
                return 0.59

            def book(self, token_id: str) -> dict[str, list[dict[str, str]]]:
                return {
                    "bids": [{"price": "0.58", "size": "1000"}],
                    "asks": [{"price": "0.60", "size": "1000"}],
                }

        candidates = _wallet_copy_candidate_pool(settings, FakeCLI(), activity)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].slug, "nba-lal-hou-2026-04-26")

    def test_wallet_copy_candidate_pool_respects_override_limits(self) -> None:
        settings = Settings.from_env()
        settings.markets_limit = 10
        settings.queue_max_candidates = 5
        settings.min_book_depth_usdc = 100
        activity = {
            "by_match_key": {
                "market-a": [{"wallet": "w1", "slug": "market-a", "side": "BUY", "size": 1000, "price": 0.60, "timestamp": "1777251862"}],
                "market-b": [{"wallet": "w2", "slug": "market-b", "side": "BUY", "size": 900, "price": 0.55, "timestamp": "1777251863"}],
            }
        }

        class FakeCLI:
            def list_markets(self, limit: int) -> list[dict]:
                return [
                    {
                        "id": "101",
                        "question": "Market A",
                        "slug": "market-a",
                        "active": True,
                        "closed": False,
                        "acceptingOrders": True,
                        "gameStartTime": (datetime.now(timezone.utc) + timedelta(hours=6)).isoformat().replace("+00:00", "Z"),
                        "clobTokenIds": '["tok1","tok2"]',
                        "volume": "50000",
                    },
                    {
                        "id": "102",
                        "question": "Market B",
                        "slug": "market-b",
                        "active": True,
                        "closed": False,
                        "acceptingOrders": True,
                        "gameStartTime": (datetime.now(timezone.utc) + timedelta(hours=6)).isoformat().replace("+00:00", "Z"),
                        "clobTokenIds": '["tok3","tok4"]',
                        "volume": "40000",
                    },
                ]

            def midpoint(self, token_id: str) -> float:
                return 0.60 if token_id == "tok1" else 0.55

            def book(self, token_id: str) -> dict[str, list[dict[str, str]]]:
                return {
                    "bids": [{"price": "0.54", "size": "1000"}],
                    "asks": [{"price": "0.56", "size": "1000"}],
                }

        candidates = _wallet_copy_candidate_pool(
            settings,
            FakeCLI(),
            activity,
            queue_limit=1,
            prebook_candidate_limit=1,
        )

        self.assertEqual(len(candidates), 1)

    def test_paper_executor_open_position_uses_executable_ask_price(self) -> None:
        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings.positions_path = root / "positions.json"
            settings.trades_path = root / "trades.json"
            executor = PaperExecutor(settings)
            candidate = MarketCandidate(
                market_id="m1",
                question="Executable open",
                slug="executable-open",
                token_id="tok1",
                midpoint=0.50,
                best_bid=0.45,
                best_ask=0.55,
                bids_depth=500,
                asks_depth=550,
                spread=0.10,
                hours_to_resolution=24,
                total_volume=10000,
                book_imbalance=0.0,
            )
            book = {
                "asks": [{"price": "0.55", "size": "200"}],
                "bids": [{"price": "0.45", "size": "200"}],
            }

            trade = executor.open_position(candidate, "BUY", 110.0, 0.8, 0.1, book=book)

        self.assertIsNotNone(trade)
        self.assertEqual(trade["entry_contract_price"], 0.55)
        self.assertEqual(trade["shares"], 200.0)
        self.assertEqual(trade["notional_usdc"], 110.0)

    def test_paper_executor_close_position_respects_available_bid_depth(self) -> None:
        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings.positions_path = root / "positions.json"
            settings.trades_path = root / "trades.json"
            executor = PaperExecutor(settings)
            position = {
                "position_id": "p1",
                "market_id": "m1",
                "token_id": "tok1",
                "question": "Partial close",
                "side": "BUY",
                "shares": 200.0,
                "notional_usdc": 100.0,
                "entry_price": 0.50,
                "entry_contract_price": 0.50,
                "expected_gap": 0.1,
                "opened_at": "2026-04-27T00:00:00Z",
                "thesis_confidence": 0.8,
                "mode": "paper",
                "category": "test",
            }
            settings.positions_path.write_text(json.dumps([position]), encoding="utf-8")
            settings.trades_path.write_text("[]", encoding="utf-8")
            book = {
                "bids": [{"price": "0.45", "size": "50"}],
                "asks": [{"price": "0.55", "size": "50"}],
            }

            trade = executor.close_position(position, 0.50, "TARGET_HIT", book=book)
            remaining = json.loads(settings.positions_path.read_text(encoding="utf-8"))

        self.assertIsNotNone(trade)
        self.assertFalse(trade["fully_closed"])
        self.assertEqual(trade["shares"], 50.0)
        self.assertEqual(trade["proceeds_usdc"], 22.5)
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["shares"], 150.0)
        self.assertEqual(remaining[0]["notional_usdc"], 75.0)

    def test_select_rotation_position_prefers_old_low_confidence_position(self) -> None:
        settings = Settings.from_env()
        settings.rotation_enabled = True
        settings.rotation_min_holding_minutes = 30
        settings.rotation_min_priority_score_delta = 0.0
        candidate = MarketCandidate(
            market_id="candidate",
            question="New candidate",
            slug="new-candidate",
            token_id="tok-new",
            midpoint=0.5,
            best_bid=0.49,
            best_ask=0.51,
            bids_depth=1000,
            asks_depth=1000,
            spread=0.02,
            hours_to_resolution=24,
            total_volume=10000,
            book_imbalance=0.0,
            priority_score=50.0,
        )
        positions = [
            {
                "position_id": "p1",
                "market_id": "old-low",
                "token_id": "tok1",
                "opened_at": (datetime.now(timezone.utc) - timedelta(minutes=90)).isoformat().replace("+00:00", "Z"),
                "thesis_confidence": 0.2,
                "expected_gap": 0.01,
            },
            {
                "position_id": "p2",
                "market_id": "old-strong",
                "token_id": "tok2",
                "opened_at": (datetime.now(timezone.utc) - timedelta(minutes=90)).isoformat().replace("+00:00", "Z"),
                "thesis_confidence": 0.9,
                "expected_gap": 0.2,
            },
        ]

        chosen = _select_rotation_position(settings, candidate, candidate_score=18.0, positions=positions)

        self.assertIsNotNone(chosen)
        self.assertEqual(chosen["position_id"], "p1")

    def test_remaining_bankroll_respects_portfolio_fraction_above_one(self) -> None:
        settings = Settings.from_env()
        settings.bankroll_usdc = 1000.0
        settings.max_portfolio_fraction = 1.5
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings.positions_path = root / "positions.json"
            settings.trades_path = root / "trades.json"
            settings.marks_path = root / "marks.json"
            settings.positions_path.write_text('[{"position_id":"p1","notional_usdc":1200.0}]', encoding="utf-8")
            settings.trades_path.write_text("[]", encoding="utf-8")
            settings.marks_path.write_text('{"summary":{"total_unrealized_pnl_usdc":0.0}}', encoding="utf-8")

            class FakeExecutor:
                def open_notional_usdc(self) -> float:
                    return 1200.0

            self.assertEqual(_remaining_bankroll_usdc(settings, FakeExecutor()), 300.0)

    def test_risk_bankroll_uses_lower_equity_on_drawdown(self) -> None:
        settings = Settings.from_env()
        settings.bankroll_usdc = 1000.0
        settings.use_equity_for_risk_caps = True
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings.trades_path = root / "trades.json"
            settings.marks_path = root / "marks.json"
            settings.trades_path.write_text(
                '[{"type":"CLOSE","pnl_usdc":200.0,"position_id":"p1","closed_at":"2026-05-01T00:00:00Z"}]',
                encoding="utf-8",
            )
            settings.marks_path.write_text('{"summary":{"total_unrealized_pnl_usdc":-500.0}}', encoding="utf-8")
            self.assertEqual(_current_bankroll_usdc(settings), 1200.0)
            self.assertEqual(_current_equity_usdc(settings), 700.0)
            self.assertEqual(_risk_bankroll_usdc(settings), 700.0)

    def test_forced_derisk_reason_triggers_for_primary_book_drawdown(self) -> None:
        settings = Settings.from_env()
        settings.forced_derisk_enabled = True
        settings.forced_derisk_min_holding_minutes = 60
        settings.forced_derisk_max_drawdown_fraction = 0.20
        settings.forced_derisk_max_contract_loss = 0.15
        settings.positions_path = Path("state/positions.json")
        settings.trades_path = Path("state/trades.json")
        reason = _forced_derisk_reason(
            settings,
            {
                "side": "SELL",
                "entry_price": 0.20,
                "entry_contract_price": 0.80,
                "notional_usdc": 1000.0,
            },
            current_contract_price=0.55,
            unrealized_pnl=-300.0,
            age_hours=8.0,
        )
        self.assertEqual(reason, "FORCED_DERISK_CONTRACT (0.250 >= 0.150)")

    def test_trade_candidates_rotates_out_weak_position_when_fully_allocated(self) -> None:
        settings = Settings.from_env()
        settings.bankroll_usdc = 1000.0
        settings.max_portfolio_fraction = 1.0
        settings.max_position_fraction = 0.5
        settings.max_kelly_fraction = 0.5
        settings.min_thesis_confidence = 0.35
        settings.consensus_votes_required = 1
        settings.strategy_health_gate_enabled = False
        settings.rotation_enabled = True
        settings.rotation_min_holding_minutes = 30
        settings.rotation_min_priority_score_delta = 0.0
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings.queue_path = root / "queue.json"
            settings.theses_path = root / "theses.json"
            settings.target_activity_path = root / "target_activity.json"
            settings.positions_path = root / "positions.json"
            settings.trades_path = root / "trades.json"
            settings.marks_path = root / "marks.json"
            settings.queue_path.write_text(
                """
                [
                  {
                    "market_id":"candidate-market",
                    "question":"New candidate",
                    "slug":"candidate-market",
                    "token_id":"candidate-token",
                    "midpoint":0.40,
                    "best_bid":0.39,
                    "best_ask":0.41,
                    "bids_depth":5000,
                    "asks_depth":5000,
                    "spread":0.02,
                    "hours_to_resolution":24,
                    "total_volume":200000,
                    "book_imbalance":0.4,
                    "priority_score":12.0,
                    "category":"crypto"
                  }
                ]
                """,
                encoding="utf-8",
            )
            settings.theses_path.write_text(
                """
                [
                  {
                    "market_id":"candidate-market",
                    "token_id":"candidate-token",
                    "estimated_probability":0.62,
                    "confidence":0.7,
                    "thesis":"stronger than incumbent",
                    "catalysts":[],
                    "crowd_error":"underpricing",
                    "source":"openai"
                  }
                ]
                """,
                encoding="utf-8",
            )
            settings.target_activity_path.write_text('{"by_match_key":{}}', encoding="utf-8")
            settings.marks_path.write_text('{"summary":{"total_unrealized_pnl_usdc":0.0}}', encoding="utf-8")
            settings.positions_path.write_text(
                f"""
                [
                  {{
                    "position_id":"old-position",
                    "market_id":"old-market",
                    "token_id":"old-token",
                    "question":"Old position",
                    "side":"BUY",
                    "shares":2000.0,
                    "notional_usdc":1000.0,
                    "entry_price":0.50,
                    "expected_gap":0.01,
                    "opened_at":"{(datetime.now(timezone.utc) - timedelta(minutes=90)).isoformat().replace("+00:00", "Z")}",
                    "thesis_confidence":0.2,
                    "mode":"paper",
                    "category":"crypto"
                  }}
                ]
                """,
                encoding="utf-8",
            )
            settings.trades_path.write_text("[]", encoding="utf-8")

            class FakeCLI:
                def midpoint(self, token_id: str) -> float:
                    return 0.5 if token_id == "old-token" else 0.4

            decisions = trade_candidates(settings, FakeCLI())
            actions = [item["action"] for item in decisions]

            self.assertIn("ROTATE_OUT", actions)
            self.assertIn("OPEN", actions)
            trades = json.loads(settings.trades_path.read_text(encoding="utf-8"))
            self.assertEqual([trade["type"] for trade in trades], ["CLOSE", "OPEN"])

    def test_strategy_health_gate_blocks_negative_recent_pnl(self) -> None:
        settings = Settings.from_env()
        settings.strategy_health_gate_enabled = True
        settings.strategy_health_lookback_hours = 24
        settings.strategy_health_min_closed_trades = 2
        settings.strategy_health_min_realized_pnl_usdc = 1.0
        settings.strategy_health_block_degraded = True
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            trades_path = root / "trades.json"
            status_path = root / "status.json"
            now = datetime.now(timezone.utc)
            trades_path.write_text(
                json.dumps(
                    [
                        {"type": "CLOSE", "closed_at": (now - timedelta(hours=1)).isoformat().replace("+00:00", "Z"), "pnl_usdc": -5.0},
                        {"type": "CLOSE", "closed_at": (now - timedelta(hours=2)).isoformat().replace("+00:00", "Z"), "pnl_usdc": 1.0},
                    ]
                ),
                encoding="utf-8",
            )
            status_path.write_text(json.dumps({"status": "running"}), encoding="utf-8")
            gate = _strategy_health_gate(
                settings,
                trades_path=trades_path,
                status_path=status_path,
                strategy_name="test_strategy",
            )
            self.assertTrue(gate["blocked"])
            self.assertIn("recent realized pnl", gate["reason"])

    def test_strategy_health_gate_blocks_degraded_runner(self) -> None:
        settings = Settings.from_env()
        settings.strategy_health_gate_enabled = True
        settings.strategy_health_lookback_hours = 24
        settings.strategy_health_min_closed_trades = 0
        settings.strategy_health_min_realized_pnl_usdc = 0.0
        settings.strategy_health_block_degraded = True
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            trades_path = root / "trades.json"
            status_path = root / "status.json"
            trades_path.write_text("[]", encoding="utf-8")
            status_path.write_text(json.dumps({"status": "degraded", "last_error": "timeout"}), encoding="utf-8")
            gate = _strategy_health_gate(
                settings,
                trades_path=trades_path,
                status_path=status_path,
                strategy_name="test_strategy",
            )
            self.assertTrue(gate["blocked"])
            self.assertIn("runner degraded", gate["reason"])

    def test_strategy_health_gate_allows_5m_warmup_without_recent_closes(self) -> None:
        settings = Settings.from_env()
        settings.strategy_health_gate_enabled = True
        settings.strategy_health_lookback_hours = 24
        settings.strategy_health_min_closed_trades = 3
        settings.strategy_health_min_realized_pnl_usdc = 1.0
        settings.strategy_health_block_degraded = True
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            trades_path = root / "trades.json"
            status_path = root / "status.json"
            trades_path.write_text("[]", encoding="utf-8")
            status_path.write_text(json.dumps({"status": "running"}), encoding="utf-8")
            gate = _strategy_health_gate(
                settings,
                trades_path=trades_path,
                status_path=status_path,
                strategy_name="crypto_5m_sniper",
            )
            self.assertFalse(gate["blocked"])
            self.assertIn("warmup mode", gate["reason"])

    def test_strategy_health_gate_allows_wallet_copy_at_break_even(self) -> None:
        settings = Settings.from_env()
        settings.strategy_health_gate_enabled = True
        settings.strategy_health_lookback_hours = 24
        settings.strategy_health_min_closed_trades = 3
        settings.strategy_health_min_realized_pnl_usdc = 1.0
        settings.strategy_health_block_degraded = True
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            trades_path = root / "trades.json"
            status_path = root / "status.json"
            now = datetime.now(timezone.utc)
            trades_path.write_text(
                json.dumps(
                    [
                        {"type": "CLOSE", "closed_at": (now - timedelta(hours=1)).isoformat().replace("+00:00", "Z"), "pnl_usdc": 1.0},
                        {"type": "CLOSE", "closed_at": (now - timedelta(hours=2)).isoformat().replace("+00:00", "Z"), "pnl_usdc": -1.0},
                        {"type": "CLOSE", "closed_at": (now - timedelta(hours=3)).isoformat().replace("+00:00", "Z"), "pnl_usdc": 0.0},
                    ]
                ),
                encoding="utf-8",
            )
            status_path.write_text(json.dumps({"status": "running"}), encoding="utf-8")
            gate = _strategy_health_gate(
                settings,
                trades_path=trades_path,
                status_path=status_path,
                strategy_name="wallet_copy",
            )
            self.assertFalse(gate["blocked"])

    def test_strategy_health_gate_allows_threshold_snapshot_probation_when_flat(self) -> None:
        settings = Settings.from_env()
        settings.strategy_health_gate_enabled = True
        settings.strategy_health_lookback_hours = 24
        settings.strategy_health_min_closed_trades = 3
        settings.strategy_health_min_realized_pnl_usdc = 1.0
        settings.strategy_health_block_degraded = True
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            trades_path = root / "threshold_trades.json"
            status_path = root / "status.json"
            positions_path = root / "threshold_positions.json"
            marks_path = root / "threshold_marks.json"
            now = datetime.now(timezone.utc)
            trades_path.write_text(
                json.dumps(
                    [
                        {"type": "CLOSE", "closed_at": (now - timedelta(hours=1)).isoformat().replace("+00:00", "Z"), "pnl_usdc": -5.0},
                        {"type": "CLOSE", "closed_at": (now - timedelta(hours=2)).isoformat().replace("+00:00", "Z"), "pnl_usdc": 0.0},
                        {"type": "CLOSE", "closed_at": (now - timedelta(hours=3)).isoformat().replace("+00:00", "Z"), "pnl_usdc": 0.0},
                    ]
                ),
                encoding="utf-8",
            )
            positions_path.write_text("[]", encoding="utf-8")
            marks_path.write_text(json.dumps({"summary": {"total_unrealized_pnl_usdc": 0.0}}), encoding="utf-8")
            status_path.write_text(json.dumps({"status": "running"}), encoding="utf-8")
            gate = _strategy_health_gate(
                settings,
                trades_path=trades_path,
                status_path=status_path,
                strategy_name="crypto_threshold_snapshot",
            )
            self.assertFalse(gate["blocked"])
            self.assertTrue(gate["probation_allowed"])

    def test_strategy_health_gate_allows_threshold_snapshot_probation_with_small_open_count(self) -> None:
        settings = Settings.from_env()
        settings.strategy_health_gate_enabled = True
        settings.strategy_health_lookback_hours = 24
        settings.strategy_health_min_closed_trades = 3
        settings.strategy_health_min_realized_pnl_usdc = 1.0
        settings.strategy_health_block_degraded = True
        settings.strategy_probation_max_open_positions = 4
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            trades_path = root / "threshold_trades.json"
            status_path = root / "status.json"
            positions_path = root / "threshold_positions.json"
            marks_path = root / "threshold_marks.json"
            now = datetime.now(timezone.utc)
            trades_path.write_text(
                json.dumps(
                    [
                        {"type": "CLOSE", "closed_at": (now - timedelta(hours=1)).isoformat().replace("+00:00", "Z"), "pnl_usdc": -5.0},
                        {"type": "CLOSE", "closed_at": (now - timedelta(hours=2)).isoformat().replace("+00:00", "Z"), "pnl_usdc": 0.0},
                        {"type": "CLOSE", "closed_at": (now - timedelta(hours=3)).isoformat().replace("+00:00", "Z"), "pnl_usdc": 0.0},
                    ]
                ),
                encoding="utf-8",
            )
            positions_path.write_text(json.dumps([{"position_id": "p1"}, {"position_id": "p2"}]), encoding="utf-8")
            marks_path.write_text(json.dumps({"summary": {"total_unrealized_pnl_usdc": -10.0}}), encoding="utf-8")
            status_path.write_text(json.dumps({"status": "running"}), encoding="utf-8")
            gate = _strategy_health_gate(
                settings,
                trades_path=trades_path,
                status_path=status_path,
                strategy_name="crypto_threshold_snapshot",
            )
            self.assertFalse(gate["blocked"])
            self.assertTrue(gate["probation_allowed"])

    def test_force_close_position_zero_proceeds_flattens_paper_threshold_position(self) -> None:
        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings = replace(
                settings,
                positions_path=root / "positions.json",
                trades_path=root / "trades.json",
            )
            position = {
                "position_id": "p1",
                "market_id": "m1",
                "token_id": "t1",
                "question": "Ethereum above 2,340 on May 5, 5PM ET?",
                "category": "crypto",
                "side": "SELL",
                "entry_price": 0.99,
                "entry_contract_price": 0.01,
                "shares": 100.0,
                "notional_usdc": 1.0,
                "opened_at": "2026-05-05T20:51:32Z",
                "strategy_tag": "crypto_threshold_snapshot",
            }
            settings.positions_path.write_text(json.dumps([position]), encoding="utf-8")
            settings.trades_path.write_text("[]", encoding="utf-8")
            trade = _force_close_position_zero_proceeds(settings, position, reason="THRESHOLD_STALE_EXIT")
            self.assertIsNotNone(trade)
            self.assertEqual(json.loads(settings.positions_path.read_text(encoding="utf-8")), [])
            trades = json.loads(settings.trades_path.read_text(encoding="utf-8"))
            self.assertEqual(trades[-1]["reason"], "THRESHOLD_STALE_EXIT")
            self.assertEqual(trades[-1]["proceeds_usdc"], 0.0)

    def test_intraday_registry_population_metrics_counts_watchlist_transitions(self) -> None:
        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings = replace(settings, intraday_registry_enabled=True, intraday_registry_path=root / "registry.json")
            now = datetime.now(timezone.utc)
            watch_threshold = {
                "market_id": "1",
                "question": "Will MegaETH perform an airdrop by June 30?",
                "slug": "megaeth-airdrop-june-30",
                "hours_to_resolution": 0.5,
                "registry_watch_only": True,
                "active": True,
                "endDate": (now + timedelta(minutes=30)).isoformat().replace("+00:00", "Z"),
            }
            watch_updown = {
                "market_id": "2",
                "question": "Bitcoin Up or Down",
                "slug": "btc-up-or-down-5m",
                "hours_to_resolution": 0.05,
                "registry_watch_only": True,
                "active": True,
                "endDate": (now + timedelta(minutes=3)).isoformat().replace("+00:00", "Z"),
            }
            settings.intraday_registry_path.write_text(
                json.dumps({"updated_at": now.isoformat().replace("+00:00", "Z"), "market_count": 2, "markets": [watch_threshold, watch_updown]}),
                encoding="utf-8",
            )
            with patch("bot.core._intraday_registry_is_fresh", return_value=True):
                metrics = _intraday_registry_population_metrics(settings)
            self.assertEqual(metrics["watchlist_market_count"], 2)
            self.assertEqual(metrics["watchlist_updown_count"], 1)
            self.assertEqual(metrics["watchlist_threshold_transition_count"], 2)
            self.assertEqual(metrics["watchlist_imminent_transition_count"], 1)

    def test_intraday_box_execution_state_labels_supply_and_arb(self) -> None:
        self.assertEqual(_intraday_box_execution_state({}, {}), "no imminent supply")
        self.assertEqual(_intraday_box_execution_state({"imminent_updown_count": 1}, {}), "imminent supply, no arb")
        self.assertEqual(_intraday_box_execution_state({"positive_gross_edge_count": 1}, {}), "positive gross arb observed")
        self.assertEqual(_intraday_box_execution_state({"positive_net_edge_count": 1}, {}), "positive net arb observed")
        self.assertEqual(_intraday_box_execution_state({}, {"executed_pairs": 1}), "arb executed")

    def test_append_intraday_edge_alerts_writes_incremental_events(self) -> None:
        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings = replace(settings, intraday_registry_edge_alerts_path=root / "intraday_edge_alerts.json")
            payload = _append_intraday_edge_alerts(
                settings,
                registry_result={
                    "watchlist_imminent_transition_count": 2,
                    "imminent_box_arb_entries": 1,
                    "positive_gross_edge_count": 1,
                    "positive_net_edge_count": 1,
                    "max_gross_edge_per_share": 0.01,
                    "max_net_edge_per_share": 0.005,
                    "imminent_updown_count": 2,
                },
                previous_result={
                    "watchlist_imminent_transition_count": 0,
                    "imminent_box_arb_entries": 0,
                    "positive_gross_edge_count": 0,
                    "positive_net_edge_count": 0,
                },
                box_tape={"executed_pairs": 1},
            )
            self.assertEqual(payload["summary"]["execution_state"], "arb executed")
            self.assertGreaterEqual(payload["count"], 5)
            event_types = {entry["event_type"] for entry in payload["entries"]}
            self.assertIn("watchlist_to_imminent", event_types)
            self.assertIn("imminent_box_observed", event_types)
            self.assertIn("positive_gross_edge", event_types)
            self.assertIn("positive_net_edge", event_types)
            self.assertIn("arb_executed", event_types)

    def test_intraday_ws_asset_batches_chunks_and_dedupes_assets(self) -> None:
        settings = Settings.from_env()
        settings.intraday_registry_ws_batch_asset_limit = 2
        batches = _intraday_ws_asset_batches(
            settings,
            [
                {"clobTokenIds": ["a", "b"]},
                {"clobTokenIds": ["b", "c"]},
            ],
        )
        self.assertEqual(batches, [["a", "b"], ["c"]])

    def test_intraday_audit_sqlite_records_and_summarizes(self) -> None:
        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings = replace(settings, intraday_registry_audit_sqlite_path=root / "intraday_audit.sqlite3")
            _record_intraday_audit_ws_events(
                settings,
                [
                    {
                        "recorded_at": "2026-05-06T16:00:00Z",
                        "event_type": "best_bid_ask",
                        "market_id": "m1",
                        "asset_id": "a1",
                        "question": "Bitcoin Up or Down",
                        "slug": "btc-updown",
                        "hours_to_resolution": 0.03,
                        "best_bid": 0.48,
                        "best_ask": 0.49,
                        "midpoint": 0.485,
                        "bids_depth_usdc": 50.0,
                        "asks_depth_usdc": 60.0,
                        "min_depth_usdc": 50.0,
                        "raw_event": {"event_type": "best_bid_ask"},
                        "market": {"id": "m1"},
                    }
                ],
            )
            _record_intraday_audit_book_snapshots(
                settings,
                [
                    {
                        "seen_at": "2026-05-06T16:00:01Z",
                        "market_id": "m1",
                        "token_id": "a1",
                        "question": "Bitcoin Up or Down",
                        "slug": "btc-updown",
                        "asset": "btc",
                        "is_updown": True,
                        "hours_to_resolution": 0.03,
                        "midpoint": 0.485,
                        "best_bid": 0.48,
                        "best_ask": 0.49,
                        "spread": 0.01,
                        "bids_depth_usdc": 50.0,
                        "asks_depth_usdc": 60.0,
                        "min_depth_usdc": 50.0,
                    }
                ],
                source="official_clob_book",
            )
            _record_intraday_audit_arb_opportunities(
                settings,
                [
                    {
                        "seen_at": "2026-05-06T16:00:02Z",
                        "market_id": "m1",
                        "token_id": "a1",
                        "question": "Bitcoin Up or Down",
                        "slug": "btc-updown",
                        "asset": "btc",
                        "hours_to_resolution": 0.03,
                        "midpoint": 0.485,
                        "best_bid": 0.48,
                        "best_ask": 0.49,
                        "spread": 0.01,
                        "min_depth_usdc": 50.0,
                        "filled_shares": 10.0,
                        "yes_contract_price": 0.48,
                        "no_contract_price": 0.49,
                        "combined_contract_price": 0.97,
                        "gross_edge_per_share": 0.03,
                        "net_edge_per_share": 0.02,
                    }
                ],
                source="official_clob_book",
            )
            _record_intraday_audit_imminent_candidates(
                settings,
                [
                    {
                        "seen_at": "2026-05-06T16:00:01Z",
                        "market_id": "m2",
                        "token_id": "a2",
                        "question": "Ethereum Up or Down",
                        "slug": "eth-updown",
                        "asset": "eth",
                        "hours_to_resolution": 0.02,
                        "midpoint": 0.501,
                        "best_bid": 0.49,
                        "best_ask": 0.51,
                        "spread": 0.02,
                        "min_depth_usdc": 12.0,
                        "meets_min_depth": False,
                        "filled_shares": 0.0,
                        "yes_contract_price": 0.51,
                        "no_contract_price": 0.50,
                        "combined_contract_price": 1.01,
                        "gross_edge_per_share": 0.0,
                        "net_edge_per_share": -0.01,
                    }
                ],
                source="official_clob_book_raw_imminent",
            )
            _record_intraday_audit_maker_box_simulations(
                settings,
                [
                    {
                        "seen_at": "2026-05-06T16:00:01Z",
                        "market_id": "m2",
                        "token_id": "a2",
                        "question": "Ethereum Up or Down",
                        "slug": "eth-updown",
                        "asset": "eth",
                        "hours_to_resolution": 0.02,
                        "midpoint": 0.501,
                        "best_bid": 0.49,
                        "best_ask": 0.51,
                        "spread": 0.02,
                        "min_depth_usdc": 12.0,
                        "maker_filled_shares": 5.0,
                        "maker_yes_contract_price": 0.49,
                        "maker_no_contract_price": 0.49,
                        "maker_combined_contract_price": 0.98,
                        "maker_gross_edge_per_share": 0.02,
                        "maker_net_edge_per_share": 0.015,
                    }
                ],
                source="official_clob_book_maker_proxy",
            )
            _record_intraday_audit_aggressive_box_simulations(
                settings,
                [
                    {
                        "seen_at": "2026-05-06T16:00:01Z",
                        "market_id": "m2",
                        "token_id": "a2",
                        "question": "Ethereum Up or Down",
                        "slug": "eth-updown",
                        "asset": "eth",
                        "hours_to_resolution": 0.02,
                        "midpoint": 0.501,
                        "best_bid": 0.49,
                        "best_ask": 0.51,
                        "spread": 0.02,
                        "min_depth_usdc": 12.0,
                        "aggressive_filled_shares": 5.0,
                        "aggressive_yes_contract_price": 0.49,
                        "aggressive_no_contract_price": 0.49,
                        "aggressive_combined_contract_price": 0.98,
                        "aggressive_gross_edge_per_share": 0.02,
                        "aggressive_expected_gross_edge_per_share": 0.01,
                        "aggressive_net_edge_per_share": 0.015,
                        "aggressive_expected_net_edge_per_share": 0.005,
                        "aggressive_fill_probability": 0.5,
                        "aggressive_maker_fee_per_share": 0.0,
                        "aggressive_estimated_slippage_per_share": 0.0,
                        "aggressive_price_concession_ticks": 0,
                        "aggressive_tick_size": 0.01,
                    }
                ],
                source="official_clob_book_aggressive_proxy",
            )
            _record_intraday_audit_watchlist_transitions(
                settings,
                [
                    {
                        "seen_at": "2026-05-06T16:00:01Z",
                        "market_id": "m3",
                        "token_id": "a3",
                        "question": "Solana Up or Down",
                        "slug": "sol-updown",
                        "asset": "sol",
                        "transition_stage": "imminent_updown",
                        "hours_to_resolution": 0.01,
                        "midpoint": 0.5,
                        "best_bid": 0.49,
                        "best_ask": 0.51,
                        "spread": 0.02,
                        "bids_depth_usdc": 25.0,
                        "asks_depth_usdc": 30.0,
                        "min_depth_usdc": 25.0,
                    }
                ],
                source="registry_watchlist_transition",
            )
            _record_intraday_audit_market_lifecycle(
                settings,
                [
                    {
                        "seen_at": "2026-05-06T16:00:03Z",
                        "market_id": "m4",
                        "token_id": "a4",
                        "question": "Bitcoin above 82,000 on May 6, 2PM ET?",
                        "slug": "btc-above-82000-may-6-2pm",
                        "asset": "btc",
                        "is_updown": False,
                        "hours_to_resolution": 0.4,
                        "midpoint": 0.03,
                        "best_bid": 0.02,
                        "best_ask": 0.04,
                        "spread": 0.02,
                        "min_depth_usdc": 11.48,
                    }
                ],
                source="official_clob_book",
            )
            _record_intraday_audit_cycle(
                settings,
                runner="intraday_registry",
                payload={
                    "execution_state": "positive net arb observed",
                    "market_count": 12,
                    "threshold_live_markets": 3,
                    "imminent_box_arb_entries": 1,
                    "positive_gross_edge_count": 1,
                    "positive_net_edge_count": 1,
                    "executed_pairs": 0,
                    "websocket_connected": True,
                    "websocket_messages": 20,
                },
            )
            summary = summarize_intraday_audit(settings, lookback_minutes=10_000_000)
            self.assertEqual(summary["ws_event_count"], 1)
            self.assertEqual(summary["book_snapshot_count"], 1)
            self.assertEqual(summary["raw_imminent_candidate_count"], 1)
            self.assertEqual(summary["maker_box_simulation_count"], 1)
            self.assertEqual(summary["aggressive_box_simulation_count"], 1)
            self.assertEqual(summary["watchlist_imminent_candidate_count"], 1)
            self.assertEqual(summary["arb_opportunity_count"], 1)
            self.assertEqual(summary["positive_gross_edge_count"], 1)
            self.assertEqual(summary["positive_net_edge_count"], 1)
            self.assertEqual(summary["maker_positive_gross_edge_count"], 1)
            self.assertEqual(summary["maker_positive_net_edge_count"], 1)
            self.assertEqual(summary["aggressive_positive_gross_edge_count"], 1)
            self.assertEqual(summary["aggressive_positive_net_edge_count"], 1)
            self.assertEqual(summary["aggressive_positive_expected_net_edge_count"], 1)
            self.assertEqual(summary["registry_cycle_count"], 1)
            self.assertEqual(summary["max_gross_edge_per_share"], 0.03)
            self.assertEqual(summary["max_net_edge_per_share"], 0.02)
            self.assertEqual(summary["maker_max_gross_edge_per_share"], 0.02)
            self.assertEqual(summary["maker_max_net_edge_per_share"], 0.015)
            self.assertEqual(summary["aggressive_max_gross_edge_per_share"], 0.02)
            self.assertEqual(summary["aggressive_max_net_edge_per_share"], 0.015)
            self.assertEqual(summary["aggressive_max_expected_net_edge_per_share"], 0.005)
            self.assertIsNotNone(summary["latest_raw_imminent_at"])
            self.assertIsNotNone(summary["latest_maker_simulation_at"])
            self.assertIsNotNone(summary["latest_aggressive_simulation_at"])
            self.assertIsNotNone(summary["latest_watchlist_imminent_at"])
            lifecycle = summarize_intraday_lifecycle(settings, limit=5)
            self.assertEqual(lifecycle["total_markets"], 4)
            self.assertEqual(lifecycle["updown_markets"], 3)
            self.assertEqual(lifecycle["threshold_markets"], 1)
            self.assertEqual(lifecycle["updown_sub_60m_markets"], 3)
            self.assertEqual(lifecycle["updown_sub_15m_markets"], 3)
            self.assertEqual(lifecycle["updown_imminent_transition_markets"], 1)
            self.assertEqual(lifecycle["threshold_sub_60m_markets"], 1)
            self.assertEqual(lifecycle["best_updown_combined_contract_price"], 0.97)
            self.assertEqual(lifecycle["best_updown_net_edge_per_share"], 0.02)
            self.assertTrue(lifecycle["recent_updown_markets"])

    def test_intraday_discovery_comparator_records_and_summarizes(self) -> None:
        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings = replace(settings, intraday_registry_audit_sqlite_path=root / "intraday_audit.sqlite3")
            _record_intraday_discovery_comparator_batch(
                settings,
                batch_id="batch-1",
                snapshots=[
                    {
                        "source": "gamma_search_updown",
                        "total_markets": 2,
                        "live_upcoming_count": 1,
                        "near_term_count": 1,
                        "imminent_count": 0,
                        "closed_count": 1,
                        "inactive_count": 0,
                        "accepting_orders_count": 1,
                        "min_hours_to_resolution": 4.5,
                        "max_hours_to_resolution": 18.0,
                        "error": None,
                        "sample_questions": ["Bitcoin Up or Down - May 7, 1:00PM-1:05PM ET"],
                        "markets": [
                            {
                                "market_id": "m1",
                                "question": "Bitcoin Up or Down - May 7, 1:00PM-1:05PM ET",
                                "slug": "btc-updown",
                                "asset": "btc",
                                "hours_to_resolution": 4.5,
                                "is_live_upcoming": True,
                                "is_near_term": True,
                                "is_imminent": False,
                                "active": True,
                                "closed": False,
                                "archived": False,
                                "accepting_orders": True,
                                "market_json": "{}",
                            },
                            {
                                "market_id": "m2",
                                "question": "Bitcoin Up or Down - May 5, 1:00PM-1:05PM ET",
                                "slug": "btc-updown-old",
                                "asset": "btc",
                                "hours_to_resolution": 0.0,
                                "is_live_upcoming": False,
                                "is_near_term": False,
                                "is_imminent": False,
                                "active": True,
                                "closed": True,
                                "archived": False,
                                "accepting_orders": False,
                                "market_json": "{}",
                            },
                        ],
                    },
                    {
                        "source": "cli_markets",
                        "total_markets": 1,
                        "live_upcoming_count": 1,
                        "near_term_count": 1,
                        "imminent_count": 0,
                        "closed_count": 0,
                        "inactive_count": 0,
                        "accepting_orders_count": 1,
                        "min_hours_to_resolution": 4.5,
                        "max_hours_to_resolution": 4.5,
                        "error": None,
                        "sample_questions": ["Bitcoin Up or Down - May 7, 1:00PM-1:05PM ET"],
                        "markets": [
                            {
                                "market_id": "m1",
                                "question": "Bitcoin Up or Down - May 7, 1:00PM-1:05PM ET",
                                "slug": "btc-updown",
                                "asset": "btc",
                                "hours_to_resolution": 4.5,
                                "is_live_upcoming": True,
                                "is_near_term": True,
                                "is_imminent": False,
                                "active": True,
                                "closed": False,
                                "archived": False,
                                "accepting_orders": True,
                                "market_json": "{}",
                            }
                        ],
                    },
                ],
            )
            summary = summarize_intraday_discovery_comparator(settings, limit=5)
            self.assertEqual(summary["batch_id"], "batch-1")
            self.assertEqual(len(summary["sources"]), 2)
            self.assertEqual(summary["live_overlap_count"], 1)
            self.assertEqual(summary["near_term_overlap_count"], 1)
            self.assertEqual(summary["imminent_overlap_count"], 0)
            self.assertTrue(summary["recent_near_term_markets"])
            self.assertEqual(summary["recent_near_term_markets"][0]["question"], "Bitcoin Up or Down - May 7, 1:00PM-1:05PM ET")

    def test_compare_intraday_discovery_sources_includes_gamma_events_by_tag(self) -> None:
        settings = Settings.from_env()
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings = replace(
                settings,
                intraday_registry_audit_sqlite_path=root / "intraday_audit.sqlite3",
                intraday_registry_watchlist_max_minutes_to_resolution=360,
                intraday_registry_imminent_max_minutes_to_resolution=15,
            )
            cli = PolymarketCLI(settings)
            market = {
                "id": "m1",
                "question": "Bitcoin Up or Down",
                "slug": "bitcoin-up-or-down-live",
                "endDate": (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat().replace("+00:00", "Z"),
                "active": True,
                "closed": False,
                "archived": False,
                "acceptingOrders": True,
            }
            with (
                patch("bot.core._gamma_search_updown_market_payload", return_value={"matches": [], "upcoming": [], "match_count": 0, "upcoming_count": 0}),
                patch("bot.core._gamma_updown_tag_candidates", return_value=[{"id": "1", "slug": "btc-up-or-down", "label": "BTC Up or Down"}]),
                patch("bot.core._fetch_gamma_events_by_tag_ref", return_value=[market, dict(market)]),
                patch("bot.core._fetch_gamma_intraday_markets_page", return_value=[]),
                patch.object(PolymarketCLI, "list_markets", return_value=[]),
            ):
                result = compare_intraday_discovery_sources(settings, cli)
            tag_source = next(snapshot for snapshot in result["sources"] if snapshot["source"] == "gamma_events_by_tag")
            self.assertEqual(tag_source["tag_candidate_count"], 1)
            self.assertEqual(tag_source["total_markets"], 1)
            self.assertEqual(tag_source["live_upcoming_count"], 1)
            self.assertEqual(tag_source["near_term_count"], 1)
            self.assertEqual(tag_source["imminent_count"], 1)
            self.assertEqual(result["live_overlap_count"], 0)


if __name__ == "__main__":
    unittest.main()
