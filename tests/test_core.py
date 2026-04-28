import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from bot.accounting import dedupe_closed_trades, effective_position_shares, rebuild_closed_trades
from bot.config import Settings
from bot.dashboard import _build_lifetime_stats
from bot.core import (
    _find_target_trades,
    _current_bankroll_usdc,
    _current_cycle_result_from_files,
    _extract_openai_output_text,
    _is_transient_cli_error,
    _market_prefilter,
    _remaining_bankroll_usdc,
    _select_rotation_position,
    _side_market_price,
    _side_probability,
    _within_market_cooldown,
    _wallet_copy_candidate_pool,
    build_theses,
    MarketCandidate,
    PolymarketCLI,
    kelly_size,
    refresh_target_activity,
    trade_candidates,
    whale_copy_vote,
    wallet_copy_ab_vote,
    wallet_copy_variant_vote,
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
            settings.positions_path.write_text('[{"position_id":"p1","notional_usdc":1200.0}]', encoding="utf-8")
            settings.trades_path.write_text("[]", encoding="utf-8")

            class FakeExecutor:
                def open_notional_usdc(self) -> float:
                    return 1200.0

            self.assertEqual(_remaining_bankroll_usdc(settings, FakeExecutor()), 300.0)

    def test_trade_candidates_rotates_out_weak_position_when_fully_allocated(self) -> None:
        settings = Settings.from_env()
        settings.bankroll_usdc = 1000.0
        settings.max_portfolio_fraction = 1.0
        settings.max_position_fraction = 0.5
        settings.max_kelly_fraction = 0.5
        settings.min_thesis_confidence = 0.35
        settings.consensus_votes_required = 1
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


if __name__ == "__main__":
    unittest.main()
