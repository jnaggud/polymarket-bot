from __future__ import annotations

import html
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from statistics import mean
from typing import Any

from bot.accounting import rebuild_closed_trades
from bot.config import Settings


def _json_load(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    text = path.read_text(encoding="utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        decoder = json.JSONDecoder()
        idx = 0
        last_value = default
        length = len(text)
        while idx < length:
            while idx < length and text[idx].isspace():
                idx += 1
            if idx >= length:
                break
            try:
                value, next_idx = decoder.raw_decode(text, idx)
            except json.JSONDecodeError:
                return default if last_value is default else last_value
            last_value = value
            idx = next_idx
        return last_value


def _sqlite_intraday_audit_summary(path: Path, lookback_minutes: int = 60) -> dict[str, Any]:
    if not path.exists():
        return {
            "db_exists": False,
            "lookback_minutes": lookback_minutes,
            "ws_event_count": 0,
            "book_snapshot_count": 0,
            "arb_opportunity_count": 0,
            "raw_imminent_candidate_count": 0,
            "maker_box_simulation_count": 0,
            "aggressive_box_simulation_count": 0,
            "watchlist_imminent_candidate_count": 0,
            "positive_gross_edge_count": 0,
            "positive_net_edge_count": 0,
            "maker_positive_gross_edge_count": 0,
            "maker_positive_net_edge_count": 0,
            "aggressive_positive_gross_edge_count": 0,
            "aggressive_positive_net_edge_count": 0,
            "aggressive_positive_expected_net_edge_count": 0,
            "max_gross_edge_per_share": 0.0,
            "max_net_edge_per_share": 0.0,
            "maker_max_gross_edge_per_share": 0.0,
            "maker_max_net_edge_per_share": 0.0,
            "aggressive_max_gross_edge_per_share": 0.0,
            "aggressive_max_net_edge_per_share": 0.0,
            "aggressive_max_expected_net_edge_per_share": 0.0,
            "latest_ws_event_at": None,
            "latest_book_snapshot_at": None,
            "latest_arb_observation_at": None,
            "latest_raw_imminent_at": None,
            "latest_maker_simulation_at": None,
            "latest_aggressive_simulation_at": None,
            "latest_watchlist_imminent_at": None,
            "latest_positive_gross_at": None,
            "latest_positive_net_at": None,
            "last_registry_cycle_at": None,
            "btc_raw_imminent_candidate_count": 0,
            "btc_le_1_02_count": 0,
            "btc_le_1_01_count": 0,
            "btc_le_1_005_count": 0,
            "btc_eq_1_00_count": 0,
            "btc_best_combined_contract_price": 0.0,
        }
    since = (datetime.now(timezone.utc) - timedelta(minutes=max(int(lookback_minutes), 1))).isoformat().replace("+00:00", "Z")
    try:
        with closing(sqlite3.connect(path)) as conn, conn:
            ws = conn.execute(
                "SELECT COUNT(*), COALESCE(MAX(recorded_at), '') FROM ws_events WHERE recorded_at >= ?",
                (since,),
            ).fetchone()
            books = conn.execute(
                "SELECT COUNT(*), COALESCE(MAX(recorded_at), '') FROM book_snapshots WHERE recorded_at >= ?",
                (since,),
            ).fetchone()
            arbs = conn.execute(
                "SELECT COUNT(*), COALESCE(MAX(recorded_at), '') FROM arb_opportunities WHERE recorded_at >= ?",
                (since,),
            ).fetchone()
            maker = conn.execute(
                "SELECT COUNT(*), COALESCE(MAX(recorded_at), '') FROM maker_box_simulations WHERE recorded_at >= ?",
                (since,),
            ).fetchone()
            aggressive = conn.execute(
                "SELECT COUNT(*), COALESCE(MAX(recorded_at), '') FROM aggressive_box_simulations WHERE recorded_at >= ?",
                (since,),
            ).fetchone()
            raw_imminent = conn.execute(
                "SELECT COUNT(*), COALESCE(MAX(recorded_at), '') FROM imminent_box_candidates WHERE recorded_at >= ?",
                (since,),
            ).fetchone()
            watchlist_imminent = conn.execute(
                """
                SELECT COUNT(*), COALESCE(MAX(recorded_at), '')
                FROM watchlist_transitions
                WHERE recorded_at >= ? AND transition_stage = 'imminent_updown'
                """,
                (since,),
            ).fetchone()
            gross = conn.execute(
                """
                SELECT COUNT(*), COALESCE(MAX(gross_edge_per_share), 0), COALESCE(MAX(recorded_at), '')
                FROM arb_opportunities
                WHERE recorded_at >= ? AND gross_edge_per_share > 0
                """,
                (since,),
            ).fetchone()
            net = conn.execute(
                """
                SELECT COUNT(*), COALESCE(MAX(net_edge_per_share), 0), COALESCE(MAX(recorded_at), '')
                FROM arb_opportunities
                WHERE recorded_at >= ? AND net_edge_per_share > 0
                """,
                (since,),
            ).fetchone()
            maker_gross = conn.execute(
                """
                SELECT COUNT(*), COALESCE(MAX(gross_edge_per_share), 0), COALESCE(MAX(recorded_at), '')
                FROM maker_box_simulations
                WHERE recorded_at >= ?
                  AND filled_shares > 0
                  AND combined_contract_price > 0
                  AND gross_edge_per_share > 0
                """,
                (since,),
            ).fetchone()
            maker_net = conn.execute(
                """
                SELECT COUNT(*), COALESCE(MAX(net_edge_per_share), 0), COALESCE(MAX(recorded_at), '')
                FROM maker_box_simulations
                WHERE recorded_at >= ?
                  AND filled_shares > 0
                  AND combined_contract_price > 0
                  AND net_edge_per_share > 0
                """,
                (since,),
            ).fetchone()
            aggressive_gross = conn.execute(
                """
                SELECT COUNT(*), COALESCE(MAX(gross_edge_per_share), 0), COALESCE(MAX(recorded_at), '')
                FROM aggressive_box_simulations
                WHERE recorded_at >= ?
                  AND filled_shares > 0
                  AND combined_contract_price > 0
                  AND gross_edge_per_share > 0
                """,
                (since,),
            ).fetchone()
            aggressive_net = conn.execute(
                """
                SELECT COUNT(*), COALESCE(MAX(net_edge_per_share), 0), COALESCE(MAX(recorded_at), '')
                FROM aggressive_box_simulations
                WHERE recorded_at >= ?
                  AND filled_shares > 0
                  AND combined_contract_price > 0
                  AND net_edge_per_share > 0
                """,
                (since,),
            ).fetchone()
            aggressive_expected = conn.execute(
                """
                SELECT COUNT(*), COALESCE(MAX(expected_net_edge_per_share), 0), COALESCE(MAX(recorded_at), '')
                FROM aggressive_box_simulations
                WHERE recorded_at >= ?
                  AND filled_shares > 0
                  AND combined_contract_price > 0
                  AND expected_net_edge_per_share > 0
                """,
                (since,),
            ).fetchone()
            cycles = conn.execute(
                """
                SELECT COALESCE(MAX(recorded_at), '')
                FROM cycle_stats
                WHERE recorded_at >= ? AND runner = 'intraday_registry'
                """,
                (since,),
            ).fetchone()
            btc_near_miss = conn.execute(
                """
                SELECT
                    COUNT(*) AS btc_raw_imminent_candidate_count,
                    SUM(CASE WHEN combined_contract_price > 0 AND combined_contract_price <= 1.02 THEN 1 ELSE 0 END) AS btc_le_1_02_count,
                    SUM(CASE WHEN combined_contract_price > 0 AND combined_contract_price <= 1.01 THEN 1 ELSE 0 END) AS btc_le_1_01_count,
                    SUM(CASE WHEN combined_contract_price > 0 AND combined_contract_price <= 1.005 THEN 1 ELSE 0 END) AS btc_le_1_005_count,
                    SUM(CASE WHEN combined_contract_price = 1.0 THEN 1 ELSE 0 END) AS btc_eq_1_00_count,
                    COALESCE(MIN(CASE WHEN combined_contract_price > 0 THEN combined_contract_price END), 0) AS btc_best_combined_contract_price
                FROM imminent_box_candidates
                WHERE recorded_at >= ? AND asset = 'btc'
                """,
                (since,),
            ).fetchone()
            btc_maker = conn.execute(
                """
                SELECT
                    SUM(CASE WHEN gross_edge_per_share > 0 THEN 1 ELSE 0 END) AS btc_maker_positive_gross_count,
                    SUM(CASE WHEN net_edge_per_share > 0 THEN 1 ELSE 0 END) AS btc_maker_positive_net_count,
                    SUM(CASE WHEN combined_contract_price > 0 AND combined_contract_price <= 1.0 THEN 1 ELSE 0 END) AS btc_maker_le_1_00_count,
                    COALESCE(MIN(CASE WHEN combined_contract_price > 0 THEN combined_contract_price END), 0) AS btc_maker_best_combined_contract_price
                FROM maker_box_simulations
                WHERE recorded_at >= ?
                  AND asset = 'btc'
                  AND filled_shares > 0
                """,
                (since,),
            ).fetchone()
            btc_aggressive = conn.execute(
                """
                SELECT
                    SUM(CASE WHEN gross_edge_per_share > 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN net_edge_per_share > 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN expected_net_edge_per_share > 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN combined_contract_price > 0 AND combined_contract_price <= 1.0 THEN 1 ELSE 0 END),
                    COALESCE(MIN(CASE WHEN combined_contract_price > 0 THEN combined_contract_price END), 0)
                FROM aggressive_box_simulations
                WHERE recorded_at >= ?
                  AND asset = 'btc'
                  AND filled_shares > 0
                """,
                (since,),
            ).fetchone()
            btc_aggressive_sensitivity = conn.execute(
                """
                SELECT
                    SUM(CASE WHEN expected_gross_edge_per_share - (-0.0005) - estimated_slippage_per_share > 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN expected_gross_edge_per_share - 0.0 - estimated_slippage_per_share > 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN expected_gross_edge_per_share - 0.0005 - estimated_slippage_per_share > 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN expected_gross_edge_per_share - 0.0010 - estimated_slippage_per_share > 0 THEN 1 ELSE 0 END)
                FROM aggressive_box_simulations
                WHERE recorded_at >= ?
                  AND asset = 'btc'
                  AND filled_shares > 0
                  AND combined_contract_price > 0
                """,
                (since,),
            ).fetchone()
    except sqlite3.Error:
        return {
            "db_exists": True,
            "db_error": True,
            "lookback_minutes": lookback_minutes,
            "ws_event_count": 0,
            "book_snapshot_count": 0,
            "arb_opportunity_count": 0,
            "raw_imminent_candidate_count": 0,
            "maker_box_simulation_count": 0,
            "aggressive_box_simulation_count": 0,
            "watchlist_imminent_candidate_count": 0,
            "positive_gross_edge_count": 0,
            "positive_net_edge_count": 0,
            "maker_positive_gross_edge_count": 0,
            "maker_positive_net_edge_count": 0,
            "aggressive_positive_gross_edge_count": 0,
            "aggressive_positive_net_edge_count": 0,
            "aggressive_positive_expected_net_edge_count": 0,
            "max_gross_edge_per_share": 0.0,
            "max_net_edge_per_share": 0.0,
            "maker_max_gross_edge_per_share": 0.0,
            "maker_max_net_edge_per_share": 0.0,
            "aggressive_max_gross_edge_per_share": 0.0,
            "aggressive_max_net_edge_per_share": 0.0,
            "aggressive_max_expected_net_edge_per_share": 0.0,
            "latest_ws_event_at": None,
            "latest_book_snapshot_at": None,
            "latest_arb_observation_at": None,
            "latest_raw_imminent_at": None,
            "latest_maker_simulation_at": None,
            "latest_aggressive_simulation_at": None,
            "latest_watchlist_imminent_at": None,
            "latest_positive_gross_at": None,
            "latest_positive_net_at": None,
            "last_registry_cycle_at": None,
            "btc_raw_imminent_candidate_count": 0,
            "btc_le_1_02_count": 0,
            "btc_le_1_01_count": 0,
            "btc_le_1_005_count": 0,
            "btc_eq_1_00_count": 0,
            "btc_best_combined_contract_price": 0.0,
            "btc_maker_positive_gross_count": 0,
            "btc_maker_positive_net_count": 0,
            "btc_maker_le_1_00_count": 0,
            "btc_maker_best_combined_contract_price": 0.0,
            "btc_aggressive_positive_gross_count": 0,
            "btc_aggressive_positive_net_count": 0,
            "btc_aggressive_positive_expected_net_count": 0,
            "btc_aggressive_le_1_00_count": 0,
            "btc_aggressive_best_combined_contract_price": 0.0,
            "btc_aggressive_rebate_pos_expected_count": 0,
            "btc_aggressive_flat_pos_expected_count": 0,
            "btc_aggressive_low_fee_pos_expected_count": 0,
            "btc_aggressive_high_fee_pos_expected_count": 0,
        }
    return {
        "db_exists": True,
        "lookback_minutes": lookback_minutes,
        "ws_event_count": int((ws or (0, ""))[0] or 0),
        "book_snapshot_count": int((books or (0, ""))[0] or 0),
        "arb_opportunity_count": int((arbs or (0, ""))[0] or 0),
        "raw_imminent_candidate_count": int((raw_imminent or (0, ""))[0] or 0),
        "maker_box_simulation_count": int((maker or (0, ""))[0] or 0),
        "aggressive_box_simulation_count": int((aggressive or (0, ""))[0] or 0),
        "watchlist_imminent_candidate_count": int((watchlist_imminent or (0, ""))[0] or 0),
        "positive_gross_edge_count": int((gross or (0, 0, ""))[0] or 0),
        "positive_net_edge_count": int((net or (0, 0, ""))[0] or 0),
        "maker_positive_gross_edge_count": int((maker_gross or (0, 0, ""))[0] or 0),
        "maker_positive_net_edge_count": int((maker_net or (0, 0, ""))[0] or 0),
        "aggressive_positive_gross_edge_count": int((aggressive_gross or (0, 0, ""))[0] or 0),
        "aggressive_positive_net_edge_count": int((aggressive_net or (0, 0, ""))[0] or 0),
        "aggressive_positive_expected_net_edge_count": int((aggressive_expected or (0, 0, ""))[0] or 0),
        "max_gross_edge_per_share": _as_float((gross or (0, 0, ""))[1], 0.0),
        "max_net_edge_per_share": _as_float((net or (0, 0, ""))[1], 0.0),
        "maker_max_gross_edge_per_share": _as_float((maker_gross or (0, 0, ""))[1], 0.0),
        "maker_max_net_edge_per_share": _as_float((maker_net or (0, 0, ""))[1], 0.0),
        "aggressive_max_gross_edge_per_share": _as_float((aggressive_gross or (0, 0, ""))[1], 0.0),
        "aggressive_max_net_edge_per_share": _as_float((aggressive_net or (0, 0, ""))[1], 0.0),
        "aggressive_max_expected_net_edge_per_share": _as_float((aggressive_expected or (0, 0, ""))[1], 0.0),
        "latest_ws_event_at": str((ws or (0, ""))[1] or "") or None,
        "latest_book_snapshot_at": str((books or (0, ""))[1] or "") or None,
        "latest_arb_observation_at": str((arbs or (0, ""))[1] or "") or None,
        "latest_raw_imminent_at": str((raw_imminent or (0, ""))[1] or "") or None,
        "latest_maker_simulation_at": str((maker or (0, ""))[1] or "") or None,
        "latest_aggressive_simulation_at": str((aggressive or (0, ""))[1] or "") or None,
        "latest_watchlist_imminent_at": str((watchlist_imminent or (0, ""))[1] or "") or None,
        "latest_positive_gross_at": str((gross or (0, 0, ""))[2] or "") or None,
        "latest_positive_net_at": str((net or (0, 0, ""))[2] or "") or None,
        "last_registry_cycle_at": str((cycles or ("",))[0] or "") or None,
        "btc_raw_imminent_candidate_count": int((btc_near_miss or (0, 0, 0, 0, 0, 0))[0] or 0),
        "btc_le_1_02_count": int((btc_near_miss or (0, 0, 0, 0, 0, 0))[1] or 0),
        "btc_le_1_01_count": int((btc_near_miss or (0, 0, 0, 0, 0, 0))[2] or 0),
        "btc_le_1_005_count": int((btc_near_miss or (0, 0, 0, 0, 0, 0))[3] or 0),
        "btc_eq_1_00_count": int((btc_near_miss or (0, 0, 0, 0, 0, 0))[4] or 0),
        "btc_best_combined_contract_price": _as_float((btc_near_miss or (0, 0, 0, 0, 0, 0))[5], 0.0),
        "btc_maker_positive_gross_count": int((btc_maker or (0, 0, 0, 0))[0] or 0),
        "btc_maker_positive_net_count": int((btc_maker or (0, 0, 0, 0))[1] or 0),
        "btc_maker_le_1_00_count": int((btc_maker or (0, 0, 0, 0))[2] or 0),
        "btc_maker_best_combined_contract_price": _as_float((btc_maker or (0, 0, 0, 0))[3], 0.0),
        "btc_aggressive_positive_gross_count": int((btc_aggressive or (0, 0, 0, 0, 0))[0] or 0),
        "btc_aggressive_positive_net_count": int((btc_aggressive or (0, 0, 0, 0, 0))[1] or 0),
        "btc_aggressive_positive_expected_net_count": int((btc_aggressive or (0, 0, 0, 0, 0))[2] or 0),
        "btc_aggressive_le_1_00_count": int((btc_aggressive or (0, 0, 0, 0, 0))[3] or 0),
        "btc_aggressive_best_combined_contract_price": _as_float((btc_aggressive or (0, 0, 0, 0, 0))[4], 0.0),
        "btc_aggressive_rebate_pos_expected_count": int((btc_aggressive_sensitivity or (0, 0, 0, 0))[0] or 0),
        "btc_aggressive_flat_pos_expected_count": int((btc_aggressive_sensitivity or (0, 0, 0, 0))[1] or 0),
        "btc_aggressive_low_fee_pos_expected_count": int((btc_aggressive_sensitivity or (0, 0, 0, 0))[2] or 0),
        "btc_aggressive_high_fee_pos_expected_count": int((btc_aggressive_sensitivity or (0, 0, 0, 0))[3] or 0),
    }


def _sqlite_btc_5m_decision_table(path: Path, lookback_minutes: int = 60) -> dict[str, Any]:
    fill_probabilities = [0.25, 0.50, 0.75, 1.00]
    fee_scenarios = [
        ("rebate", -0.0005),
        ("flat", 0.0),
        ("low_fee", 0.0005),
        ("high_fee", 0.0010),
    ]
    if not path.exists():
        return {"db_exists": False, "lookback_minutes": lookback_minutes, "rows": []}
    since = (datetime.now(timezone.utc) - timedelta(minutes=max(int(lookback_minutes), 1))).isoformat().replace("+00:00", "Z")
    rows: list[dict[str, Any]] = []
    try:
        with closing(sqlite3.connect(path)) as conn, conn:
            for fill_probability in fill_probabilities:
                row: dict[str, Any] = {"fill_probability": fill_probability}
                for fee_label, fee_per_share in fee_scenarios:
                    stats = conn.execute(
                        """
                        SELECT
                            COUNT(*) AS total_count,
                            SUM(
                                CASE
                                    WHEN filled_shares > 0
                                     AND combined_contract_price > 0
                                     AND ((gross_edge_per_share * ?) - ? - estimated_slippage_per_share) > 0
                                    THEN 1 ELSE 0
                                END
                            ) AS positive_expected_count,
                            COALESCE(
                                MAX(
                                    CASE
                                        WHEN filled_shares > 0 AND combined_contract_price > 0
                                        THEN ((gross_edge_per_share * ?) - ? - estimated_slippage_per_share)
                                        ELSE NULL
                                    END
                                ),
                                0
                            ) AS max_expected_net_edge
                        FROM aggressive_box_simulations
                        WHERE recorded_at >= ? AND asset = 'btc'
                        """,
                        (fill_probability, fee_per_share, fill_probability, fee_per_share, since),
                    ).fetchone()
                    row[f"{fee_label}_positive_expected_count"] = int((stats or (0, 0, 0))[1] or 0)
                    row[f"{fee_label}_max_expected_net_edge"] = _as_float((stats or (0, 0, 0))[2], 0.0)
                    row["total_count"] = int((stats or (0, 0, 0))[0] or 0)
                rows.append(row)
    except sqlite3.Error:
        return {"db_exists": True, "db_error": True, "lookback_minutes": lookback_minutes, "rows": []}
    return {"db_exists": True, "lookback_minutes": lookback_minutes, "rows": rows}


def _sqlite_intraday_lifecycle_summary(path: Path, limit: int = 8) -> dict[str, Any]:
    if not path.exists():
        return {
            "db_exists": False,
            "total_markets": 0,
            "updown_markets": 0,
            "threshold_markets": 0,
            "updown_sub_60m_markets": 0,
            "updown_sub_15m_markets": 0,
            "updown_imminent_transition_markets": 0,
            "threshold_sub_60m_markets": 0,
            "best_updown_combined_contract_price": 0.0,
            "best_updown_gross_edge_per_share": 0.0,
            "best_updown_net_edge_per_share": 0.0,
            "latest_updown_seen_at": None,
            "recent_updown_markets": [],
        }
    try:
        with closing(sqlite3.connect(path)) as conn, conn:
            conn.row_factory = sqlite3.Row
            totals = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_markets,
                    SUM(CASE WHEN is_updown = 1 THEN 1 ELSE 0 END) AS updown_markets,
                    SUM(CASE WHEN is_updown = 0 THEN 1 ELSE 0 END) AS threshold_markets,
                    SUM(CASE WHEN is_updown = 1 AND first_sub_60m_at IS NOT NULL THEN 1 ELSE 0 END) AS updown_sub_60m_markets,
                    SUM(CASE WHEN is_updown = 1 AND first_sub_15m_at IS NOT NULL THEN 1 ELSE 0 END) AS updown_sub_15m_markets,
                    SUM(CASE WHEN is_updown = 1 AND first_imminent_transition_at IS NOT NULL THEN 1 ELSE 0 END) AS updown_imminent_transition_markets,
                    SUM(CASE WHEN is_updown = 0 AND first_sub_60m_at IS NOT NULL THEN 1 ELSE 0 END) AS threshold_sub_60m_markets
                FROM market_lifecycle
                """
            ).fetchone()
            edge = conn.execute(
                """
                SELECT
                    COALESCE(MIN(best_combined_contract_price), 0) AS best_updown_combined_contract_price,
                    COALESCE(MAX(best_gross_edge_per_share), 0) AS best_updown_gross_edge_per_share,
                    COALESCE(MAX(best_net_edge_per_share), 0) AS best_updown_net_edge_per_share,
                    COALESCE(MAX(last_seen_at), '') AS latest_updown_seen_at
                FROM market_lifecycle
                WHERE is_updown = 1
                """
            ).fetchone()
            recent_updown = conn.execute(
                """
                SELECT question, asset, min_hours_to_resolution, first_sub_60m_at, first_sub_15m_at,
                       first_imminent_transition_at, best_combined_contract_price, best_gross_edge_per_share,
                       best_net_edge_per_share, last_seen_at
                FROM market_lifecycle
                WHERE is_updown = 1
                ORDER BY last_seen_at DESC
                LIMIT ?
                """,
                (max(int(limit), 1),),
            ).fetchall()
    except sqlite3.Error:
        return {
            "db_exists": True,
            "db_error": True,
            "total_markets": 0,
            "updown_markets": 0,
            "threshold_markets": 0,
            "updown_sub_60m_markets": 0,
            "updown_sub_15m_markets": 0,
            "updown_imminent_transition_markets": 0,
            "threshold_sub_60m_markets": 0,
            "best_updown_combined_contract_price": 0.0,
            "best_updown_gross_edge_per_share": 0.0,
            "best_updown_net_edge_per_share": 0.0,
            "latest_updown_seen_at": None,
            "recent_updown_markets": [],
        }
    return {
        "db_exists": True,
        "total_markets": int(totals["total_markets"] or 0),
        "updown_markets": int(totals["updown_markets"] or 0),
        "threshold_markets": int(totals["threshold_markets"] or 0),
        "updown_sub_60m_markets": int(totals["updown_sub_60m_markets"] or 0),
        "updown_sub_15m_markets": int(totals["updown_sub_15m_markets"] or 0),
        "updown_imminent_transition_markets": int(totals["updown_imminent_transition_markets"] or 0),
        "threshold_sub_60m_markets": int(totals["threshold_sub_60m_markets"] or 0),
        "best_updown_combined_contract_price": _as_float(edge["best_updown_combined_contract_price"], 0.0),
        "best_updown_gross_edge_per_share": _as_float(edge["best_updown_gross_edge_per_share"], 0.0),
        "best_updown_net_edge_per_share": _as_float(edge["best_updown_net_edge_per_share"], 0.0),
        "latest_updown_seen_at": str(edge["latest_updown_seen_at"] or "") or None,
        "recent_updown_markets": [dict(row) for row in recent_updown],
    }


def _sqlite_intraday_discovery_summary(path: Path, limit: int = 5) -> dict[str, Any]:
    if not path.exists():
        return {
            "db_exists": False,
            "batch_id": None,
            "recorded_at": None,
            "sources": [],
            "live_overlap_count": 0,
            "near_term_overlap_count": 0,
            "imminent_overlap_count": 0,
            "recent_near_term_markets": [],
        }
    try:
        with closing(sqlite3.connect(path)) as conn, conn:
            conn.row_factory = sqlite3.Row
            latest = conn.execute(
                "SELECT batch_id, recorded_at FROM discovery_comparator_runs ORDER BY recorded_at DESC LIMIT 1"
            ).fetchone()
            if latest is None:
                return {
                    "db_exists": True,
                    "batch_id": None,
                    "recorded_at": None,
                    "sources": [],
                    "live_overlap_count": 0,
                    "near_term_overlap_count": 0,
                    "imminent_overlap_count": 0,
                    "recent_near_term_markets": [],
                }
            batch_id = str(latest["batch_id"])
            recorded_at = str(latest["recorded_at"])
            source_rows = conn.execute(
                """
                SELECT source, total_markets, live_upcoming_count, near_term_count, imminent_count,
                       closed_count, inactive_count, accepting_orders_count, min_hours_to_resolution,
                       max_hours_to_resolution, error, sample_questions_json
                FROM discovery_comparator_runs
                WHERE batch_id = ?
                ORDER BY source ASC
                """,
                (batch_id,),
            ).fetchall()
            live_overlap = conn.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT market_id
                    FROM discovery_comparator_markets
                    WHERE batch_id = ? AND is_live_upcoming = 1
                    GROUP BY market_id
                    HAVING COUNT(DISTINCT source) > 1
                )
                """,
                (batch_id,),
            ).fetchone()[0]
            near_overlap = conn.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT market_id
                    FROM discovery_comparator_markets
                    WHERE batch_id = ? AND is_near_term = 1
                    GROUP BY market_id
                    HAVING COUNT(DISTINCT source) > 1
                )
                """,
                (batch_id,),
            ).fetchone()[0]
            imminent_overlap = conn.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT market_id
                    FROM discovery_comparator_markets
                    WHERE batch_id = ? AND is_imminent = 1
                    GROUP BY market_id
                    HAVING COUNT(DISTINCT source) > 1
                )
                """,
                (batch_id,),
            ).fetchone()[0]
            near_rows = conn.execute(
                """
                SELECT source, question, asset, hours_to_resolution, is_live_upcoming, is_near_term, is_imminent
                FROM discovery_comparator_markets
                WHERE batch_id = ? AND is_live_upcoming = 1
                ORDER BY is_imminent DESC, is_near_term DESC, hours_to_resolution ASC, source ASC
                LIMIT ?
                """,
                (batch_id, max(int(limit), 1)),
            ).fetchall()
    except sqlite3.Error:
        return {
            "db_exists": True,
            "db_error": True,
            "batch_id": None,
            "recorded_at": None,
            "sources": [],
            "live_overlap_count": 0,
            "near_term_overlap_count": 0,
            "imminent_overlap_count": 0,
            "recent_near_term_markets": [],
        }
    return {
        "db_exists": True,
        "batch_id": batch_id,
        "recorded_at": recorded_at,
        "sources": [
            {
                "source": str(row["source"]),
                "total_markets": int(row["total_markets"] or 0),
                "live_upcoming_count": int(row["live_upcoming_count"] or 0),
                "near_term_count": int(row["near_term_count"] or 0),
                "imminent_count": int(row["imminent_count"] or 0),
                "closed_count": int(row["closed_count"] or 0),
                "inactive_count": int(row["inactive_count"] or 0),
                "accepting_orders_count": int(row["accepting_orders_count"] or 0),
                "min_hours_to_resolution": float(row["min_hours_to_resolution"]) if row["min_hours_to_resolution"] is not None else None,
                "max_hours_to_resolution": float(row["max_hours_to_resolution"]) if row["max_hours_to_resolution"] is not None else None,
                "error": str(row["error"] or "") or None,
                "sample_questions": json.loads(str(row["sample_questions_json"] or "[]")),
            }
            for row in source_rows
        ],
        "live_overlap_count": int(live_overlap or 0),
        "near_term_overlap_count": int(near_overlap or 0),
        "imminent_overlap_count": int(imminent_overlap or 0),
        "recent_near_term_markets": [dict(row) for row in near_rows],
    }
def _fmt_money(value: float) -> str:
    return f"${value:,.2f}"


def _fmt_num(value: float | int) -> str:
    if isinstance(value, int) or float(value).is_integer():
        return f"{int(value)}"
    return f"{float(value):,.2f}"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.1f}%"


def _fmt_ts(value: str | None) -> str:
    if not value:
        return "-"
    return value


def _fmt_age_minutes(value: float | None) -> str:
    if value is None:
        return "-"
    minutes = max(int(round(value)), 0)
    days, rem = divmod(minutes, 1440)
    hours, mins = divmod(rem, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if mins or not parts:
        parts.append(f"{mins}m")
    return " ".join(parts)


def _normalize_not_trading_reason(reason: str) -> str:
    text = (reason or "").strip().lower()
    if not text:
        return "other"
    if "midpoint too close to fair value" in text:
        return "midpoint too close to fair value"
    if "outside_max_minutes_window" in text or "outside max minutes window" in text:
        return "outside max minutes window"
    if "inactive" in text:
        return "inactive market"
    if "gross edge too small" in text:
        return "gross arb edge too small"
    if "net edge too small" in text:
        return "net arb edge too small after costs"
    if "already open" in text:
        return "already open"
    if "insufficient depth" in text or "depth" in text:
        return "insufficient executable depth"
    if "no budget" in text:
        return "no budget"
    if "no candidates" in text:
        return "no candidates in current cycle"
    return reason.strip()


def _runner_is_stale(last_cycle_completed_at: str | None, stale_after_minutes: int) -> tuple[bool, str]:
    completed = _parse_ts(last_cycle_completed_at)
    if completed is None:
        return True, "no completed cycle"
    age_minutes = max((datetime.now(timezone.utc) - completed).total_seconds() / 60.0, 0.0)
    if age_minutes > max(stale_after_minutes, 1):
        return True, f"stale by {age_minutes:.0f}m"
    return False, "fresh"


def _is_warmup_intraday_strategy(strategy_name: str, lifetime_closed_trades: int) -> bool:
    return (
        strategy_name
        in {
            "crypto_5m_sniper",
            "crypto_5m_box_arb",
            "crypto_latency_5m",
            "crypto_next_window_sniper",
            "crypto_next_window_box_arb",
            "crypto_intraday_scheduled",
            "sports_early_entry",
            "penny_longshot",
        }
        and lifetime_closed_trades < 3
    )


def _is_probation_reentry_strategy(strategy_name: str) -> bool:
    return strategy_name in {"crypto_next_window_sniper", "crypto_intraday_scheduled", "crypto_threshold_snapshot"}


def _probation_reentry_allowed(
    settings: Settings,
    strategy_name: str,
    reasons: list[str],
    *,
    open_positions_count: int,
) -> bool:
    if not _is_probation_reentry_strategy(strategy_name):
        return False
    probation_max_open = max(int(settings.strategy_probation_max_open_positions), 0)
    if strategy_name == "crypto_threshold_snapshot":
        if probation_max_open > 0 and open_positions_count > probation_max_open:
            return False
    elif open_positions_count > 0:
        return False
    if not reasons:
        return False
    allowed_prefixes = ("recent realized pnl ", "only ")
    disallowed_fragments = (
        "runner degraded",
        "strategy quarantined",
        "warmup open positions",
        "warmup unrealized pnl",
    )
    if any(any(fragment in reason for fragment in disallowed_fragments) for reason in reasons):
        return False
    return all(any(reason.startswith(prefix) for prefix in allowed_prefixes) for reason in reasons)


def _as_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _normalize_category(raw: str | None) -> str:
    if not raw:
        return "unknown"
    value = raw.strip().lower()
    if not value or len(value) > 32 or "-" in value:
        return "unknown"
    aliases = {
        "cryptocurrency": "crypto",
        "blockchain": "crypto",
        "economy": "macro",
        "economic": "macro",
        "finance": "macro",
        "business": "macro",
        "elections": "politics",
        "government": "politics",
        "technology": "tech",
        "entertainment": "culture",
    }
    return aliases.get(value, value.replace("/", " ").replace("_", " ").split()[0])


def _categorize_text(question: str, hint: str = "") -> str:
    text = f"{question} {hint}".lower()
    keyword_groups = {
        "crypto": ("bitcoin", "ethereum", "eth", "btc", "token", "airdrop", "solana", "fdv", "launch"),
        "politics": ("president", "senate", "house", "election", "governor", "democrats", "republicans", "nomination", "minister", "government", "sentence", "court"),
        "macro": ("fed", "inflation", "cpi", "gdp", "treasury", "recession", "tariff", "oil", "gold", "stocks"),
        "sports": ("nba", "nfl", "mlb", "nhl", "championship", "world cup", "goal", "touchdown"),
        "tech": ("openai", "chatgpt", "ai", "tesla", "apple", "google", "meta"),
        "culture": ("movie", "oscar", "grammy", "celebrity"),
    }
    for category, keywords in keyword_groups.items():
        if any(keyword in text for keyword in keywords):
            return category
    return "unknown"


def _active_rotation_category(settings: Settings) -> str | None:
    if not settings.category_rotation:
        return None
    rotation_days = max(settings.category_rotation_days, 1)
    bucket = datetime.now(timezone.utc).toordinal() // rotation_days
    return settings.category_rotation[bucket % len(settings.category_rotation)]


def _trade_category(trade: dict[str, Any], opened_by_position: dict[str, dict[str, Any]]) -> str:
    direct = _normalize_category(str(trade.get("category", "") or ""))
    if direct != "unknown":
        return direct
    opened = opened_by_position.get(str(trade.get("position_id", "")), {})
    opened_category = _normalize_category(str(opened.get("category", "") or ""))
    if opened_category != "unknown":
        return opened_category
    raw_hint = str(trade.get("category", "") or opened.get("category", "") or "")
    return _categorize_text(str(trade.get("question", "")), raw_hint)


def _display_category(item: dict[str, Any]) -> str:
    direct = _normalize_category(str(item.get("category", "") or ""))
    if direct != "unknown":
        return direct
    return _categorize_text(str(item.get("question", "")), str(item.get("category", "")))


def _build_lifetime_stats(trades: list[dict[str, Any]], bankroll_usdc: float, unrealized_pnl_usdc: float) -> dict[str, Any]:
    opens = [trade for trade in trades if trade.get("type") == "OPEN"]
    closes = rebuild_closed_trades(trades)
    winners = [trade for trade in closes if _as_float(trade.get("pnl_usdc")) > 0]
    losers = [trade for trade in closes if _as_float(trade.get("pnl_usdc")) < 0]
    breakeven = [trade for trade in closes if _as_float(trade.get("pnl_usdc")) == 0]
    gross_profit = round(sum(_as_float(trade.get("pnl_usdc")) for trade in winners), 2)
    gross_loss = round(sum(abs(_as_float(trade.get("pnl_usdc"))) for trade in losers), 2)
    realized_pnl = round(sum(_as_float(trade.get("pnl_usdc")) for trade in closes), 2)
    closed_count = len(closes)
    win_rate = round((len(winners) / closed_count) * 100.0, 1) if closed_count else None
    profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else None
    avg_winner = round(mean(_as_float(trade.get("pnl_usdc")) for trade in winners), 2) if winners else None
    avg_loser = round(mean(_as_float(trade.get("pnl_usdc")) for trade in losers), 2) if losers else None
    best_trade = max(closes, key=lambda trade: _as_float(trade.get("pnl_usdc")), default=None)
    worst_trade = min(closes, key=lambda trade: _as_float(trade.get("pnl_usdc")), default=None)

    opened_by_position = {
        str(trade.get("position_id", "")): trade
        for trade in opens
        if trade.get("position_id")
    }
    holding_minutes: list[float] = []
    for close_trade in closes:
        opened = opened_by_position.get(str(close_trade.get("position_id", "")))
        opened_at = _parse_ts(opened.get("opened_at")) if opened else None
        closed_at = _parse_ts(close_trade.get("closed_at"))
        if opened_at and closed_at and closed_at >= opened_at:
            holding_minutes.append((closed_at - opened_at).total_seconds() / 60.0)

    curve_points = []
    running_pnl = 0.0
    sorted_closes = sorted(closes, key=lambda trade: _parse_ts(trade.get("closed_at")) or datetime.min.replace(tzinfo=timezone.utc))
    for idx, trade in enumerate(sorted_closes, start=1):
        running_pnl = round(running_pnl + _as_float(trade.get("pnl_usdc")), 2)
        curve_points.append(
            {
                "index": idx,
                "timestamp": trade.get("closed_at"),
                "equity_usdc": round(bankroll_usdc + running_pnl, 2),
                "realized_pnl_usdc": running_pnl,
                "question": trade.get("question", ""),
            }
        )

    return {
        "opened_count": len(opens),
        "closed_count": closed_count,
        "winners_count": len(winners),
        "losers_count": len(losers),
        "breakeven_count": len(breakeven),
        "win_rate_pct": win_rate,
        "gross_profit_usdc": gross_profit,
        "gross_loss_usdc": gross_loss,
        "realized_pnl_usdc": realized_pnl,
        "unrealized_pnl_usdc": round(unrealized_pnl_usdc, 2),
        "net_total_pnl_usdc": round(realized_pnl + unrealized_pnl_usdc, 2),
        "profit_factor": profit_factor,
        "avg_winner_usdc": avg_winner,
        "avg_loser_usdc": avg_loser,
        "avg_holding_minutes": round(mean(holding_minutes), 1) if holding_minutes else None,
        "best_trade": best_trade,
        "worst_trade": worst_trade,
        "equity_curve": curve_points,
    }


def _build_trade_analytics(trades: list[dict[str, Any]], positions: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    opens = [trade for trade in trades if trade.get("type") == "OPEN"]
    closes = rebuild_closed_trades(trades)
    opened_by_position = {
        str(trade.get("position_id", "")): trade
        for trade in opens
        if trade.get("position_id")
    }

    category_rows: dict[str, dict[str, Any]] = {}
    market_rows: dict[str, dict[str, Any]] = {}

    for position in positions:
        category = _trade_category(position, opened_by_position)
        category_rows.setdefault(
            category,
            {"category": category, "closed_trades": 0, "wins": 0, "realized_pnl_usdc": 0.0, "open_positions": 0},
        )
        category_rows[category]["open_positions"] += 1

    for trade in closes:
        pnl = _as_float(trade.get("pnl_usdc"), 0.0)
        category = _trade_category(trade, opened_by_position)
        category_entry = category_rows.setdefault(
            category,
            {"category": category, "closed_trades": 0, "wins": 0, "realized_pnl_usdc": 0.0, "open_positions": 0},
        )
        category_entry["closed_trades"] += 1
        category_entry["wins"] += 1 if pnl > 0 else 0
        category_entry["realized_pnl_usdc"] = round(category_entry["realized_pnl_usdc"] + pnl, 2)

        market_key = str(trade.get("question", "")).strip() or str(trade.get("market_id", ""))
        market_entry = market_rows.setdefault(
            market_key,
            {
                "question": market_key,
                "category": category,
                "closed_trades": 0,
                "wins": 0,
                "realized_pnl_usdc": 0.0,
            },
        )
        market_entry["closed_trades"] += 1
        market_entry["wins"] += 1 if pnl > 0 else 0
        market_entry["realized_pnl_usdc"] = round(market_entry["realized_pnl_usdc"] + pnl, 2)

    category_analytics = []
    for item in category_rows.values():
        closed_trades = int(item["closed_trades"])
        item["win_rate_pct"] = round((item["wins"] / closed_trades) * 100.0, 1) if closed_trades else None
        item["avg_pnl_usdc"] = round(item["realized_pnl_usdc"] / closed_trades, 2) if closed_trades else None
        category_analytics.append(item)
    category_analytics.sort(key=lambda item: (item["realized_pnl_usdc"], item["open_positions"]), reverse=True)

    market_analytics = []
    for item in market_rows.values():
        closed_trades = int(item["closed_trades"])
        item["win_rate_pct"] = round((item["wins"] / closed_trades) * 100.0, 1) if closed_trades else None
        item["avg_pnl_usdc"] = round(item["realized_pnl_usdc"] / closed_trades, 2) if closed_trades else None
        market_analytics.append(item)
    market_analytics.sort(key=lambda item: item["realized_pnl_usdc"], reverse=True)

    return {
        "categories": category_analytics[:12],
        "markets": market_analytics[:12],
    }


def _render_equity_curve(points: list[dict[str, Any]], bankroll_usdc: float) -> str:
    if not points:
        return "<div class='sub'>No closed trades yet.</div>"

    width = 960
    height = 220
    pad_x = 18
    pad_y = 18
    values = [bankroll_usdc] + [_as_float(point.get("equity_usdc"), bankroll_usdc) for point in points]
    min_value = min(values)
    max_value = max(values)
    if max_value == min_value:
        max_value += 1.0
        min_value -= 1.0

    def x_for(idx: int) -> float:
        if len(points) == 1:
            return width / 2
        return pad_x + ((idx - 1) / (len(points) - 1)) * (width - (pad_x * 2))

    def y_for(value: float) -> float:
        ratio = (value - min_value) / (max_value - min_value)
        return height - pad_y - (ratio * (height - (pad_y * 2)))

    polyline = " ".join(f"{x_for(idx):.1f},{y_for(_as_float(point.get('equity_usdc'), bankroll_usdc)):.1f}" for idx, point in enumerate(points, start=1))
    baseline = y_for(bankroll_usdc)
    last = points[-1]
    return (
        f"<svg viewBox='0 0 {width} {height}' class='curve' role='img' aria-label='Equity curve'>"
        f"<line x1='{pad_x}' y1='{baseline:.1f}' x2='{width - pad_x}' y2='{baseline:.1f}' class='curve-baseline' />"
        f"<polyline points='{polyline}' class='curve-line' />"
        f"<circle cx='{x_for(len(points)):.1f}' cy='{y_for(_as_float(last.get('equity_usdc'), bankroll_usdc)):.1f}' r='4' class='curve-dot' />"
        "</svg>"
    )


def _shadow_paths(suffix: str) -> dict[str, Path]:
    return {
        "positions": Path(f"state/{suffix}_positions.json"),
        "trades": Path(f"state/{suffix}_trades.json"),
        "marks": Path(f"state/{suffix}_marks.json"),
    }


def _shadow_status_path(suffix: str) -> Path:
    return Path(f"state/{suffix}_status.json")


def _ab_portfolio_specs(settings: Settings) -> list[tuple[str, str, str]]:
    specs = [
        ("crypto_5m_sniper", "ab_crypto_5m_sniper", "A/B Crypto 5m Sniper"),
        ("crypto_5m_box_arb", "ab_crypto_5m_box_arb", "A/B Crypto 5m Box Arb"),
        ("crypto_latency_5m", "ab_crypto_latency_5m", "A/B Crypto Latency 5m"),
        ("crypto_next_window_sniper", "ab_crypto_next_window_sniper", "A/B Crypto Next Window Sniper"),
        ("crypto_next_window_box_arb", "ab_crypto_next_window_box_arb", "A/B Crypto Next Window Box Arb"),
        ("crypto_intraday_scheduled", "ab_crypto_intraday_scheduled", "A/B Crypto Intraday Scheduled"),
        ("crypto_threshold_snapshot", "ab_crypto_threshold_snapshot", "A/B Crypto Threshold Snapshot"),
        ("sports_early_entry", "ab_sports_early_entry", "A/B Sports Early Entry"),
        ("penny_longshot", "ab_penny_longshot", "A/B Penny Longshot"),
        ("wallet_copy", "ab_wallet_copy", "A/B Wallet Copy"),
        ("wallet_copy_aggressive", "ab_wallet_copy_aggressive", "A/B Wallet Copy Aggressive"),
    ]
    if settings.ab_test_crypto_enabled:
        specs.append(("crypto_wallet_copy", "ab_crypto_wallet_copy", "A/B Crypto Wallet Copy"))
    if not settings.ab_test_crypto_5m_sniper_enabled:
        specs = [item for item in specs if item[0] != "crypto_5m_sniper"]
    if not settings.ab_test_crypto_5m_box_enabled:
        specs = [item for item in specs if item[0] != "crypto_5m_box_arb"]
    if not settings.ab_test_crypto_latency_5m_enabled:
        specs = [item for item in specs if item[0] != "crypto_latency_5m"]
    if not settings.ab_test_crypto_next_window_sniper_enabled:
        specs = [item for item in specs if item[0] != "crypto_next_window_sniper"]
    if not settings.ab_test_crypto_next_window_box_enabled:
        specs = [item for item in specs if item[0] != "crypto_next_window_box_arb"]
    if not settings.ab_test_crypto_intraday_scheduled_enabled:
        specs = [item for item in specs if item[0] != "crypto_intraday_scheduled"]
    if not settings.ab_test_crypto_threshold_snapshot_enabled:
        specs = [item for item in specs if item[0] != "crypto_threshold_snapshot"]
    if not settings.ab_test_sports_early_entry_enabled:
        specs = [item for item in specs if item[0] != "sports_early_entry"]
    if not settings.ab_test_penny_longshot_enabled:
        specs = [item for item in specs if item[0] != "penny_longshot"]
    if settings.ab_test_verified_public_enabled:
        specs.append(("verified_public", "ab_verified_public", "A/B Verified Public Traders"))
    return specs


def _ab_portfolio_labels() -> list[tuple[str, str]]:
    return [
        ("crypto_5m_sniper", "A/B Crypto 5m Sniper"),
        ("crypto_5m_box_arb", "A/B Crypto 5m Box Arb"),
        ("crypto_latency_5m", "A/B Crypto Latency 5m"),
        ("crypto_next_window_sniper", "A/B Crypto Next Window Sniper"),
        ("crypto_next_window_box_arb", "A/B Crypto Next Window Box Arb"),
        ("crypto_intraday_scheduled", "A/B Crypto Intraday Scheduled"),
        ("crypto_threshold_snapshot", "A/B Crypto Threshold Snapshot"),
        ("sports_early_entry", "A/B Sports Early Entry"),
        ("penny_longshot", "A/B Penny Longshot"),
        ("wallet_copy", "A/B Wallet Copy"),
        ("wallet_copy_aggressive", "A/B Wallet Copy Aggressive"),
        ("crypto_wallet_copy", "A/B Crypto Wallet Copy"),
        ("verified_public", "A/B Verified Public Traders"),
    ]


def _build_shadow_portfolio_state(name: str, bankroll_usdc: float) -> dict[str, Any]:
    paths = _shadow_paths(name)
    positions = _json_load(paths["positions"], [])
    trades = _json_load(paths["trades"], [])
    marks = _json_load(paths["marks"], {"positions": [], "summary": {}})
    runner_status = _json_load(_shadow_status_path(name), {})
    marks_by_position = {
        str(item.get("position_id", "")): item
        for item in marks.get("positions", [])
        if isinstance(item, dict) and item.get("position_id")
    }
    enriched_positions = [{**item, **marks_by_position.get(str(item.get("position_id", "")), {})} for item in positions]
    rebuilt_closes = rebuild_closed_trades(trades)
    realized_pnl = round(sum(_as_float(trade.get("pnl_usdc", 0.0)) for trade in rebuilt_closes), 2)
    unrealized_pnl = round(_as_float(marks.get("summary", {}).get("total_unrealized_pnl_usdc"), 0.0), 2)
    lifetime = _build_lifetime_stats(trades, bankroll_usdc, unrealized_pnl)
    open_started = [
        _parse_ts(str(item.get("opened_at") or ""))
        for item in enriched_positions
        if item.get("opened_at")
    ]
    open_started = [item for item in open_started if item is not None]
    now = datetime.now(timezone.utc)
    first_opened_at = min(open_started) if open_started else None
    minutes_since_first_open = (
        max((now - first_opened_at).total_seconds() / 60.0, 0.0)
        if first_opened_at is not None
        else None
    )
    avg_open_age_minutes = (
        mean(max((now - opened).total_seconds() / 60.0, 0.0) for opened in open_started)
        if open_started
        else None
    )
    return {
        "name": name,
        "positions": positions,
        "positions_enriched": enriched_positions,
        "trades": trades,
        "realized_pnl_usdc": realized_pnl,
        "unrealized_pnl_usdc": unrealized_pnl,
        "equity_including_open_usdc": round(bankroll_usdc + realized_pnl + unrealized_pnl, 2),
        "open_positions": len(positions),
        "open_notional_usdc": round(sum(float(item.get("notional_usdc", 0.0)) for item in positions), 2),
        "first_opened_at": first_opened_at.isoformat().replace("+00:00", "Z") if first_opened_at else None,
        "minutes_since_first_open": round(minutes_since_first_open, 1) if minutes_since_first_open is not None else None,
        "avg_open_age_minutes": round(avg_open_age_minutes, 1) if avg_open_age_minutes is not None else None,
        "simulated_close_now_pnl_usdc": unrealized_pnl,
        "lifetime": lifetime,
        "runner_status": runner_status,
        "recent_activity_24h": _build_recent_activity(trades),
    }


def _risk_bankroll_usdc(settings: Settings, compounded_bankroll: float, equity_including_open: float) -> float:
    if not settings.use_equity_for_risk_caps:
        return compounded_bankroll
    return round(max(min(compounded_bankroll, equity_including_open), 0.0), 2)


def _build_recent_activity(trades: list[dict[str, Any]], hours: int = 24) -> dict[str, Any]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    opens = 0
    scales = 0
    closes = 0
    realized_pnl = 0.0
    for trade in trades:
        timestamp = trade.get("opened_at") or trade.get("closed_at") or trade.get("timestamp")
        if not timestamp:
            continue
        try:
            event_time = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
        except ValueError:
            continue
        if event_time < cutoff:
            continue
        trade_type = str(trade.get("type", "")).upper()
        if trade_type == "OPEN":
            opens += 1
        elif trade_type == "SCALE":
            scales += 1
        elif trade_type == "CLOSE":
            closes += 1
            realized_pnl += _as_float(trade.get("pnl_usdc", 0.0), 0.0)
    return {
        "hours": hours,
        "opens": opens,
        "scales": scales,
        "closes": closes,
        "realized_pnl_usdc": round(realized_pnl, 2),
    }


def _build_strategy_health(
    settings: Settings,
    *,
    strategy_name: str,
    trades: list[dict[str, Any]],
    runner_status: dict[str, Any] | None,
    open_positions_count: int = 0,
    unrealized_pnl_usdc: float = 0.0,
) -> dict[str, Any]:
    lifetime_closed_trades = sum(1 for trade in trades if trade.get("type") == "CLOSE")
    lookback_hours = max(settings.strategy_health_lookback_hours, 1)
    min_closed_trades = settings.strategy_health_min_closed_trades
    min_realized_pnl = settings.strategy_health_min_realized_pnl_usdc
    warmup_note = ""
    if _is_warmup_intraday_strategy(strategy_name, lifetime_closed_trades):
        lookback_hours = max(lookback_hours, 72)
        min_closed_trades = 0
        min_realized_pnl = 0.0
        warmup_note = "warmup mode until 3 lifetime closes"
    elif strategy_name == "wallet_copy":
        min_realized_pnl = 0.0
    recent = _build_recent_activity(trades, hours=lookback_hours)
    runner_status = runner_status if isinstance(runner_status, dict) else {}
    runner_state = str(runner_status.get("status") or "").strip().lower()
    reasons: list[str] = []
    if settings.strategy_health_block_degraded and runner_state == "degraded":
        last_error = str(runner_status.get("last_error") or "").strip()
        reasons.append(f"runner degraded{f': {last_error}' if last_error else ''}")
    if strategy_name == "crypto_wallet_copy" and settings.crypto_wallet_copy_quarantined:
        reasons.append("strategy quarantined")
    if _is_warmup_intraday_strategy(strategy_name, lifetime_closed_trades):
        max_open = max(int(settings.strategy_warmup_max_open_positions), 0)
        max_drawdown = abs(float(settings.strategy_warmup_max_unrealized_drawdown_usdc))
        if max_open > 0 and open_positions_count > max_open:
            reasons.append(f"warmup open positions {open_positions_count} > {max_open}")
        if max_drawdown > 0 and unrealized_pnl_usdc <= -max_drawdown:
            reasons.append(f"warmup unrealized pnl {unrealized_pnl_usdc:.2f} <= -{max_drawdown:.2f}")
    recent_closes = int(recent.get("closes", 0) or 0)
    recent_pnl = _as_float(recent.get("realized_pnl_usdc"), 0.0)
    if recent_closes < min_closed_trades:
        reasons.append(
            f"only {recent_closes} closes in {lookback_hours}h "
            f"({min_closed_trades} required)"
        )
    if recent_pnl < min_realized_pnl:
        reasons.append(
            f"recent realized pnl {recent_pnl:.2f} < {min_realized_pnl:.2f}"
        )
    probation_allowed = _probation_reentry_allowed(
        settings,
        strategy_name,
        reasons,
        open_positions_count=open_positions_count,
    )
    blocked = bool(settings.strategy_health_gate_enabled and reasons and not probation_allowed)
    reason = "; ".join(reasons)
    if probation_allowed:
        reason = f"probation mode: {reason}"
    elif not reason and warmup_note:
        reason = warmup_note
    healthy = not blocked
    return {
        "strategy": strategy_name,
        "healthy": healthy,
        "blocked": blocked,
        "status_label": "probation" if probation_allowed else ("healthy" if healthy else "blocked"),
        "reason": reason or "-",
        "recent_realized_pnl_usdc": round(recent_pnl, 2),
        "recent_closed_trades": recent_closes,
        "open_positions_count": open_positions_count,
        "unrealized_pnl_usdc": round(unrealized_pnl_usdc, 2),
        "runner_status": str(runner_status.get("status", "unknown") or "unknown"),
        "probation_allowed": probation_allowed,
    }


def build_dashboard_state(settings: Settings) -> dict[str, Any]:
    positions = _json_load(settings.positions_path, [])
    trades = _json_load(settings.trades_path, [])
    queue = _json_load(settings.queue_path, [])
    theses = _json_load(settings.theses_path, [])
    marks = _json_load(settings.marks_path, {"positions": [], "summary": {}})
    missed = _json_load(settings.missed_opportunities_path, {"items": [], "updated_at": None})
    last_nonempty_queue = _json_load(settings.last_nonempty_queue_path, {"items": [], "updated_at": None})
    last_nonempty_theses = _json_load(settings.last_nonempty_theses_path, {"items": [], "updated_at": None})
    targets = _json_load(settings.targets_path, [])
    activity = _json_load(settings.target_activity_path, {"wallets": []})
    status = _json_load(settings.status_path, {})
    intraday_registry = _json_load(settings.intraday_registry_path, {})
    intraday_registry_rejections = _json_load(settings.intraday_registry_rejections_path, {"entries": []})
    intraday_registry_status = _json_load(Path("state/intraday_registry_status.json"), {})
    intraday_book_tape = _json_load(settings.intraday_registry_book_tape_path, {"entries": []})
    intraday_threshold_live = _json_load(settings.intraday_registry_threshold_live_path, {"entries": []})
    intraday_transition_tape = _json_load(settings.intraday_registry_transition_tape_path, {"entries": []})
    intraday_imminent_box_arb_tape = _json_load(settings.intraday_registry_imminent_box_arb_tape_path, {"entries": []})
    intraday_edge_alerts = _json_load(settings.intraday_registry_edge_alerts_path, {"entries": [], "summary": {}})
    intraday_audit_summary = _sqlite_intraday_audit_summary(settings.intraday_registry_audit_sqlite_path, 60)
    btc_5m_decision_table = _sqlite_btc_5m_decision_table(settings.intraday_registry_audit_sqlite_path, 60)
    intraday_lifecycle_summary = _sqlite_intraday_lifecycle_summary(settings.intraday_registry_audit_sqlite_path, 8)
    intraday_discovery_summary = _sqlite_intraday_discovery_summary(settings.intraday_registry_audit_sqlite_path, 8)

    rebuilt_closes = rebuild_closed_trades(trades)
    corrected_close_by_position = {
        str(trade.get("position_id", "")): trade
        for trade in rebuilt_closes
        if trade.get("position_id")
    }
    realized_pnl = round(sum(_as_float(trade.get("pnl_usdc", 0.0)) for trade in rebuilt_closes), 2)
    compounded_bankroll = round(max(settings.bankroll_usdc + realized_pnl, 0.0), 2)
    open_notional = round(sum(float(item.get("notional_usdc", 0.0)) for item in positions), 2)
    unrealized_pnl = round(_as_float(marks.get("summary", {}).get("total_unrealized_pnl_usdc"), 0.0), 2)
    equity_including_open = round(compounded_bankroll + unrealized_pnl, 2)
    risk_bankroll = _risk_bankroll_usdc(settings, compounded_bankroll, equity_including_open)
    marks_by_position = {item["position_id"]: item for item in marks.get("positions", [])}
    enriched_positions = []
    for item in positions:
        mark = marks_by_position.get(item["position_id"], {})
        enriched_positions.append({**item, **mark})

    latest_cycle = status.get("last_cycle_result", {})
    opened_this_cycle = [item for item in latest_cycle.get("trade_decisions", []) if item.get("action") == "OPEN"]
    scaled_this_cycle = [item for item in latest_cycle.get("trade_decisions", []) if item.get("action") == "SCALE"]
    lifetime = _build_lifetime_stats(trades, settings.bankroll_usdc, unrealized_pnl)
    analytics = _build_trade_analytics(trades, positions)
    active_rotation = _active_rotation_category(settings)
    ab_tests = {}
    if settings.ab_test_enabled and settings.ab_test_strategy == "wallet_copy":
        for key, suffix, _label in _ab_portfolio_specs(settings):
            ab_tests[key] = _build_shadow_portfolio_state(suffix, settings.bankroll_usdc)
            ab_tests[key]["health"] = _build_strategy_health(
                settings,
                strategy_name=key,
                trades=ab_tests[key]["trades"],
                runner_status=ab_tests[key]["runner_status"],
                open_positions_count=int(ab_tests[key].get("open_positions", 0) or 0),
                unrealized_pnl_usdc=_as_float(ab_tests[key].get("unrealized_pnl_usdc"), 0.0),
            )
    shadow_replay_keys = (
        "sports_early_entry",
        "penny_longshot",
        "crypto_latency_5m",
        "crypto_next_window_sniper",
        "crypto_intraday_scheduled",
        "crypto_threshold_snapshot",
    )
    label_lookup = dict(_ab_portfolio_labels())
    shadow_replay = []
    for key in shadow_replay_keys:
        if key not in ab_tests:
            continue
        portfolio = ab_tests[key]
        portfolio_health = portfolio.get("health", {}) if isinstance(portfolio.get("health"), dict) else {}
        runner_status = portfolio.get("runner_status", {}) if isinstance(portfolio.get("runner_status"), dict) else {}
        runner_result = runner_status.get("last_cycle_result", {}) if isinstance(runner_status.get("last_cycle_result"), dict) else {}
        shadow_replay.append(
            {
                "strategy": key,
                "label": label_lookup.get(key, key),
                "open_positions": int(portfolio.get("open_positions", 0) or 0),
                "first_opened_at": portfolio.get("first_opened_at"),
                "minutes_since_first_open": portfolio.get("minutes_since_first_open"),
                "avg_open_age_minutes": portfolio.get("avg_open_age_minutes"),
                "simulated_close_now_pnl_usdc": round(_as_float(portfolio.get("simulated_close_now_pnl_usdc"), 0.0), 2),
                "unrealized_pnl_usdc": round(_as_float(portfolio.get("unrealized_pnl_usdc"), 0.0), 2),
                "queue_count": int(runner_result.get("queue_count", 0) or 0),
                "opened_positions_count": int(runner_result.get("opened_positions_count", 0) or 0),
                "health": str(portfolio_health.get("status_label", "-")),
                "health_reason": str(portfolio_health.get("reason", "-")),
            }
        )
    display_trades = []
    for trade in trades:
        if trade.get("type") == "CLOSE":
            corrected = corrected_close_by_position.get(str(trade.get("position_id", "")))
            if corrected:
                display_trades.append({**trade, **{k: v for k, v in corrected.items() if k in {"pnl_usdc", "shares", "notional_usdc", "entry_price", "category", "side"}}})
                continue
        display_trades.append(trade)

    main_runner_stale, main_runner_freshness = _runner_is_stale(
        status.get("last_cycle_completed_at"),
        settings.dashboard_main_stale_after_minutes,
    )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "mode": settings.bot_mode,
            "starting_bankroll_usdc": settings.bankroll_usdc,
            "bankroll_usdc": compounded_bankroll,
            "compounded_bankroll_usdc": compounded_bankroll,
            "equity_including_open_usdc": equity_including_open,
            "risk_bankroll_usdc": risk_bankroll,
            "open_positions": len(positions),
            "closed_trades": len(rebuilt_closes),
            "realized_pnl_usdc": realized_pnl,
            "unrealized_pnl_usdc": unrealized_pnl,
            "open_notional_usdc": open_notional,
            "queue_count": len(queue),
            "thesis_count": len(theses),
            "last_nonempty_queue_count": len(last_nonempty_queue.get("items", [])),
            "last_nonempty_thesis_count": len(last_nonempty_theses.get("items", [])),
            "opened_this_cycle": len(opened_this_cycle),
            "scaled_this_cycle": len(scaled_this_cycle),
            "targets_count": len(targets),
            "tracked_wallets": len(activity.get("wallets", [])),
            "lifetime_win_rate_pct": lifetime["win_rate_pct"],
            "lifetime_profit_factor": lifetime["profit_factor"],
            "lifetime_net_total_pnl_usdc": lifetime["net_total_pnl_usdc"],
            "active_rotation_category": active_rotation,
            "market_cooldown_minutes": settings.market_cooldown_minutes,
            "max_position_usdc": round(risk_bankroll * settings.max_position_fraction, 2),
            "max_portfolio_usdc": round(risk_bankroll * settings.max_portfolio_fraction, 2),
            "scale_in_enabled": settings.scale_in_enabled,
        },
        "status": status,
        "status_meta": {
            "runner_stale": main_runner_stale,
            "runner_freshness": main_runner_freshness,
        },
        "positions": enriched_positions[-25:],
        "trades": display_trades[-50:],
        "queue": queue[:25],
        "missed": missed.get("items", latest_cycle.get("missed_opportunities", []))[:15],
        "theses": theses[:25],
        "last_nonempty_queue": last_nonempty_queue,
        "last_nonempty_theses": last_nonempty_theses,
        "targets": targets[:25],
        "lifetime": lifetime,
        "analytics": analytics,
        "ab_tests": ab_tests,
        "shadow_replay": shadow_replay,
        "health": {
            "primary": _build_strategy_health(
                settings,
                strategy_name="primary",
                trades=trades,
                runner_status=status,
                open_positions_count=len(positions),
                unrealized_pnl_usdc=unrealized_pnl,
            ),
            "ab_tests": {key: value.get("health", {}) for key, value in ab_tests.items()},
        },
        "intraday_registry": intraday_registry,
        "intraday_registry_status": intraday_registry_status,
        "intraday_registry_rejections": intraday_registry_rejections,
        "intraday_book_tape": intraday_book_tape,
        "intraday_threshold_live": intraday_threshold_live,
        "intraday_transition_tape": intraday_transition_tape,
        "intraday_imminent_box_arb_tape": intraday_imminent_box_arb_tape,
        "intraday_audit_summary": intraday_audit_summary,
        "btc_5m_decision_table": btc_5m_decision_table,
        "intraday_lifecycle_summary": intraday_lifecycle_summary,
        "intraday_discovery_summary": intraday_discovery_summary,
        "intraday_edge_alerts": intraday_edge_alerts,
    }


def _table(headers: list[str], rows: list[list[str]]) -> str:
    head = "".join(f"<th>{html.escape(header)}</th>" for header in headers)
    body_rows = []
    for row in rows:
        body_rows.append("<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>")
    body = "".join(body_rows) or '<tr><td colspan="99">No data</td></tr>'
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def render_dashboard_html(state: dict[str, Any]) -> str:
    summary = state["summary"]
    status = state.get("status", {})
    status_meta = state.get("status_meta", {}) if isinstance(state.get("status_meta"), dict) else {}
    lifetime = state["lifetime"]
    analytics = state["analytics"]
    ab_tests = state.get("ab_tests", {})
    health = state.get("health", {}) if isinstance(state.get("health"), dict) else {}
    intraday_registry = state.get("intraday_registry", {}) if isinstance(state.get("intraday_registry"), dict) else {}
    intraday_registry_status = state.get("intraday_registry_status", {}) if isinstance(state.get("intraday_registry_status"), dict) else {}
    intraday_registry_rejections = state.get("intraday_registry_rejections", {}) if isinstance(state.get("intraday_registry_rejections"), dict) else {}
    intraday_book_tape = state.get("intraday_book_tape", {}) if isinstance(state.get("intraday_book_tape"), dict) else {}
    intraday_threshold_live = state.get("intraday_threshold_live", {}) if isinstance(state.get("intraday_threshold_live"), dict) else {}
    intraday_transition_tape = state.get("intraday_transition_tape", {}) if isinstance(state.get("intraday_transition_tape"), dict) else {}
    intraday_imminent_box_arb_tape = state.get("intraday_imminent_box_arb_tape", {}) if isinstance(state.get("intraday_imminent_box_arb_tape"), dict) else {}
    intraday_edge_alerts = state.get("intraday_edge_alerts", {}) if isinstance(state.get("intraday_edge_alerts"), dict) else {}
    intraday_audit_summary = state.get("intraday_audit_summary", {}) if isinstance(state.get("intraday_audit_summary"), dict) else {}
    btc_5m_decision_table = state.get("btc_5m_decision_table", {}) if isinstance(state.get("btc_5m_decision_table"), dict) else {}
    intraday_lifecycle_summary = state.get("intraday_lifecycle_summary", {}) if isinstance(state.get("intraday_lifecycle_summary"), dict) else {}
    intraday_discovery_summary = state.get("intraday_discovery_summary", {}) if isinstance(state.get("intraday_discovery_summary"), dict) else {}
    registry_result = intraday_registry_status.get("last_cycle_result", {}) if isinstance(intraday_registry_status.get("last_cycle_result"), dict) else {}
    shadow_replay = state.get("shadow_replay", []) if isinstance(state.get("shadow_replay"), list) else []
    why_not_trading: list[dict[str, Any]] = []
    threshold_portfolio = ab_tests.get("crypto_threshold_snapshot", {}) if isinstance(ab_tests.get("crypto_threshold_snapshot"), dict) else {}
    threshold_recent = threshold_portfolio.get("recent_activity_24h", {}) if isinstance(threshold_portfolio.get("recent_activity_24h"), dict) else {}
    threshold_proven = bool(
        threshold_recent.get("opens", 0)
        or threshold_portfolio.get("open_positions", 0)
        or (intraday_transition_tape.get("count", 0) if isinstance(intraday_transition_tape, dict) else 0)
    )
    box_portfolio = ab_tests.get("crypto_5m_box_arb", {}) if isinstance(ab_tests.get("crypto_5m_box_arb"), dict) else {}
    box_runner = box_portfolio.get("runner_status", {}) if isinstance(box_portfolio.get("runner_status"), dict) else {}
    box_result = box_runner.get("last_cycle_result", {}) if isinstance(box_runner.get("last_cycle_result"), dict) else {}
    box_tape = box_result.get("arb_tape", {}) if isinstance(box_result.get("arb_tape"), dict) else {}
    imminent_box_count = int(intraday_imminent_box_arb_tape.get("count", 0) or 0) if isinstance(intraday_imminent_box_arb_tape, dict) else 0
    box_observed = bool(imminent_box_count or box_tape.get("candidates_seen", 0) or box_tape.get("executable_pairs", 0) or box_tape.get("executed_pairs", 0))
    edge_alert_summary = intraday_edge_alerts.get("summary", {}) if isinstance(intraday_edge_alerts.get("summary"), dict) else {}
    execution_state = str(edge_alert_summary.get("execution_state") or registry_result.get("box_execution_state") or "unknown")
    threshold_state = "threshold path active" if (
        threshold_recent.get("opens", 0)
        or threshold_portfolio.get("open_positions", 0)
    ) else (
        "threshold eligible, waiting" if (
            int(registry_result.get("threshold_live_markets", 0) or 0)
            or (intraday_transition_tape.get("count", 0) if isinstance(intraday_transition_tape, dict) else 0)
        ) else "no threshold transitions"
    )
    badges = "".join(
        [
            "<div class='panel' style='margin-bottom:16px;'><h2>Live Status</h2><div class='mini-grid'>",
            f"<div class='mini-card'><div class='label'>Threshold Path</div><div class='value'>{'proven' if threshold_proven else 'not yet proven'}</div></div>",
            f"<div class='mini-card'><div class='label'>5m Box Arb</div><div class='value'>{'observed' if box_observed else 'not yet observed'}</div></div>",
            "</div></div>",
        ]
    )
    execution_state_table = _table(
        ["Signal", "Value"],
        [
            ["5m Box State", html.escape(execution_state)],
            ["Threshold State", html.escape(threshold_state)],
            ["Positive Gross Edge", html.escape(_fmt_num(edge_alert_summary.get("positive_gross_edge_count", 0)))],
            ["Positive Net Edge", html.escape(_fmt_num(edge_alert_summary.get("positive_net_edge_count", 0)))],
            ["Max Gross Edge / Share", html.escape(_fmt_money(_as_float(edge_alert_summary.get("max_gross_edge_per_share"), 0.0)))],
            ["Max Net Edge / Share", html.escape(_fmt_money(_as_float(edge_alert_summary.get("max_net_edge_per_share"), 0.0)))],
        ],
    )
    cards = "".join(
        [
            f"<div class='card'><div class='label'>Mode</div><div class='value'>{html.escape(str(summary['mode']))}</div></div>",
            f"<div class='card'><div class='label'>Starting Bankroll</div><div class='value'>{_fmt_money(summary['starting_bankroll_usdc'])}</div></div>",
            f"<div class='card'><div class='label'>Compounded Bankroll</div><div class='value'>{_fmt_money(summary['compounded_bankroll_usdc'])}</div></div>",
            f"<div class='card'><div class='label'>Realized PnL</div><div class='value'>{_fmt_money(summary['realized_pnl_usdc'])}</div></div>",
            f"<div class='card'><div class='label'>Unrealized PnL</div><div class='value'>{_fmt_money(summary['unrealized_pnl_usdc'])}</div></div>",
            f"<div class='card'><div class='label'>Net Total PnL</div><div class='value'>{_fmt_money(summary['lifetime_net_total_pnl_usdc'])}</div></div>",
            f"<div class='card'><div class='label'>Equity incl. Open</div><div class='value'>{_fmt_money(summary['equity_including_open_usdc'])}</div></div>",
            f"<div class='card'><div class='label'>Risk Bankroll</div><div class='value'>{_fmt_money(summary['risk_bankroll_usdc'])}</div></div>",
            f"<div class='card'><div class='label'>Open Capital</div><div class='value'>{_fmt_money(summary['open_notional_usdc'])}</div></div>",
            f"<div class='card'><div class='label'>Open Positions</div><div class='value'>{summary['open_positions']}</div></div>",
            f"<div class='card'><div class='label'>Latest Queue</div><div class='value'>{summary['queue_count']}</div></div>",
            f"<div class='card'><div class='label'>Latest Theses</div><div class='value'>{summary['thesis_count']}</div></div>",
            f"<div class='card'><div class='label'>Last Non-Empty Queue</div><div class='value'>{summary['last_nonempty_queue_count']}</div></div>",
            f"<div class='card'><div class='label'>Opened This Cycle</div><div class='value'>{summary['opened_this_cycle']}</div></div>",
            f"<div class='card'><div class='label'>Scaled This Cycle</div><div class='value'>{summary['scaled_this_cycle']}</div></div>",
            f"<div class='card'><div class='label'>Lifetime Win Rate</div><div class='value'>{html.escape(_fmt_pct(summary['lifetime_win_rate_pct']))}</div></div>",
            f"<div class='card'><div class='label'>Profit Factor</div><div class='value'>{html.escape(_fmt_num(summary['lifetime_profit_factor']) if summary['lifetime_profit_factor'] is not None else '-')}</div></div>",
            f"<div class='card'><div class='label'>Active Rotation</div><div class='value'>{html.escape(str(summary['active_rotation_category'] or 'all'))}</div></div>",
            f"<div class='card'><div class='label'>Cooldown</div><div class='value'>{html.escape(str(summary['market_cooldown_minutes']))}m</div></div>",
            f"<div class='card'><div class='label'>Max Position</div><div class='value'>{_fmt_money(summary['max_position_usdc'])}</div></div>",
            f"<div class='card'><div class='label'>Max Portfolio</div><div class='value'>{_fmt_money(summary['max_portfolio_usdc'])}</div></div>",
            f"<div class='card'><div class='label'>Scale-In</div><div class='value'>{'on' if summary['scale_in_enabled'] else 'off'}</div></div>",
            f"<div class='card'><div class='label'>Targets</div><div class='value'>{summary['targets_count']}</div></div>",
        ]
    )
    for key, label in _ab_portfolio_labels():
        if key not in ab_tests:
            continue
        portfolio = ab_tests[key]
        cards += "".join(
            [
                f"<div class='card'><div class='label'>{label} PnL</div><div class='value'>{_fmt_money(portfolio['lifetime']['net_total_pnl_usdc'])}</div></div>",
                f"<div class='card'><div class='label'>{label} Win Rate</div><div class='value'>{html.escape(_fmt_pct(portfolio['lifetime']['win_rate_pct']))}</div></div>",
                f"<div class='card'><div class='label'>{label} Open</div><div class='value'>{portfolio['open_positions']}</div></div>",
            ]
        )

    lifetime_table = _table(
        ["Metric", "Value"],
        [
            ["Starting Bankroll", html.escape(_fmt_money(summary["starting_bankroll_usdc"]))],
            ["Compounded Bankroll", html.escape(_fmt_money(summary["compounded_bankroll_usdc"]))],
            ["Equity Including Open PnL", html.escape(_fmt_money(summary["equity_including_open_usdc"]))],
            ["Total Opens", html.escape(_fmt_num(lifetime["opened_count"]))],
            ["Total Closes", html.escape(_fmt_num(lifetime["closed_count"]))],
            ["Winners / Losers / Flat", html.escape(f"{lifetime['winners_count']} / {lifetime['losers_count']} / {lifetime['breakeven_count']}")],
            ["Win Rate", html.escape(_fmt_pct(lifetime["win_rate_pct"]))],
            ["Gross Profit", html.escape(_fmt_money(lifetime["gross_profit_usdc"]))],
            ["Gross Loss", html.escape(_fmt_money(-lifetime["gross_loss_usdc"]))],
            ["Profit Factor", html.escape(_fmt_num(lifetime["profit_factor"]) if lifetime["profit_factor"] is not None else "-")],
            ["Average Winner", html.escape(_fmt_money(lifetime["avg_winner_usdc"]) if lifetime["avg_winner_usdc"] is not None else "-")],
            ["Average Loser", html.escape(_fmt_money(lifetime["avg_loser_usdc"]) if lifetime["avg_loser_usdc"] is not None else "-")],
            ["Average Hold", html.escape(f"{_fmt_num(lifetime['avg_holding_minutes'])} min" if lifetime["avg_holding_minutes"] is not None else "-")],
            [
                "Best Trade",
                html.escape(
                    f"{lifetime['best_trade'].get('question', '')} ({_fmt_money(_as_float(lifetime['best_trade'].get('pnl_usdc')) )})"
                    if lifetime["best_trade"]
                    else "-"
                ),
            ],
            [
                "Worst Trade",
                html.escape(
                    f"{lifetime['worst_trade'].get('question', '')} ({_fmt_money(_as_float(lifetime['worst_trade'].get('pnl_usdc')) )})"
                    if lifetime["worst_trade"]
                    else "-"
                ),
            ],
        ],
    )

    positions_table = _table(
        ["Question", "Category", "Side", "Entry", "Mark", "uPnL", "Notional", "Opened"],
        [
            [
                html.escape(str(item.get("question", ""))),
                html.escape(_display_category(item)),
                html.escape(str(item.get("side", ""))),
                f"{float(item.get('entry_price', 0.0)):.3f}",
                f"{float(item.get('current_midpoint', item.get('entry_price', 0.0))):.3f}",
                _fmt_money(float(item.get("unrealized_pnl_usdc", 0.0))),
                _fmt_money(float(item.get("notional_usdc", 0.0))),
                html.escape(_fmt_ts(item.get("opened_at"))),
            ]
            for item in reversed(state["positions"])
        ],
    )
    trades_table = _table(
        ["Type", "Question", "Category", "Notional", "PnL", "Reason", "Time"],
        [
            [
                html.escape(str(item.get("type", ""))),
                html.escape(str(item.get("question", ""))),
                html.escape(_display_category(item)),
                _fmt_money(float(item.get("add_notional_usdc", item.get("notional_usdc", 0.0)) or 0.0)),
                _fmt_money(float(item.get("pnl_usdc", 0.0))),
                html.escape(str(item.get("reason", ""))),
                html.escape(_fmt_ts(item.get("closed_at") or item.get("scaled_at") or item.get("opened_at"))),
            ]
            for item in reversed(state["trades"])
        ],
    )
    queue_table = _table(
        ["Question", "Category", "Mid", "Depth", "Hours", "Spread", "Score"],
        [
            [
                html.escape(str(item.get("question", ""))),
                html.escape(_display_category(item)),
                f"{float(item.get('midpoint', 0.0)):.3f}",
                _fmt_money(min(float(item.get("bids_depth", 0.0)), float(item.get("asks_depth", 0.0)))),
                f"{float(item.get('hours_to_resolution', 0.0)):.1f}",
                f"{float(item.get('spread', 0.0)):.3f}",
                f"{float(item.get('priority_score', 0.0)):.2f}",
            ]
            for item in state["queue"]
        ],
    )
    last_nonempty_queue_table = _table(
        ["Question", "Category", "Mid", "Depth", "Hours", "Spread", "Score"],
        [
            [
                html.escape(str(item.get("question", ""))),
                html.escape(_display_category(item)),
                f"{float(item.get('midpoint', 0.0)):.3f}",
                _fmt_money(min(float(item.get("bids_depth", 0.0)), float(item.get("asks_depth", 0.0)))),
                f"{float(item.get('hours_to_resolution', 0.0)):.1f}",
                f"{float(item.get('spread', 0.0)):.3f}",
                f"{float(item.get('priority_score', 0.0)):.2f}",
            ]
            for item in state["last_nonempty_queue"].get("items", [])[:25]
        ],
    )
    missed_table = _table(
        ["Question", "Category", "Reason", "Mid", "Score"],
        [
            [
                html.escape(str(item.get("question", ""))),
                html.escape(_display_category(item)),
                html.escape(str(item.get("reason", ""))),
                f"{float(item.get('midpoint', 0.0)):.3f}",
                f"{float(item.get('priority_score', 0.0)):.2f}",
            ]
            for item in state["missed"]
        ],
    )
    category_analytics_table = _table(
        ["Category", "Closed", "Open", "Win Rate", "Realized", "Avg/Trade"],
        [
            [
                html.escape(str(item.get("category", "unknown"))),
                html.escape(_fmt_num(item.get("closed_trades", 0))),
                html.escape(_fmt_num(item.get("open_positions", 0))),
                html.escape(_fmt_pct(item.get("win_rate_pct"))),
                html.escape(_fmt_money(_as_float(item.get("realized_pnl_usdc"), 0.0))),
                html.escape(_fmt_money(_as_float(item.get("avg_pnl_usdc"), 0.0)) if item.get("avg_pnl_usdc") is not None else "-"),
            ]
            for item in analytics["categories"]
        ],
    )
    market_analytics_table = _table(
        ["Market", "Category", "Closed", "Win Rate", "Realized", "Avg/Trade"],
        [
            [
                html.escape(str(item.get("question", ""))),
                html.escape(str(item.get("category", "unknown"))),
                html.escape(_fmt_num(item.get("closed_trades", 0))),
                html.escape(_fmt_pct(item.get("win_rate_pct"))),
                html.escape(_fmt_money(_as_float(item.get("realized_pnl_usdc"), 0.0))),
                html.escape(_fmt_money(_as_float(item.get("avg_pnl_usdc"), 0.0)) if item.get("avg_pnl_usdc") is not None else "-"),
            ]
            for item in analytics["markets"]
        ],
    )
    targets_table = _table(
        ["Wallet", "Username", "PnL", "Volume", "Rank"],
        [
            [
                html.escape(str(item.get("wallet", ""))),
                html.escape(str(item.get("username", ""))),
                _fmt_money(float(item.get("total_pnl", 0.0))),
                _fmt_money(float(item.get("volume", item.get("notional_usdc", 0.0)))),
                html.escape(str(item.get("rank", ""))),
            ]
            for item in state["targets"]
        ],
    )
    ab_panels = []
    for key, label in _ab_portfolio_labels():
        if key not in ab_tests:
            continue
        portfolio = ab_tests[key]
        portfolio_health = portfolio.get("health", {}) if isinstance(portfolio.get("health"), dict) else {}
        runner_status = portfolio.get("runner_status", {}) if isinstance(portfolio.get("runner_status"), dict) else {}
        runner_result = runner_status.get("last_cycle_result", {}) if isinstance(runner_status.get("last_cycle_result"), dict) else {}
        recent_activity = portfolio.get("recent_activity_24h", {}) if isinstance(portfolio.get("recent_activity_24h"), dict) else {}
        activity_cards = (
            '<div class="mini-grid">'
            f"<div class='mini-card'><div class='label'>Last 24h Opens</div><div class='value'>{recent_activity.get('opens', 0)}</div></div>"
            f"<div class='mini-card'><div class='label'>Last 24h Scales</div><div class='value'>{recent_activity.get('scales', 0)}</div></div>"
            f"<div class='mini-card'><div class='label'>Last 24h Closes</div><div class='value'>{recent_activity.get('closes', 0)}</div></div>"
            f"<div class='mini-card'><div class='label'>Last 24h PnL</div><div class='value'>{html.escape(_fmt_money(recent_activity.get('realized_pnl_usdc', 0.0)))}</div></div>"
            "</div>"
        )
        table = _table(
            ["Metric", "Value"],
            [
                ["Strategy", key],
                ["Realized PnL", html.escape(_fmt_money(portfolio["realized_pnl_usdc"]))],
                ["Unrealized PnL", html.escape(_fmt_money(portfolio["unrealized_pnl_usdc"]))],
                ["Net Total PnL", html.escape(_fmt_money(portfolio["lifetime"]["net_total_pnl_usdc"]))],
                ["Open Positions", html.escape(_fmt_num(portfolio["open_positions"]))],
                ["Open Capital", html.escape(_fmt_money(portfolio["open_notional_usdc"]))],
                ["First Open", html.escape(_fmt_ts(portfolio.get("first_opened_at")))],
                ["Since First Open", html.escape(_fmt_age_minutes(portfolio.get("minutes_since_first_open")))],
                ["Avg Open Age", html.escape(_fmt_age_minutes(portfolio.get("avg_open_age_minutes")))],
                ["Warm-up Unrealized PnL", html.escape(_fmt_money(_as_float(portfolio_health.get("unrealized_pnl_usdc"), portfolio["unrealized_pnl_usdc"])))],
                ["Simulated Close-Now PnL", html.escape(_fmt_money(_as_float(portfolio.get("simulated_close_now_pnl_usdc"), 0.0)))],
                ["Win Rate", html.escape(_fmt_pct(portfolio["lifetime"]["win_rate_pct"]))],
                ["Profit Factor", html.escape(_fmt_num(portfolio["lifetime"]["profit_factor"]) if portfolio["lifetime"]["profit_factor"] is not None else "-")],
                ["Total Closes", html.escape(_fmt_num(portfolio["lifetime"]["closed_count"]))],
                ["Health", html.escape(str(portfolio_health.get("status_label", "-")))],
                ["Health Reason", html.escape(str(portfolio_health.get("reason", "-")))],
                ["Runner Status", html.escape(str(runner_status.get("status", "-")))],
                ["Cycle Started", html.escape(_fmt_ts(runner_status.get("last_cycle_started_at")))],
                ["Cycle Completed", html.escape(_fmt_ts(runner_status.get("last_cycle_completed_at")))],
                ["Cycle Mode", html.escape("full_sync" if runner_status.get("full_sync") else ("light" if runner_status.get("cycle_index") else "-"))],
                ["Cycle Opens / Scales", html.escape(f"{runner_result.get('opened_positions_count', 0)} / {runner_result.get('scaled_positions_count', 0)}")],
                ["Last 24h Opens / Scales / Closes", html.escape(f"{recent_activity.get('opens', 0)} / {recent_activity.get('scales', 0)} / {recent_activity.get('closes', 0)}")],
                ["Last 24h Realized PnL", html.escape(_fmt_money(recent_activity.get("realized_pnl_usdc", 0.0)))],
                ["Runner Error", html.escape(str(runner_status.get("last_error") or "-"))],
            ],
        )
        extra_detail_html = ""
        if key == "crypto_wallet_copy":
            crypto_positions_table = _table(
                ["Question", "Side", "Entry", "Mark", "UPnL", "Notional", "Opened"],
                [
                    [
                        html.escape(str(item.get("question", ""))),
                        html.escape(str(item.get("side", ""))),
                        f"{_as_float(item.get('entry_price'), 0.0):.3f}",
                        f"{_as_float(item.get('current_midpoint'), _as_float(item.get('entry_price'), 0.0)):.3f}",
                        html.escape(_fmt_money(_as_float(item.get("unrealized_pnl_usdc"), 0.0))),
                        html.escape(_fmt_money(_as_float(item.get("notional_usdc"), 0.0))),
                        html.escape(_fmt_ts(str(item.get("opened_at", "")) or None)),
                    ]
                    for item in portfolio.get("positions_enriched", [])[:12]
                ],
            )
            crypto_trades_table = _table(
                ["Type", "Question", "PnL", "Reason", "Time"],
                [
                    [
                        html.escape(str(item.get("type", ""))),
                        html.escape(str(item.get("question", ""))),
                        html.escape(_fmt_money(_as_float(item.get("pnl_usdc"), 0.0))),
                        html.escape(str(item.get("reason", ""))),
                        html.escape(_fmt_ts(str(item.get("closed_at") or item.get("opened_at") or "") or None)),
                    ]
                    for item in portfolio.get("trades", [])[-12:][::-1]
                ],
            )
            extra_detail_html = (
                "<div class='subpanel-grid'>"
                "<div><h3>Crypto Open Positions</h3>"
                + crypto_positions_table
                + "</div>"
                "<div><h3>Crypto Recent Trades</h3>"
                + crypto_trades_table
                + "</div>"
                "</div>"
            )
        ab_panels.append(
            '<div class="panel"><h2>'
            + label
            + "</h2>"
            + activity_cards
            + table
            + extra_detail_html
            + _render_equity_curve(portfolio["lifetime"]["equity_curve"], summary["starting_bankroll_usdc"])
            + "</div>"
        )
    rejection_entries = intraday_registry_rejections.get("entries", []) if isinstance(intraday_registry_rejections.get("entries"), list) else []
    intraday_registry_html = ""
    if intraday_registry or intraday_registry_status or rejection_entries:
        tape_entries = intraday_book_tape.get("entries", []) if isinstance(intraday_book_tape.get("entries"), list) else []
        threshold_live_entries = intraday_threshold_live.get("entries", []) if isinstance(intraday_threshold_live.get("entries"), list) else []
        transition_tape_entries = intraday_transition_tape.get("entries", []) if isinstance(intraday_transition_tape.get("entries"), list) else []
        imminent_box_entries = intraday_imminent_box_arb_tape.get("entries", []) if isinstance(intraday_imminent_box_arb_tape.get("entries"), list) else []
        edge_alert_entries = intraday_edge_alerts.get("entries", []) if isinstance(intraday_edge_alerts.get("entries"), list) else []
        registry_table = _table(
            ["Metric", "Value"],
            [
                ["Runner Status", html.escape(str(intraday_registry_status.get("status", "-")))],
                ["Cycle Started", html.escape(_fmt_ts(intraday_registry_status.get("last_cycle_started_at")))],
                ["Cycle Completed", html.escape(_fmt_ts(intraday_registry_status.get("last_cycle_completed_at")))],
                ["Registry Markets", html.escape(_fmt_num(intraday_registry.get("market_count", 0)))],
                ["WS Connected", html.escape(str(registry_result.get("websocket_connected", False)).lower())],
                ["WS Messages", html.escape(_fmt_num(registry_result.get("websocket_messages", 0)))],
                ["WS New Markets", html.escape(_fmt_num(registry_result.get("websocket_new_markets", 0)))],
                ["WS Updated Markets", html.escape(_fmt_num(registry_result.get("websocket_updated_markets", 0)))],
                ["WS Enriched", html.escape(_fmt_num(registry_result.get("websocket_enriched_markets", 0)))],
                ["Book Snapshots", html.escape(_fmt_num(registry_result.get("book_snapshots_recorded", 0)))],
                ["Snapshots With Mid", html.escape(_fmt_num(registry_result.get("book_snapshots_with_midpoint", 0)))],
                ["Snapshots With Depth", html.escape(_fmt_num(registry_result.get("book_snapshots_with_depth", 0)))],
                ["Threshold Live Markets", html.escape(_fmt_num(registry_result.get("threshold_live_markets", 0)))],
                ["Transition Tape Entries", html.escape(_fmt_num(registry_result.get("transition_tape_entries", 0)))],
                ["Imminent Box Arb Entries", html.escape(_fmt_num(registry_result.get("imminent_box_arb_entries", 0)))],
                ["Watchlist Markets", html.escape(_fmt_num(registry_result.get("watchlist_market_count", 0)))],
                ["Watchlist Up/Down", html.escape(_fmt_num(registry_result.get("watchlist_updown_count", 0)))],
                ["Imminent Up/Down", html.escape(_fmt_num(registry_result.get("imminent_updown_count", 0)))],
                ["Gamma Tag Candidates", html.escape(_fmt_num(registry_result.get("gamma_tag_candidate_count", 0)))],
                ["Gamma Live Upcoming", html.escape(_fmt_num(registry_result.get("gamma_live_upcoming_count", 0)))],
                ["Gamma Near-Term", html.escape(_fmt_num(registry_result.get("gamma_near_term_count", 0)))],
                ["Gamma Raw Fetched", html.escape(_fmt_num(registry_result.get("gamma_refresh_raw_fetched_markets", 0)))],
                ["Watchlist Horizon", html.escape(f"{_fmt_num(registry_result.get('watchlist_max_minutes_to_resolution', 0))}m")],
                ["Watchlist -> Threshold", html.escape(_fmt_num(registry_result.get("watchlist_threshold_transition_count", 0)))],
                ["Watchlist -> Imminent", html.escape(_fmt_num(registry_result.get("watchlist_imminent_transition_count", 0)))],
                ["Runner Error", html.escape(str(intraday_registry_status.get("last_error") or registry_result.get("websocket_error") or "-"))],
            ],
        )
        tape_table = _table(
            ["Seen", "Question", "Hours", "Mid", "Depth", "Spread"],
            [
                [
                    html.escape(_fmt_ts(item.get("seen_at"))),
                    html.escape(str(item.get("question", ""))),
                    html.escape(_fmt_num(item.get("hours_to_resolution", "-")) if item.get("hours_to_resolution") not in (None, "") else "-"),
                    html.escape(_fmt_num(item.get("midpoint", "-")) if item.get("midpoint") not in (None, "") else "-"),
                    html.escape(_fmt_money(item.get("min_depth_usdc", 0.0))),
                    html.escape(_fmt_num(item.get("spread", "-")) if item.get("spread") not in (None, "") else "-"),
                ]
                for item in reversed(tape_entries[-15:])
            ],
        )
        threshold_live_table = _table(
            ["Question", "Hours", "Mid", "Depth", "Seen"],
            [
                [
                    html.escape(str(item.get("question", ""))),
                    html.escape(_fmt_num(item.get("hours_to_resolution", "-")) if item.get("hours_to_resolution") not in (None, "") else "-"),
                    html.escape(_fmt_num(item.get("midpoint", "-")) if item.get("midpoint") not in (None, "") else "-"),
                    html.escape(_fmt_money(item.get("min_depth_usdc", 0.0))),
                    html.escape(_fmt_ts(item.get("seen_at"))),
                ]
                for item in threshold_live_entries[:15]
            ],
        )
        transition_tape_table = _table(
            ["Stage", "Question", "Hours", "Mid", "Depth", "Spread", "Seen"],
            [
                [
                    html.escape(str(item.get("transition_stage", ""))),
                    html.escape(str(item.get("question", ""))),
                    html.escape(_fmt_num(item.get("hours_to_resolution", "-")) if item.get("hours_to_resolution") not in (None, "") else "-"),
                    html.escape(_fmt_num(item.get("midpoint", "-")) if item.get("midpoint") not in (None, "") else "-"),
                    html.escape(_fmt_money(item.get("min_depth_usdc", 0.0))),
                    html.escape(_fmt_num(item.get("spread", "-")) if item.get("spread") not in (None, "") else "-"),
                    html.escape(_fmt_ts(item.get("seen_at"))),
                ]
                for item in transition_tape_entries[:15]
            ],
        )
        imminent_box_tape_table = _table(
            ["Question", "Hours", "Yes Ask", "No Ask", "Gross Edge", "Net Edge", "Depth", "Seen"],
            [
                [
                    html.escape(str(item.get("question", ""))),
                    html.escape(_fmt_num(item.get("hours_to_resolution", "-")) if item.get("hours_to_resolution") not in (None, "") else "-"),
                    html.escape(_fmt_num(item.get("yes_contract_price", "-")) if item.get("yes_contract_price") not in (None, "") else "-"),
                    html.escape(_fmt_num(item.get("no_contract_price", "-")) if item.get("no_contract_price") not in (None, "") else "-"),
                    html.escape(_fmt_money(_as_float(item.get("gross_edge_per_share"), 0.0))),
                    html.escape(_fmt_money(_as_float(item.get("net_edge_per_share"), 0.0))),
                    html.escape(_fmt_money(item.get("min_depth_usdc", 0.0))),
                    html.escape(_fmt_ts(item.get("seen_at"))),
                ]
                for item in imminent_box_entries[:15]
            ],
        )
        edge_alerts_table = _table(
            ["Seen", "Event", "Message"],
            [
                [
                    html.escape(_fmt_ts(item.get("seen_at"))),
                    html.escape(str(item.get("event_type", ""))),
                    html.escape(str(item.get("message", ""))),
                ]
                for item in reversed(edge_alert_entries[-15:])
            ],
        )
        audit_table = _table(
            ["Metric", "Value"],
            [
                ["Lookback", html.escape(f"{int(intraday_audit_summary.get('lookback_minutes', 60) or 60)}m")],
                ["DB Present", html.escape("true" if intraday_audit_summary.get("db_exists") else "false")],
                ["WS Events", html.escape(_fmt_num(intraday_audit_summary.get("ws_event_count", 0)))],
                ["Book Snapshots", html.escape(_fmt_num(intraday_audit_summary.get("book_snapshot_count", 0)))],
                ["Watchlist Imminent Candidates", html.escape(_fmt_num(intraday_audit_summary.get("watchlist_imminent_candidate_count", 0)))],
                ["Raw Imminent 5m Candidates", html.escape(_fmt_num(intraday_audit_summary.get("raw_imminent_candidate_count", 0)))],
                ["Maker Box Simulations", html.escape(_fmt_num(intraday_audit_summary.get("maker_box_simulation_count", 0)))],
                ["Aggressive Box Sims", html.escape(_fmt_num(intraday_audit_summary.get("aggressive_box_simulation_count", 0)))],
                ["Arb Observations", html.escape(_fmt_num(intraday_audit_summary.get("arb_opportunity_count", 0)))],
                ["Positive Gross Edge", html.escape(_fmt_num(intraday_audit_summary.get("positive_gross_edge_count", 0)))],
                ["Positive Net Edge", html.escape(_fmt_num(intraday_audit_summary.get("positive_net_edge_count", 0)))],
                ["Maker Positive Gross", html.escape(_fmt_num(intraday_audit_summary.get("maker_positive_gross_edge_count", 0)))],
                ["Maker Positive Net", html.escape(_fmt_num(intraday_audit_summary.get("maker_positive_net_edge_count", 0)))],
                ["Aggressive Pos Gross", html.escape(_fmt_num(intraday_audit_summary.get("aggressive_positive_gross_edge_count", 0)))],
                ["Aggressive Pos Net", html.escape(_fmt_num(intraday_audit_summary.get("aggressive_positive_net_edge_count", 0)))],
                ["Aggressive Pos Exp Net", html.escape(_fmt_num(intraday_audit_summary.get("aggressive_positive_expected_net_edge_count", 0)))],
                ["Max Gross Edge / Share", html.escape(_fmt_money(_as_float(intraday_audit_summary.get("max_gross_edge_per_share"), 0.0)))],
                ["Max Net Edge / Share", html.escape(_fmt_money(_as_float(intraday_audit_summary.get("max_net_edge_per_share"), 0.0)))],
                ["Maker Max Gross / Share", html.escape(_fmt_money(_as_float(intraday_audit_summary.get("maker_max_gross_edge_per_share"), 0.0)))],
                ["Maker Max Net / Share", html.escape(_fmt_money(_as_float(intraday_audit_summary.get("maker_max_net_edge_per_share"), 0.0)))],
                ["Aggressive Max Gross / Share", html.escape(_fmt_money(_as_float(intraday_audit_summary.get("aggressive_max_gross_edge_per_share"), 0.0)))],
                ["Aggressive Max Net / Share", html.escape(_fmt_money(_as_float(intraday_audit_summary.get("aggressive_max_net_edge_per_share"), 0.0)))],
                ["Aggressive Max Exp Net / Share", html.escape(_fmt_money(_as_float(intraday_audit_summary.get("aggressive_max_expected_net_edge_per_share"), 0.0)))],
                ["BTC Raw Imminent", html.escape(_fmt_num(intraday_audit_summary.get("btc_raw_imminent_candidate_count", 0)))],
                ["BTC <= 1.02", html.escape(_fmt_num(intraday_audit_summary.get("btc_le_1_02_count", 0)))],
                ["BTC <= 1.01", html.escape(_fmt_num(intraday_audit_summary.get("btc_le_1_01_count", 0)))],
                ["BTC <= 1.005", html.escape(_fmt_num(intraday_audit_summary.get("btc_le_1_005_count", 0)))],
                ["BTC = 1.00", html.escape(_fmt_num(intraday_audit_summary.get("btc_eq_1_00_count", 0)))],
                ["BTC Best Combo", html.escape(_fmt_money(_as_float(intraday_audit_summary.get("btc_best_combined_contract_price"), 0.0)))],
                ["BTC Maker <= 1.00", html.escape(_fmt_num(intraday_audit_summary.get("btc_maker_le_1_00_count", 0)))],
                ["BTC Maker Pos Gross", html.escape(_fmt_num(intraday_audit_summary.get("btc_maker_positive_gross_count", 0)))],
                ["BTC Maker Pos Net", html.escape(_fmt_num(intraday_audit_summary.get("btc_maker_positive_net_count", 0)))],
                ["BTC Maker Best Combo", html.escape(_fmt_money(_as_float(intraday_audit_summary.get("btc_maker_best_combined_contract_price"), 0.0)))],
                ["BTC Aggressive <= 1.00", html.escape(_fmt_num(intraday_audit_summary.get("btc_aggressive_le_1_00_count", 0)))],
                ["BTC Aggressive Pos Gross", html.escape(_fmt_num(intraday_audit_summary.get("btc_aggressive_positive_gross_count", 0)))],
                ["BTC Aggressive Pos Net", html.escape(_fmt_num(intraday_audit_summary.get("btc_aggressive_positive_net_count", 0)))],
                ["BTC Aggressive Pos Exp Net", html.escape(_fmt_num(intraday_audit_summary.get("btc_aggressive_positive_expected_net_count", 0)))],
                ["BTC Aggressive Best Combo", html.escape(_fmt_money(_as_float(intraday_audit_summary.get("btc_aggressive_best_combined_contract_price"), 0.0)))],
                ["Fee Sensitivity Rebate", html.escape(_fmt_num(intraday_audit_summary.get("btc_aggressive_rebate_pos_expected_count", 0)))],
                ["Fee Sensitivity Flat", html.escape(_fmt_num(intraday_audit_summary.get("btc_aggressive_flat_pos_expected_count", 0)))],
                ["Fee Sensitivity Low Fee", html.escape(_fmt_num(intraday_audit_summary.get("btc_aggressive_low_fee_pos_expected_count", 0)))],
                ["Fee Sensitivity High Fee", html.escape(_fmt_num(intraday_audit_summary.get("btc_aggressive_high_fee_pos_expected_count", 0)))],
                ["Latest WS Event", html.escape(_fmt_ts(intraday_audit_summary.get("latest_ws_event_at")))],
                ["Latest Book Snapshot", html.escape(_fmt_ts(intraday_audit_summary.get("latest_book_snapshot_at")))],
                ["Latest Watchlist Imminent", html.escape(_fmt_ts(intraday_audit_summary.get("latest_watchlist_imminent_at")))],
                ["Latest Raw Imminent", html.escape(_fmt_ts(intraday_audit_summary.get("latest_raw_imminent_at")))],
                ["Latest Maker Sim", html.escape(_fmt_ts(intraday_audit_summary.get("latest_maker_simulation_at")))],
                ["Latest Aggressive Sim", html.escape(_fmt_ts(intraday_audit_summary.get("latest_aggressive_simulation_at")))],
                ["Latest Arb Observation", html.escape(_fmt_ts(intraday_audit_summary.get("latest_arb_observation_at")))],
                ["Latest Positive Gross", html.escape(_fmt_ts(intraday_audit_summary.get("latest_positive_gross_at")))],
                ["Latest Positive Net", html.escape(_fmt_ts(intraday_audit_summary.get("latest_positive_net_at")))],
                ["Last Registry Cycle", html.escape(_fmt_ts(intraday_audit_summary.get("last_registry_cycle_at")))],
            ],
        )
        btc_decision_rows = btc_5m_decision_table.get("rows", []) if isinstance(btc_5m_decision_table.get("rows"), list) else []
        btc_decision_table = _table(
            ["Fill Prob", "Rebate", "Flat", "Low Fee", "High Fee"],
            [
                [
                    html.escape(f"{int(round(_as_float(item.get('fill_probability'), 0.0) * 100))}%"),
                    html.escape(
                        f"{_fmt_num(item.get('rebate_positive_expected_count', 0))} / {_fmt_money(_as_float(item.get('rebate_max_expected_net_edge'), 0.0))}"
                    ),
                    html.escape(
                        f"{_fmt_num(item.get('flat_positive_expected_count', 0))} / {_fmt_money(_as_float(item.get('flat_max_expected_net_edge'), 0.0))}"
                    ),
                    html.escape(
                        f"{_fmt_num(item.get('low_fee_positive_expected_count', 0))} / {_fmt_money(_as_float(item.get('low_fee_max_expected_net_edge'), 0.0))}"
                    ),
                    html.escape(
                        f"{_fmt_num(item.get('high_fee_positive_expected_count', 0))} / {_fmt_money(_as_float(item.get('high_fee_max_expected_net_edge'), 0.0))}"
                    ),
                ]
                for item in btc_decision_rows
            ],
        )
        lifecycle_table = _table(
            ["Metric", "Value"],
            [
                ["Tracked Markets", html.escape(_fmt_num(intraday_lifecycle_summary.get("total_markets", 0)))],
                ["Up/Down Markets", html.escape(_fmt_num(intraday_lifecycle_summary.get("updown_markets", 0)))],
                ["Threshold Markets", html.escape(_fmt_num(intraday_lifecycle_summary.get("threshold_markets", 0)))],
                ["Up/Down Ever <60m", html.escape(_fmt_num(intraday_lifecycle_summary.get("updown_sub_60m_markets", 0)))],
                ["Up/Down Ever <15m", html.escape(_fmt_num(intraday_lifecycle_summary.get("updown_sub_15m_markets", 0)))],
                ["Up/Down Imminent Transitions", html.escape(_fmt_num(intraday_lifecycle_summary.get("updown_imminent_transition_markets", 0)))],
                ["Threshold Ever <60m", html.escape(_fmt_num(intraday_lifecycle_summary.get("threshold_sub_60m_markets", 0)))],
                ["Best Up/Down Combined Price", html.escape(_fmt_money(_as_float(intraday_lifecycle_summary.get("best_updown_combined_contract_price"), 0.0)))],
                ["Best Up/Down Gross Edge / Share", html.escape(_fmt_money(_as_float(intraday_lifecycle_summary.get("best_updown_gross_edge_per_share"), 0.0)))],
                ["Best Up/Down Net Edge / Share", html.escape(_fmt_money(_as_float(intraday_lifecycle_summary.get("best_updown_net_edge_per_share"), 0.0)))],
                ["Latest Up/Down Seen", html.escape(_fmt_ts(intraday_lifecycle_summary.get("latest_updown_seen_at")))],
            ],
        )
        lifecycle_recent_rows = intraday_lifecycle_summary.get("recent_updown_markets", []) if isinstance(intraday_lifecycle_summary.get("recent_updown_markets"), list) else []
        lifecycle_recent_table = _table(
            ["Question", "Asset", "Min Hrs", "<60m", "<15m", "Imminent", "Best Combo", "Best Net", "Last Seen"],
            [
                [
                    html.escape(str(item.get("question", ""))),
                    html.escape(str(item.get("asset", ""))),
                    html.escape(_fmt_num(_as_float(item.get("min_hours_to_resolution"), 0.0))),
                    html.escape(_fmt_ts(item.get("first_sub_60m_at"))),
                    html.escape(_fmt_ts(item.get("first_sub_15m_at"))),
                    html.escape(_fmt_ts(item.get("first_imminent_transition_at"))),
                    html.escape(_fmt_money(_as_float(item.get("best_combined_contract_price"), 0.0))),
                    html.escape(_fmt_money(_as_float(item.get("best_net_edge_per_share"), 0.0))),
                    html.escape(_fmt_ts(item.get("last_seen_at"))),
                ]
                for item in lifecycle_recent_rows
            ],
        )
        discovery_table = _table(
            ["Source", "Total", "Live", "Near-Term", "Imminent", "Closed", "Inactive", "Min Hrs", "Max Hrs", "Error"],
            [
                [
                    html.escape(str(item.get("source", ""))),
                    html.escape(_fmt_num(item.get("total_markets", 0))),
                    html.escape(_fmt_num(item.get("live_upcoming_count", 0))),
                    html.escape(_fmt_num(item.get("near_term_count", 0))),
                    html.escape(_fmt_num(item.get("imminent_count", 0))),
                    html.escape(_fmt_num(item.get("closed_count", 0))),
                    html.escape(_fmt_num(item.get("inactive_count", 0))),
                    html.escape(_fmt_num(item.get("min_hours_to_resolution", "-")) if item.get("min_hours_to_resolution") is not None else "-"),
                    html.escape(_fmt_num(item.get("max_hours_to_resolution", "-")) if item.get("max_hours_to_resolution") is not None else "-"),
                    html.escape(str(item.get("error") or "-")),
                ]
                for item in (intraday_discovery_summary.get("sources", []) if isinstance(intraday_discovery_summary.get("sources"), list) else [])
            ],
        )
        discovery_recent_table = _table(
            ["Source", "Question", "Asset", "Hours", "Live", "Near-Term", "Imminent"],
            [
                [
                    html.escape(str(item.get("source", ""))),
                    html.escape(str(item.get("question", ""))),
                    html.escape(str(item.get("asset", ""))),
                    html.escape(_fmt_num(item.get("hours_to_resolution", "-")) if item.get("hours_to_resolution") is not None else "-"),
                    html.escape("yes" if item.get("is_live_upcoming") else "no"),
                    html.escape("yes" if item.get("is_near_term") else "no"),
                    html.escape("yes" if item.get("is_imminent") else "no"),
                ]
                for item in (intraday_discovery_summary.get("recent_near_term_markets", []) if isinstance(intraday_discovery_summary.get("recent_near_term_markets"), list) else [])
            ],
        )
        discovery_meta_table = _table(
            ["Metric", "Value"],
            [
                ["Batch", html.escape(str(intraday_discovery_summary.get("batch_id") or "-"))],
                ["Recorded At", html.escape(_fmt_ts(intraday_discovery_summary.get("recorded_at")))],
                ["Live Overlap", html.escape(_fmt_num(intraday_discovery_summary.get("live_overlap_count", 0)))],
                ["Near-Term Overlap", html.escape(_fmt_num(intraday_discovery_summary.get("near_term_overlap_count", 0)))],
                ["Imminent Overlap", html.escape(_fmt_num(intraday_discovery_summary.get("imminent_overlap_count", 0)))],
            ],
        )
        rejections_table = _table(
            ["Seen", "Source", "Reason", "Question", "Asset", "Hours"],
            [
                [
                    html.escape(_fmt_ts(item.get("seen_at"))),
                    html.escape(str(item.get("source", ""))),
                    html.escape(str(item.get("reason", ""))),
                    html.escape(str(item.get("question", ""))),
                    html.escape(str(item.get("asset_guess", "-") or "-")),
                    html.escape(_fmt_num(item.get("hours_to_resolution", "-")) if item.get("hours_to_resolution") not in (None, "") else "-"),
                ]
                for item in reversed(rejection_entries[-25:])
            ],
        )
        intraday_registry_html = (
            f"<div class='panel'><h2>Intraday Registry</h2>{registry_table}</div>"
            f"<div class='panel'><h2>Execution State</h2>{execution_state_table}</div>"
            f"<div class='panel'><h2>Short-Horizon Book Tape</h2>{tape_table}</div>"
            f"<div class='panel'><h2>Threshold Live Cache</h2>{threshold_live_table}</div>"
            f"<div class='panel'><h2>Transition Tape</h2>{transition_tape_table}</div>"
            f"<div class='panel'><h2>Imminent 5m Box Arb Tape</h2>{imminent_box_tape_table}</div>"
            f"<div class='panel'><h2>Official Feed Audit</h2>{audit_table}</div>"
            f"<div class='panel'><h2>BTC 5m Decision Table</h2>"
            f"<p>Cells show <code>positive expected count / max expected net edge per share</code> under the aggressive maker model.</p>"
            f"{btc_decision_table}</div>"
            f"<div class='panel'><h2>Lifecycle Audit</h2>{lifecycle_table}{lifecycle_recent_table}</div>"
            f"<div class='panel'><h2>Discovery Comparator</h2>{discovery_meta_table}{discovery_table}{discovery_recent_table}</div>"
            f"<div class='panel'><h2>Intraday Edge Alerts</h2>{edge_alerts_table}</div>"
            f"<div class='panel'><h2>Intraday Registry Rejections</h2>{rejections_table}</div>"
        )
    ab_panels_html = "".join(ab_panels)
    primary_health = health.get("primary", {}) if isinstance(health.get("primary"), dict) else {}
    health_rows = [
        [
            "Primary",
            html.escape(str(primary_health.get("status_label", "-"))),
            html.escape(_fmt_money(_as_float(primary_health.get("recent_realized_pnl_usdc"), 0.0))),
            html.escape(_fmt_num(primary_health.get("recent_closed_trades", 0))),
            html.escape(str(primary_health.get("runner_status", "-"))),
            html.escape(str(primary_health.get("reason", "-"))),
        ]
    ]
    for key, label in _ab_portfolio_labels():
        if key not in ab_tests:
            continue
        item_health = health.get("ab_tests", {}).get(key, {}) if isinstance(health.get("ab_tests", {}), dict) else {}
        health_rows.append(
            [
                label,
                html.escape(str(item_health.get("status_label", "-"))),
                html.escape(_fmt_money(_as_float(item_health.get("recent_realized_pnl_usdc"), 0.0))),
                html.escape(_fmt_num(item_health.get("recent_closed_trades", 0))),
                html.escape(str(item_health.get("runner_status", "-"))),
                html.escape(str(item_health.get("reason", "-"))),
            ]
        )
    strategy_health_table = _table(
        ["Strategy", "Health", "24h Realized", "24h Closes", "Runner", "Reason"],
        health_rows,
    )
    arb_tape_rows = []
    for key, label in _ab_portfolio_labels():
        if key not in {"crypto_5m_box_arb", "crypto_next_window_box_arb"} or key not in ab_tests:
            continue
        runner_status = ab_tests[key].get("runner_status", {}) if isinstance(ab_tests[key].get("runner_status"), dict) else {}
        runner_result = runner_status.get("last_cycle_result", {}) if isinstance(runner_status.get("last_cycle_result"), dict) else {}
        tape = runner_result.get("arb_tape", {}) if isinstance(runner_result.get("arb_tape"), dict) else {}
        arb_tape_rows.append(
            [
                label,
                html.escape(_fmt_num(tape.get("candidates_seen", 0))),
                html.escape(_fmt_num(tape.get("executable_pairs", 0))),
                html.escape(_fmt_num(tape.get("gross_edge_eligible", 0))),
                html.escape(_fmt_num(tape.get("net_edge_eligible", 0))),
                html.escape(_fmt_num(tape.get("executed_pairs", 0))),
                html.escape(_fmt_num(tape.get("gross_edge_rejects", 0))),
                html.escape(_fmt_num(tape.get("net_edge_rejects", 0))),
                html.escape(_fmt_money(_as_float(tape.get("avg_gross_edge_per_share"), 0.0))),
                html.escape(_fmt_money(_as_float(tape.get("avg_net_edge_per_share"), 0.0))),
            ]
        )
    arb_tape_table = _table(
        ["Strategy", "Seen", "Executable", "Gross OK", "Net OK", "Executed", "Gross Rejects", "Net Rejects", "Avg Gross Edge", "Avg Net Edge"],
        arb_tape_rows,
    )
    why_not_counts: dict[tuple[str, str], dict[str, Any]] = {}

    def add_why_not(source: str, reason: str, count: int = 1, example: str = "") -> None:
        bucket = _normalize_not_trading_reason(reason)
        key = (source, bucket)
        row = why_not_counts.setdefault(key, {"source": source, "reason": bucket, "count": 0, "examples": []})
        row["count"] += max(int(count), 0)
        if example and example not in row["examples"] and len(row["examples"]) < 2:
            row["examples"].append(example)

    for key, label in _ab_portfolio_labels():
        if key not in ab_tests:
            continue
        runner_status = ab_tests[key].get("runner_status", {}) if isinstance(ab_tests[key].get("runner_status"), dict) else {}
        runner_result = runner_status.get("last_cycle_result", {}) if isinstance(runner_status.get("last_cycle_result"), dict) else {}
        trade_decisions = runner_result.get("trade_decisions", []) if isinstance(runner_result.get("trade_decisions"), list) else []
        skip_count = 0
        for item in trade_decisions:
            if item.get("action") != "SKIP":
                continue
            skip_count += 1
            add_why_not(label, str(item.get("reason", "other")), example=str(item.get("question", "")))
        queue_count = int(runner_result.get("queue_count", 0) or 0)
        opened_count = int(runner_result.get("opened_positions_count", 0) or 0)
        if queue_count == 0 and skip_count == 0 and opened_count == 0:
            add_why_not(label, "no candidates in current cycle")

        tape = runner_result.get("arb_tape", {}) if isinstance(runner_result.get("arb_tape"), dict) else {}
        if tape:
            if int(tape.get("candidates_seen", 0) or 0) == 0:
                add_why_not(label, "no candidates in current cycle")
            if int(tape.get("depth_rejects", 0) or 0) > 0:
                add_why_not(label, "insufficient executable depth", int(tape.get("depth_rejects", 0) or 0))
            if int(tape.get("gross_edge_rejects", 0) or 0) > 0:
                add_why_not(label, "gross arb edge too small", int(tape.get("gross_edge_rejects", 0) or 0))
            if int(tape.get("net_edge_rejects", 0) or 0) > 0:
                add_why_not(label, "net arb edge too small after costs", int(tape.get("net_edge_rejects", 0) or 0))
            if int(tape.get("already_open_skips", 0) or 0) > 0:
                add_why_not(label, "already open", int(tape.get("already_open_skips", 0) or 0))
            if int(tape.get("no_budget_skips", 0) or 0) > 0:
                add_why_not(label, "no budget", int(tape.get("no_budget_skips", 0) or 0))

    rejection_entries = intraday_registry_rejections.get("entries", []) if isinstance(intraday_registry_rejections.get("entries"), list) else []
    for item in rejection_entries[-100:]:
        add_why_not("Intraday Feed", str(item.get("reason", "other")), example=str(item.get("question", "")))

    why_not_trading = sorted(
        why_not_counts.values(),
        key=lambda item: (-int(item.get("count", 0) or 0), str(item.get("source", "")), str(item.get("reason", ""))),
    )
    why_not_trading_table = _table(
        ["Source", "Reason", "Count", "Latest Example"],
        [
            [
                html.escape(str(item.get("source", ""))),
                html.escape(str(item.get("reason", ""))),
                html.escape(_fmt_num(item.get("count", 0))),
                html.escape(str((item.get("examples") or ["-"])[0])),
            ]
            for item in why_not_trading
        ],
    )
    shadow_replay_table = _table(
        ["Strategy", "Open", "First Open", "Since First", "Avg Open Age", "Close-Now PnL", "Queue", "Health"],
        [
            [
                html.escape(str(item.get("label", ""))),
                html.escape(_fmt_num(item.get("open_positions", 0))),
                html.escape(_fmt_ts(item.get("first_opened_at"))),
                html.escape(_fmt_age_minutes(item.get("minutes_since_first_open"))),
                html.escape(_fmt_age_minutes(item.get("avg_open_age_minutes"))),
                html.escape(_fmt_money(_as_float(item.get("simulated_close_now_pnl_usdc"), 0.0))),
                html.escape(_fmt_num(item.get("queue_count", 0))),
                html.escape(str(item.get("health", "-"))),
            ]
            for item in shadow_replay
        ],
    )

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Polymarket Paper Bot</title>
  <meta http-equiv="refresh" content="10">
  <style>
    :root {{
      --bg: #f4f0e8;
      --ink: #18211f;
      --muted: #5a6762;
      --panel: #fffdf8;
      --line: #d8d0c2;
      --accent: #0d7a5f;
      --accent-2: #b85c38;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: Georgia, "Iowan Old Style", serif; color: var(--ink); background:
      radial-gradient(circle at top left, #fff7df 0, transparent 28%),
      radial-gradient(circle at top right, #dff5ee 0, transparent 24%),
      linear-gradient(180deg, #efe7d9, var(--bg)); }}
    .wrap {{ max-width: 1200px; margin: 0 auto; padding: 24px; }}
    h1, h2 {{ margin: 0 0 12px; }}
    .sub {{ color: var(--muted); margin-bottom: 24px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-bottom: 24px; }}
    .mini-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; margin: 0 0 12px; }}
    .mini-card {{ background: #faf7f0; border: 1px solid var(--line); border-radius: 12px; padding: 12px; }}
    .mini-card .label {{ color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .08em; }}
    .mini-card .value {{ font-size: 24px; margin-top: 6px; }}
    .subpanel-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; margin: 12px 0; }}
    h3 {{ margin: 0 0 10px; font-size: 18px; }}
    .card, .panel {{ background: var(--panel); border: 1px solid var(--line); border-radius: 16px; padding: 16px; box-shadow: 0 6px 24px rgba(24,33,31,.06); }}
    .card .label {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .08em; }}
    .card .value {{ font-size: 28px; margin-top: 8px; }}
    .panels {{ display: grid; grid-template-columns: 1fr; gap: 16px; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ text-align: left; padding: 10px 8px; border-bottom: 1px solid var(--line); vertical-align: top; }}
    th {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .08em; }}
    .status {{ display: inline-block; padding: 6px 10px; border-radius: 999px; background: #e4f5ef; color: var(--accent); font-weight: 700; }}
    .error {{ color: var(--accent-2); white-space: pre-wrap; }}
    .curve {{ width: 100%; height: auto; display: block; margin-top: 8px; overflow: visible; }}
    .curve-line {{ fill: none; stroke: var(--accent); stroke-width: 3; stroke-linecap: round; stroke-linejoin: round; }}
    .curve-baseline {{ stroke: var(--line); stroke-dasharray: 6 4; stroke-width: 1.5; }}
    .curve-dot {{ fill: var(--accent-2); }}
    @media (max-width: 768px) {{
      .wrap {{ padding: 16px; }}
      .card .value {{ font-size: 24px; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Polymarket Paper Bot</h1>
    <div class="sub">Generated at {html.escape(state["generated_at"])}. Auto-refresh every 10 seconds.</div>
    <div class="grid">{cards}</div>
    {badges}
    <div class="panel" style="margin-bottom:16px;">
      <h2>Runner Status</h2>
      <div class="sub"><span class="status">{html.escape(str(status.get("status", "unknown")))}</span></div>
      <div>Last cycle started: {html.escape(_fmt_ts(status.get("last_cycle_started_at")))}</div>
      <div>Last cycle completed: {html.escape(_fmt_ts(status.get("last_cycle_completed_at")))}</div>
      <div>Freshness: {html.escape(str(status_meta.get("runner_freshness") or "-"))}</div>
      <div>Latest queue / theses / opens / scales: {summary['queue_count']} / {summary['thesis_count']} / {summary['opened_this_cycle']} / {summary['scaled_this_cycle']}</div>
      <div>Rotation / cooldown: {html.escape(str(summary['active_rotation_category'] or 'all'))} / {summary['market_cooldown_minutes']}m</div>
      <div>Risk caps: max position {html.escape(_fmt_money(summary['max_position_usdc']))}, max portfolio {html.escape(_fmt_money(summary['max_portfolio_usdc']))}</div>
      <div>Last non-empty queue snapshot: {html.escape(_fmt_ts(state["last_nonempty_queue"].get("updated_at")))}</div>
      <div>Last error: <span class="error">{html.escape(str(status.get("last_error") or "-"))}</span></div>
    </div>
    <div class="panel" style="margin-bottom:16px;">
      <h2>Strategy Health</h2>
      {strategy_health_table}
    </div>
    <div class="panel" style="margin-bottom:16px;">
      <h2>Shadow Replay</h2>
      <div class="sub">Immediate mark-to-market view for the new shadow books. Close-Now PnL is simulated liquidation against current marks, not realized profit.</div>
      {shadow_replay_table}
    </div>
    <div class="panel" style="margin-bottom:16px;">
      <h2>Arb Tape</h2>
      <div class="sub">Last-cycle box-arbitrage diagnostics. Gross edge is before estimated fees/slippage; net edge is after those estimated costs.</div>
      {arb_tape_table}
    </div>
    <div class="panel" style="margin-bottom:16px;">
      <h2>Why Not Trading</h2>
      <div class="sub">Current skip and rejection reasons aggregated from the latest strategy cycles and intraday feed rejections.</div>
      {why_not_trading_table}
    </div>
    <div class="panels">
      <div class="panel"><h2>Lifetime Stats</h2>{lifetime_table}</div>
      <div class="panel"><h2>Equity Curve</h2>{_render_equity_curve(lifetime["equity_curve"], summary["starting_bankroll_usdc"])}</div>
      {ab_panels_html}
      {intraday_registry_html}
      <div class="panel"><h2>Category Analytics</h2>{category_analytics_table}</div>
      <div class="panel"><h2>Market Analytics</h2>{market_analytics_table}</div>
      <div class="panel"><h2>Open Positions</h2>{positions_table}</div>
      <div class="panel"><h2>Recent Trades</h2>{trades_table}</div>
      <div class="panel"><h2>Latest Scan Queue</h2>{queue_table}</div>
      <div class="panel"><h2>Top Opportunities Missed</h2>{missed_table}</div>
      <div class="panel"><h2>Last Non-Empty Queue</h2>{last_nonempty_queue_table}</div>
      <div class="panel"><h2>Target Wallets</h2>{targets_table}</div>
    </div>
  </div>
</body>
</html>"""


def serve_dashboard(settings: Settings, host: str, port: int) -> None:
    class Handler(BaseHTTPRequestHandler):
        def _respond(self, code: int, content_type: str, body: bytes) -> None:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            state = build_dashboard_state(settings)
            if self.path == "/api/state":
                self._respond(200, "application/json; charset=utf-8", json.dumps(state, indent=2).encode("utf-8"))
                return
            if self.path == "/" or self.path.startswith("/?"):
                html_body = render_dashboard_html(state).encode("utf-8")
                self._respond(200, "text/html; charset=utf-8", html_body)
                return
            self._respond(404, "text/plain; charset=utf-8", b"not found")

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer((host, port), Handler)
    print(f"[{datetime.now(timezone.utc).isoformat()}] dashboard listening on http://{host}:{port}", flush=True)
    server.serve_forever()
