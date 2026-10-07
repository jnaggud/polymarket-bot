import gzip
import json
import sqlite3
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from latency_bot.config import LatencyBotSettings
from latency_bot.dashboard import build_latency_bot_dashboard_state, render_latency_bot_dashboard_html
from latency_bot.demo import create_demo_workspace, demo_settings
from latency_bot.maintenance import (
    RETENTION_CONFIRMATION,
    apply_retention,
    database_inventory,
    retention_plan,
)
from latency_bot.validation import build_validation_report, load_validation_records


class OperationsToolingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.anchor = datetime(2026, 10, 7, 18, 0, tzinfo=timezone.utc)
        self.manifest = create_demo_workspace(
            self.root / "demo",
            anchor=self.anchor,
            base_settings=LatencyBotSettings.from_env(),
        )
        self.settings = demo_settings(self.root / "demo", LatencyBotSettings.from_env())

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_demo_workspace_is_credential_free_and_renders(self) -> None:
        self.assertFalse(self.manifest["contains_credentials"])
        self.assertFalse(self.manifest["contains_live_trades"])
        self.assertEqual(self.manifest["closed_trades"], 24)
        state = build_latency_bot_dashboard_state(self.settings, fast=True)
        html = render_latency_bot_dashboard_html(state)
        self.assertEqual(state["status"]["runner_status"], "demo")
        self.assertIn("Strategy Truth", html)

    def test_validation_report_keeps_live_gate_closed_without_reconciliation_sample(self) -> None:
        records = load_validation_records(Path(self.manifest["validation_path"]))
        report = build_validation_report(records)
        self.assertEqual(report["record_count"], 24)
        self.assertTrue(report["gates"]["baseline_comparison"])
        self.assertFalse(report["gates"]["independent_fill_reconciliation"])
        self.assertFalse(report["live_pilot_eligible"])

    def test_retention_preview_is_bounded_and_preserves_audit_tables(self) -> None:
        old_ts = self.anchor - timedelta(days=30)
        with closing(sqlite3.connect(self.settings.db_path)) as connection:
            connection.execute(
                "INSERT INTO binance_ticks (ts, asset, mid) VALUES (?, 'btc', 90000)",
                (old_ts.isoformat().replace("+00:00", "Z"),),
            )
            connection.commit()
        plan = retention_plan(self.settings.db_path, retention_days=14, batch_size=1, now=self.anchor)
        binance = next(item for item in plan["tables"] if item["table"] == "binance_ticks")
        self.assertEqual(binance["candidate_rows"], 1)
        self.assertIn("positions", plan["protected_history"])

    def test_retention_requires_confirmation_and_archives_before_delete(self) -> None:
        old_ts = self.anchor - timedelta(days=30)
        with closing(sqlite3.connect(self.settings.db_path)) as connection:
            connection.execute(
                "INSERT INTO binance_ticks (ts, asset, mid) VALUES (?, 'btc', 90000)",
                (old_ts.isoformat().replace("+00:00", "Z"),),
            )
            connection.commit()
        with self.assertRaises(ValueError):
            apply_retention(
                self.settings.db_path,
                retention_days=14,
                batch_size=10,
                archive_dir=self.root / "archive",
                confirmation="",
                now=self.anchor,
            )
        result = apply_retention(
            self.settings.db_path,
            retention_days=14,
            batch_size=10,
            archive_dir=self.root / "archive",
            confirmation=RETENTION_CONFIRMATION,
            now=self.anchor,
        )
        binance = next(item for item in result["tables"] if item["table"] == "binance_ticks")
        self.assertEqual(binance["deleted_rows"], 1)
        with gzip.open(binance["archive_path"], "rt", encoding="utf-8") as handle:
            archive_lines = [json.loads(line) for line in handle]
        self.assertEqual(archive_lines[0]["_metadata"]["row_count"], 1)
        self.assertEqual(archive_lines[1]["asset"], "btc")

        with closing(sqlite3.connect(self.settings.db_path)) as connection:
            connection.execute(
                "INSERT INTO binance_ticks (ts, asset, mid) VALUES (?, 'eth', 3000)",
                (old_ts.isoformat().replace("+00:00", "Z"),),
            )
            connection.commit()
        repeated = apply_retention(
            self.settings.db_path,
            retention_days=14,
            batch_size=10,
            archive_dir=self.root / "archive",
            confirmation=RETENTION_CONFIRMATION,
            now=self.anchor,
        )
        repeated_binance = next(item for item in repeated["tables"] if item["table"] == "binance_ticks")
        self.assertNotEqual(binance["archive_path"], repeated_binance["archive_path"])
        self.assertTrue(Path(binance["archive_path"]).exists())
        self.assertTrue(Path(repeated_binance["archive_path"]).exists())

    def test_inventory_reports_versioned_schema(self) -> None:
        inventory = database_inventory(self.settings.db_path)
        self.assertEqual(inventory["schema_version"], 2)
        self.assertGreater(inventory["file_bytes"], 0)


if __name__ == "__main__":
    unittest.main()
