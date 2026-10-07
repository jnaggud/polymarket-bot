from __future__ import annotations

import gzip
import json
import os
import shutil
import sqlite3
import uuid
from dataclasses import dataclass
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


RETENTION_CONFIRMATION = "RETENTION_APPLY"


@dataclass(frozen=True)
class RetentionTarget:
    table: str
    timestamp_column: str
    description: str


# Intentionally excludes orders, fills, positions, events, equity, risk, and
# reconciliation records. Those tables are the durable audit trail.
RETENTION_TARGETS = (
    RetentionTarget("binance_ticks", "ts", "raw CEX observations"),
    RetentionTarget("polymarket_books", "ts", "raw Polymarket order-book observations"),
    RetentionTarget("fair_values", "ts", "derived fair-value observations"),
    RetentionTarget("signals", "ts", "primary strategy decisions"),
    RetentionTarget("shadow_signals", "ts", "shadow strategy decisions"),
    RetentionTarget("cex_latency_paper_signals", "ts", "latency-paper decisions"),
    RetentionTarget("shadow_variant_signals", "ts", "variant-grid decisions"),
    RetentionTarget("complete_set_arb_signals", "ts", "complete-set observations"),
    RetentionTarget("late_resolution_capture_signals", "ts", "late-resolution observations"),
    RetentionTarget("polymarket_us_arb_ticks", "ts", "Polymarket US probe observations"),
    RetentionTarget("kalshi_arb_ticks", "ts", "Kalshi probe observations"),
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_connection(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


def database_inventory(db_path: Path, *, include_timestamp_ranges: bool = False) -> dict[str, Any]:
    """Return constant-time database and retention-range diagnostics."""
    if not db_path.exists():
        raise FileNotFoundError(db_path)
    with closing(_read_connection(db_path)) as connection:
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0] or 0)
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0] or 0)
        freelist_count = int(connection.execute("PRAGMA freelist_count").fetchone()[0] or 0)
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0] or 0)
        journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0] or "")
        existing_tables = {
            str(row["name"])
            for row in connection.execute("SELECT name FROM sqlite_schema WHERE type = 'table'").fetchall()
        }
        ranges: list[dict[str, Any]] = []
        if include_timestamp_ranges:
            for target in RETENTION_TARGETS:
                if target.table not in existing_tables:
                    continue
                oldest = connection.execute(
                    f"SELECT {target.timestamp_column} FROM {target.table} "
                    f"ORDER BY {target.timestamp_column} ASC LIMIT 1"
                ).fetchone()
                newest = connection.execute(
                    f"SELECT {target.timestamp_column} FROM {target.table} "
                    f"ORDER BY {target.timestamp_column} DESC LIMIT 1"
                ).fetchone()
                ranges.append(
                    {
                        "table": target.table,
                        "description": target.description,
                        "oldest": oldest[0] if oldest else None,
                        "newest": newest[0] if newest else None,
                    }
                )
    logical_bytes = page_count * page_size
    reclaimable_bytes = freelist_count * page_size
    return {
        "db_path": str(db_path),
        "file_bytes": db_path.stat().st_size,
        "logical_bytes": logical_bytes,
        "reclaimable_bytes": reclaimable_bytes,
        "page_count": page_count,
        "page_size": page_size,
        "freelist_pages": freelist_count,
        "schema_version": user_version,
        "journal_mode": journal_mode,
        "retention_ranges": ranges,
    }


def retention_plan(
    db_path: Path,
    *,
    retention_days: int = 14,
    batch_size: int = 25_000,
    now: datetime | None = None,
) -> dict[str, Any]:
    if retention_days < 1:
        raise ValueError("retention_days must be at least 1")
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    cutoff = _iso((now or _utc_now()) - timedelta(days=retention_days))
    with closing(_read_connection(db_path)) as connection:
        existing_tables = {
            str(row["name"])
            for row in connection.execute("SELECT name FROM sqlite_schema WHERE type = 'table'").fetchall()
        }
        tables: list[dict[str, Any]] = []
        for target in RETENTION_TARGETS:
            if target.table not in existing_tables:
                continue
            rows = connection.execute(
                f"SELECT rowid FROM {target.table} "
                f"WHERE {target.timestamp_column} < ? ORDER BY {target.timestamp_column} LIMIT ?",
                (cutoff, batch_size + 1),
            ).fetchall()
            tables.append(
                {
                    "table": target.table,
                    "description": target.description,
                    "candidate_rows": min(len(rows), batch_size),
                    "more_rows_available": len(rows) > batch_size,
                }
            )
    return {
        "mode": "preview",
        "cutoff": cutoff,
        "retention_days": retention_days,
        "batch_size_per_table": batch_size,
        "candidate_rows": sum(int(item["candidate_rows"]) for item in tables),
        "tables": tables,
        "protected_history": ["orders", "fills", "positions", "position events", "equity", "risk events"],
    }


def _archive_rows(path: Path, *, metadata: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    try:
        with gzip.open(temporary_path, "wt", encoding="utf-8") as handle:
            handle.write(json.dumps({"_metadata": metadata}, sort_keys=True) + "\n")
            for row in rows:
                handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def apply_retention(
    db_path: Path,
    *,
    retention_days: int,
    batch_size: int,
    archive_dir: Path,
    confirmation: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Archive and delete one bounded batch from each high-volume table."""
    if confirmation != RETENTION_CONFIRMATION:
        raise ValueError(f"confirmation must equal {RETENTION_CONFIRMATION!r}")
    if retention_days < 1 or batch_size < 1:
        raise ValueError("retention_days and batch_size must be positive")
    operation_time = now or _utc_now()
    cutoff = _iso(operation_time - timedelta(days=retention_days))
    stamp = f"{operation_time.strftime('%Y%m%dT%H%M%S%fZ')}-{uuid.uuid4().hex[:8]}"
    connection = sqlite3.connect(db_path, timeout=30.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 30000")
    results: list[dict[str, Any]] = []
    try:
        existing_tables = {
            str(row["name"])
            for row in connection.execute("SELECT name FROM sqlite_schema WHERE type = 'table'").fetchall()
        }
        for target in RETENTION_TARGETS:
            if target.table not in existing_tables:
                continue
            connection.execute("BEGIN IMMEDIATE")
            selected = connection.execute(
                f"SELECT rowid AS _rowid, * FROM {target.table} "
                f"WHERE {target.timestamp_column} < ? ORDER BY {target.timestamp_column} LIMIT ?",
                (cutoff, batch_size),
            ).fetchall()
            if not selected:
                connection.commit()
                results.append({"table": target.table, "archived_rows": 0, "deleted_rows": 0})
                continue
            row_payloads = [dict(row) for row in selected]
            archive_path = archive_dir / f"{target.table}-{stamp}.jsonl.gz"
            _archive_rows(
                archive_path,
                metadata={
                    "table": target.table,
                    "timestamp_column": target.timestamp_column,
                    "cutoff": cutoff,
                    "archived_at": _iso(operation_time),
                    "source_db": str(db_path),
                    "row_count": len(row_payloads),
                },
                rows=row_payloads,
            )
            rowids = [int(row["_rowid"]) for row in selected]
            deleted = 0
            for offset in range(0, len(rowids), 900):
                chunk = rowids[offset : offset + 900]
                placeholders = ",".join("?" for _ in chunk)
                cursor = connection.execute(f"DELETE FROM {target.table} WHERE rowid IN ({placeholders})", chunk)
                deleted += int(cursor.rowcount)
            connection.commit()
            results.append(
                {
                    "table": target.table,
                    "archived_rows": len(row_payloads),
                    "deleted_rows": deleted,
                    "archive_path": str(archive_path),
                }
            )
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return {
        "mode": "applied",
        "cutoff": cutoff,
        "retention_days": retention_days,
        "batch_size_per_table": batch_size,
        "archive_dir": str(archive_dir),
        "deleted_rows": sum(int(item["deleted_rows"]) for item in results),
        "tables": results,
        "next_step": "Repeat the same command until candidate_rows is zero; VACUUM only during a maintenance window.",
    }


def checkpoint_database(db_path: Path) -> dict[str, Any]:
    connection = sqlite3.connect(db_path, timeout=30.0)
    try:
        connection.execute("PRAGMA busy_timeout = 30000")
        row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    finally:
        connection.close()
    return {"busy": int(row[0]), "wal_pages": int(row[1]), "checkpointed_pages": int(row[2])}


def vacuum_database(db_path: Path, *, confirmation: str) -> dict[str, Any]:
    if confirmation != RETENTION_CONFIRMATION:
        raise ValueError(f"confirmation must equal {RETENTION_CONFIRMATION!r}")
    before = db_path.stat().st_size
    free_bytes = shutil.disk_usage(db_path.parent).free
    required_free_bytes = int(before * 1.10)
    if free_bytes < required_free_bytes:
        raise RuntimeError(
            f"VACUUM requires approximately {required_free_bytes} free bytes; only {free_bytes} are available. "
            "Copy the compacted database to a larger volume or free disk space first."
        )
    connection = sqlite3.connect(db_path, timeout=30.0)
    try:
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("VACUUM")
    finally:
        connection.close()
    return {"before_bytes": before, "after_bytes": db_path.stat().st_size}
