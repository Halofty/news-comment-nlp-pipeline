from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from storage.postgres import (
    event_to_postgres_records,
    write_micro_batch_to_postgres,
)


def _valid_event() -> dict:
    now = datetime(2026, 8, 24, tzinfo=timezone.utc)
    return {
        "event_id": "a" * 64,
        "source_type": "news",
        "source_name": "gdelt",
        "event_timestamp": now,
        "collected_timestamp": now,
        "language": "en",
        "title": "Title",
        "text_original": "Original text",
        "text_clean": "Original text",
        "url": "https://example.com",
        "community": None,
        "engagement": 3,
        "schema_version": 1,
        "metadata": {"source_id": "1"},
        "_raw_json": json.dumps({"event_id": "a" * 64}),
        "kafka_topic": "raw-text",
        "kafka_partition": 1,
        "kafka_offset": 7,
        "contract_valid": True,
        "quality_policy_version": 1,
        "quality_status": "accept",
        "quality_flags": [],
        "exclusion_reason": None,
        "character_count": 13,
        "utf8_byte_count": 13,
        "control_character_count": 0,
        "zero_width_count": 0,
        "max_combining_mark_run": 0,
        "url_count": 0,
        "url_ratio": 0.0,
        "repetition_ratio": 0.0,
        "was_normalized": False,
        "was_truncated": False,
        "output_route": "processed",
    }


def _rejected_event() -> dict:
    return {
        "contract_valid": False,
        "kafka_topic": "raw-text",
        "kafka_partition": 2,
        "kafka_offset": 9,
        "kafka_timestamp": datetime(2026, 8, 24, tzinfo=timezone.utc),
        "kafka_key": "bad-1",
        "_raw_json": "not-json",
        "contract_errors": ["MALFORMED_JSON"],
    }


def test_event_rows_map_to_valid_and_rejected_storage_records() -> None:
    kind, valid = event_to_postgres_records(_valid_event(), batch_id=4)
    assert kind == "valid"
    assert valid["raw"]["event_id"] == "a" * 64
    assert json.loads(valid["raw"]["metadata_json"]) == {"source_id": "1"}
    assert valid["clean"]["batch_id"] == 4

    kind, rejected = event_to_postgres_records(_rejected_event(), batch_id=4)
    assert kind == "rejected"
    assert rejected["raw_event"] == "not-json"
    assert rejected["contract_errors"] == ["MALFORMED_JSON"]


def test_postgres_records_replace_nul_without_dropping_other_text() -> None:
    event = _valid_event()
    event["text_original"] = "before\x00after"
    event["text_clean"] = "before\x00after"
    event["metadata"] = {"value": "a\x00b"}

    _, records = event_to_postgres_records(event, batch_id=1)

    assert records["raw"]["text_original"] == "before\ufffdafter"
    assert records["clean"]["text_clean"] == "before\ufffdafter"
    assert json.loads(records["raw"]["metadata_json"])["value"] == "a\ufffdb"


class _FakeCursor:
    def __init__(self, *, committed: bool = False) -> None:
        self.committed = committed
        self.executed: list[tuple[str, object]] = []
        self.executed_many: list[tuple[str, list[dict]]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def execute(self, query, params=None) -> None:
        self.executed.append((query, params))

    def executemany(self, query, params) -> None:
        self.executed_many.append((query, list(params)))

    def fetchone(self):
        return (1,) if self.committed else None


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return self._cursor


def test_postgres_write_is_transactional_and_records_batch_commit(monkeypatch) -> None:
    cursor = _FakeCursor()
    monkeypatch.setitem(
        sys.modules,
        "psycopg",
        SimpleNamespace(connect=lambda _dsn: _FakeConnection(cursor)),
    )

    result = write_micro_batch_to_postgres(
        [_valid_event(), _rejected_event()],
        dsn="postgresql://test",
        consumer_name="consumer-a",
        batch_id=3,
        route_counts={"processed": 1, "contract_rejected": 1},
        chunk_size=1,
    )

    assert result.status == "committed"
    assert (result.input_rows, result.valid_rows, result.rejected_rows) == (2, 1, 1)
    assert len(cursor.executed_many) == 3
    assert "INSERT INTO stream_batch_commits" in cursor.executed[-1][0]


def test_committed_batch_skips_rows(monkeypatch) -> None:
    cursor = _FakeCursor(committed=True)
    monkeypatch.setitem(
        sys.modules,
        "psycopg",
        SimpleNamespace(connect=lambda _dsn: _FakeConnection(cursor)),
    )

    result = write_micro_batch_to_postgres(
        iter([_valid_event()]),
        dsn="postgresql://test",
        consumer_name="consumer-a",
        batch_id=3,
        route_counts={},
    )

    assert result.status == "already_committed"
    assert not cursor.executed_many


def test_initial_migration_defines_idempotency_tables() -> None:
    migration = Path("sql/migrations/001_core_storage.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS raw_text_events" in migration
    assert "CREATE TABLE IF NOT EXISTS text_documents_clean" in migration
    assert "CREATE TABLE IF NOT EXISTS contract_rejected_events" in migration
    assert "PRIMARY KEY (consumer_name, batch_id)" in migration
