from __future__ import annotations

import sys
from decimal import Decimal
from types import SimpleNamespace

from storage.llm_postgres import (
    DOCUMENT_ANALYSIS_UPSERT,
    LLM_BATCH_UPSERT,
    LLM_REQUEST_UPSERT,
    build_llm_postgres_records,
    write_llm_batch_to_postgres,
)


def _fixture() -> tuple[dict, list[dict], list[dict]]:
    state = {
        "id": "batch-openai-1",
        "model": "gpt-5.6-luna",
        "input_file_id": "file-input",
        "output_file_id": "file-output",
        "status": "completed",
        "created_at": 1_788_000_000,
        "completed_at": 1_788_000_100,
        "request_counts": {"total": 1, "completed": 1, "failed": 0},
        "usage": {"input_tokens": 100, "output_tokens": 20},
        "metadata": {
            "llm_batch_id": "internal-1",
            "prompt_version": "prompt-v1",
        },
    }
    manifest = [
        {
            "custom_id": "custom-1",
            "event_id": "event-1",
            "model": "gpt-5.6-luna",
            "prompt_version": "prompt-v1",
            "attempt": 1,
        }
    ]
    results = [
        {
            "custom_id": "custom-1",
            "event_id": "event-1",
            "model": "gpt-5.6-luna",
            "prompt_version": "prompt-v1",
            "sentiment": "mixed",
            "sentiment_score": 0.1,
            "topics": ["economy"],
            "keywords": ["wages"],
            "summary": "A synthetic summary.",
            "usage": {"input_tokens": 100, "output_tokens": 20},
        }
    ]
    return state, manifest, results


def test_build_llm_postgres_records_accounts_for_validated_result() -> None:
    state, manifest, results = _fixture()
    batch, requests, analyses = build_llm_postgres_records(
        batch_state=state,
        manifest_rows=manifest,
        result_rows=results,
        total_cost_usd=Decimal("0.000022"),
    )

    assert batch["llm_batch_id"] == "internal-1"
    assert batch["total_cost_usd"] == "0.000022"
    assert requests[0]["status"] == "completed"
    assert requests[0]["validation_result"] == "validated"
    assert analyses[0]["topics"] == ["economy"]


class _Cursor:
    def __init__(self) -> None:
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


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def cursor(self) -> _Cursor:
        return self._cursor


def test_write_llm_batch_uses_transactional_upserts(monkeypatch) -> None:
    state, manifest, results = _fixture()
    cursor = _Cursor()
    monkeypatch.setitem(
        sys.modules,
        "psycopg",
        SimpleNamespace(connect=lambda _dsn: _Connection(cursor)),
    )

    written = write_llm_batch_to_postgres(
        dsn="postgresql://test",
        batch_state=state,
        manifest_rows=manifest,
        result_rows=results,
    )

    assert written.request_rows == 1
    assert written.analysis_rows == 1
    assert cursor.executed[0][0] == LLM_BATCH_UPSERT
    assert cursor.executed_many[0][0] == LLM_REQUEST_UPSERT
    assert cursor.executed_many[1][0] == DOCUMENT_ANALYSIS_UPSERT
    assert "ON CONFLICT" in LLM_BATCH_UPSERT
    assert "ON CONFLICT" in LLM_REQUEST_UPSERT
    assert "ON CONFLICT" in DOCUMENT_ANALYSIS_UPSERT
