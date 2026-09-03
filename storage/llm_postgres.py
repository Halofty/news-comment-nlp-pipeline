from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping, Sequence


LLM_BATCH_UPSERT = """
INSERT INTO llm_batch_jobs (
    llm_batch_id, openai_batch_id, input_file_id, output_file_id, model,
    prompt_version, status, requested_count, completed_count, failed_count,
    input_tokens, output_tokens, total_cost_usd, submitted_at, completed_at
) VALUES (
    %(llm_batch_id)s, %(openai_batch_id)s, %(input_file_id)s,
    %(output_file_id)s, %(model)s, %(prompt_version)s, %(status)s,
    %(requested_count)s, %(completed_count)s, %(failed_count)s,
    %(input_tokens)s, %(output_tokens)s, %(total_cost_usd)s,
    %(submitted_at)s, %(completed_at)s
)
ON CONFLICT (llm_batch_id) DO UPDATE SET
    openai_batch_id = EXCLUDED.openai_batch_id,
    input_file_id = EXCLUDED.input_file_id,
    output_file_id = EXCLUDED.output_file_id,
    model = EXCLUDED.model,
    prompt_version = EXCLUDED.prompt_version,
    status = EXCLUDED.status,
    requested_count = EXCLUDED.requested_count,
    completed_count = EXCLUDED.completed_count,
    failed_count = EXCLUDED.failed_count,
    input_tokens = EXCLUDED.input_tokens,
    output_tokens = EXCLUDED.output_tokens,
    total_cost_usd = EXCLUDED.total_cost_usd,
    submitted_at = EXCLUDED.submitted_at,
    completed_at = EXCLUDED.completed_at,
    updated_at = now()
"""

LLM_REQUEST_UPSERT = """
INSERT INTO llm_batch_requests (
    custom_id, llm_batch_id, event_id, attempt, status, validation_result,
    error_code, input_tokens, output_tokens
) VALUES (
    %(custom_id)s, %(llm_batch_id)s, %(event_id)s, %(attempt)s, %(status)s,
    %(validation_result)s, %(error_code)s, %(input_tokens)s,
    %(output_tokens)s
)
ON CONFLICT (custom_id) DO UPDATE SET
    llm_batch_id = EXCLUDED.llm_batch_id,
    event_id = EXCLUDED.event_id,
    attempt = EXCLUDED.attempt,
    status = EXCLUDED.status,
    validation_result = EXCLUDED.validation_result,
    error_code = EXCLUDED.error_code,
    input_tokens = EXCLUDED.input_tokens,
    output_tokens = EXCLUDED.output_tokens,
    updated_at = now()
"""

DOCUMENT_ANALYSIS_UPSERT = """
INSERT INTO document_analyses (
    event_id, prompt_version, model, sentiment, sentiment_score, topics,
    keywords, summary, custom_id
) VALUES (
    %(event_id)s, %(prompt_version)s, %(model)s, %(sentiment)s,
    %(sentiment_score)s, %(topics)s, %(keywords)s, %(summary)s,
    %(custom_id)s
)
ON CONFLICT (event_id, prompt_version) DO UPDATE SET
    model = EXCLUDED.model,
    sentiment = EXCLUDED.sentiment,
    sentiment_score = EXCLUDED.sentiment_score,
    topics = EXCLUDED.topics,
    keywords = EXCLUDED.keywords,
    summary = EXCLUDED.summary,
    custom_id = EXCLUDED.custom_id,
    analyzed_at = now()
"""


@dataclass(frozen=True)
class LLMPostgresWriteResult:
    llm_batch_id: str
    request_rows: int
    analysis_rows: int
    failed_rows: int


def _timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise TypeError(f"unsupported timestamp value: {type(value).__name__}")


def _required_text(value: Any, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} must not be empty")
    return text


def _usage(row: Mapping[str, Any]) -> tuple[int | None, int | None]:
    usage = row.get("usage") or {}
    if not isinstance(usage, Mapping):
        raise TypeError("result usage must be an object")
    input_tokens = usage.get("input_tokens")
    output_tokens = usage.get("output_tokens")
    return (
        int(input_tokens) if input_tokens is not None else None,
        int(output_tokens) if output_tokens is not None else None,
    )


def build_llm_postgres_records(
    *,
    batch_state: Mapping[str, Any],
    manifest_rows: Sequence[Mapping[str, Any]],
    result_rows: Sequence[Mapping[str, Any]],
    total_cost_usd: Decimal | str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    if not manifest_rows:
        raise ValueError("manifest must contain at least one request")

    metadata = batch_state.get("metadata") or {}
    if not isinstance(metadata, Mapping):
        raise TypeError("batch metadata must be an object")
    llm_batch_id = _required_text(
        metadata.get("llm_batch_id") or batch_state.get("id"),
        field="llm_batch_id",
    )
    manifest = {
        _required_text(row.get("custom_id"), field="manifest custom_id"): row
        for row in manifest_rows
    }
    if len(manifest) != len(manifest_rows):
        raise ValueError("manifest contains duplicate custom_id values")
    results = {
        _required_text(row.get("custom_id"), field="result custom_id"): row
        for row in result_rows
    }
    if len(results) != len(result_rows):
        raise ValueError("results contain duplicate custom_id values")
    unknown = set(results) - set(manifest)
    if unknown:
        raise ValueError(f"results contain unknown custom_id values: {sorted(unknown)}")

    first_manifest = manifest_rows[0]
    model = _required_text(
        batch_state.get("model")
        or metadata.get("model")
        or first_manifest.get("model"),
        field="model",
    )
    prompt_version = _required_text(
        metadata.get("prompt_version") or first_manifest.get("prompt_version"),
        field="prompt_version",
    )
    request_counts = batch_state.get("request_counts") or {}
    batch_usage = batch_state.get("usage") or {}
    if not isinstance(request_counts, Mapping) or not isinstance(batch_usage, Mapping):
        raise TypeError("batch request_counts and usage must be objects")

    request_records: list[dict[str, Any]] = []
    analysis_records: list[dict[str, Any]] = []
    terminal = str(batch_state.get("status") or "prepared")
    for custom_id, manifest_row in manifest.items():
        result = results.get(custom_id)
        input_tokens = output_tokens = None
        if result is not None:
            input_tokens, output_tokens = _usage(result)
        request_records.append(
            {
                "custom_id": custom_id,
                "llm_batch_id": llm_batch_id,
                "event_id": _required_text(
                    manifest_row.get("event_id"), field="manifest event_id"
                ),
                "attempt": int(manifest_row.get("attempt", 1)),
                "status": (
                    "completed"
                    if result is not None
                    else "failed"
                    if terminal in {"completed", "failed", "expired", "cancelled"}
                    else "submitted"
                ),
                "validation_result": (
                    "validated" if result is not None else "missing_result"
                ),
                "error_code": None if result is not None else "MISSING_RESULT",
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
            }
        )
        if result is None:
            continue
        analysis_records.append(
            {
                "event_id": _required_text(result.get("event_id"), field="event_id"),
                "prompt_version": _required_text(
                    result.get("prompt_version") or prompt_version,
                    field="prompt_version",
                ),
                "model": _required_text(result.get("model") or model, field="model"),
                "sentiment": _required_text(
                    result.get("sentiment"), field="sentiment"
                ),
                "sentiment_score": float(result["sentiment_score"]),
                "topics": list(result.get("topics") or []),
                "keywords": list(result.get("keywords") or []),
                "summary": _required_text(result.get("summary"), field="summary"),
                "custom_id": custom_id,
            }
        )

    batch_record = {
        "llm_batch_id": llm_batch_id,
        "openai_batch_id": batch_state.get("id"),
        "input_file_id": batch_state.get("input_file_id"),
        "output_file_id": batch_state.get("output_file_id"),
        "model": model,
        "prompt_version": prompt_version,
        "status": terminal,
        "requested_count": int(request_counts.get("total", len(manifest))),
        "completed_count": int(request_counts.get("completed", len(results))),
        "failed_count": int(
            request_counts.get("failed", len(manifest) - len(results))
        ),
        "input_tokens": (
            int(batch_usage["input_tokens"])
            if batch_usage.get("input_tokens") is not None
            else None
        ),
        "output_tokens": (
            int(batch_usage["output_tokens"])
            if batch_usage.get("output_tokens") is not None
            else None
        ),
        "total_cost_usd": (
            str(Decimal(total_cost_usd)) if total_cost_usd is not None else None
        ),
        "submitted_at": _timestamp(
            batch_state.get("in_progress_at") or batch_state.get("created_at")
        ),
        "completed_at": _timestamp(batch_state.get("completed_at")),
    }
    return batch_record, request_records, analysis_records


def write_llm_batch_to_postgres(
    *,
    dsn: str,
    batch_state: Mapping[str, Any],
    manifest_rows: Sequence[Mapping[str, Any]],
    result_rows: Sequence[Mapping[str, Any]],
    total_cost_usd: Decimal | str | None = None,
) -> LLMPostgresWriteResult:
    if not dsn.strip():
        raise ValueError("PostgreSQL DSN must not be empty")
    batch, requests, analyses = build_llm_postgres_records(
        batch_state=batch_state,
        manifest_rows=manifest_rows,
        result_rows=result_rows,
        total_cost_usd=total_cost_usd,
    )
    try:
        import psycopg
    except ImportError as error:
        raise RuntimeError(
            "LLM PostgreSQL sink requires psycopg; install requirements.txt"
        ) from error

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(LLM_BATCH_UPSERT, batch)
            cursor.executemany(LLM_REQUEST_UPSERT, requests)
            if analyses:
                cursor.executemany(DOCUMENT_ANALYSIS_UPSERT, analyses)
    return LLMPostgresWriteResult(
        llm_batch_id=str(batch["llm_batch_id"]),
        request_rows=len(requests),
        analysis_rows=len(analyses),
        failed_rows=len(requests) - len(analyses),
    )


def summarize_llm_storage(*, dsn: str, llm_batch_ids: Sequence[str]) -> dict[str, int]:
    if not llm_batch_ids:
        return {"batch_rows": 0, "request_rows": 0, "analysis_rows": 0}
    try:
        import psycopg
    except ImportError as error:
        raise RuntimeError(
            "LLM PostgreSQL verification requires psycopg; install requirements.txt"
        ) from error
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM llm_batch_jobs WHERE llm_batch_id = ANY(%s)",
                (list(llm_batch_ids),),
            )
            batch_rows = int(cursor.fetchone()[0])
            cursor.execute(
                "SELECT count(*) FROM llm_batch_requests WHERE llm_batch_id = ANY(%s)",
                (list(llm_batch_ids),),
            )
            request_rows = int(cursor.fetchone()[0])
            cursor.execute(
                """
                SELECT count(*)
                FROM document_analyses analyses
                JOIN llm_batch_requests requests USING (custom_id)
                WHERE requests.llm_batch_id = ANY(%s)
                """,
                (list(llm_batch_ids),),
            )
            analysis_rows = int(cursor.fetchone()[0])
    return {
        "batch_rows": batch_rows,
        "request_rows": request_rows,
        "analysis_rows": analysis_rows,
    }
