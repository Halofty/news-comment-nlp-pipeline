from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Mapping, Sequence


RAW_EVENT_UPSERT = """
INSERT INTO raw_text_events (
    event_id, source_type, source_name, event_time, collected_at, language,
    title, text_original, url, community, engagement, schema_version,
    metadata, raw_payload, kafka_topic, kafka_partition, kafka_offset
) VALUES (
    %(event_id)s, %(source_type)s, %(source_name)s, %(event_timestamp)s,
    %(collected_timestamp)s, %(language)s, %(title)s, %(text_original)s,
    %(url)s, %(community)s, %(engagement)s, %(schema_version)s,
    %(metadata_json)s::jsonb, %(raw_payload)s, %(kafka_topic)s,
    %(kafka_partition)s, %(kafka_offset)s
)
ON CONFLICT (event_id) DO UPDATE SET
    collected_at = EXCLUDED.collected_at,
    engagement = EXCLUDED.engagement,
    metadata = EXCLUDED.metadata,
    raw_payload = EXCLUDED.raw_payload,
    kafka_topic = EXCLUDED.kafka_topic,
    kafka_partition = EXCLUDED.kafka_partition,
    kafka_offset = EXCLUDED.kafka_offset,
    updated_at = now()
"""

CLEAN_DOCUMENT_UPSERT = """
INSERT INTO text_documents_clean (
    event_id, text_clean, quality_policy_version, quality_status,
    quality_flags, exclusion_reason, character_count, utf8_byte_count,
    control_character_count, zero_width_count, max_combining_mark_run,
    url_count, url_ratio, repetition_ratio, was_normalized, was_truncated,
    output_route, batch_id
) VALUES (
    %(event_id)s, %(text_clean)s, %(quality_policy_version)s,
    %(quality_status)s, %(quality_flags)s, %(exclusion_reason)s,
    %(character_count)s, %(utf8_byte_count)s, %(control_character_count)s,
    %(zero_width_count)s, %(max_combining_mark_run)s, %(url_count)s,
    %(url_ratio)s, %(repetition_ratio)s, %(was_normalized)s,
    %(was_truncated)s, %(output_route)s, %(batch_id)s
)
ON CONFLICT (event_id) DO UPDATE SET
    text_clean = EXCLUDED.text_clean,
    quality_policy_version = EXCLUDED.quality_policy_version,
    quality_status = EXCLUDED.quality_status,
    quality_flags = EXCLUDED.quality_flags,
    exclusion_reason = EXCLUDED.exclusion_reason,
    character_count = EXCLUDED.character_count,
    utf8_byte_count = EXCLUDED.utf8_byte_count,
    control_character_count = EXCLUDED.control_character_count,
    zero_width_count = EXCLUDED.zero_width_count,
    max_combining_mark_run = EXCLUDED.max_combining_mark_run,
    url_count = EXCLUDED.url_count,
    url_ratio = EXCLUDED.url_ratio,
    repetition_ratio = EXCLUDED.repetition_ratio,
    was_normalized = EXCLUDED.was_normalized,
    was_truncated = EXCLUDED.was_truncated,
    output_route = EXCLUDED.output_route,
    batch_id = EXCLUDED.batch_id,
    updated_at = now()
"""

REJECTED_EVENT_UPSERT = """
INSERT INTO contract_rejected_events (
    kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, kafka_key,
    raw_event, contract_errors, batch_id
) VALUES (
    %(kafka_topic)s, %(kafka_partition)s, %(kafka_offset)s,
    %(kafka_timestamp)s, %(kafka_key)s, %(raw_event)s,
    %(contract_errors)s, %(batch_id)s
)
ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO UPDATE SET
    contract_errors = EXCLUDED.contract_errors,
    batch_id = EXCLUDED.batch_id
"""


@dataclass(frozen=True)
class PostgresWriteResult:
    status: str
    input_rows: int
    valid_rows: int
    rejected_rows: int


def _as_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    if isinstance(row, Mapping):
        return dict(row)
    raise TypeError("PostgreSQL sink rows must be mappings or Spark Rows")


def _safe_text(value: Any) -> Any:
    if isinstance(value, str):
        return value.replace("\x00", "\ufffd")
    if isinstance(value, dict):
        return {key: _safe_text(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_safe_text(item) for item in value]
    return value


def event_to_postgres_records(
    row: Any, *, batch_id: int
) -> tuple[str, dict[str, Any]]:
    event = _as_dict(row)
    if not event.get("contract_valid"):
        return "rejected", {
            "kafka_topic": event.get("kafka_topic"),
            "kafka_partition": event.get("kafka_partition"),
            "kafka_offset": event.get("kafka_offset"),
            "kafka_timestamp": event.get("kafka_timestamp"),
            "kafka_key": _safe_text(event.get("kafka_key")),
            "raw_event": _safe_text(event.get("_raw_json")),
            "contract_errors": event.get("contract_errors") or [],
            "batch_id": batch_id,
        }

    raw = {
        "event_id": event["event_id"],
        "source_type": event["source_type"],
        "source_name": event["source_name"],
        "event_timestamp": event["event_timestamp"],
        "collected_timestamp": event["collected_timestamp"],
        "language": _safe_text(event["language"]),
        "title": _safe_text(event.get("title")),
        "text_original": _safe_text(event["text_original"]),
        "url": _safe_text(event.get("url")),
        "community": _safe_text(event.get("community")),
        "engagement": event.get("engagement"),
        "schema_version": event["schema_version"],
        "metadata_json": json.dumps(
            _safe_text(event.get("metadata") or {}), ensure_ascii=False
        ),
        "raw_payload": _safe_text(event["_raw_json"]),
        "kafka_topic": event["kafka_topic"],
        "kafka_partition": event["kafka_partition"],
        "kafka_offset": event["kafka_offset"],
    }
    clean = {
        "event_id": event["event_id"],
        "text_clean": _safe_text(event["text_clean"]),
        "quality_policy_version": event["quality_policy_version"],
        "quality_status": event["quality_status"],
        "quality_flags": event.get("quality_flags") or [],
        "exclusion_reason": event.get("exclusion_reason"),
        "character_count": event["character_count"],
        "utf8_byte_count": event["utf8_byte_count"],
        "control_character_count": event["control_character_count"],
        "zero_width_count": event["zero_width_count"],
        "max_combining_mark_run": event["max_combining_mark_run"],
        "url_count": event["url_count"],
        "url_ratio": event["url_ratio"],
        "repetition_ratio": event["repetition_ratio"],
        "was_normalized": event["was_normalized"],
        "was_truncated": event["was_truncated"],
        "output_route": event["output_route"],
        "batch_id": batch_id,
    }
    return "valid", {"raw": raw, "clean": clean}


def _chunks(values: Iterable[Any], size: int) -> Iterator[list[Any]]:
    chunk: list[Any] = []
    for value in values:
        chunk.append(value)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def write_micro_batch_to_postgres(
    rows: Iterable[Any],
    *,
    dsn: str,
    consumer_name: str,
    batch_id: int,
    route_counts: Mapping[str, int],
    chunk_size: int = 500,
) -> PostgresWriteResult:
    if not dsn.strip():
        raise ValueError("PostgreSQL DSN must not be empty")
    if not consumer_name.strip():
        raise ValueError("consumer_name must not be empty")
    if chunk_size < 1:
        raise ValueError("chunk_size must be positive")

    try:
        import psycopg
    except ImportError as error:
        raise RuntimeError(
            "PostgreSQL sink requires psycopg; install requirements.txt"
        ) from error

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (consumer_name,),
            )
            cursor.execute(
                "SELECT 1 FROM stream_batch_commits WHERE consumer_name = %s AND batch_id = %s",
                (consumer_name, batch_id),
            )
            if cursor.fetchone() is not None:
                return PostgresWriteResult("already_committed", 0, 0, 0)

            valid_rows = 0
            rejected_rows = 0
            for chunk in _chunks(rows, chunk_size):
                raw_records: list[dict[str, Any]] = []
                clean_records: list[dict[str, Any]] = []
                rejected_records: list[dict[str, Any]] = []
                for row in chunk:
                    kind, record = event_to_postgres_records(row, batch_id=batch_id)
                    if kind == "valid":
                        raw_records.append(record["raw"])
                        clean_records.append(record["clean"])
                        valid_rows += 1
                    else:
                        rejected_records.append(record)
                        rejected_rows += 1
                if raw_records:
                    cursor.executemany(RAW_EVENT_UPSERT, raw_records)
                    cursor.executemany(CLEAN_DOCUMENT_UPSERT, clean_records)
                if rejected_records:
                    cursor.executemany(REJECTED_EVENT_UPSERT, rejected_records)

            input_rows = valid_rows + rejected_rows
            cursor.execute(
                """
                INSERT INTO stream_batch_commits (
                    consumer_name, batch_id, input_rows, route_counts
                ) VALUES (%s, %s, %s, %s::jsonb)
                """,
                (
                    consumer_name,
                    batch_id,
                    input_rows,
                    json.dumps(dict(route_counts), sort_keys=True),
                ),
            )
    return PostgresWriteResult("committed", input_rows, valid_rows, rejected_rows)
