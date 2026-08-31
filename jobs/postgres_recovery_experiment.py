from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds


UPSERT = """
INSERT INTO raw_text_events (
    event_id, source_type, source_name, event_time, collected_at, language,
    title, text_original, url, community, engagement, schema_version,
    metadata, raw_payload, kafka_topic, kafka_partition, kafka_offset
) VALUES (
    %(event_id)s, %(source_type)s, %(source_name)s, %(event_timestamp)s,
    %(collected_timestamp)s, %(language)s, %(title)s, %(text)s,
    %(url)s, %(community)s, %(engagement)s, %(schema_version)s,
    %(metadata_json)s::jsonb, %(raw_payload)s, %(kafka_topic)s,
    %(kafka_partition)s, %(kafka_offset)s
)
ON CONFLICT (event_id) DO UPDATE SET
    engagement = EXCLUDED.engagement,
    metadata = EXCLUDED.metadata,
    updated_at = now()
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sample_records(root: Path, *, per_source: int) -> list[dict[str, Any]]:
    dataset = ds.dataset(root, format="parquet", partitioning="hive")
    columns = [
        "event_id",
        "source_type",
        "source_name",
        "event_timestamp",
        "collected_timestamp",
        "language",
        "title",
        "text",
        "url",
        "community",
        "engagement",
        "schema_version",
        "metadata_json",
    ]
    records: list[dict[str, Any]] = []
    for partition, source in enumerate(("reddit", "web_news")):
        table = dataset.scanner(
            columns=columns,
            filter=ds.field("source_name") == source,
        ).head(per_source)
        rows = sorted(table.to_pylist(), key=lambda row: row["event_id"])
        if len(rows) != per_source:
            raise ValueError(
                f"expected {per_source} {source} rows, found {len(rows)}"
            )
        for offset, row in enumerate(rows):
            row["metadata_json"] = row["metadata_json"] or "{}"
            row["raw_payload"] = json.dumps(row, default=str, ensure_ascii=False)
            row["kafka_topic"] = "week5-db-recovery"
            row["kafka_partition"] = partition
            row["kafka_offset"] = offset
            records.append(row)
    return records


def count_ids(connection: Any, event_ids: list[str]) -> tuple[int, int]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT count(*), count(DISTINCT event_id)
            FROM raw_text_events WHERE event_id = ANY(%s)
            """,
            (event_ids,),
        )
        row = cursor.fetchone()
    return int(row[0]), int(row[1])


def run(
    *,
    input_root: Path,
    wrong_dsn: str,
    correct_dsn: str,
    per_source: int,
    report_path: Path,
) -> dict[str, Any]:
    import psycopg

    started = time.perf_counter()
    records = sample_records(input_root, per_source=per_source)
    event_ids = [row["event_id"] for row in records]
    report: dict[str, Any] = {
        "started_at": utc_now(),
        "input_root": str(input_root),
        "sample_per_source": per_source,
        "attempted_rows": len(records),
        "source_rows": {"reddit": per_source, "web_news": per_source},
    }

    failure_started = time.perf_counter()
    try:
        psycopg.connect(wrong_dsn, connect_timeout=2)
    except psycopg.OperationalError as error:
        report["failure_reproduced"] = True
        report["failure_type"] = type(error).__name__
        report["failure_message"] = str(error).splitlines()[0]
        report["failure_seconds"] = round(time.perf_counter() - failure_started, 3)
    else:
        raise RuntimeError("wrong DSN unexpectedly connected")

    recovery_started = time.perf_counter()
    with psycopg.connect(correct_dsn) as connection:
        before_rows, _ = count_ids(connection, event_ids)
        with connection.cursor() as cursor:
            cursor.executemany(UPSERT, records)
        after_rows, after_distinct = count_ids(connection, event_ids)
    report.update(
        {
            "rows_before_recovery": before_rows,
            "rows_after_recovery": after_rows,
            "distinct_ids_after_recovery": after_distinct,
            "missing_rows_after_recovery": len(records) - after_rows,
            "duplicate_rows_after_recovery": after_rows - after_distinct,
        }
    )

    repeat_started = time.perf_counter()
    with psycopg.connect(correct_dsn) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(UPSERT, records)
        repeated_rows, repeated_distinct = count_ids(connection, event_ids)
    report.update(
        {
            "rows_after_duplicate_run": repeated_rows,
            "distinct_ids_after_duplicate_run": repeated_distinct,
            "duplicate_rows_after_duplicate_run": repeated_rows - repeated_distinct,
            "duplicate_run_seconds": round(time.perf_counter() - repeat_started, 3),
            "recovery_seconds": round(time.perf_counter() - recovery_started, 3),
            "status": "success",
            "finished_at": utc_now(),
            "duration_seconds": round(time.perf_counter() - started, 3),
        }
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reproduce and recover a local PostgreSQL connection failure"
    )
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--wrong-dsn", required=True)
    parser.add_argument("--correct-dsn", required=True)
    parser.add_argument("--per-source", type=int, default=100)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if args.per_source < 1:
        raise ValueError("per-source must be positive")
    report = run(
        input_root=args.input_root,
        wrong_dsn=args.wrong_dsn,
        correct_dsn=args.correct_dsn,
        per_source=args.per_source,
        report_path=args.report,
    )
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
