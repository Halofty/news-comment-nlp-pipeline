from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any


SUCCESS_EVENT_ORDER = [
    "run_started",
    "spark_session_started",
    "input_loaded",
    "contract_validated",
    "deduplication_completed",
    "output_written",
    "metrics_collected",
    "spark_session_stopped",
    "report_written",
    "run_completed",
]

FORBIDDEN_PAYLOAD_KEYS = {
    "_raw_json",
    "author",
    "community",
    "event_id",
    "metadata",
    "source_name",
    "text",
    "title",
    "url",
}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _all_keys(value: Any) -> set[str]:
    if isinstance(value, dict):
        return set(value) | set().union(*(_all_keys(item) for item in value.values()))
    if isinstance(value, list):
        return set().union(*(_all_keys(item) for item in value))
    return set()


def validate_run_log(
    log_path: Path, *, report_path: Path | None = None
) -> dict[str, Any]:
    records = _read_jsonl(log_path)
    if not records:
        raise ValueError("operation log is empty")

    events = [record.get("event") for record in records]
    if events != SUCCESS_EVENT_ORDER:
        raise ValueError(f"unexpected event order: {events}")
    if [record.get("sequence") for record in records] != list(
        range(1, len(records) + 1)
    ):
        raise ValueError("log sequence is not contiguous")

    run_ids = {record.get("run_id") for record in records}
    if len(run_ids) != 1 or None in run_ids:
        raise ValueError("log must contain exactly one non-null run_id")

    timestamps = [
        datetime.fromisoformat(record["timestamp"].replace("Z", "+00:00"))
        for record in records
    ]
    elapsed = [record["elapsed_seconds"] for record in records]
    if timestamps != sorted(timestamps) or elapsed != sorted(elapsed):
        raise ValueError("timestamps and elapsed time must be monotonic")

    forbidden_present = _all_keys(records) & FORBIDDEN_PAYLOAD_KEYS
    if forbidden_present:
        raise ValueError(f"payload keys present in operation log: {sorted(forbidden_present)}")

    by_event = {record["event"]: record for record in records}
    loaded = by_event["input_loaded"]
    contract = by_event["contract_validated"]
    dedup = by_event["deduplication_completed"]
    metrics = by_event["metrics_collected"]
    completed = by_event["run_completed"]

    input_rows = loaded["input_rows"]
    accounted_rows = (
        dedup["unique_valid_rows"]
        + dedup["duplicate_event_id_rows"]
        + contract["contract_rejected_rows"]
    )
    quality_rows = sum(metrics["quality_status_counts"].values())
    if accounted_rows != input_rows or metrics["accounted_rows"] != input_rows:
        raise ValueError("row accounting mismatch in operation log")
    if quality_rows != dedup["unique_valid_rows"]:
        raise ValueError("quality status counts do not match unique rows")
    if completed["input_rows"] != input_rows or completed["accounted_rows"] != input_rows:
        raise ValueError("completion event does not match input accounting")

    if report_path is not None:
        report = json.loads(report_path.read_text(encoding="utf-8"))
        report_accounting = report["row_accounting"]
        if report["input"]["sha256"] != loaded["input_sha256"]:
            raise ValueError("input checksum differs between log and report")
        for key in ("input_rows", "accounted_rows", "unique_valid_rows"):
            if report_accounting[key] != completed[key]:
                raise ValueError(f"{key} differs between log and report")

    checks = [
        "event_order",
        "sequence_continuity",
        "single_run_id",
        "monotonic_time",
        "row_accounting",
        "quality_status_accounting",
        "payload_key_exclusion",
    ]
    if report_path:
        checks.append("report_consistency")

    return {
        "validation_status": "pass",
        "run_id": next(iter(run_ids)),
        "event_count": len(records),
        "input_rows": input_rows,
        "accounted_rows": accounted_rows,
        "unique_valid_rows": dedup["unique_valid_rows"],
        "duplicate_event_id_rows": dedup["duplicate_event_id_rows"],
        "contract_rejected_rows": contract["contract_rejected_rows"],
        "quality_status_counts": metrics["quality_status_counts"],
        "pipeline_duration_seconds": round(
            by_event["metrics_collected"]["elapsed_seconds"]
            - by_event["spark_session_started"]["elapsed_seconds"],
            3,
        ),
        "total_duration_seconds": completed["elapsed_seconds"],
        "payload_keys_present": [],
        "checks": checks,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate a Spark operational JSONL log")
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    print(
        json.dumps(
            validate_run_log(args.log, report_path=args.report),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
