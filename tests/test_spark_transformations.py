from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from jobs.generate_synthetic_events import generate_events
from jobs.validate_run_log import validate_run_log
from spark_jobs.process_sample import (
    choose_output_partitions,
    create_spark_session,
    process_batch,
)
from spark_jobs.run_logging import JsonlRunLogger
from spark_jobs.schemas import RAW_EVENT_SCHEMA
from storage.jsonl import write_jsonl


def test_raw_event_schema_is_explicit_and_contains_corrupt_record() -> None:
    assert RAW_EVENT_SCHEMA.fieldNames() == [
        "event_id",
        "source_type",
        "source_name",
        "event_time",
        "collected_at",
        "language",
        "title",
        "text",
        "url",
        "community",
        "engagement",
        "schema_version",
        "metadata",
        "_corrupt_record",
    ]


def test_synthetic_generator_is_deterministic_and_includes_duplicates() -> None:
    first = list(generate_events(101))
    second = list(generate_events(101))

    assert first == second
    assert len(first) == 101
    assert first[50] == first[49]
    assert first[100] == first[99]
    assert len({event["event_id"] for event in first}) == 99


def test_output_partition_policy_scales_and_is_bounded() -> None:
    assert choose_output_partitions(100) == 2
    assert choose_output_partitions(1_000) == 4
    assert choose_output_partitions(100_000) == 64


def test_committed_spark_profiles_account_for_every_row() -> None:
    for name in ("spark-100-profile.json", "spark-1000-profile.json"):
        path = Path("analysis/reports") / name
        profile = json.loads(path.read_text(encoding="utf-8"))
        status_total = sum(profile["quality_status_counts"].values())

        assert profile["input_rows"] == profile["accounted_rows"]
        assert (
            profile["unique_output_rows"]
            + profile["duplicate_event_id_rows"]
            + profile["contract_rejected_rows"]
            == profile["input_rows"]
        )
        assert status_total == profile["unique_output_rows"]


def test_committed_spark_run_log_passes_audit() -> None:
    audit = validate_run_log(Path("analysis/reports/spark-1000-run-log.jsonl"))

    assert audit["validation_status"] == "pass"
    assert audit["event_count"] == 10
    assert audit["input_rows"] == audit["accounted_rows"] == 1_000
    assert audit["unique_valid_rows"] == 981
    assert audit["payload_keys_present"] == []


def test_spark_batch_accounts_for_every_input_row() -> None:
    with TemporaryDirectory() as temporary_directory:
        root = Path(temporary_directory)
        input_path = root / "events.jsonl"
        output_path = root / "output"
        log_path = root / "run.jsonl"
        write_jsonl(generate_events(101), input_path)
        unexpected = next(generate_events(1))
        unexpected["unexpected_field"] = "must be rejected"
        with input_path.open("a", encoding="utf-8") as file:
            file.write(json.dumps({"event_id": "missing-fields"}) + "\n")
            file.write(json.dumps(unexpected) + "\n")
            file.write("not-json\n")

        spark = create_spark_session(master="local[1]", app_name="spark-test")
        spark.sparkContext.setLogLevel("ERROR")
        try:
            report = process_batch(
                spark,
                input_path=input_path,
                output_path=output_path,
                output_partitions=2,
                output_format="jsonl",
                run_logger=JsonlRunLogger(log_path, run_id="test-run"),
            )
        finally:
            spark.stop()

        output_rows = sum(
            1
            for line in (output_path / "events" / "events.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
            if line.strip()
        )

        accounting = report["row_accounting"]
        assert accounting["input_rows"] == 104
        assert accounting["schema_parsing_success_rows"] == 103
        assert accounting["contract_rejected_rows"] == 3
        assert accounting["duplicate_event_id_rows"] == 2
        assert accounting["unique_valid_rows"] == 99
        assert accounting["accounted_rows"] == 104
        assert output_rows == 99
        assert report["contract_error_counts"]["MALFORMED_JSON"] == 1
        assert report["contract_error_counts"]["MISSING_FIELDS"] >= 1
        assert report["contract_error_counts"]["UNEXPECTED_FIELDS"] == 1
        assert report["quality_status_counts"]["quarantine"] >= 1
        assert report["quality_status_counts"]["reject"] >= 1

        log_records = [
            json.loads(line)
            for line in log_path.read_text(encoding="utf-8").splitlines()
        ]
        assert [record["event"] for record in log_records] == [
            "input_loaded",
            "contract_validated",
            "deduplication_completed",
            "output_written",
            "metrics_collected",
        ]
        assert [record["sequence"] for record in log_records] == [1, 2, 3, 4, 5]
        assert {record["run_id"] for record in log_records} == {"test-run"}
        assert log_records[-1]["input_rows"] == 104
        assert log_records[-1]["accounted_rows"] == 104

        serialized_log = json.dumps(log_records, ensure_ascii=False)
        for forbidden_key in (
            '"text"',
            '"title"',
            '"url"',
            '"event_id"',
            '"author"',
            '"_raw_json"',
        ):
            assert forbidden_key not in serialized_log
