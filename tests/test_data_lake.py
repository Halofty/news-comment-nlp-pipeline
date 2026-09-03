from __future__ import annotations

from pathlib import Path

import pytest

from storage.data_lake import iter_data_artifacts, route_artifact
from storage.jsonl import write_jsonl


def test_data_directories_route_to_separate_buckets(tmp_path: Path) -> None:
    data = tmp_path / "data"
    raw = data / "raw/reddit/file.parquet"
    response = data / "llm_response/run/result.jsonl"
    raw.parent.mkdir(parents=True)
    response.parent.mkdir(parents=True)
    raw.write_bytes(b"PAR1")
    response.write_text("{}\n", encoding="utf-8")

    raw_artifact = route_artifact(raw, data_root=data)
    response_artifact = route_artifact(response, data_root=data)

    assert (raw_artifact.bucket, raw_artifact.key) == (
        "news-raw",
        "raw/reddit/file.parquet",
    )
    assert (response_artifact.bucket, response_artifact.key) == (
        "news-llm",
        "responses/run/result.jsonl",
    )

    airflow_llm = data / "airflow-output/end-to-end-llm/run/requests.jsonl"
    airflow_llm.parent.mkdir(parents=True)
    airflow_llm.write_text("{}\n", encoding="utf-8")
    routed = route_artifact(airflow_llm, data_root=data)
    assert routed.bucket == "news-llm"
    assert routed.key == (
        "airflow/airflow-output/end-to-end-llm/run/requests.jsonl"
    )

    spark_output = data / "airflow-output/reddit-day/run/output/events/part.parquet"
    spark_report = data / "airflow-output/reddit-day/run/report.json"
    spark_output.parent.mkdir(parents=True)
    spark_output.write_bytes(b"PAR1")
    spark_report.write_text("{}\n", encoding="utf-8")
    output_route = route_artifact(spark_output, data_root=data)
    report_route = route_artifact(spark_report, data_root=data)
    assert (output_route.bucket, output_route.key) == (
        "news-processed",
        "airflow/reddit-day/run/output/events/part.parquet",
    )
    assert (report_route.bucket, report_route.key) == (
        "news-reports",
        "airflow/reddit-day/run/report.json",
    )


def test_inventory_excludes_partial_and_sparse_work_files(tmp_path: Path) -> None:
    data = tmp_path / "data"
    raw = data / "raw"
    raw.mkdir(parents=True)
    (raw / "complete.parquet").write_bytes(b"complete")
    (raw / "download.parquet.part").write_bytes(b"partial")
    (raw / "recovery.sparse.parquet").write_bytes(b"sparse")

    artifacts = list(iter_data_artifacts(data))

    assert [item.key for item in artifacts] == ["raw/complete.parquet"]


def test_jsonl_writer_publishes_when_minio_backend_is_enabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "data/raw/events.jsonl"
    published: list[Path] = []
    monkeypatch.setenv("PIPELINE_STORAGE_BACKEND", "minio")
    monkeypatch.setattr(
        "storage.data_lake.publish_artifact_if_enabled",
        lambda path: published.append(path),
    )
    event = {
        "event_id": "a" * 64,
        "source_type": "news",
        "source_name": "web_news",
        "event_time": "2012-01-01T00:00:00Z",
        "collected_at": "2026-09-03T00:00:00Z",
        "language": "en",
        "title": "Title",
        "text": "Title",
        "url": "https://example.com",
        "community": None,
        "engagement": None,
        "schema_version": 1,
        "metadata": {},
    }

    assert write_jsonl([event], output) == 1
    assert published == [output]
