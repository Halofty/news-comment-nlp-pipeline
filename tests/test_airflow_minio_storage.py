from __future__ import annotations

from pathlib import Path

from orchestration.object_storage import build_minio_sync_config, sync_spark_output
from tests.test_object_store import FakeS3Client


def _spark_config(root: Path) -> dict:
    return {
        "output_path": "data/runs/output",
        "run_directory": "data/runs",
        "run_label": "reddit daily",
        "airflow_run_id": "manual__2016-01-01T00:00:00+00:00",
    }


def test_minio_sync_config_uses_safe_run_prefix(tmp_path: Path) -> None:
    config = build_minio_sync_config(
        project_root=tmp_path, spark_config=_spark_config(tmp_path)
    )
    assert config["prefix"].startswith("airflow/reddit-daily/manual__2016-01-01")
    assert config["output_path"] == str(tmp_path / "data/runs/output")


def test_spark_output_is_uploaded_with_run_report(tmp_path: Path) -> None:
    output = tmp_path / "data/runs/output/events"
    output.mkdir(parents=True)
    (output / "part-00000.jsonl").write_text("{}\n", encoding="utf-8")
    client = FakeS3Client()
    client.buckets.add("news-processed")

    result = sync_spark_output(
        project_root=tmp_path,
        spark_config=_spark_config(tmp_path),
        spark_verification={"unique_valid_rows": 1},
        enabled=True,
        client=client,
    )

    assert result["status"] == "completed"
    assert result["object_count"] == 1
    assert result["uploaded"] == 1
    assert "objects" not in result
    assert (tmp_path / "data/runs/minio-storage.json").is_file()
