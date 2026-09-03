from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Mapping

from storage.object_store import (
    ObjectStoreConfig,
    ensure_buckets,
    upload_directory_verified,
    upload_file_verified,
)

SAFE_COMPONENT = re.compile(r"[^a-zA-Z0-9_.-]+")


def _safe_component(value: str, *, fallback: str) -> str:
    normalized = SAFE_COMPONENT.sub("-", value).strip("-._")
    return normalized[:120] or fallback


def build_minio_sync_config(
    *, project_root: Path, spark_config: Mapping[str, Any]
) -> dict[str, str]:
    root = project_root.resolve()
    output_path = (root / str(spark_config["output_path"])).resolve()
    try:
        output_path.relative_to(root)
    except ValueError as error:
        raise ValueError("Spark output_path must stay inside the project") from error
    run_label = _safe_component(str(spark_config["run_label"]), fallback="spark-batch")
    run_id = _safe_component(str(spark_config["airflow_run_id"]), fallback="manual-run")
    report_path = (root / str(spark_config["run_directory"]) / "minio-storage.json").resolve()
    return {
        "output_path": str(output_path),
        "prefix": f"airflow/{run_label}/{run_id}/output",
        "report_path": str(report_path),
    }


def sync_spark_output(
    *,
    project_root: Path,
    spark_config: Mapping[str, Any],
    spark_verification: Mapping[str, Any],
    enabled: bool,
    client: Any | None = None,
) -> dict[str, Any]:
    if not enabled:
        return {"status": "disabled", "object_count": 0, "total_bytes": 0}
    config = build_minio_sync_config(
        project_root=project_root, spark_config=spark_config
    )
    if int(spark_verification["unique_valid_rows"]) < 1:
        raise ValueError("Spark output contains no valid rows to store")
    s3_client = client or ObjectStoreConfig.from_env().create_client()
    bucket = os.getenv("MINIO_PROCESSED_BUCKET", "news-processed")
    result = upload_directory_verified(
        s3_client,
        local_directory=Path(config["output_path"]),
        bucket=bucket,
        prefix=config["prefix"],
    )
    result.update(
        {
            "status": "completed",
            "spark_unique_valid_rows": int(spark_verification["unique_valid_rows"]),
        }
    )
    report_path = Path(config["report_path"])
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    report_bucket = os.getenv("MINIO_REPORTS_BUCKET", "news-reports")
    ensure_buckets(s3_client, [report_bucket])
    upload_file_verified(
        s3_client,
        local_path=report_path,
        bucket=report_bucket,
        key=f"airflow/{config['prefix'].removeprefix('airflow/').removesuffix('/output')}/minio-storage.json",
    )
    summary = {name: value for name, value in result.items() if name != "objects"}
    summary["report_path"] = str(report_path.relative_to(project_root.resolve()))
    return summary
