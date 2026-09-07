from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


def _resolve_inside(project_root: Path, value: str | Path) -> Path:
    root = project_root.resolve()
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as error:
        raise ValueError("serving artifact must stay inside the project") from error
    return resolved


def build_serving_snapshot(
    *,
    project_root: Path,
    spark_report_path: str | Path,
    minio_result: Mapping[str, Any],
    llm_result: Mapping[str, Any],
    output_path: str | Path,
) -> dict[str, Any]:
    report_path = _resolve_inside(project_root, spark_report_path)
    target = _resolve_inside(project_root, output_path)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    accounting = report["row_accounting"]
    snapshot = {
        "status": "ready",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source_report": str(report_path.relative_to(project_root.resolve())),
        "input_rows": int(accounting["input_rows"]),
        "accounted_rows": int(accounting["accounted_rows"]),
        "unique_valid_rows": int(accounting["unique_valid_rows"]),
        "duplicate_event_id_rows": int(accounting["duplicate_event_id_rows"]),
        "contract_rejected_rows": int(accounting["contract_rejected_rows"]),
        "spark_duration_seconds": float(report["runtime"]["duration_seconds"]),
        "source_counts": report.get("source_counts") or {},
        "quality_status_counts": report.get("quality_status_counts") or {},
        "object_storage": {
            "status": str(minio_result.get("status") or "unknown"),
            "object_count": int(minio_result.get("object_count") or 0),
            "total_bytes": int(minio_result.get("total_bytes") or 0),
        },
        "llm": {
            "prepared_rows": int(llm_result.get("prepared_rows") or 0),
            "skipped_rows": int(llm_result.get("skipped_rows") or 0),
            "budget_status": str(llm_result.get("budget_status") or "unknown"),
            "submission_status": str(
                llm_result.get("submission_status") or "unknown"
            ),
        },
    }
    if snapshot["input_rows"] != snapshot["accounted_rows"]:
        raise ValueError("serving snapshot row accounting mismatch")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(target)
    from storage.data_lake import publish_artifact_if_enabled

    publish_artifact_if_enabled(target)
    return {**snapshot, "snapshot_path": str(target.relative_to(project_root.resolve()))}


def load_serving_snapshots(
    *, data_root: Path = Path("data/airflow-output"), limit: int = 20
) -> list[dict[str, Any]]:
    if limit < 1:
        raise ValueError("snapshot limit must be positive")
    if not data_root.exists():
        return []
    paths = sorted(
        data_root.rglob("serving-snapshot.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )[:limit]
    snapshots: list[dict[str, Any]] = []
    for path in paths:
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            continue
        snapshots.append({**value, "snapshot_path": str(path)})
    return snapshots
