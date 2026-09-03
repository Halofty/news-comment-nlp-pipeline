from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Mapping

SAFE_LABEL_PATTERN = re.compile(r"[^a-zA-Z0-9_.-]+")
ALLOWED_INPUT_ROOTS = ("data", "sample")


def _safe_component(value: str, *, fallback: str) -> str:
    normalized = SAFE_LABEL_PATTERN.sub("-", value).strip("-._")
    return normalized[:120] or fallback


def _resolve_within(root: Path, relative_path: str, *, field: str) -> Path:
    if not relative_path or Path(relative_path).is_absolute():
        raise ValueError(f"{field} must be a non-empty project-relative path")
    resolved = (root / relative_path).resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as error:
        raise ValueError(f"{field} must stay inside the project directory") from error
    return resolved


def prepare_run_config(
    *,
    project_root: Path,
    params: Mapping[str, Any],
    airflow_run_id: str,
) -> dict[str, Any]:
    project_root = project_root.resolve()
    input_file = str(params["input_file"])
    input_path = _resolve_within(project_root, input_file, field="input_file")
    if not input_path.is_file():
        from storage.data_lake import materialize_artifact_if_enabled

        materialize_artifact_if_enabled(input_path)
    if input_path.suffix != ".jsonl":
        raise ValueError("input_file must have a .jsonl suffix")
    if input_path.relative_to(project_root).parts[0] not in ALLOWED_INPUT_ROOTS:
        raise ValueError("input_file must be under data/ or sample/")
    if not input_path.is_file():
        raise FileNotFoundError(f"input_file does not exist: {input_file}")

    output_root_value = str(params["output_root"])
    output_root = _resolve_within(
        project_root, output_root_value, field="output_root"
    )
    if output_root.relative_to(project_root).parts[0] != "data":
        raise ValueError("output_root must be under data/")

    run_label = _safe_component(str(params["run_label"]), fallback="spark-batch")
    run_id = _safe_component(airflow_run_id, fallback="manual-run")
    run_directory = output_root / run_label / run_id
    output_format = str(params["output_format"])
    if output_format not in {"parquet", "jsonl"}:
        raise ValueError("output_format must be parquet or jsonl")
    partitions = int(params["partitions"])
    if not 1 <= partitions <= 64:
        raise ValueError("partitions must be between 1 and 64")

    return {
        "project_root": str(project_root),
        "input_file": str(input_path.relative_to(project_root)),
        "input_sha256": _sha256(input_path),
        "run_label": run_label,
        "airflow_run_id": airflow_run_id,
        "run_directory": str(run_directory.relative_to(project_root)),
        "output_path": str((run_directory / "output").relative_to(project_root)),
        "report_path": str(
            (run_directory / "report.json").relative_to(project_root)
        ),
        "log_path": str((run_directory / "run.log.jsonl").relative_to(project_root)),
        "output_format": output_format,
        "partitions": partitions,
        "spark_master": str(params["spark_master"]),
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_spark_command(config: Mapping[str, Any]) -> list[str]:
    command = [
        sys.executable,
        "-m",
        "spark_jobs.process_sample",
        "--input",
        str(config["input_file"]),
        "--output",
        str(config["output_path"]),
        "--report",
        str(config["report_path"]),
        "--log",
        str(config["log_path"]),
        "--master",
        str(config["spark_master"]),
        "--partitions",
        str(config["partitions"]),
        "--format",
        str(config["output_format"]),
    ]
    return command


def run_spark_batch(
    config: Mapping[str, Any],
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> str:
    project_root = Path(str(config["project_root"]))
    report_path = project_root / str(config["report_path"])
    report_path.parent.mkdir(parents=True, exist_ok=True)
    command = build_spark_command(config)
    print("Executing:", " ".join(command))
    runner(command, cwd=project_root, check=True, text=True)
    if not report_path.is_file():
        raise RuntimeError(f"Spark completed without a report: {report_path}")
    return str(config["report_path"])


def verify_report(*, project_root: Path, report_path: str) -> dict[str, Any]:
    resolved = _resolve_within(project_root.resolve(), report_path, field="report_path")
    report = json.loads(resolved.read_text(encoding="utf-8"))
    accounting = report["row_accounting"]
    input_rows = int(accounting["input_rows"])
    accounted_rows = int(accounting["accounted_rows"])
    if input_rows < 1:
        raise ValueError("Spark report contains no input rows")
    if input_rows != accounted_rows:
        raise ValueError(
            f"row accounting mismatch: input={input_rows}, accounted={accounted_rows}"
        )
    summary = {
        "report_path": report_path,
        "input_rows": input_rows,
        "accounted_rows": accounted_rows,
        "unique_valid_rows": int(accounting["unique_valid_rows"]),
        "duplicate_event_id_rows": int(accounting["duplicate_event_id_rows"]),
        "contract_rejected_rows": int(accounting["contract_rejected_rows"]),
        "duration_seconds": report["runtime"]["duration_seconds"],
    }
    print("Verified Spark report:", json.dumps(summary, ensure_ascii=False))
    return summary
