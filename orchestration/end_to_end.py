from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from orchestration.llm_batch import prepare_config as prepare_llm_config


def prepare_llm_from_spark(
    *,
    project_root: Path,
    pipeline_config: Mapping[str, Any],
    spark_config: Mapping[str, Any],
    spark_verification: Mapping[str, Any],
    params: Mapping[str, Any],
) -> dict[str, Any]:
    unique_rows = int(spark_verification["unique_valid_rows"])
    if unique_rows < 1:
        raise ValueError("Spark produced no valid rows for LLM analysis")

    output_path = Path(str(spark_config["output_path"])) / "events"
    if str(spark_config["output_format"]) == "jsonl":
        output_path /= "events.jsonl"
    absolute_output = project_root / output_path
    if not absolute_output.exists():
        raise FileNotFoundError(absolute_output)

    llm_config = prepare_llm_config(
        project_root=project_root,
        params={
            "input_path": str(output_path),
            "output_root": str(params["llm_output_root"]),
            "model": str(params["model"]),
            "limit": min(int(params["llm_limit"]), unique_rows),
            "daily_budget_usd": str(params["daily_budget_usd"]),
            "submit": bool(params["submit"]),
        },
        airflow_run_id=str(pipeline_config["airflow_run_id"]),
    )
    llm_config["spark_report_path"] = str(spark_verification["report_path"])
    llm_config["spark_unique_valid_rows"] = unique_rows
    return llm_config
