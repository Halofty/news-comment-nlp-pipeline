from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import DAG, Param, get_current_context, task

from orchestration.spark_batch import prepare_run_config, run_spark_batch, verify_report

PROJECT_ROOT = Path("/opt/airflow/project")

with DAG(
    dag_id="spark_parameterized_text_batch",
    description="Run the existing TextEvent Spark batch with trigger-time parameters",
    schedule=None,
    start_date=datetime(2026, 8, 27, tzinfo=timezone.utc),
    catchup=False,
    default_args={
        "owner": "news-comment-nlp-pipeline",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    params={
        "input_file": Param(
            "sample/synthetic-events.jsonl",
            type="string",
            pattern="^(data|sample)/.+\\.jsonl$",
            title="Input JSONL file",
            description="Project-relative TextEvent v1 JSONL path",
        ),
        "run_label": Param(
            "manual-sample",
            type="string",
            minLength=1,
            maxLength=80,
            title="Run label",
            description="Separates outputs from different parameter runs",
        ),
        "output_root": Param(
            "data/airflow-output",
            type="string",
            pattern="^data/.+",
            title="Output root",
        ),
        "output_format": Param(
            "parquet",
            type="string",
            enum=["parquet", "jsonl"],
            title="Output format",
        ),
        "partitions": Param(
            2,
            type="integer",
            minimum=1,
            maximum=64,
            title="Output partitions",
        ),
        "spark_master": Param(
            "local[2]",
            type="string",
            title="Spark master",
            description="The Airflow development image executes Spark locally",
        ),
    },
    tags=["assignment", "spark", "parameterized"],
) as dag:

    @task(task_id="prepare_parameters")
    def prepare_parameters() -> dict:
        context = get_current_context()
        config = prepare_run_config(
            project_root=PROJECT_ROOT,
            params=context["params"],
            airflow_run_id=context["run_id"],
        )
        print("Resolved parameters:", config)
        return config

    @task(task_id="run_existing_spark_job")
    def execute_spark(config: dict) -> str:
        return run_spark_batch(config)

    @task(task_id="verify_row_accounting")
    def check_result(report_path: str) -> dict:
        return verify_report(project_root=PROJECT_ROOT, report_path=report_path)

    check_result(execute_spark(prepare_parameters()))

