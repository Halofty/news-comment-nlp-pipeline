from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import DAG, Param, get_current_context, task

from orchestration.reddit_daily import (
    collect_daily_comments,
    prepare_collected_spark_config,
    prepare_daily_config,
)
from orchestration.spark_batch import run_spark_batch, verify_report

PROJECT_ROOT = Path("/opt/airflow/project")

with DAG(
    dag_id="reddit_daily_spark_batch",
    description="Collect one day of Reddit comments and run the existing Spark batch",
    schedule=None,
    start_date=datetime(2026, 8, 27, tzinfo=timezone.utc),
    catchup=False,
    default_args={
        "owner": "news-comment-nlp-pipeline",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    params={
        "start_date": Param(
            "2016-01-01",
            type="string",
            pattern="^\\d{4}-\\d{2}-\\d{2}$",
            title="Reddit comment date",
        ),
        "end_date": Param(
            "2016-01-01",
            type="string",
            pattern="^\\d{4}-\\d{2}-\\d{2}$",
            title="Date confirmation",
            description="Must be identical to start_date",
        ),
        "limit": Param(
            1000,
            type="integer",
            minimum=0,
            maximum=10000,
            title="Maximum valid comments",
            description="Use 0 to collect every comment for the selected day",
        ),
        "output_root": Param(
            "data/airflow-output", type="string", pattern="^data/.+"
        ),
        "output_format": Param(
            "parquet", type="string", enum=["parquet", "jsonl"]
        ),
        "partitions": Param(2, type="integer", minimum=1, maximum=64),
        "spark_master": Param("local[2]", type="string"),
    },
    tags=["assignment", "reddit", "spark", "parameterized"],
) as dag:

    @task(task_id="prepare_parameters")
    def prepare_parameters() -> dict:
        context = get_current_context()
        config = prepare_daily_config(
            project_root=PROJECT_ROOT,
            params=context["params"],
            airflow_run_id=context["run_id"],
        )
        print("Resolved parameters:", config)
        return config

    @task(task_id="collect_reddit_day")
    def collect_reddit_day(config: dict) -> dict:
        return collect_daily_comments(config)

    @task(task_id="run_existing_spark_job")
    def execute_spark(config: dict, collected: dict) -> str:
        spark_config = prepare_collected_spark_config(config=config, collected=collected)
        return run_spark_batch(spark_config)

    @task(task_id="verify_row_accounting")
    def check_result(report_path: str) -> dict:
        return verify_report(project_root=PROJECT_ROOT, report_path=report_path)

    prepared = prepare_parameters()
    collected = collect_reddit_day(prepared)
    check_result(execute_spark(prepared, collected))
