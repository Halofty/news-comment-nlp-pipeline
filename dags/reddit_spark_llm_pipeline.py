from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import DAG, Param, get_current_context, task

from orchestration.end_to_end import prepare_llm_from_spark
from orchestration.llm_batch import prepare_requests, submit_or_dry_run, verify_submission
from orchestration.object_storage import sync_spark_output
from orchestration.reddit_daily import (
    collect_daily_comments,
    prepare_collected_spark_config,
    prepare_daily_config,
)
from orchestration.spark_batch import run_spark_batch, verify_report

PROJECT_ROOT = Path("/opt/airflow/project")

with DAG(
    dag_id="reddit_spark_llm_pipeline",
    description="Collect Reddit, process with Spark, then prepare or submit an LLM Batch",
    schedule=None,
    start_date=datetime(2026, 9, 3, tzinfo=timezone.utc),
    catchup=False,
    default_args={
        "owner": "news-comment-nlp-pipeline",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    params={
        "start_date": Param("2016-01-01", type="string", pattern="^\\d{4}-\\d{2}-\\d{2}$"),
        "end_date": Param("2016-01-01", type="string", pattern="^\\d{4}-\\d{2}-\\d{2}$"),
        "limit": Param(1000, type="integer", minimum=100, maximum=10_000),
        "output_root": Param("data/airflow-output", type="string", pattern="^data/.+"),
        "output_format": Param("parquet", type="string", enum=["parquet", "jsonl"]),
        "partitions": Param(2, type="integer", minimum=1, maximum=64),
        "spark_master": Param("local[2]", type="string"),
        "llm_output_root": Param("data/airflow-output/end-to-end-llm", type="string", pattern="^data/.+"),
        "llm_limit": Param(100, type="integer", minimum=1, maximum=50_000),
        "model": Param("gpt-5.6-luna", const="gpt-5.6-luna"),
        "daily_budget_usd": Param("1.00", type="string"),
        "submit": Param(False, type="boolean"),
        "minio_enabled": Param(True, type="boolean"),
    },
    tags=["reddit", "spark", "openai-batch", "end-to-end", "manual"],
) as dag:

    @task(task_id="prepare_parameters")
    def prepare_parameters() -> dict:
        context = get_current_context()
        return prepare_daily_config(
            project_root=PROJECT_ROOT,
            params=context["params"],
            airflow_run_id=context["run_id"],
        )

    @task(task_id="collect_reddit_day")
    def collect_reddit_day(config: dict) -> dict:
        return collect_daily_comments(config)

    @task(task_id="run_spark")
    def run_spark(config: dict, collected: dict) -> dict:
        spark_config = prepare_collected_spark_config(config=config, collected=collected)
        report_path = run_spark_batch(spark_config)
        return {"config": spark_config, "report_path": report_path}

    @task(task_id="verify_spark")
    def verify_spark(spark_run: dict) -> dict:
        return verify_report(
            project_root=PROJECT_ROOT, report_path=spark_run["report_path"]
        )

    @task(task_id="prepare_llm_parameters")
    def prepare_llm(
        config: dict, spark_run: dict, spark_result: dict, minio_result: dict
    ) -> dict:
        context = get_current_context()
        llm_config = prepare_llm_from_spark(
            project_root=PROJECT_ROOT,
            pipeline_config=config,
            spark_config=spark_run["config"],
            spark_verification=spark_result,
            params=context["params"],
        )
        llm_config["minio_result"] = minio_result
        return llm_config

    @task(task_id="store_spark_output_in_minio")
    def store_in_minio(spark_run: dict, spark_result: dict) -> dict:
        context = get_current_context()
        return sync_spark_output(
            project_root=PROJECT_ROOT,
            spark_config=spark_run["config"],
            spark_verification=spark_result,
            enabled=bool(context["params"]["minio_enabled"]),
        )

    @task(task_id="build_and_budget_check")
    def build_requests(config: dict) -> dict:
        return prepare_requests(config)

    @task(task_id="submit_or_dry_run")
    def submit(config: dict, preflight: dict) -> dict:
        return submit_or_dry_run(config, preflight)

    @task(task_id="verify_pipeline")
    def verify_pipeline(
        spark_result: dict, minio_result: dict, preflight: dict, submission: dict
    ) -> dict:
        llm_result = verify_submission(preflight, submission)
        return {
            "spark": spark_result,
            "object_storage": minio_result,
            "llm": llm_result,
            "status": "completed",
        }

    pipeline_config = prepare_parameters()
    collected = collect_reddit_day(pipeline_config)
    spark_run = run_spark(pipeline_config, collected)
    spark_result = verify_spark(spark_run)
    minio_result = store_in_minio(spark_run, spark_result)
    llm_config = prepare_llm(
        pipeline_config, spark_run, spark_result, minio_result
    )
    preflight = build_requests(llm_config)
    submission = submit(llm_config, preflight)
    verify_pipeline(spark_result, minio_result, preflight, submission)
