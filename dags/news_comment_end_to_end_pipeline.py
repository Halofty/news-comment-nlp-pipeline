from __future__ import annotations

import json
import os
import fcntl
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from airflow.sdk import DAG, Param, get_current_context, task

from jobs.openai_batch import _observability_sink
from jobs.watch_openai_batch import watch_batch
from llm_analysis import OpenAIBatchClient
from llm_analysis.group_daily import PROMPT_VERSION, build_group_daily_batch
from notifications import SlackWebhookNotifier
from observability import (
    BatchObservation,
    StageObservation,
    gpt_5_6_luna_batch_pricing,
    reconcile_usage,
)
from observability.openai_batch import load_sample_batch, total_cost
from orchestration.kafka_batch import (
    capture_topic_offsets,
    ensure_batch_topics,
    publish_jsonl_batch,
    write_kafka_ledger,
)
from orchestration.llm_batch import submit_or_dry_run
from orchestration.object_storage import sync_spark_output
from orchestration.reddit_daily import collect_daily_comments
from orchestration.spark_batch import (
    prepare_kafka_run_config,
    run_kafka_spark_batch,
    verify_kafka_report,
)
from orchestration.unified_daily import (
    collect_web_news,
    merge_daily_sources,
    prepare_date_range_configs,
)
from serving.snapshot import build_serving_snapshot
from storage.llm_postgres import apply_llm_analysis_migrations, write_llm_batch_to_postgres


PROJECT_ROOT = Path("/opt/airflow/project")
ANALYSIS_GROUPS_CONFIG = "config/analysis-groups.yaml"
REDDIT_ARCHIVE_ROOT = "data/raw/reddit-archive/data"
REDDIT_SOURCE_MODE = "auto"
WEB_NEWS_ROOT = "data/raw/google-news"
WEB_NEWS_SOURCE_MODE = "auto"
OUTPUT_ROOT = "data/airflow-output/unified"
SPARK_PARTITIONS = 2
SPARK_MASTER = "local[2]"
SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7"
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_RAW_TOPIC = os.environ.get("KAFKA_RAW_TOPIC", "raw-text")
KAFKA_DLQ_TOPIC = os.environ.get("KAFKA_DLQ_TOPIC", "raw-text-dlq")
MODEL = "gpt-5.6-luna"
MAX_OUTPUT_TOKENS = 900
DAILY_BUDGET_USD = Decimal("1.00")
POLL_INTERVAL_SECONDS = 60
TIMEOUT_SECONDS = 90_000
DAILY_PROCESSING_SLOTS = 2
OPENAI_BATCH_SLOTS = 10


def _acquire_batch_slot(*, airflow_run_id: str, limit: int):
    safe_run_id = "".join(
        character if character.isalnum() or character in "-_" else "_"
        for character in airflow_run_id
    )
    slot_root = PROJECT_ROOT / "data" / "airflow-home" / "batch-slots" / safe_run_id
    slot_root.mkdir(parents=True, exist_ok=True)
    while True:
        for slot_number in range(limit):
            handle = (slot_root / f"slot-{slot_number}.lock").open("a+")
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return handle
            except BlockingIOError:
                handle.close()
        time.sleep(5)


def _release_batch_slot(handle) -> None:
    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    handle.close()


with DAG(
    dag_id="news_comment_end_to_end_pipeline",
    description="Reddit + Google News -> Kafka -> Spark -> MinIO -> grouped LLM -> PostgreSQL/Slack",
    schedule=None,
    # Keep the DAG start date safely in the past. Airflow evaluates this in UTC;
    # using the project completion date here made early-KST manual runs precede
    # the DAG start date and finish immediately without creating task instances.
    start_date=datetime(2012, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=32,
    default_args={
        "owner": "news-comment-nlp-pipeline",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    params={
        "start_date": Param("2012-02-01", type="string", pattern="^\\d{4}-\\d{2}-\\d{2}$"),
        "end_date": Param("2012-02-01", type="string", pattern="^\\d{4}-\\d{2}-\\d{2}$"),
        "limit": Param(0, type="integer", minimum=0),
        "selected_groups": Param(
            ["economy"],
            type="array",
            items={"type": "string", "enum": ["politics", "economy", "technology", "environment"]},
            minItems=1,
            maxItems=4,
            uniqueItems=True,
        ),
        "batch_limit": Param(
            OPENAI_BATCH_SLOTS,
            type="integer",
            minimum=1,
            maximum=OPENAI_BATCH_SLOTS,
        ),
        "submit": Param(False, type="boolean"),
    },
    tags=["final", "reddit", "web-news", "kafka", "spark", "minio", "openai", "langfuse", "slack"],
) as dag:

    @task(task_id="prepare_parameters")
    def prepare_parameters() -> list[dict]:
        context = get_current_context()
        params = context["params"]
        return prepare_date_range_configs(
            project_root=PROJECT_ROOT,
            params={
                **params,
                "analysis_groups_config": ANALYSIS_GROUPS_CONFIG,
                "reddit_archive_root": REDDIT_ARCHIVE_ROOT,
                "reddit_source_mode": REDDIT_SOURCE_MODE,
                "output_root": OUTPUT_ROOT,
                "output_format": "parquet",
                "partitions": SPARK_PARTITIONS,
                "spark_master": SPARK_MASTER,
                "web_news_root": WEB_NEWS_ROOT,
                "web_news_source_mode": WEB_NEWS_SOURCE_MODE,
            },
            airflow_run_id=context["run_id"],
        )

    @task(task_id="prepare_llm_storage")
    def prepare_llm_storage() -> bool:
        """Apply shared PostgreSQL DDL once before concurrent mapped writes."""
        apply_llm_analysis_migrations(
            dsn=os.environ.get("POSTGRES_DSN", ""),
            migrations_root=PROJECT_ROOT / "sql" / "migrations",
        )
        return True

    @task(task_id="prepare_kafka_topics")
    def prepare_kafka_topics() -> dict:
        return ensure_batch_topics(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            partitions=3,
        )

    @task(
        task_id="collect_and_merge_sources",
        max_active_tis_per_dag=DAILY_PROCESSING_SLOTS,
    )
    def collect_and_merge_sources(config: dict) -> dict:
        reddit = collect_daily_comments(config)
        news = collect_web_news(config)
        merged = merge_daily_sources(config, reddit, news)
        return {
            "daily_config": config,
            "merged": merged,
        }

    @task(
        task_id="capture_kafka_start_offsets",
        max_active_tis_per_dag=DAILY_PROCESSING_SLOTS,
    )
    def capture_kafka_start(payload: dict) -> dict:
        offsets = capture_topic_offsets(KAFKA_BOOTSTRAP_SERVERS, KAFKA_RAW_TOPIC)
        return {**payload, "kafka_starting_offsets": offsets}

    @task(
        task_id="publish_to_kafka",
        max_active_tis_per_dag=DAILY_PROCESSING_SLOTS,
    )
    def publish_kafka(payload: dict) -> dict:
        context = get_current_context()
        config = payload["daily_config"]
        publication = publish_jsonl_batch(
            project_root=PROJECT_ROOT,
            input_file=payload["merged"]["input_file"],
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_RAW_TOPIC,
            pipeline_run_id=str(context["run_id"]),
            analysis_date=str(config["collection"]["start_date"]),
        )
        if int(publication["published_rows"]) != int(payload["merged"]["unique_rows"]):
            raise RuntimeError("Kafka publication count differs from merged unique rows")
        return {**payload, "kafka_publication": publication}

    @task(
        task_id="capture_kafka_end_offsets",
        max_active_tis_per_dag=DAILY_PROCESSING_SLOTS,
    )
    def capture_kafka_end(payload: dict) -> dict:
        ending_offsets = capture_topic_offsets(KAFKA_BOOTSTRAP_SERVERS, KAFKA_RAW_TOPIC)
        ledger = write_kafka_ledger(
            project_root=PROJECT_ROOT,
            run_directory=str(payload["daily_config"]["run_directory"]),
            publication=payload["kafka_publication"],
            starting_offsets=payload["kafka_starting_offsets"],
            ending_offsets=ending_offsets,
        )
        return {**payload, "kafka_ledger": ledger}

    @task(task_id="prepare_kafka_spark", max_active_tis_per_dag=DAILY_PROCESSING_SLOTS)
    def prepare_spark(payload: dict) -> dict:
        context = get_current_context()
        config = payload["daily_config"]
        spark_config = prepare_kafka_run_config(
            project_root=PROJECT_ROOT,
            params={
                "input_file": payload["merged"]["input_file"],
                "run_label": f"news-comment-{config['collection']['start_date']}",
                "output_root": OUTPUT_ROOT,
                "output_format": "parquet",
                "partitions": SPARK_PARTITIONS,
                "spark_master": SPARK_MASTER,
                "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
                "kafka_dlq_topic": KAFKA_DLQ_TOPIC,
                "spark_kafka_package": SPARK_KAFKA_PACKAGE,
            },
            airflow_run_id=context["run_id"],
            kafka_ledger=payload["kafka_ledger"],
        )
        return {**payload, "spark_config": spark_config}

    @task(task_id="run_kafka_spark_batch", max_active_tis_per_dag=DAILY_PROCESSING_SLOTS)
    def run_spark(payload: dict) -> dict:
        report_path = run_kafka_spark_batch(payload["spark_config"])
        return {**payload, "spark_report_path": report_path}

    @task(task_id="verify_kafka_spark_accounting", max_active_tis_per_dag=DAILY_PROCESSING_SLOTS)
    def verify_spark(payload: dict) -> dict:
        verification = verify_kafka_report(
            project_root=PROJECT_ROOT,
            report_path=payload["spark_report_path"],
        )
        return {**payload, "verification": verification}

    @task(
        task_id="store_processed_in_minio",
        max_active_tis_per_dag=DAILY_PROCESSING_SLOTS,
    )
    def store_minio(payload: dict) -> dict:
        minio = sync_spark_output(
            project_root=PROJECT_ROOT,
            spark_config=payload["spark_config"],
            spark_verification=payload["verification"],
            enabled=True,
        )
        return {**payload, "minio": minio}

    @task(
        task_id="build_group_daily_batch",
        max_active_tis_per_dag=DAILY_PROCESSING_SLOTS,
    )
    def build_llm(payload: dict) -> dict:
        params = get_current_context()["params"]
        config = payload["daily_config"]
        output_root = PROJECT_ROOT / str(config["run_directory"]) / "llm"
        report = build_group_daily_batch(
            input_path=PROJECT_ROOT / str(payload["spark_config"]["output_path"]) / "events",
            config_path=PROJECT_ROOT / ANALYSIS_GROUPS_CONFIG,
            selected_groups=list(params["selected_groups"]),
            analysis_date=date.fromisoformat(str(config["collection"]["start_date"])),
            request_path=output_root / "requests.jsonl",
            manifest_path=output_root / "manifest.jsonl",
            report_path=output_root / "preflight.json",
            model=MODEL,
            max_output_tokens=MAX_OUTPUT_TOKENS,
            budget_usd=DAILY_BUDGET_USD,
        )
        report["output_root"] = str(output_root)
        return {**payload, "preflight": report}

    @task(
        task_id="submit_wait_validate_store_notify",
        max_active_tis_per_dag=OPENAI_BATCH_SLOTS,
    )
    def submit_batch(payload: dict) -> dict:
        context = get_current_context()
        params = context["params"]
        preflight = payload["preflight"]
        analysis_date = payload["daily_config"]["collection"]["start_date"]
        state_path = Path(preflight["output_root"]) / "batch-state.json"
        slot_handle = None
        if bool(params["submit"]):
            slot_handle = _acquire_batch_slot(
                airflow_run_id=str(context["run_id"]),
                limit=int(params["batch_limit"]),
            )
        try:
            existing_state = (
                json.loads(state_path.read_text(encoding="utf-8"))
                if bool(params["submit"]) and state_path.is_file()
                else None
            )
            if existing_state and existing_state.get("id"):
                result = {
                    "status": str(existing_state.get("status") or "submitted"),
                    "batch_id": str(existing_state["id"]),
                    "request_rows": int(preflight["request_rows"]),
                    "state_path": str(state_path),
                    "reused_existing_batch": True,
                }
            else:
                result = submit_or_dry_run(
                    {
                        "request_path": preflight["request_path"],
                        "state_path": str(state_path),
                        "internal_batch_id": f"{context['run_id']}:{analysis_date}",
                        "model": MODEL,
                        "prompt_version": PROMPT_VERSION,
                        "submit": bool(params["submit"]),
                    },
                    preflight,
                )
            if result["status"] != "dry_run":
                state = json.loads(state_path.read_text(encoding="utf-8"))
                sink = _observability_sink(state_path)
                sink.record_batch(
                    BatchObservation(
                        batch_id=f"{context['run_id']}:{analysis_date}",
                        openai_batch_id=str(state["id"]),
                        model=str(state.get("model") or MODEL),
                        document_count=int((state.get("request_counts") or {}).get("total", preflight["request_rows"])),
                        status=str(state["status"]),
                        submitted_at=datetime.fromtimestamp(float(state["created_at"]), tz=timezone.utc),
                    )
                )
                sink.flush()
            return _finalize_batch(payload, result, str(context["run_id"]))
        finally:
            if slot_handle is not None:
                _release_batch_slot(slot_handle)

    def _finalize_batch(payload: dict, submission: dict, airflow_run_id: str) -> dict:
        preflight = payload["preflight"]
        if submission["status"] == "dry_run":
            final = {"status": "dry_run", "validated_rows": 0, "postgres_rows": 0}
            return {**payload, "final": final}
        output_root = Path(preflight["output_root"])
        analysis_date = payload["daily_config"]["collection"]["start_date"]
        notifier = SlackWebhookNotifier(os.environ.get("SLACK_WEBHOOK_URL", ""))
        notification = watch_batch(
            client=OpenAIBatchClient(),
            notifier=notifier,
            batch_id=str(submission["batch_id"]),
            manifest_path=Path(preflight["manifest_path"]),
            result_path=output_root / "raw-results.jsonl",
            validated_path=output_root / "validated-results.jsonl",
            batch_state_path=output_root / "batch-state.json",
            notification_state_path=output_root / "notification-state.json",
            poll_interval_seconds=POLL_INTERVAL_SECONDS,
            timeout_seconds=TIMEOUT_SECONDS,
        )
        state = json.loads((output_root / "batch-state.json").read_text(encoding="utf-8"))
        sink = _observability_sink(output_root / "batch-state.json")
        observation_marker = output_root / "usage-observed.json"
        total_cost_usd = None
        if not observation_marker.exists():
            input_tokens = int((state.get("usage") or {}).get("input_tokens", 0))
            sample = load_sample_batch(
                batch_path=output_root / "batch-state.json",
                manifest_path=Path(preflight["manifest_path"]),
                results_path=output_root / "raw-results.jsonl",
                pricing=gpt_5_6_luna_batch_pricing(input_tokens),
            )
            reconciliation = reconcile_usage(
                batch_id=sample.batch.batch_id,
                generations=sample.generations,
                batch_usage=sample.batch_usage,
                expected_completed_count=sample.expected_completed_count,
            )
            total_cost_usd = total_cost(sample.generations)
            sink.record_batch(sample.batch)
            sink.record_stage(
                StageObservation(
                    batch_id=sample.batch.batch_id,
                    stage="validate-and-store-group-daily-results",
                    status="completed" if int((notification.get("validation") or {}).get("failed_rows", 0)) == 0 else "failed",
                    started_at=datetime.now(timezone.utc),
                    completed_at=datetime.now(timezone.utc),
                    error_code=(None if int((notification.get("validation") or {}).get("failed_rows", 0)) == 0 else "SCHEMA_VALIDATION_FAILED"),
                )
            )
            for generation in sample.generations:
                sink.record_generation(generation)
            sink.record_reconciliation(reconciliation)
            sink.flush()
            observation_marker.write_text(
                json.dumps(
                    {
                        "recorded": True,
                        "reconciliation_status": reconciliation.status,
                        "total_cost_usd": str(total_cost_usd),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            from storage.data_lake import publish_artifact_if_enabled

            publish_artifact_if_enabled(observation_marker)
        else:
            marker = json.loads(observation_marker.read_text(encoding="utf-8"))
            total_cost_usd = marker.get("total_cost_usd")
        postgres_rows = 0
        dsn = os.environ.get("POSTGRES_DSN", "")
        manifest = [json.loads(line) for line in Path(preflight["manifest_path"]).read_text(encoding="utf-8").splitlines() if line.strip()]
        results = [json.loads(line) for line in (output_root / "validated-results.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
        written = write_llm_batch_to_postgres(
            dsn=dsn,
            batch_state=state,
            manifest_rows=manifest,
            result_rows=results,
            total_cost_usd=total_cost_usd,
        )
        postgres_rows = written.analysis_rows
        final = {
            "status": notification.get("notified_status", "completed"),
            "validated_rows": int((notification.get("validation") or {}).get("validated_rows", 0)),
            "postgres_rows": postgres_rows,
        }
        return {**payload, "final": final}

    @task(
        task_id="read_final_result",
        max_active_tis_per_dag=DAILY_PROCESSING_SLOTS,
    )
    def read_final_result(payload: dict) -> dict:
        spark_config = payload["spark_config"]
        preflight = payload["preflight"]
        final = payload["final"]
        snapshot_path = Path(str(spark_config["run_directory"])) / "serving-snapshot.json"
        llm_result = {
            "prepared_rows": int(preflight["request_rows"]),
            "skipped_rows": int(preflight["skipped_rows"]),
            "budget_status": str(preflight["budget_status"]),
            "submission_status": str(final["status"]),
        }
        return build_serving_snapshot(
            project_root=PROJECT_ROOT,
            spark_report_path=payload["verification"]["report_path"],
            minio_result=payload["minio"],
            llm_result=llm_result,
            output_path=snapshot_path,
        )

    daily_configs = prepare_parameters()
    kafka_topics_ready = prepare_kafka_topics()
    llm_storage_ready = prepare_llm_storage()
    collected = collect_and_merge_sources.expand(config=daily_configs)
    kafka_started = capture_kafka_start.expand(payload=collected)
    kafka_topics_ready >> kafka_started
    kafka_published = publish_kafka.expand(payload=kafka_started)
    kafka_bounded = capture_kafka_end.expand(payload=kafka_published)
    spark_configured = prepare_spark.expand(payload=kafka_bounded)
    spark_ran = run_spark.expand(payload=spark_configured)
    spark_verified = verify_spark.expand(payload=spark_ran)
    stored = store_minio.expand(payload=spark_verified)
    requests_built = build_llm.expand(payload=stored)
    finalized = submit_batch.expand(payload=requests_built)
    llm_storage_ready >> finalized
    read_final_result.expand(payload=finalized)
