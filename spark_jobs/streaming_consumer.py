from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession, functions as F

from spark_jobs.run_logging import JsonlRunLogger
from spark_jobs.runtime import configure_java_home
from spark_jobs.schemas import RAW_EVENT_SCHEMA
from spark_jobs.transformations import transform_events
from storage.postgres import write_micro_batch_to_postgres


DEFAULT_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7"
OUTPUT_ROUTES = (
    "processed",
    "quarantine",
    "quality_rejected",
    "contract_rejected",
)
INTERNAL_COLUMNS = (
    "_raw_json",
    "_json_keys",
    "_corrupt_record",
    "_dedup_key",
    "_dedup_timestamp",
    "output_route",
)


@dataclass(frozen=True)
class StreamingConfig:
    bootstrap_servers: str
    input_topic: str
    dlq_topic: str
    output_path: str | Path
    checkpoint_path: str | Path
    starting_offsets: str = "earliest"
    watermark_delay: str = "10 minutes"
    max_offsets_per_trigger: int | None = 10_000
    publish_dlq: bool = True
    output_format: str = "parquet"
    postgres_dsn: str | None = None
    consumer_name: str = "text-event-kafka-consumer"
    postgres_chunk_size: int = 500

    def validate(self) -> None:
        if not self.bootstrap_servers.strip():
            raise ValueError("bootstrap_servers must not be empty")
        if not self.input_topic.strip() or not self.dlq_topic.strip():
            raise ValueError("Kafka topic names must not be empty")
        if self.input_topic == self.dlq_topic:
            raise ValueError("input_topic and dlq_topic must differ")
        if self.starting_offsets not in {"earliest", "latest"}:
            try:
                json.loads(self.starting_offsets)
            except json.JSONDecodeError as error:
                raise ValueError(
                    "starting_offsets must be earliest, latest, or Kafka offset JSON"
                ) from error
        if not self.watermark_delay.strip():
            raise ValueError("watermark_delay must not be empty")
        if self.max_offsets_per_trigger is not None and self.max_offsets_per_trigger < 1:
            raise ValueError("max_offsets_per_trigger must be positive")
        if _storage_location(self.output_path) == _storage_location(
            self.checkpoint_path
        ):
            raise ValueError("output_path and checkpoint_path must differ")
        if self.output_format not in {"parquet", "jsonl"}:
            raise ValueError("output_format must be parquet or jsonl")
        if self.output_format == "jsonl" and _is_s3a(self.output_path):
            raise ValueError("s3a output requires parquet format")
        if not self.consumer_name.strip():
            raise ValueError("consumer_name must not be empty")
        if self.postgres_chunk_size < 1:
            raise ValueError("postgres_chunk_size must be positive")


def _is_s3a(value: str | Path) -> bool:
    return str(value).startswith("s3a://")


def _storage_location(value: str | Path) -> str:
    raw = str(value).strip()
    if raw.startswith("s3a://"):
        suffix = raw[len("s3a://") :].strip("/")
        if not suffix or "/../" in f"/{suffix}/" or "/./" in f"/{suffix}/":
            raise ValueError("s3a location requires a bucket and safe object prefix")
        return f"s3a://{suffix}"
    if "://" in raw:
        raise ValueError("storage location must be a local path or s3a:// URI")
    return str(Path(raw).resolve())


def _join_storage_location(value: str | Path, *parts: str) -> str:
    root = _storage_location(value)
    if root.startswith("s3a://"):
        return "/".join((root.rstrip("/"), *(part.strip("/") for part in parts)))
    return str(Path(root).joinpath(*parts))


def create_streaming_spark_session(
    *,
    master: str,
    app_name: str,
    kafka_package: str | None,
    kafka_classpath: str | None = None,
    storage_settings: Mapping[str, str] | None = None,
) -> SparkSession:
    configure_java_home()
    python_executable = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable
    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
    )
    if kafka_package:
        builder = builder.config("spark.jars.packages", kafka_package)
    if kafka_classpath:
        builder = builder.config(
            "spark.driver.extraClassPath", kafka_classpath
        ).config("spark.executor.extraClassPath", kafka_classpath)
    for name, value in (storage_settings or {}).items():
        builder = builder.config(name, value)
    return builder.getOrCreate()


def read_kafka_stream(spark: SparkSession, config: StreamingConfig) -> DataFrame:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", config.input_topic)
        .option("startingOffsets", config.starting_offsets)
        .option("failOnDataLoss", "true")
    )
    if config.max_offsets_per_trigger is not None:
        reader = reader.option(
            "maxOffsetsPerTrigger", str(config.max_offsets_per_trigger)
        )
    return reader.load()


def parse_kafka_messages(messages: DataFrame) -> DataFrame:
    raw_json = F.col("value").cast("string")
    parsed = F.from_json(
        raw_json,
        RAW_EVENT_SCHEMA,
        {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
    )
    return messages.select(
        raw_json.alias("_raw_json"),
        F.json_object_keys(raw_json).alias("_json_keys"),
        parsed.alias("_parsed"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).select(
        "_raw_json",
        "_json_keys",
        "_parsed.*",
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
    )


def add_output_route(events: DataFrame) -> DataFrame:
    return events.withColumn(
        "output_route",
        F.when(~F.col("contract_valid"), F.lit("contract_rejected"))
        .when(F.col("quality_status") == "reject", F.lit("quality_rejected"))
        .when(F.col("quality_status") == "quarantine", F.lit("quarantine"))
        .otherwise(F.lit("processed")),
    )


def transform_kafka_messages(messages: DataFrame) -> DataFrame:
    return add_output_route(transform_events(parse_kafka_messages(messages)))


def prepare_stream(events: DataFrame, *, watermark_delay: str) -> DataFrame:
    with_deduplication = events.withColumn(
        "_dedup_timestamp",
        F.coalesce("event_timestamp", "kafka_timestamp"),
    ).withColumn(
        "_dedup_key",
        F.when(F.col("contract_valid"), F.col("event_id")).otherwise(
            F.concat_ws(
                ":",
                F.col("kafka_topic"),
                F.col("kafka_partition"),
                F.col("kafka_offset"),
            )
        ),
    )
    watermarked = with_deduplication.withWatermark(
        "_dedup_timestamp", watermark_delay
    )
    if hasattr(watermarked, "dropDuplicatesWithinWatermark"):
        return watermarked.dropDuplicatesWithinWatermark(["_dedup_key"])
    return watermarked.dropDuplicates(["_dedup_key"])


def _write_route(
    events: DataFrame, path: str | Path, *, output_format: str
) -> None:
    public_events = events.drop(*INTERNAL_COLUMNS)
    if output_format == "parquet":
        (
            public_events.write.mode("overwrite")
            .partitionBy("source_type")
            .parquet(_storage_location(path))
        )
        return

    local_path = Path(_storage_location(path))
    local_path.mkdir(parents=True, exist_ok=True)
    output_file = local_path / "events.jsonl"
    with output_file.open("w", encoding="utf-8") as file:
        for line in public_events.toJSON().toLocalIterator():
            file.write(line + "\n")


def _write_contract_dlq_to_kafka(
    events: DataFrame, *, bootstrap_servers: str, dlq_topic: str
) -> None:
    dlq_records = events.select(
        F.concat_ws(
            ":",
            F.col("kafka_topic"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
        )
        .cast("binary")
        .alias("key"),
        F.to_json(
            F.struct(
                F.lit("CONTRACT_REJECTED").alias("reason"),
                "contract_errors",
                F.col("_raw_json").alias("raw_event"),
                F.col("kafka_topic").alias("source_topic"),
                F.col("kafka_partition").alias("source_partition"),
                F.col("kafka_offset").alias("source_offset"),
                F.current_timestamp().alias("rejected_at"),
            )
        )
        .cast("binary")
        .alias("value"),
    )
    (
        dlq_records.write.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", dlq_topic)
        .save()
    )


def process_micro_batch(
    events: DataFrame,
    batch_id: int,
    *,
    config: StreamingConfig,
    run_logger: JsonlRunLogger | None = None,
) -> None:
    started = time.perf_counter()
    cached = events.cache()
    try:
        counts = {
            row["output_route"]: int(row["count"])
            for row in cached.groupBy("output_route").count().collect()
        }
        total_rows = sum(counts.values())
        for route in OUTPUT_ROUTES:
            count = counts.get(route, 0)
            if count == 0:
                continue
            route_events = cached.filter(F.col("output_route") == route)
            batch_path = _join_storage_location(
                config.output_path,
                route,
                f"batch_id={batch_id:020d}",
            )
            _write_route(
                route_events,
                batch_path,
                output_format=config.output_format,
            )
            if route == "contract_rejected" and config.publish_dlq:
                _write_contract_dlq_to_kafka(
                    route_events,
                    bootstrap_servers=config.bootstrap_servers,
                    dlq_topic=config.dlq_topic,
                )

        postgres_result = None
        if config.postgres_dsn:
            postgres_result = write_micro_batch_to_postgres(
                cached.toLocalIterator(),
                dsn=config.postgres_dsn,
                consumer_name=config.consumer_name,
                batch_id=batch_id,
                route_counts={route: counts.get(route, 0) for route in OUTPUT_ROUTES},
                chunk_size=config.postgres_chunk_size,
            )

        if run_logger:
            run_logger.emit(
                "micro_batch_completed",
                batch_id=batch_id,
                input_rows=total_rows,
                route_counts={route: counts.get(route, 0) for route in OUTPUT_ROUTES},
                postgres_status=(
                    postgres_result.status if postgres_result else "disabled"
                ),
                stage_duration_seconds=round(time.perf_counter() - started, 3),
            )
    finally:
        cached.unpersist()


def start_consumer(
    spark: SparkSession,
    *,
    config: StreamingConfig,
    run_logger: JsonlRunLogger | None = None,
    available_now: bool = False,
    trigger_interval: str = "10 seconds",
):
    config.validate()
    messages = read_kafka_stream(spark, config)
    routed = prepare_stream(
        transform_kafka_messages(messages),
        watermark_delay=config.watermark_delay,
    )
    writer = (
        routed.writeStream.queryName("text-event-kafka-consumer")
        .option("checkpointLocation", _storage_location(config.checkpoint_path))
        .foreachBatch(
            lambda events, batch_id: process_micro_batch(
                events,
                batch_id,
                config=config,
                run_logger=run_logger,
            )
        )
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=trigger_interval)
    return writer.start()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Consume TextEvent v1 from Kafka with Spark Structured Streaming"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument("--input-topic", default="raw-text")
    parser.add_argument("--dlq-topic", default="raw-text-dlq")
    parser.add_argument("--output", required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--log", type=Path)
    parser.add_argument("--starting-offsets", default="earliest")
    parser.add_argument("--watermark-delay", default="10 minutes")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=10_000)
    parser.add_argument("--trigger-interval", default="10 seconds")
    parser.add_argument("--format", choices=("parquet", "jsonl"), default="parquet")
    parser.add_argument("--available-now", action="store_true")
    parser.add_argument("--no-publish-dlq", action="store_true")
    parser.add_argument(
        "--postgres-dsn",
        default=os.getenv("POSTGRES_DSN"),
        help="Optional PostgreSQL DSN; enables transactional idempotent storage",
    )
    parser.add_argument(
        "--consumer-name",
        default="text-event-kafka-consumer",
        help="Stable name used with batch_id as the PostgreSQL commit key",
    )
    parser.add_argument("--postgres-chunk-size", type=int, default=500)
    parser.add_argument(
        "--master",
        default=os.getenv("SPARK_MASTER_URL", "local[2]"),
        help="Spark master URL; Compose uses spark://spark-master:7077",
    )
    parser.add_argument("--kafka-package", default=DEFAULT_KAFKA_PACKAGE)
    parser.add_argument(
        "--no-resolve-kafka-package",
        action="store_true",
        help="Do not use Maven package resolution; requires --kafka-classpath",
    )
    parser.add_argument(
        "--kafka-classpath",
        default=os.getenv("SPARK_KAFKA_CLASSPATH"),
        help="Pre-downloaded connector JAR classpath; useful for local Windows runs",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    log_path = args.log or (
        Path("data/logs/stream-consumer.jsonl")
        if _is_s3a(args.checkpoint)
        else Path(args.checkpoint) / "consumer-run.jsonl"
    )
    run_logger = JsonlRunLogger(log_path)
    config = StreamingConfig(
        bootstrap_servers=args.bootstrap_servers,
        input_topic=args.input_topic,
        dlq_topic=args.dlq_topic,
        output_path=args.output,
        checkpoint_path=args.checkpoint,
        starting_offsets=args.starting_offsets,
        watermark_delay=args.watermark_delay,
        max_offsets_per_trigger=args.max_offsets_per_trigger,
        publish_dlq=not args.no_publish_dlq,
        output_format=args.format,
        postgres_dsn=args.postgres_dsn,
        consumer_name=args.consumer_name,
        postgres_chunk_size=args.postgres_chunk_size,
    )
    run_logger.emit(
        "consumer_started",
        input_topic=config.input_topic,
        dlq_topic=config.dlq_topic,
        starting_offsets=config.starting_offsets,
        watermark_delay=config.watermark_delay,
        available_now=args.available_now,
        postgres_enabled=bool(config.postgres_dsn),
    )
    spark = None
    query = None
    try:
        storage_settings = None
        if _is_s3a(config.output_path) or _is_s3a(config.checkpoint_path):
            from spark_jobs.minio_roundtrip import build_s3a_config

            storage_settings = build_s3a_config(
                endpoint=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
                access_key=os.getenv("MINIO_ROOT_USER", "news_pipeline"),
                secret_key=os.getenv(
                    "MINIO_ROOT_PASSWORD", "news_pipeline_minio_dev"
                ),
            )
        spark = create_streaming_spark_session(
            master=args.master,
            app_name="text-event-kafka-consumer",
            kafka_package=None if args.no_resolve_kafka_package else args.kafka_package,
            kafka_classpath=args.kafka_classpath,
            storage_settings=storage_settings,
        )
        spark.sparkContext.setLogLevel("WARN")
        run_logger.emit(
            "spark_session_started",
            spark_version=spark.version,
            python_version=platform.python_version(),
            java_version=spark.sparkContext._jvm.java.lang.System.getProperty(
                "java.version"
            ),
            master=spark.sparkContext.master,
        )
        query = start_consumer(
            spark,
            config=config,
            run_logger=run_logger,
            available_now=args.available_now,
            trigger_interval=args.trigger_interval,
        )
        run_logger.emit("stream_started", query_id=str(query.id))
        query.awaitTermination()
        run_logger.emit(
            "consumer_completed",
            query_id=str(query.id),
            status="available_now_complete" if args.available_now else "terminated",
        )
    except Exception as error:
        run_logger.emit("consumer_failed", error_type=type(error).__name__)
        raise
    finally:
        if query is not None and query.isActive:
            query.stop()
        if spark is not None:
            spark.stop()
        from storage.data_lake import publish_artifact_if_enabled

        publish_artifact_if_enabled(log_path)


if __name__ == "__main__":
    main()
