from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, functions as F

from spark_jobs.schemas import RAW_EVENT_SCHEMA
from spark_jobs.run_logging import JsonlRunLogger
from spark_jobs.runtime import configure_java_home
from spark_jobs.transformations import deduplicate_valid_events, transform_events


def create_spark_session(*, master: str, app_name: str) -> SparkSession:
    configure_java_home()
    python_executable = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable
    return (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_events(spark: SparkSession, input_path: Path) -> DataFrame:
    raw_lines = spark.read.text(str(input_path.resolve())).withColumnRenamed(
        "value", "_raw_json"
    )
    parsed = F.from_json(
        "_raw_json",
        RAW_EVENT_SCHEMA,
        {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
    )
    return raw_lines.withColumn("_parsed", parsed).select(
        "_raw_json",
        F.json_object_keys("_raw_json").alias("_json_keys"),
        "_parsed.*",
    )


def choose_output_partitions(input_rows: int) -> int:
    return max(2, min(64, math.ceil(input_rows / 250)))


def _count_map(rows: list[Any], key_name: str) -> dict[str, int]:
    return {str(row[key_name]): int(row["count"]) for row in rows}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_jsonl(dataframe: DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        for line in dataframe.toJSON().toLocalIterator():
            file.write(line + "\n")


def process_batch(
    spark: SparkSession,
    *,
    input_path: Path,
    output_path: Path,
    output_partitions: int | None = None,
    output_format: str = "parquet",
    run_logger: JsonlRunLogger | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    stage_started = time.perf_counter()
    raw = read_events(spark, input_path)
    input_partitions = raw.rdd.getNumPartitions()
    input_rows = raw.count()
    input_sha256 = _sha256(input_path)
    if run_logger:
        run_logger.emit(
            "input_loaded",
            stage_duration_seconds=round(time.perf_counter() - stage_started, 3),
            input_rows=input_rows,
            input_partitions=input_partitions,
            input_bytes=input_path.stat().st_size,
            input_sha256=input_sha256,
        )

    stage_started = time.perf_counter()
    transformed = transform_events(raw).cache()
    contract_valid_rows = transformed.filter("contract_valid").count()
    contract_rejected_rows = input_rows - contract_valid_rows
    malformed_json_rows = transformed.filter(
        F.array_contains("contract_errors", "MALFORMED_JSON")
    ).count()
    if run_logger:
        run_logger.emit(
            "contract_validated",
            stage_duration_seconds=round(time.perf_counter() - stage_started, 3),
            contract_valid_rows=contract_valid_rows,
            contract_rejected_rows=contract_rejected_rows,
            malformed_json_rows=malformed_json_rows,
        )

    stage_started = time.perf_counter()
    valid_unique, rejected = deduplicate_valid_events(transformed)
    valid_unique = valid_unique.cache()
    rejected = rejected.cache()
    unique_valid_rows = valid_unique.count()
    rejected_rows = rejected.count()
    duplicate_rows = contract_valid_rows - unique_valid_rows
    if run_logger:
        run_logger.emit(
            "deduplication_completed",
            stage_duration_seconds=round(time.perf_counter() - stage_started, 3),
            duplicate_event_id_rows=duplicate_rows,
            unique_valid_rows=unique_valid_rows,
            rejected_rows=rejected_rows,
        )

    selected_partitions = output_partitions or choose_output_partitions(input_rows)
    if selected_partitions < 1:
        raise ValueError("output_partitions must be positive")

    partitioned_events = valid_unique.repartition(selected_partitions, "source_type")
    stage_started = time.perf_counter()
    if output_format == "parquet":
        events_output = output_path / "events"
        rejected_output = output_path / "rejected"
        (
            partitioned_events.write.mode("overwrite")
            .partitionBy("source_type")
            .parquet(str(events_output.resolve()))
        )
        rejected.coalesce(1).write.mode("overwrite").parquet(
            str(rejected_output.resolve())
        )
    elif output_format == "jsonl":
        events_output = output_path / "events" / "events.jsonl"
        rejected_output = output_path / "rejected" / "rejected.jsonl"
        _write_jsonl(partitioned_events, events_output)
        _write_jsonl(rejected.coalesce(1), rejected_output)
    else:
        raise ValueError("output_format must be parquet or jsonl")
    if run_logger:
        run_logger.emit(
            "output_written",
            stage_duration_seconds=round(time.perf_counter() - stage_started, 3),
            output_format=output_format,
            output_partitions=selected_partitions,
            unique_valid_rows=unique_valid_rows,
            rejected_rows=rejected_rows,
        )

    stage_started = time.perf_counter()
    quality_status_counts = _count_map(
        valid_unique.groupBy("quality_status").count().collect(),
        "quality_status",
    )
    quality_flag_counts = _count_map(
        valid_unique.select(F.explode("quality_flags").alias("quality_flag"))
        .groupBy("quality_flag")
        .count()
        .collect(),
        "quality_flag",
    )
    source_counts = _count_map(
        valid_unique.groupBy("source_type").count().collect(), "source_type"
    )
    contract_error_counts = _count_map(
        rejected.select(F.explode("contract_errors").alias("contract_error"))
        .groupBy("contract_error")
        .count()
        .collect(),
        "contract_error",
    )
    length_summary = valid_unique.agg(
        F.min("character_count").alias("minimum_characters"),
        F.max("character_count").alias("maximum_characters"),
        F.avg("character_count").alias("mean_characters"),
        F.max("utf8_byte_count").alias("maximum_utf8_bytes"),
    ).first()

    accounted_rows = unique_valid_rows + duplicate_rows + rejected_rows
    if accounted_rows != input_rows:
        raise RuntimeError(
            f"row accounting mismatch: input={input_rows}, accounted={accounted_rows}"
        )
    if run_logger:
        run_logger.emit(
            "metrics_collected",
            stage_duration_seconds=round(time.perf_counter() - stage_started, 3),
            input_rows=input_rows,
            accounted_rows=accounted_rows,
            quality_status_counts=quality_status_counts,
            quality_flag_counts=quality_flag_counts,
            contract_error_counts=contract_error_counts,
        )

    duration_seconds = round(time.perf_counter() - started, 3)
    report = {
        "report_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "input": {
            "path": str(input_path.as_posix()),
            "sha256": input_sha256,
            "rows": input_rows,
            "partitions": input_partitions,
        },
        "output": {
            "path": str(output_path.as_posix()),
            "format": output_format,
            "partition_column": "source_type" if output_format == "parquet" else None,
            "partitions_requested": selected_partitions,
            "unique_valid_rows": unique_valid_rows,
            "rejected_rows": rejected_rows,
        },
        "row_accounting": {
            "input_rows": input_rows,
            "schema_parsing_success_rows": input_rows - malformed_json_rows,
            "malformed_json_rows": malformed_json_rows,
            "contract_valid_rows_before_deduplication": contract_valid_rows,
            "contract_rejected_rows": contract_rejected_rows,
            "duplicate_event_id_rows": duplicate_rows,
            "unique_valid_rows": unique_valid_rows,
            "accounted_rows": accounted_rows,
        },
        "source_counts": source_counts,
        "quality_status_counts": quality_status_counts,
        "quality_flag_counts": quality_flag_counts,
        "contract_error_counts": contract_error_counts,
        "text_metrics": {
            "minimum_characters": length_summary["minimum_characters"],
            "maximum_characters": length_summary["maximum_characters"],
            "mean_characters": round(length_summary["mean_characters"], 3),
            "maximum_utf8_bytes": length_summary["maximum_utf8_bytes"],
        },
        "runtime": {
            "duration_seconds": duration_seconds,
            "python_version": platform.python_version(),
            "spark_version": spark.version,
            "java_version": spark.sparkContext._jvm.java.lang.System.getProperty(
                "java.version"
            ),
            "master": spark.sparkContext.master,
        },
        "limitations": [
            "A bounded input sample is not representative of the full source distribution.",
            "The reference quality policy currently uses a scalar Python UDF for exact fixture parity.",
            "Performance conclusions require a larger representative dataset and UDF benchmark.",
            "The local JSONL sink streams partitions through the driver and is not a distributed production sink.",
        ],
    }

    valid_unique.unpersist()
    rejected.unpersist()
    transformed.unpersist()
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Process TextEvent JSONL with an explicit Spark schema"
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument(
        "--log",
        type=Path,
        help="Operational JSONL log (defaults to <report stem>.log.jsonl)",
    )
    parser.add_argument(
        "--master",
        default=os.getenv("SPARK_MASTER_URL", "local[2]"),
        help="Spark master URL; Compose uses spark://spark-master:7077",
    )
    parser.add_argument("--partitions", type=int)
    parser.add_argument("--format", choices=("parquet", "jsonl"), default="parquet")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    log_path = args.log or args.report.with_suffix(".log.jsonl")
    run_logger = JsonlRunLogger(log_path)
    run_logger.emit(
        "run_started",
        app_name="text-event-sample",
        master=args.master,
        output_format=args.format,
    )
    spark = None
    try:
        spark = create_spark_session(master=args.master, app_name="text-event-sample")
        spark.sparkContext.setLogLevel("WARN")
        run_logger.emit(
            "spark_session_started",
            spark_version=spark.version,
            master=spark.sparkContext.master,
        )
        report = process_batch(
            spark,
            input_path=args.input,
            output_path=args.output,
            output_partitions=args.partitions,
            output_format=args.format,
            run_logger=run_logger,
        )
        spark.stop()
        spark = None
        run_logger.emit("spark_session_stopped")

        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        run_logger.emit("report_written", report_version=report["report_version"])
        run_logger.emit(
            "run_completed",
            input_rows=report["row_accounting"]["input_rows"],
            accounted_rows=report["row_accounting"]["accounted_rows"],
            unique_valid_rows=report["row_accounting"]["unique_valid_rows"],
        )
    except Exception as error:
        run_logger.emit("run_failed", error_type=type(error).__name__)
        raise
    finally:
        if spark is not None:
            spark.stop()
    print(json.dumps(report["row_accounting"], ensure_ascii=False))
    print(
        f"wrote {args.format} output to {args.output}, report to {args.report}, "
        f"and operational log to {log_path}"
    )


if __name__ == "__main__":
    main()
