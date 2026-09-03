from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

from spark_jobs.minio_roundtrip import build_s3a_config
from spark_jobs.runtime import configure_java_home
from spark_jobs.streaming_consumer import OUTPUT_ROUTES, _storage_location


def _create_session(*, master: str, settings: Mapping[str, str]) -> SparkSession:
    configure_java_home()
    os.environ["PYSPARK_PYTHON"] = sys.executable
    builder = SparkSession.builder.master(master).appName(
        "minio-streaming-checkpoint-recovery-verification"
    )
    for name, value in settings.items():
        builder = builder.config(name, value)
    return builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()


def _read_route(spark: SparkSession, uri: str) -> DataFrame | None:
    try:
        return spark.read.option("recursiveFileLookup", "true").parquet(uri)
    except AnalysisException as error:
        if "PATH_NOT_FOUND" in str(error):
            return None
        raise


def _list_hadoop_files(spark: SparkSession, uri: str) -> tuple[int, int]:
    jvm = spark.sparkContext._jvm
    configuration = spark.sparkContext._jsc.hadoopConfiguration()
    root = jvm.org.apache.hadoop.fs.Path(uri)
    filesystem = root.getFileSystem(configuration)
    pending = [root]
    object_count = 0
    total_bytes = 0
    while pending:
        current = pending.pop()
        for status in filesystem.listStatus(current):
            if status.isDirectory():
                pending.append(status.getPath())
            else:
                object_count += 1
                total_bytes += int(status.getLen())
    return object_count, total_bytes


def _load_run(path: Path) -> dict[str, Any]:
    rows = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    micro_batches = [row for row in rows if row.get("event") == "micro_batch_completed"]
    starts = [row for row in rows if row.get("event") == "stream_started"]
    completions = [row for row in rows if row.get("event") == "consumer_completed"]
    return {
        "path": str(path),
        "query_ids": [str(row["query_id"]) for row in starts],
        "micro_batch_count": len(micro_batches),
        "processed_rows": sum(int(row.get("input_rows", 0)) for row in micro_batches),
        "batch_ids": [int(row["batch_id"]) for row in micro_batches],
        "completed": len(completions) == 1,
    }


def verify(
    spark: SparkSession,
    *,
    output_uri: str,
    checkpoint_uri: str,
    run_logs: list[Path],
    expected_run_rows: list[int],
    expected_total_rows: int,
) -> dict[str, Any]:
    started = time.perf_counter()
    if len(run_logs) != len(expected_run_rows):
        raise ValueError("run_logs and expected_run_rows must have the same length")

    route_rows: dict[str, int] = {}
    frames: list[DataFrame] = []
    for route in OUTPUT_ROUTES:
        frame = _read_route(spark, f"{output_uri.rstrip('/')}/{route}")
        count = 0 if frame is None else frame.count()
        route_rows[route] = count
        if frame is not None:
            frames.append(frame.select("event_id"))
    stored_rows = sum(route_rows.values())
    if not frames:
        raise RuntimeError("streaming output contains no Parquet data")
    event_ids = frames[0]
    for frame in frames[1:]:
        event_ids = event_ids.unionByName(frame)
    unique_event_ids = event_ids.distinct().count()

    runs = [_load_run(path) for path in run_logs]
    actual_run_rows = [int(run["processed_rows"]) for run in runs]
    checkpoint_objects, checkpoint_bytes = _list_hadoop_files(
        spark, checkpoint_uri
    )
    result = {
        "status": "completed",
        "generated_at": datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "output_uri": output_uri,
        "checkpoint_uri": checkpoint_uri,
        "route_rows": route_rows,
        "stored_rows": stored_rows,
        "unique_event_ids": unique_event_ids,
        "duplicate_event_ids": stored_rows - unique_event_ids,
        "expected_run_rows": expected_run_rows,
        "actual_run_rows": actual_run_rows,
        "runs": runs,
        "checkpoint_objects": checkpoint_objects,
        "checkpoint_bytes": checkpoint_bytes,
        "duration_seconds": round(time.perf_counter() - started, 3),
        "spark_version": spark.version,
        "master": spark.sparkContext.master,
    }
    if actual_run_rows != expected_run_rows:
        raise RuntimeError(
            f"restart run row mismatch: expected={expected_run_rows}, actual={actual_run_rows}"
        )
    if stored_rows != expected_total_rows or unique_event_ids != expected_total_rows:
        raise RuntimeError(
            "output row mismatch: "
            f"expected={expected_total_rows}, stored={stored_rows}, unique={unique_event_ids}"
        )
    if checkpoint_objects < 1 or checkpoint_bytes < 1:
        raise RuntimeError("MinIO checkpoint contains no objects")
    if not all(bool(run["completed"]) for run in runs):
        raise RuntimeError("one or more streaming runs did not complete")
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify Spark restart behavior with MinIO S3A checkpoint"
    )
    parser.add_argument("--output-uri", required=True)
    parser.add_argument("--checkpoint-uri", required=True)
    parser.add_argument("--run-log", action="append", type=Path, required=True)
    parser.add_argument("--expected-run-rows", required=True)
    parser.add_argument("--expected-total-rows", type=int, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--master", default=os.getenv("SPARK_MASTER_URL", "local[2]"))
    parser.add_argument("--endpoint", default=os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
    parser.add_argument("--access-key", default=os.getenv("MINIO_ROOT_USER", "news_pipeline"))
    parser.add_argument(
        "--secret-key",
        default=os.getenv("MINIO_ROOT_PASSWORD", "news_pipeline_minio_dev"),
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    expected_run_rows = [
        int(value.strip())
        for value in args.expected_run_rows.split(",")
        if value.strip()
    ]
    output_uri = _storage_location(args.output_uri)
    checkpoint_uri = _storage_location(args.checkpoint_uri)
    settings = build_s3a_config(
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
    )
    spark = _create_session(master=args.master, settings=settings)
    try:
        spark.sparkContext.setLogLevel("WARN")
        result = verify(
            spark,
            output_uri=output_uri,
            checkpoint_uri=checkpoint_uri,
            run_logs=args.run_log,
            expected_run_rows=expected_run_rows,
            expected_total_rows=args.expected_total_rows,
        )
    finally:
        spark.stop()
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    from storage.data_lake import publish_artifact_if_enabled

    publish_artifact_if_enabled(args.report)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
