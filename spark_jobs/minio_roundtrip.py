from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from pyspark.sql import SparkSession

from spark_jobs.runtime import configure_java_home


def build_s3a_config(
    *, endpoint: str, access_key: str, secret_key: str
) -> dict[str, str]:
    if not endpoint.startswith(("http://", "https://")):
        raise ValueError("MinIO endpoint must start with http:// or https://")
    if not access_key or not secret_key:
        raise ValueError("MinIO access key and secret key are required")
    return {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": endpoint.rstrip("/"),
        "spark.hadoop.fs.s3a.access.key": access_key,
        "spark.hadoop.fs.s3a.secret.key": secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(
            endpoint.startswith("https://")
        ).lower(),
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
    }


def create_session(*, master: str, settings: Mapping[str, str]) -> SparkSession:
    configure_java_home()
    os.environ["PYSPARK_PYTHON"] = sys.executable
    builder = SparkSession.builder.master(master).appName("minio-s3a-roundtrip")
    for name, value in settings.items():
        builder = builder.config(name, value)
    return builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()


def run_roundtrip(
    spark: SparkSession, *, input_uri: str, output_uri: str
) -> dict[str, object]:
    started = time.perf_counter()
    source = spark.read.text(input_uri)
    input_rows = source.count()
    if input_rows < 1:
        raise ValueError("MinIO fixture contains no rows")
    source.write.mode("overwrite").parquet(output_uri)
    output_rows = spark.read.parquet(output_uri).count()
    if input_rows != output_rows:
        raise RuntimeError(
            f"MinIO row count mismatch: input={input_rows}, output={output_rows}"
        )
    return {
        "status": "completed",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "input_uri": input_uri,
        "output_uri": output_uri,
        "input_rows": input_rows,
        "output_rows": output_rows,
        "duration_seconds": round(time.perf_counter() - started, 3),
        "spark_version": spark.version,
        "master": spark.sparkContext.master,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify Spark S3A read/write against MinIO")
    raw_bucket = os.getenv("MINIO_RAW_BUCKET", "news-raw")
    processed_bucket = os.getenv("MINIO_PROCESSED_BUCKET", "news-processed")
    parser.add_argument(
        "--input-uri",
        default=f"s3a://{raw_bucket}/fixtures/text-events/synthetic-events.jsonl",
    )
    parser.add_argument(
        "--output-uri",
        default=(
            f"s3a://{processed_bucket}/fixtures/text-events/synthetic-events-parquet"
        ),
    )
    parser.add_argument("--endpoint", default=os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
    parser.add_argument("--access-key", default=os.getenv("MINIO_ROOT_USER", "news_pipeline"))
    parser.add_argument(
        "--secret-key",
        default=os.getenv("MINIO_ROOT_PASSWORD", "news_pipeline_minio_dev"),
    )
    parser.add_argument("--master", default=os.getenv("SPARK_MASTER_URL", "local[2]"))
    parser.add_argument("--report", type=Path, required=True)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = build_s3a_config(
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
    )
    spark = create_session(master=args.master, settings=settings)
    try:
        spark.sparkContext.setLogLevel("WARN")
        report = run_roundtrip(
            spark, input_uri=args.input_uri, output_uri=args.output_uri
        )
    finally:
        spark.stop()
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
