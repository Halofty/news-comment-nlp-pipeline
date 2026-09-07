from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

from pyspark.sql import DataFrame, functions as F

from spark_jobs.process_sample import process_raw_events
from spark_jobs.run_logging import JsonlRunLogger
from spark_jobs.streaming_consumer import (
    DEFAULT_KAFKA_PACKAGE,
    _write_contract_dlq_to_kafka,
    create_streaming_spark_session,
    parse_kafka_messages,
)


def read_bounded_kafka(
    spark,
    *,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str,
    ending_offsets: str,
) -> DataFrame:
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("endingOffsets", ending_offsets)
        .option("failOnDataLoss", "true")
        .load()
    )


def filter_run_messages(messages: DataFrame, *, pipeline_run_id: str, analysis_date: str) -> DataFrame:
    parsed = parse_kafka_messages(messages)
    return parsed.filter(
        (F.col("metadata")["pipeline_run_id"] == pipeline_run_id)
        & (F.col("metadata")["analysis_date"] == analysis_date)
    )


def _offset_digest(starting_offsets: str, ending_offsets: str, pipeline_run_id: str) -> str:
    payload = f"{starting_offsets}\n{ending_offsets}\n{pipeline_run_id}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Process one bounded Kafka offset range with Spark")
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--starting-offsets", required=True)
    parser.add_argument("--ending-offsets", required=True)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--analysis-date", required=True)
    parser.add_argument("--expected-rows", required=True, type=int)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--log", required=True, type=Path)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--partitions", type=int, default=2)
    parser.add_argument("--format", choices=("parquet", "jsonl"), default="parquet")
    parser.add_argument("--dlq-topic", default="raw-text-dlq")
    parser.add_argument("--kafka-package", default=DEFAULT_KAFKA_PACKAGE)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logger = JsonlRunLogger(args.log)
    logger.emit(
        "kafka_batch_started",
        topic=args.topic,
        analysis_date=args.analysis_date,
        expected_rows=args.expected_rows,
    )
    spark = create_streaming_spark_session(
        master=args.master,
        app_name=f"kafka-batch-{args.analysis_date}",
        kafka_package=args.kafka_package or None,
    )
    try:
        messages = read_bounded_kafka(
            spark,
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            starting_offsets=args.starting_offsets,
            ending_offsets=args.ending_offsets,
        ).cache()
        range_rows = messages.count()
        run_events = filter_run_messages(
            messages,
            pipeline_run_id=args.pipeline_run_id,
            analysis_date=args.analysis_date,
        ).cache()
        matched_rows = run_events.count()
        if matched_rows != args.expected_rows:
            raise RuntimeError(
                f"Kafka published/matched mismatch: expected={args.expected_rows}, matched={matched_rows}"
            )
        input_bytes = int(
            run_events.agg(F.coalesce(F.sum(F.length(F.encode("_raw_json", "UTF-8"))), F.lit(0))).first()[0]
        )
        report = process_raw_events(
            spark,
            raw=run_events,
            input_path=f"kafka://{args.topic}",
            input_sha256=_offset_digest(
                args.starting_offsets,
                args.ending_offsets,
                args.pipeline_run_id,
            ),
            input_bytes=input_bytes,
            output_path=args.output,
            output_partitions=args.partitions,
            output_format=args.format,
            run_logger=logger,
            contract_dlq_writer=lambda rejected: _write_contract_dlq_to_kafka(
                rejected,
                bootstrap_servers=args.bootstrap_servers,
                dlq_topic=args.dlq_topic,
            ),
        )
        report["kafka"] = {
            "topic": args.topic,
            "pipeline_run_id": args.pipeline_run_id,
            "analysis_date": args.analysis_date,
            "starting_offsets": json.loads(args.starting_offsets),
            "ending_offsets": json.loads(args.ending_offsets),
            "offset_range_rows": range_rows,
            "matched_run_rows": matched_rows,
            "ignored_foreign_rows": range_rows - matched_rows,
            "expected_published_rows": args.expected_rows,
            "dlq_rows": report["row_accounting"]["contract_rejected_rows"],
        }
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        logger.emit(
            "kafka_batch_completed",
            input_rows=matched_rows,
            offset_range_rows=range_rows,
            ignored_foreign_rows=range_rows - matched_rows,
            accounted_rows=report["row_accounting"]["accounted_rows"],
        )
    except Exception as error:
        logger.emit("kafka_batch_failed", error_type=type(error).__name__)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
