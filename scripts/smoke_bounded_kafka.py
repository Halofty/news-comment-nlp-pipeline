from __future__ import annotations

import argparse
import json
import uuid
from pathlib import Path

from orchestration.kafka_batch import (
    capture_topic_offsets,
    ensure_batch_topics,
    publish_jsonl_batch,
    write_kafka_ledger,
)
from orchestration.spark_batch import (
    prepare_kafka_run_config,
    run_kafka_spark_batch,
    verify_kafka_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish a small JSONL sample and verify an exact Kafka-to-Spark batch"
    )
    parser.add_argument("--input", default="sample/synthetic-events.jsonl")
    parser.add_argument("--bootstrap-servers", default="kafka:29092")
    parser.add_argument("--topic", default="raw-text")
    parser.add_argument("--dlq-topic", default="raw-text-dlq")
    parser.add_argument("--analysis-date", default="2012-01-01")
    parser.add_argument("--output-root", default="data/validation/kafka-bounded-smoke")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument(
        "--kafka-package",
        default="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    project_root = Path(__file__).resolve().parents[1]
    pipeline_run_id = f"smoke-{uuid.uuid4().hex[:12]}"
    run_directory = f"{args.output_root}/{pipeline_run_id}"

    ensure_batch_topics(bootstrap_servers=args.bootstrap_servers)
    starting_offsets = capture_topic_offsets(args.bootstrap_servers, args.topic)
    publication = publish_jsonl_batch(
        project_root=project_root,
        input_file=args.input,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        pipeline_run_id=pipeline_run_id,
        analysis_date=args.analysis_date,
    )
    ending_offsets = capture_topic_offsets(args.bootstrap_servers, args.topic)
    ledger = write_kafka_ledger(
        project_root=project_root,
        run_directory=run_directory,
        publication=publication,
        starting_offsets=starting_offsets,
        ending_offsets=ending_offsets,
    )
    config = prepare_kafka_run_config(
        project_root=project_root,
        airflow_run_id=pipeline_run_id,
        kafka_ledger=ledger,
        params={
            "input_file": args.input,
            "output_root": args.output_root,
            "run_label": "spark",
            "output_format": "parquet",
            "partitions": 1,
            "spark_master": args.master,
            "kafka_bootstrap_servers": args.bootstrap_servers,
            "kafka_dlq_topic": args.dlq_topic,
            "spark_kafka_package": args.kafka_package,
        },
    )
    report_path = run_kafka_spark_batch(config)
    summary = verify_kafka_report(project_root=project_root, report_path=report_path)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
