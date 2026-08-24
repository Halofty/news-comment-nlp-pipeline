from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from pyspark.sql import functions as F

from core.events import stable_event_id
from jobs.generate_synthetic_events import generate_events
from spark_jobs.process_sample import create_spark_session
from spark_jobs.streaming_consumer import (
    StreamingConfig,
    build_parser,
    prepare_stream,
    process_micro_batch,
    transform_kafka_messages,
)


def test_cli_uses_standalone_master_from_environment(monkeypatch) -> None:
    monkeypatch.setenv("SPARK_MASTER_URL", "spark://spark-master:7077")

    args = build_parser().parse_args(
        ["--output", "data/output", "--checkpoint", "data/checkpoint"]
    )

    assert args.master == "spark://spark-master:7077"


def test_streaming_config_rejects_unsafe_or_invalid_settings() -> None:
    root = Path("data/test-stream")
    valid = StreamingConfig(
        bootstrap_servers="localhost:9092",
        input_topic="raw-text",
        dlq_topic="raw-text-dlq",
        output_path=root / "output",
        checkpoint_path=root / "checkpoint",
    )
    valid.validate()

    with pytest.raises(ValueError, match="must differ"):
        StreamingConfig(
            bootstrap_servers="localhost:9092",
            input_topic="raw-text",
            dlq_topic="raw-text",
            output_path=root / "output",
            checkpoint_path=root / "checkpoint",
        ).validate()

    with pytest.raises(ValueError, match="starting_offsets"):
        StreamingConfig(
            bootstrap_servers="localhost:9092",
            input_topic="raw-text",
            dlq_topic="raw-text-dlq",
            output_path=root / "output",
            checkpoint_path=root / "checkpoint",
            starting_offsets="middle",
        ).validate()


def test_kafka_messages_are_routed_and_written_by_micro_batch() -> None:
    normal = next(generate_events(1))
    tombstone = {**normal, "event_id": stable_event_id("test", "tombstone"), "text": "[removed]"}
    pii = {
        **normal,
        "event_id": stable_event_id("test", "pii"),
        "text": "Please contact person@example.com for details.",
    }
    unexpected = {
        **normal,
        "event_id": stable_event_id("test", "unexpected"),
        "unexpected_field": "reject me",
    }
    payloads = [
        json.dumps(normal),
        json.dumps(tombstone),
        json.dumps(pii),
        json.dumps(unexpected),
    ]
    timestamp = datetime(2026, 8, 23, tzinfo=timezone.utc)

    spark = create_spark_session(master="local[1]", app_name="stream-routing-test")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        messages = spark.createDataFrame(
            [
                (
                    f"key-{offset}".encode(),
                    payload.encode(),
                    "raw-text",
                    0,
                    offset,
                    timestamp,
                )
                for offset, payload in enumerate(payloads)
            ],
            "key binary, value binary, topic string, partition int, offset long, timestamp timestamp",
        )
        routed = transform_kafka_messages(messages).cache()
        try:
            counts = {
                row["output_route"]: row["count"]
                for row in routed.groupBy("output_route").count().collect()
            }
            assert counts == {
                "processed": 1,
                "quality_rejected": 1,
                "quarantine": 1,
                "contract_rejected": 1,
            }

            provenance = routed.orderBy("kafka_offset").first()
            assert provenance["kafka_topic"] == "raw-text"
            assert provenance["kafka_partition"] == 0
            assert provenance["kafka_offset"] == 0

            streaming_messages = spark.readStream.format("rate").load().select(
                F.lit(b"key").alias("key"),
                F.lit(payloads[0].encode()).alias("value"),
                F.lit("raw-text").alias("topic"),
                F.lit(0).alias("partition"),
                F.col("value").alias("offset"),
                "timestamp",
            )
            prepared = prepare_stream(
                transform_kafka_messages(streaming_messages),
                watermark_delay="10 minutes",
            )
            assert prepared.isStreaming
            assert "DeduplicateWithinWatermark" in str(
                prepared._jdf.queryExecution().analyzed()
            )

            with TemporaryDirectory() as temporary_directory:
                root = Path(temporary_directory)
                config = StreamingConfig(
                    bootstrap_servers="localhost:9092",
                    input_topic="raw-text",
                    dlq_topic="raw-text-dlq",
                    output_path=root / "output",
                    checkpoint_path=root / "checkpoint",
                    publish_dlq=False,
                    output_format="jsonl",
                )
                process_micro_batch(routed, 7, config=config)

                for route in counts:
                    batch_path = (
                        root / "output" / route / "batch_id=00000000000000000007"
                    )
                    assert len(
                        (batch_path / "events.jsonl")
                        .read_text(encoding="utf-8")
                        .splitlines()
                    ) == 1
        finally:
            routed.unpersist()
    finally:
        spark.stop()
