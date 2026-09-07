from __future__ import annotations

import json
from pathlib import Path

from core.events import stable_event_id
from orchestration.kafka_batch import (
    capture_topic_offsets,
    offset_span,
    publish_jsonl_batch,
    write_kafka_ledger,
)
from orchestration.spark_batch import build_kafka_spark_command, prepare_kafka_run_config


class _Topic:
    error = None
    partitions = {0: object(), 1: object()}


class _Metadata:
    topics = {"raw-text": _Topic()}


class _OffsetConsumer:
    def __init__(self, config) -> None:
        self.config = config
        self.closed = False

    def list_topics(self, topic, timeout):
        assert topic == "raw-text"
        return _Metadata()

    def get_watermark_offsets(self, partition, timeout, cached):
        return (0, 10 + partition.partition)

    def close(self):
        self.closed = True


class _ProducerClient:
    def __init__(self) -> None:
        self.records = []

    def produce(self, topic, **kwargs):
        self.records.append((topic, kwargs))
        kwargs["on_delivery"](None, object())

    def poll(self, timeout):
        return 0

    def flush(self, timeout=None):
        return 0


def _event() -> dict:
    return {
        "event_id": stable_event_id("reddit", "bounded-1"),
        "source_type": "comment",
        "source_name": "reddit",
        "event_time": "2012-02-01T00:00:00Z",
        "collected_at": "2026-09-07T00:00:00Z",
        "language": "en",
        "title": None,
        "text": "Economic conditions are changing.",
        "url": None,
        "community": "Economics",
        "engagement": 1,
        "schema_version": 1,
        "metadata": {},
    }


def test_capture_offsets_and_span() -> None:
    offsets = capture_topic_offsets(
        "kafka:29092",
        "raw-text",
        consumer_factory=_OffsetConsumer,
    )
    assert offsets == {"raw-text": {"0": 10, "1": 11}}
    assert offset_span(
        {"raw-text": {"0": 7, "1": 8}},
        offsets,
    ) == 6


def test_publish_enriches_run_metadata(tmp_path: Path) -> None:
    source = tmp_path / "data/input.jsonl"
    source.parent.mkdir()
    source.write_text(json.dumps(_event()) + "\n", encoding="utf-8")
    client = _ProducerClient()

    result = publish_jsonl_batch(
        project_root=tmp_path,
        input_file="data/input.jsonl",
        bootstrap_servers="kafka:29092",
        topic="raw-text",
        pipeline_run_id="manual__bounded",
        analysis_date="2012-02-01",
        client_factory=lambda _servers, _client_id: client,
    )

    payload = json.loads(client.records[0][1]["value"])
    assert result["published_rows"] == 1
    assert payload["metadata"]["pipeline_run_id"] == "manual__bounded"
    assert payload["metadata"]["analysis_date"] == "2012-02-01"


def test_ledger_and_spark_command_preserve_exact_offsets(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_STORAGE_BACKEND", "local")
    ledger = write_kafka_ledger(
        project_root=tmp_path,
        run_directory="data/run",
        publication={
            "topic": "raw-text",
            "pipeline_run_id": "manual__bounded",
            "analysis_date": "2012-02-01",
            "published_rows": 3,
        },
        starting_offsets={"raw-text": {"0": 10}},
        ending_offsets={"raw-text": {"0": 13}},
    )
    source = tmp_path / "data/input.jsonl"
    source.write_text(json.dumps(_event()) + "\n", encoding="utf-8")
    config = prepare_kafka_run_config(
        project_root=tmp_path,
        params={
            "input_file": "data/input.jsonl",
            "output_root": "data/output",
            "run_label": "bounded",
            "output_format": "parquet",
            "partitions": 2,
            "spark_master": "local[2]",
            "kafka_bootstrap_servers": "kafka:29092",
            "kafka_dlq_topic": "raw-text-dlq",
            "spark_kafka_package": "spark-kafka-package",
        },
        airflow_run_id="manual__bounded",
        kafka_ledger=ledger,
    )
    command = build_kafka_spark_command(config)

    assert ledger["offset_span_rows"] == 3
    assert json.loads((tmp_path / ledger["ledger_path"]).read_text())["published_rows"] == 3
    assert command[command.index("--starting-offsets") + 1] == '{"raw-text":{"0":10}}'
    assert command[command.index("--ending-offsets") + 1] == '{"raw-text":{"0":13}}'
    assert command[command.index("--expected-rows") + 1] == "3"
