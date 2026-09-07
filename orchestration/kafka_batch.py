from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Mapping

from core.events import validate_event
from jobs.init_kafka import TOPICS, build_topic_specs, create_admin_client, ensure_topics
from producers.kafka import KafkaEventProducer, create_producer
from storage.data_lake import publish_artifact_if_enabled
from storage.jsonl import read_jsonl


OffsetReader = Callable[[str, str], dict[str, dict[str, int]]]


def ensure_batch_topics(*, bootstrap_servers: str, partitions: int = 3) -> dict[str, list[str]]:
    admin = create_admin_client(bootstrap_servers)
    specs = build_topic_specs(
        TOPICS,
        partitions=partitions,
        replication_factor=1,
    )
    created, existing = ensure_topics(admin, specs)
    return {"created": created, "existing": existing}


def capture_topic_offsets(
    bootstrap_servers: str,
    topic: str,
    *,
    timeout_seconds: float = 10.0,
    consumer_factory: Callable[[Mapping[str, Any]], Any] | None = None,
) -> dict[str, dict[str, int]]:
    if not bootstrap_servers.strip() or not topic.strip():
        raise ValueError("bootstrap_servers and topic must not be empty")
    try:
        from confluent_kafka import Consumer, TopicPartition
    except ImportError as error:
        raise RuntimeError("Kafka offsets require 'confluent-kafka'") from error

    factory = consumer_factory or Consumer
    consumer = factory(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "airflow-bounded-offset-reader",
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(topic=topic, timeout=timeout_seconds)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise RuntimeError(f"Kafka topic metadata unavailable: {topic}")
        offsets: dict[str, int] = {}
        for partition in sorted(topic_metadata.partitions):
            _, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition),
                timeout=timeout_seconds,
                cached=False,
            )
            offsets[str(partition)] = int(high)
        if not offsets:
            raise RuntimeError(f"Kafka topic has no partitions: {topic}")
        return {topic: offsets}
    finally:
        consumer.close()


def offset_span(
    starting_offsets: Mapping[str, Mapping[str, int]],
    ending_offsets: Mapping[str, Mapping[str, int]],
) -> int:
    if set(starting_offsets) != set(ending_offsets):
        raise ValueError("starting and ending offset topics differ")
    total = 0
    for topic, starts in starting_offsets.items():
        ends = ending_offsets[topic]
        if set(starts) != set(ends):
            raise ValueError(f"partition set differs for topic {topic}")
        for partition, start in starts.items():
            end = int(ends[partition])
            start_value = int(start)
            if end < start_value:
                raise ValueError(f"ending offset precedes start for {topic}[{partition}]")
            total += end - start_value
    return total


def publish_jsonl_batch(
    *,
    project_root: Path,
    input_file: str,
    bootstrap_servers: str,
    topic: str,
    pipeline_run_id: str,
    analysis_date: str,
    client_factory: Callable[[str, str], Any] = create_producer,
) -> dict[str, Any]:
    path = (project_root.resolve() / input_file).resolve()
    path.relative_to(project_root.resolve())
    if not path.is_file():
        raise FileNotFoundError(path)
    client = client_factory(
        bootstrap_servers,
        f"airflow-{analysis_date}-{pipeline_run_id}"[:240],
    )
    producer = KafkaEventProducer(client, topic=topic)
    published_rows = 0
    try:
        for event in read_jsonl(path):
            enriched = dict(event)
            enriched["metadata"] = {
                **dict(event.get("metadata") or {}),
                "pipeline_run_id": pipeline_run_id,
                "analysis_date": analysis_date,
            }
            producer.send(validate_event(enriched))
            published_rows += 1
    finally:
        producer.close()
    if published_rows < 1:
        raise ValueError("Kafka batch input contains no events")
    return {
        "topic": topic,
        "pipeline_run_id": pipeline_run_id,
        "analysis_date": analysis_date,
        "published_rows": published_rows,
        "input_file": input_file,
    }


def write_kafka_ledger(
    *,
    project_root: Path,
    run_directory: str,
    publication: Mapping[str, Any],
    starting_offsets: Mapping[str, Mapping[str, int]],
    ending_offsets: Mapping[str, Mapping[str, int]],
) -> dict[str, Any]:
    span = offset_span(starting_offsets, ending_offsets)
    published_rows = int(publication["published_rows"])
    if span < published_rows:
        raise RuntimeError(
            f"Kafka offset span is smaller than published rows: span={span}, published={published_rows}"
        )
    ledger = {
        **dict(publication),
        "starting_offsets": starting_offsets,
        "ending_offsets": ending_offsets,
        "offset_span_rows": span,
        "foreign_rows_possible": span > published_rows,
    }
    path = project_root / run_directory / "kafka-ledger.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(ledger, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    publish_artifact_if_enabled(path)
    ledger["ledger_path"] = str(path.relative_to(project_root))
    return ledger
