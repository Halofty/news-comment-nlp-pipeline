from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Callable, Iterable, Iterator
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

REQUIRED_FIELDS = {
    "event_id",
    "source_type",
    "source_name",
    "event_time",
    "collected_at",
    "text",
    "schema_version",
}


class ProducerClient(Protocol):
    def produce(self, topic: str, **kwargs: Any) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float | None = None) -> int: ...


class DeliveryError(RuntimeError):
    """Raised when Kafka could not deliver one or more messages."""


def parse_event_time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("event_time must be an ISO-8601 string")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError("event_time must include a timezone")
    return parsed


def validate_event(event: Any, *, line_number: int | None = None) -> dict[str, Any]:
    location = f" on line {line_number}" if line_number is not None else ""
    if not isinstance(event, dict):
        raise ValueError(f"event{location} must be a JSON object")

    missing = sorted(REQUIRED_FIELDS.difference(event))
    if missing:
        raise ValueError(f"event{location} is missing fields: {', '.join(missing)}")
    if not isinstance(event["event_id"], str) or not event["event_id"].strip():
        raise ValueError(f"event_id{location} must be a non-empty string")
    if not isinstance(event["text"], str) or not event["text"].strip():
        raise ValueError(f"text{location} must be a non-empty string")
    parse_event_time(event["event_time"])
    return event


def read_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"invalid JSON on line {line_number}: {error.msg}"
                ) from error
            yield validate_event(event, line_number=line_number)


def order_events(
    events: Iterable[dict[str, Any]], *, sort_by_event_time: bool
) -> Iterable[dict[str, Any]]:
    if not sort_by_event_time:
        return events
    return sorted(events, key=lambda event: parse_event_time(event["event_time"]))


def create_producer(bootstrap_servers: str, client_id: str) -> ProducerClient:
    try:
        from confluent_kafka import Producer
    except ImportError as error:
        raise RuntimeError(
            "Kafka production requires 'confluent-kafka'. "
            "Run: python3 -m pip install -r requirements.txt"
        ) from error

    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "zstd",
        }
    )


class KafkaEventProducer:
    def __init__(
        self,
        client: ProducerClient,
        *,
        topic: str,
        flush_timeout: float = 30.0,
    ) -> None:
        self.client = client
        self.topic = topic
        self.flush_timeout = flush_timeout
        self.delivered = 0
        self.delivery_errors: list[str] = []

    def _on_delivery(self, error: Any, message: Any) -> None:
        if error is not None:
            self.delivery_errors.append(str(error))
            return
        self.delivered += 1

    def send(self, event: dict[str, Any]) -> None:
        validate_event(event)
        timestamp_ms = int(parse_event_time(event["event_time"]).timestamp() * 1000)
        payload = json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
        while True:
            try:
                self.client.produce(
                    self.topic,
                    key=event["event_id"].encode("utf-8"),
                    value=payload,
                    timestamp=timestamp_ms,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                self.client.poll(0.5)
        self.client.poll(0)

    def close(self) -> None:
        remaining = self.client.flush(self.flush_timeout)
        if remaining:
            raise DeliveryError(f"{remaining} Kafka messages were not delivered")
        if self.delivery_errors:
            preview = "; ".join(self.delivery_errors[:3])
            raise DeliveryError(
                f"{len(self.delivery_errors)} Kafka messages failed: {preview}"
            )


def replay_events(
    events: Iterable[dict[str, Any]],
    producer: KafkaEventProducer,
    *,
    speed: float = 0,
    max_delay: float = 60.0,
    sleeper: Callable[[float], None] = time.sleep,
) -> int:
    if speed < 0:
        raise ValueError("speed must be zero or positive")
    if max_delay < 0:
        raise ValueError("max_delay must be zero or positive")

    previous_time: datetime | None = None
    sent = 0
    try:
        for event in events:
            event_time = parse_event_time(event["event_time"])
            if speed > 0 and previous_time is not None:
                event_gap = max(0.0, (event_time - previous_time).total_seconds())
                delay = min(event_gap / speed, max_delay)
                if delay > 0:
                    sleeper(delay)
            producer.send(event)
            previous_time = event_time
            sent += 1
    finally:
        producer.close()
    return sent


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish common-schema JSONL events to Kafka"
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--topic", default=os.getenv("KAFKA_RAW_TOPIC", "raw-text")
    )
    parser.add_argument("--client-id", default="text-event-producer")
    parser.add_argument(
        "--speed",
        type=float,
        default=0,
        help="Replay multiplier; 0 sends without waiting, 10 is ten times faster",
    )
    parser.add_argument(
        "--max-delay",
        type=float,
        default=60,
        help="Maximum wait between events in seconds",
    )
    parser.add_argument(
        "--sort-by-event-time",
        action="store_true",
        help="Sort all input in memory before replaying",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    events = order_events(
        read_jsonl(args.input), sort_by_event_time=args.sort_by_event_time
    )
    client = create_producer(args.bootstrap_servers, args.client_id)
    producer = KafkaEventProducer(client, topic=args.topic)
    count = replay_events(
        events,
        producer,
        speed=args.speed,
        max_delay=args.max_delay,
    )
    print(f"delivered {count} events to Kafka topic '{args.topic}'")


if __name__ == "__main__":
    main()
