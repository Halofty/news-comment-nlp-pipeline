from __future__ import annotations

import argparse
import json
import os
from typing import Any, Protocol

from core.events import validate_event


class ConsumerClient(Protocol):
    def subscribe(self, topics: list[str]) -> None: ...
    def poll(self, timeout: float) -> Any: ...
    def close(self) -> None: ...


def create_consumer(
    bootstrap_servers: str, *, group_id: str, from_beginning: bool
) -> ConsumerClient:
    try:
        from confluent_kafka import Consumer
    except ImportError as error:
        raise RuntimeError(
            "Kafka inspection requires 'confluent-kafka'. "
            "Run: python3 -m pip install -r requirements.txt"
        ) from error
    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
            "enable.auto.commit": False,
        }
    )


def inspect_messages(
    consumer: ConsumerClient,
    *,
    topic: str,
    limit: int,
    idle_timeout: float,
) -> list[dict[str, Any]]:
    if limit < 1:
        raise ValueError("limit must be at least 1")
    if idle_timeout <= 0:
        raise ValueError("idle_timeout must be positive")
    events: list[dict[str, Any]] = []
    consumer.subscribe([topic])
    try:
        while len(events) < limit:
            message = consumer.poll(idle_timeout)
            if message is None:
                break
            if message.error():
                raise RuntimeError(f"Kafka consume failed: {message.error()}")
            try:
                event = json.loads(message.value())
            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                raise ValueError(
                    f"invalid JSON at {topic}[{message.partition()}] "
                    f"offset {message.offset()}"
                ) from error
            events.append(validate_event(event))
    finally:
        consumer.close()
    return events


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read and validate a finite sample from a Kafka topic"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_RAW_TOPIC", "raw-text"))
    parser.add_argument("--group-id", default="text-event-inspector")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--idle-timeout", type=float, default=5)
    parser.add_argument("--from-beginning", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    consumer = create_consumer(
        args.bootstrap_servers,
        group_id=args.group_id,
        from_beginning=args.from_beginning,
    )
    events = inspect_messages(
        consumer, topic=args.topic, limit=args.limit, idle_timeout=args.idle_timeout
    )
    for event in events:
        print(json.dumps(event, ensure_ascii=False))
    print(f"validated {len(events)} events from Kafka topic '{args.topic}'")


if __name__ == "__main__":
    main()

