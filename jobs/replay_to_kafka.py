from __future__ import annotations

import argparse
import os
import time
from collections.abc import Callable, Iterable
from datetime import datetime
from pathlib import Path
from typing import Any

from core.events import parse_event_time
from producers.kafka import KafkaEventProducer, create_producer
from storage.jsonl import read_jsonl


def order_events(
    events: Iterable[dict[str, Any]], *, sort_by_event_time: bool
) -> Iterable[dict[str, Any]]:
    if not sort_by_event_time:
        return events
    return sorted(events, key=lambda event: parse_event_time(event["event_time"]))


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
