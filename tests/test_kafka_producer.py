from __future__ import annotations

import json
import tempfile
from pathlib import Path

from core.events import parse_event_time, stable_event_id
from jobs.replay_to_kafka import order_events, replay_events
from producers.kafka import DeliveryError, KafkaEventProducer
from storage.jsonl import read_jsonl


def make_event(event_id: str, event_time: str) -> dict:
    return {
        "event_id": stable_event_id("reddit", event_id),
        "source_type": "comment",
        "source_name": "reddit",
        "event_time": event_time,
        "collected_at": "2026-08-20T00:00:00Z",
        "language": "unknown",
        "title": None,
        "text": f"event {event_id}",
        "url": None,
        "community": "example_community",
        "engagement": 0,
        "schema_version": 1,
        "metadata": {},
    }


class FakeMessage:
    pass


class FakeProducerClient:
    def __init__(self, *, delivery_error=None, remaining=0) -> None:
        self.records = []
        self.delivery_error = delivery_error
        self.remaining = remaining
        self.flush_timeout = None

    def produce(self, topic, **kwargs) -> None:
        self.records.append((topic, kwargs))
        kwargs["on_delivery"](self.delivery_error, FakeMessage())

    def poll(self, timeout) -> int:
        return 0

    def flush(self, timeout=None) -> int:
        self.flush_timeout = timeout
        return self.remaining


def test_send_uses_event_id_key_and_event_time_timestamp() -> None:
    client = FakeProducerClient()
    producer = KafkaEventProducer(client, topic="raw-text")
    event = make_event("event-1", "2026-08-20T01:02:03Z")

    producer.send(event)
    producer.close()

    topic, message = client.records[0]
    assert topic == "raw-text"
    assert message["key"] == stable_event_id("reddit", "event-1").encode()
    assert message["timestamp"] == int(
        parse_event_time("2026-08-20T01:02:03Z").timestamp() * 1000
    )
    assert json.loads(message["value"])["text"] == "event event-1"
    assert producer.delivered == 1


def test_replay_applies_speed_and_max_delay() -> None:
    client = FakeProducerClient()
    producer = KafkaEventProducer(client, topic="raw-text")
    delays = []
    events = [
        make_event("1", "2026-08-20T00:00:00Z"),
        make_event("2", "2026-08-20T00:01:40Z"),
        make_event("3", "2026-08-20T00:01:50Z"),
    ]

    sent = replay_events(
        events,
        producer,
        speed=10,
        max_delay=5,
        sleeper=delays.append,
    )

    assert sent == 3
    assert delays == [5, 1]


def test_order_events_sorts_when_requested() -> None:
    events = [
        make_event("later", "2026-08-20T02:00:00Z"),
        make_event("earlier", "2026-08-20T01:00:00Z"),
    ]
    ordered = order_events(events, sort_by_event_time=True)
    assert [event["text"] for event in ordered] == ["event earlier", "event later"]


def test_read_jsonl_reports_invalid_line() -> None:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "events.jsonl"
        path.write_text("{}\nnot-json\n", encoding="utf-8")
        try:
            list(read_jsonl(path))
        except ValueError as error:
            assert "line 1" in str(error)
        else:
            raise AssertionError("invalid event must raise ValueError")


def test_close_raises_on_delivery_failure() -> None:
    client = FakeProducerClient(delivery_error="broker unavailable")
    producer = KafkaEventProducer(client, topic="raw-text")
    producer.send(make_event("1", "2026-08-20T00:00:00Z"))
    try:
        producer.close()
    except DeliveryError as error:
        assert "broker unavailable" in str(error)
    else:
        raise AssertionError("delivery failure must raise DeliveryError")
