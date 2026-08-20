from __future__ import annotations

import json

from core.events import stable_event_id
from jobs.init_kafka import ensure_topics
from jobs.inspect_kafka import inspect_messages


def make_event() -> dict:
    return {
        "event_id": stable_event_id("gdelt", "https://example.com/news"),
        "source_type": "news",
        "source_name": "gdelt",
        "event_time": "2026-08-20T00:00:00Z",
        "collected_at": "2026-08-20T00:01:00Z",
        "language": "english",
        "title": "Example headline",
        "text": "Example headline",
        "url": "https://example.com/news",
        "community": None,
        "engagement": None,
        "schema_version": 1,
        "metadata": {"text_scope": "title_only"},
    }


class Future:
    def __init__(self, error=None):
        self.error = error

    def result(self):
        if self.error:
            raise self.error


class Admin:
    def create_topics(self, topics):
        return {"raw-text": Future(), "raw-text-dlq": Future(Exception("TOPIC_ALREADY_EXISTS"))}


class Message:
    def __init__(self, value):
        self._value = value

    def error(self):
        return None

    def value(self):
        return self._value

    def partition(self):
        return 0

    def offset(self):
        return 1


class Consumer:
    def __init__(self, messages):
        self.messages = iter(messages)
        self.topics = []
        self.closed = False

    def subscribe(self, topics):
        self.topics = topics

    def poll(self, timeout):
        return next(self.messages, None)

    def close(self):
        self.closed = True


def test_ensure_topics_is_idempotent() -> None:
    created, existing = ensure_topics(Admin(), [object(), object()])
    assert created == ["raw-text"]
    assert existing == ["raw-text-dlq"]


def test_inspector_reads_and_validates_finite_sample() -> None:
    event = make_event()
    consumer = Consumer([Message(json.dumps(event).encode())])
    result = inspect_messages(consumer, topic="raw-text", limit=10, idle_timeout=0.1)
    assert result == [event]
    assert consumer.topics == ["raw-text"]
    assert consumer.closed

