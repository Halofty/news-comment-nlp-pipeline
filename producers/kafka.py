from __future__ import annotations

import json
from typing import Any, Protocol

from core.events import parse_event_time, validate_event


class ProducerClient(Protocol):
    def produce(self, topic: str, **kwargs: Any) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float | None = None) -> int: ...


class DeliveryError(RuntimeError):
    """Raised when Kafka could not deliver one or more messages."""


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
