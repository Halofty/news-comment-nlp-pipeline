from __future__ import annotations

import argparse
import os
from collections.abc import Iterable
from typing import Any, Protocol


class AdminClient(Protocol):
    def create_topics(self, topics: list[Any]) -> dict[str, Any]: ...


TOPICS = {
    "raw-text": {"retention.ms": str(7 * 24 * 60 * 60 * 1000)},
    "raw-text-dlq": {"retention.ms": str(30 * 24 * 60 * 60 * 1000)},
}


def create_admin_client(bootstrap_servers: str) -> AdminClient:
    try:
        from confluent_kafka.admin import AdminClient as ConfluentAdminClient
    except ImportError as error:
        raise RuntimeError(
            "Kafka administration requires 'confluent-kafka'. "
            "Run: python3 -m pip install -r requirements.txt"
        ) from error
    return ConfluentAdminClient({"bootstrap.servers": bootstrap_servers})


def build_topic_specs(
    names: Iterable[str], *, partitions: int, replication_factor: int
) -> list[Any]:
    if partitions < 1:
        raise ValueError("partitions must be at least 1")
    if replication_factor < 1:
        raise ValueError("replication_factor must be at least 1")
    from confluent_kafka.admin import NewTopic

    return [
        NewTopic(
            name,
            num_partitions=partitions,
            replication_factor=replication_factor,
            config=TOPICS[name],
        )
        for name in names
    ]


def ensure_topics(admin: AdminClient, specs: list[Any]) -> tuple[list[str], list[str]]:
    created: list[str] = []
    existing: list[str] = []
    for name, future in admin.create_topics(specs).items():
        try:
            future.result()
            created.append(name)
        except Exception as error:
            if "TOPIC_ALREADY_EXISTS" in str(error):
                existing.append(name)
                continue
            raise RuntimeError(f"could not create Kafka topic '{name}': {error}") from error
    return created, existing


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create ingestion Kafka topics")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--replication-factor", type=int, default=1)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    admin = create_admin_client(args.bootstrap_servers)
    specs = build_topic_specs(
        TOPICS, partitions=args.partitions, replication_factor=args.replication_factor
    )
    created, existing = ensure_topics(admin, specs)
    if created:
        print(f"created topics: {', '.join(created)}")
    if existing:
        print(f"topics already exist: {', '.join(existing)}")


if __name__ == "__main__":
    main()

