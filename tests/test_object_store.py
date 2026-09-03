from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from storage.object_store import (
    download_file_verified,
    ensure_buckets,
    normalize_object_key,
    upload_directory_verified,
    upload_file_verified,
)


class NotFound(Exception):
    response = {"Error": {"Code": "404"}}


class FakeS3Client:
    def __init__(self) -> None:
        self.buckets: set[str] = set()
        self.objects: dict[tuple[str, str], dict] = {}
        self.upload_calls = 0

    def head_bucket(self, *, Bucket: str) -> None:
        if Bucket not in self.buckets:
            raise NotFound()

    def create_bucket(self, *, Bucket: str) -> None:
        self.buckets.add(Bucket)

    def head_object(self, *, Bucket: str, Key: str) -> dict:
        try:
            value = self.objects[(Bucket, Key)]
        except KeyError as error:
            raise NotFound() from error
        return {
            "ContentLength": len(value["body"]),
            "Metadata": value["metadata"],
            "ETag": '"fake-etag"',
        }

    def upload_file(self, filename: str, bucket: str, key: str, *, ExtraArgs: dict) -> None:
        self.upload_calls += 1
        self.objects[(bucket, key)] = {
            "body": Path(filename).read_bytes(),
            "metadata": ExtraArgs["Metadata"],
        }

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        Path(filename).write_bytes(self.objects[(bucket, key)]["body"])


def test_upload_is_checksum_verified_and_idempotent(tmp_path: Path) -> None:
    client = FakeS3Client()
    client.buckets.add("news-raw")
    fixture = tmp_path / "fixture.jsonl"
    fixture.write_text('{"event_id":"one"}\n', encoding="utf-8")

    first = upload_file_verified(
        client,
        local_path=fixture,
        bucket="news-raw",
        key="fixtures/fixture.jsonl",
    )
    second = upload_file_verified(
        client,
        local_path=fixture,
        bucket="news-raw",
        key="fixtures/fixture.jsonl",
    )

    assert first.status == "uploaded"
    assert second.status == "unchanged"
    assert first.sha256 == hashlib.sha256(fixture.read_bytes()).hexdigest()
    assert client.upload_calls == 1


def test_directory_sync_and_verified_download(tmp_path: Path) -> None:
    client = FakeS3Client()
    client.buckets.add("news-processed")
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.txt").write_text("a", encoding="utf-8")
    nested = source / "nested"
    nested.mkdir()
    (nested / "b.txt").write_text("bb", encoding="utf-8")

    report = upload_directory_verified(
        client,
        local_directory=source,
        bucket="news-processed",
        prefix="runs/test/output",
    )
    destination = tmp_path / "download" / "a.txt"
    downloaded = download_file_verified(
        client,
        bucket="news-processed",
        key="runs/test/output/a.txt",
        destination=destination,
    )

    assert report["object_count"] == 2
    assert report["uploaded"] == 2
    assert report["total_bytes"] == 3
    assert downloaded.status == "downloaded"
    assert destination.read_text(encoding="utf-8") == "a"


def test_bucket_creation_and_object_key_validation() -> None:
    client = FakeS3Client()
    assert ensure_buckets(client, ["news-raw", "news-processed"]) == [
        "news-raw",
        "news-processed",
    ]
    assert ensure_buckets(client, ["news-raw"]) == []
    assert normalize_object_key("year=2012/month=01/file.jsonl") == (
        "year=2012/month=01/file.jsonl"
    )
    with pytest.raises(ValueError):
        normalize_object_key("../secret")
