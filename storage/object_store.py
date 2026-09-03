from __future__ import annotations

import hashlib
import os
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping

BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


@dataclass(frozen=True)
class ObjectStoreConfig:
    endpoint_url: str
    access_key: str
    secret_key: str = field(repr=False)
    region_name: str = "us-east-1"

    @classmethod
    def from_env(cls) -> "ObjectStoreConfig":
        endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000").rstrip("/")
        access_key = os.getenv("MINIO_ROOT_USER", "")
        secret_key = os.getenv("MINIO_ROOT_PASSWORD", "")
        if not endpoint.startswith(("http://", "https://")):
            raise ValueError("MINIO_ENDPOINT must start with http:// or https://")
        if not access_key or not secret_key:
            raise ValueError("MINIO_ROOT_USER and MINIO_ROOT_PASSWORD are required")
        return cls(endpoint, access_key, secret_key)

    def create_client(self) -> Any:
        try:
            import boto3
            from botocore.config import Config
        except ImportError as error:
            raise RuntimeError("MinIO storage requires boto3; install requirements.txt") from error
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region_name,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )


@dataclass(frozen=True)
class ObjectWriteResult:
    bucket: str
    key: str
    status: str
    size_bytes: int
    sha256: str
    etag: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def validate_bucket(bucket: str) -> str:
    value = bucket.strip()
    if not BUCKET_PATTERN.fullmatch(value):
        raise ValueError(f"invalid S3 bucket name: {bucket!r}")
    return value


def normalize_object_key(key: str) -> str:
    value = key.strip().replace("\\", "/")
    path = PurePosixPath(value)
    if not value or value.startswith("/") or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("object key must be a non-empty relative path without '.' or '..'")
    return path.as_posix()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_buckets(client: Any, buckets: Iterable[str]) -> list[str]:
    created: list[str] = []
    for raw_bucket in buckets:
        bucket = validate_bucket(raw_bucket)
        try:
            client.head_bucket(Bucket=bucket)
        except Exception as error:
            if not _is_not_found(error):
                raise
            client.create_bucket(Bucket=bucket)
            created.append(bucket)
    return created


def upload_file_verified(
    client: Any,
    *,
    local_path: Path,
    bucket: str,
    key: str,
    metadata: Mapping[str, str] | None = None,
) -> ObjectWriteResult:
    path = local_path.resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    bucket = validate_bucket(bucket)
    key = normalize_object_key(key)
    size_bytes = path.stat().st_size
    sha256 = file_sha256(path)

    existing = _head_or_none(client, bucket=bucket, key=key)
    if existing is not None and _matches(existing, size_bytes=size_bytes, sha256=sha256):
        return _result(bucket, key, "unchanged", size_bytes, sha256, existing)

    object_metadata = {str(name): str(value) for name, value in (metadata or {}).items()}
    object_metadata["sha256"] = sha256
    client.upload_file(
        str(path),
        bucket,
        key,
        ExtraArgs={"Metadata": object_metadata},
    )
    stored = client.head_object(Bucket=bucket, Key=key)
    if not _matches(stored, size_bytes=size_bytes, sha256=sha256):
        raise RuntimeError(f"uploaded object verification failed: s3://{bucket}/{key}")
    return _result(bucket, key, "uploaded", size_bytes, sha256, stored)


def upload_directory_verified(
    client: Any,
    *,
    local_directory: Path,
    bucket: str,
    prefix: str,
) -> dict[str, Any]:
    directory = local_directory.resolve()
    if not directory.is_dir():
        raise NotADirectoryError(directory)
    prefix = normalize_object_key(prefix).rstrip("/")
    results = [
        upload_file_verified(
            client,
            local_path=path,
            bucket=bucket,
            key=f"{prefix}/{path.relative_to(directory).as_posix()}",
        )
        for path in sorted(item for item in directory.rglob("*") if item.is_file())
    ]
    if not results:
        raise ValueError(f"directory contains no files: {directory}")
    return {
        "bucket": validate_bucket(bucket),
        "prefix": prefix,
        "object_count": len(results),
        "uploaded": sum(result.status == "uploaded" for result in results),
        "unchanged": sum(result.status == "unchanged" for result in results),
        "total_bytes": sum(result.size_bytes for result in results),
        "objects": [result.to_dict() for result in results],
    }


def download_file_verified(
    client: Any,
    *,
    bucket: str,
    key: str,
    destination: Path,
) -> ObjectWriteResult:
    bucket = validate_bucket(bucket)
    key = normalize_object_key(key)
    stored = client.head_object(Bucket=bucket, Key=key)
    expected_sha256 = str(stored.get("Metadata", {}).get("sha256", ""))
    if not expected_sha256:
        raise RuntimeError(f"object has no sha256 metadata: s3://{bucket}/{key}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    client.download_file(bucket, key, str(destination))
    actual_size = destination.stat().st_size
    actual_sha256 = file_sha256(destination)
    if actual_size != int(stored["ContentLength"]) or actual_sha256 != expected_sha256:
        raise RuntimeError(f"downloaded object verification failed: s3://{bucket}/{key}")
    return _result(bucket, key, "downloaded", actual_size, actual_sha256, stored)


def _head_or_none(client: Any, *, bucket: str, key: str) -> Mapping[str, Any] | None:
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except Exception as error:
        if _is_not_found(error):
            return None
        raise


def _is_not_found(error: Exception) -> bool:
    response = getattr(error, "response", {})
    code = str(response.get("Error", {}).get("Code", ""))
    return code in {"404", "NoSuchBucket", "NoSuchKey", "NotFound"}


def _matches(head: Mapping[str, Any], *, size_bytes: int, sha256: str) -> bool:
    return int(head.get("ContentLength", -1)) == size_bytes and str(
        head.get("Metadata", {}).get("sha256", "")
    ) == sha256


def _result(
    bucket: str,
    key: str,
    status: str,
    size_bytes: int,
    sha256: str,
    head: Mapping[str, Any],
) -> ObjectWriteResult:
    return ObjectWriteResult(
        bucket=bucket,
        key=key,
        status=status,
        size_bytes=size_bytes,
        sha256=sha256,
        etag=str(head.get("ETag", "")).strip('"'),
    )
