from __future__ import annotations

import fnmatch
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from storage.object_store import (
    ObjectStoreConfig,
    ObjectWriteResult,
    download_file_verified,
    upload_file_verified,
)


@dataclass(frozen=True)
class ArtifactRoute:
    directory: str
    bucket_env: str
    default_bucket: str
    prefix: str


ROUTES = (
    ArtifactRoute("raw", "MINIO_RAW_BUCKET", "news-raw", "raw"),
    ArtifactRoute("airflow-input", "MINIO_RAW_BUCKET", "news-raw", "airflow-input"),
    ArtifactRoute("selected", "MINIO_PROCESSED_BUCKET", "news-processed", "selected"),
    ArtifactRoute("experiments", "MINIO_PROCESSED_BUCKET", "news-processed", "experiments"),
    ArtifactRoute("airflow-output", "MINIO_PROCESSED_BUCKET", "news-processed", "airflow-output"),
    ArtifactRoute(
        "airflow-output-minio",
        "MINIO_PROCESSED_BUCKET",
        "news-processed",
        "airflow-output-minio",
    ),
    ArtifactRoute("validation", "MINIO_PROCESSED_BUCKET", "news-processed", "validation"),
    ArtifactRoute("llm", "MINIO_LLM_BUCKET", "news-llm", "requests"),
    ArtifactRoute("llm_response", "MINIO_LLM_BUCKET", "news-llm", "responses"),
    ArtifactRoute("reports", "MINIO_REPORTS_BUCKET", "news-reports", "reports"),
    ArtifactRoute("logs", "MINIO_REPORTS_BUCKET", "news-reports", "logs"),
    ArtifactRoute(
        "minio-validation",
        "MINIO_REPORTS_BUCKET",
        "news-reports",
        "minio-validation",
    ),
    ArtifactRoute(
        "ingestion-guide", "MINIO_REPORTS_BUCKET", "news-reports", "ingestion-guide"
    ),
    ArtifactRoute(
        "system-diagrams", "MINIO_REPORTS_BUCKET", "news-reports", "system-diagrams"
    ),
)

EXCLUDED_PATTERNS = (
    "*.part",
    "*.tmp",
    "*.prefix",
    "*.footer",
    "*.sparse.parquet",
    "*.crc",
    "minio-data-migration*.json",
    "minio-data-migration*.jsonl",
)


@dataclass(frozen=True)
class DataArtifact:
    local_path: Path
    bucket: str
    key: str
    size_bytes: int


def route_artifact(path: Path, *, data_root: Path) -> DataArtifact:
    root = data_root.resolve()
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(root)
    except ValueError as error:
        raise ValueError("artifact path must stay inside PIPELINE_DATA_ROOT") from error
    if len(relative.parts) < 2:
        raise ValueError("artifact must be inside a routed data directory")
    directory = relative.parts[0]
    if directory in {"airflow-output", "airflow-output-minio"}:
        suffix = Path(*relative.parts[1:]).as_posix()
        if any("llm" in part.casefold() for part in relative.parts[1:3]):
            bucket = os.getenv("MINIO_LLM_BUCKET", "news-llm")
            key = f"airflow/{directory}/{suffix}"
        elif "output" in relative.parts[1:]:
            bucket = os.getenv("MINIO_PROCESSED_BUCKET", "news-processed")
            key = f"airflow/{suffix}"
        else:
            bucket = os.getenv("MINIO_REPORTS_BUCKET", "news-reports")
            key = f"airflow/{suffix}"
        return DataArtifact(
            local_path=resolved,
            bucket=bucket,
            key=key,
            size_bytes=resolved.stat().st_size if resolved.is_file() else 0,
        )
    route = next((item for item in ROUTES if item.directory == directory), None)
    if route is None:
        raise ValueError(f"no MinIO route for data directory: {directory}")
    suffix = Path(*relative.parts[1:]).as_posix()
    return DataArtifact(
        local_path=resolved,
        bucket=os.getenv(route.bucket_env, route.default_bucket),
        key=f"{route.prefix}/{suffix}",
        size_bytes=resolved.stat().st_size if resolved.is_file() else 0,
    )


def is_canonical_artifact(path: Path) -> bool:
    return path.is_file() and not any(
        fnmatch.fnmatch(path.name, pattern) for pattern in EXCLUDED_PATTERNS
    )


def iter_data_artifacts(data_root: Path) -> Iterator[DataArtifact]:
    root = data_root.resolve()
    for route in ROUTES:
        directory = root / route.directory
        if not directory.is_dir():
            continue
        for path in sorted(directory.rglob("*")):
            if is_canonical_artifact(path):
                yield route_artifact(path, data_root=root)


def publish_artifact(
    path: Path,
    *,
    data_root: Path | None = None,
    client: Any | None = None,
) -> ObjectWriteResult:
    root = data_root or Path(os.getenv("PIPELINE_DATA_ROOT", "data"))
    artifact = route_artifact(path, data_root=root)
    s3_client = client or ObjectStoreConfig.from_env().create_client()
    return upload_file_verified(
        s3_client,
        local_path=artifact.local_path,
        bucket=artifact.bucket,
        key=artifact.key,
        metadata={"pipeline-path": str(artifact.local_path.relative_to(root.resolve()))},
    )


def publish_artifact_if_enabled(path: Path) -> ObjectWriteResult | None:
    backend = os.getenv("PIPELINE_STORAGE_BACKEND", "local").strip().casefold()
    if backend in {"", "local"}:
        return None
    if backend != "minio":
        raise ValueError("PIPELINE_STORAGE_BACKEND must be local or minio")
    return publish_artifact(path)


def materialize_artifact_if_enabled(path: Path) -> ObjectWriteResult | None:
    if path.exists():
        return None
    backend = os.getenv("PIPELINE_STORAGE_BACKEND", "local").strip().casefold()
    if backend in {"", "local"}:
        return None
    if backend != "minio":
        raise ValueError("PIPELINE_STORAGE_BACKEND must be local or minio")
    root = Path(os.getenv("PIPELINE_DATA_ROOT", "data"))
    artifact = route_artifact(path, data_root=root)
    client = ObjectStoreConfig.from_env().create_client()
    return download_file_verified(
        client,
        bucket=artifact.bucket,
        key=artifact.key,
        destination=path,
    )
