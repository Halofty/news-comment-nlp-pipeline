from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from storage.object_store import (
    ObjectStoreConfig,
    download_file_verified,
    ensure_buckets,
    upload_directory_verified,
    upload_file_verified,
)


def _client() -> Any:
    return ObjectStoreConfig.from_env().create_client()


def _buckets() -> list[str]:
    return [
        os.getenv("MINIO_RAW_BUCKET", "news-raw"),
        os.getenv("MINIO_PROCESSED_BUCKET", "news-processed"),
        os.getenv("MINIO_CHECKPOINT_BUCKET", "news-checkpoints"),
    ]


def _write_report(report: dict[str, Any], path: Path | None) -> None:
    rendered = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


def run_init(args: argparse.Namespace) -> None:
    client = _client()
    created = ensure_buckets(client, _buckets())
    _write_report({"status": "ok", "created": created, "buckets": _buckets()}, args.report)


def run_upload(args: argparse.Namespace) -> None:
    result = upload_file_verified(
        _client(), local_path=args.file, bucket=args.bucket, key=args.key
    )
    _write_report(result.to_dict(), args.report)


def run_sync(args: argparse.Namespace) -> None:
    result = upload_directory_verified(
        _client(), directory=args.directory, bucket=args.bucket, prefix=args.prefix
    )
    _write_report(result, args.report)


def run_download(args: argparse.Namespace) -> None:
    result = download_file_verified(
        _client(), bucket=args.bucket, key=args.key, destination=args.output
    )
    _write_report(result.to_dict(), args.report)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Upload and verify files in local MinIO")
    subparsers = parser.add_subparsers(required=True)

    init = subparsers.add_parser("init", help="create configured buckets if absent")
    init.add_argument("--report", type=Path)
    init.set_defaults(handler=run_init)

    upload = subparsers.add_parser("upload", help="upload one file with SHA-256 metadata")
    upload.add_argument("--file", type=Path, required=True)
    upload.add_argument("--bucket", default=os.getenv("MINIO_RAW_BUCKET", "news-raw"))
    upload.add_argument("--key", required=True)
    upload.add_argument("--report", type=Path)
    upload.set_defaults(handler=run_upload)

    sync = subparsers.add_parser("sync", help="upload every file below a directory")
    sync.add_argument("--directory", type=Path, required=True)
    sync.add_argument("--bucket", default=os.getenv("MINIO_PROCESSED_BUCKET", "news-processed"))
    sync.add_argument("--prefix", required=True)
    sync.add_argument("--report", type=Path)
    sync.set_defaults(handler=run_sync)

    download = subparsers.add_parser("download", help="download and verify one object")
    download.add_argument("--bucket", default=os.getenv("MINIO_RAW_BUCKET", "news-raw"))
    download.add_argument("--key", required=True)
    download.add_argument("--output", type=Path, required=True)
    download.add_argument("--report", type=Path)
    download.set_defaults(handler=run_download)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    args.handler(args)


if __name__ == "__main__":
    main()
