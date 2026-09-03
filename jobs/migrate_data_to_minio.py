from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from storage.data_lake import iter_data_artifacts, publish_artifact
from storage.object_store import ObjectStoreConfig, ensure_buckets, upload_file_verified


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _append(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, ensure_ascii=False) + "\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Copy canonical data/ artifacts to MinIO")
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument(
        "--manifest", type=Path, default=Path("data/logs/minio-data-migration.jsonl")
    )
    parser.add_argument(
        "--report", type=Path, default=Path("data/reports/minio-data-migration.json")
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--progress-every", type=int, default=25)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.progress_every < 1:
        raise ValueError("progress-every must be positive")
    artifacts = list(iter_data_artifacts(args.data_root))
    expected_bytes = sum(item.size_bytes for item in artifacts)
    started = time.perf_counter()
    counts = {"uploaded": 0, "unchanged": 0, "failed": 0}
    transferred_bytes = 0
    args.manifest.unlink(missing_ok=True)
    client = None if args.dry_run else ObjectStoreConfig.from_env().create_client()
    if client is not None:
        ensure_buckets(client, sorted({item.bucket for item in artifacts}))
    for index, artifact in enumerate(artifacts, 1):
        record: dict[str, Any] = {
            "timestamp": _now(),
            "index": index,
            "total": len(artifacts),
            "local_path": str(artifact.local_path.relative_to(args.data_root.resolve())),
            "bucket": artifact.bucket,
            "key": artifact.key,
            "size_bytes": artifact.size_bytes,
        }
        try:
            if args.dry_run:
                record["status"] = "planned"
            else:
                result = upload_file_verified(
                    client,
                    local_path=artifact.local_path,
                    bucket=artifact.bucket,
                    key=artifact.key,
                    metadata={"pipeline-path": record["local_path"]},
                )
                record.update(result.to_dict())
                counts[result.status] += 1
                transferred_bytes += result.size_bytes if result.status == "uploaded" else 0
        except Exception as error:
            counts["failed"] += 1
            record.update(status="failed", error_type=type(error).__name__)
            _append(args.manifest, record)
            raise
        _append(args.manifest, record)
        if index == 1 or index == len(artifacts) or index % args.progress_every == 0:
            print(
                json.dumps(
                    {
                        "index": index,
                        "total": len(artifacts),
                        "status": record["status"],
                        "bucket": artifact.bucket,
                        "key": artifact.key,
                        "size_bytes": artifact.size_bytes,
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
    report = {
        "status": "planned" if args.dry_run else "completed",
        "generated_at": _now(),
        "artifact_count": len(artifacts),
        "expected_bytes": expected_bytes,
        "transferred_bytes": transferred_bytes,
        **counts,
        "duration_seconds": round(time.perf_counter() - started, 3),
        "excluded_patterns": [
            "*.part",
            "*.tmp",
            "*.prefix",
            "*.footer",
            "*.sparse.parquet",
            "*.crc",
            "minio-data-migration*.json",
            "minio-data-migration*.jsonl",
        ],
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    if client is not None:
        publish_artifact(args.manifest, data_root=args.data_root, client=client)
        publish_artifact(args.report, data_root=args.data_root, client=client)
    print(json.dumps(report, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
