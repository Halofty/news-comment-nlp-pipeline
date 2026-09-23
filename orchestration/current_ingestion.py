from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from collectors.current.google_news import collect_google_news_window
from collectors.current.models import CollectionManifest, CollectionWindow
from collectors.current.reddit import RedditCredentials, collect_reddit_window
from storage.collection_cursor import FileCollectionCursorStore
from storage.data_lake import publish_artifact_if_enabled
from storage.jsonl import write_jsonl


def _atomic_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_bytes(value)
        temporary.replace(path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    _atomic_bytes(
        path,
        (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
    )


def _atomic_raw_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    count = 0
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(dict(row), ensure_ascii=False) + "\n")
                count += 1
        temporary.replace(path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return count


def _parse_timestamp(value: str) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("cursor timestamp must include a timezone")
    return parsed


def _effective_window(
    requested: CollectionWindow,
    *,
    cursor: Mapping[str, Any] | None,
) -> CollectionWindow:
    if cursor is None:
        return requested
    cursor_end = _parse_timestamp(str(cursor["window_end"]))
    if cursor_end < requested.start_utc:
        return CollectionWindow(cursor_end, requested.end)
    return requested


def _window_directory(project_root: Path, source: str, window: CollectionWindow) -> Path:
    return (
        project_root
        / "data"
        / "raw"
        / "current"
        / source
        / f"collected_date={window.collected_date}"
        / f"window_start={window.window_id}"
    )


def _publish(paths: Iterable[Path]) -> list[dict[str, Any]]:
    published: list[dict[str, Any]] = []
    for path in paths:
        result = publish_artifact_if_enabled(path)
        if result is not None:
            published.append(result.to_dict())
    return published


def _finalize_source(
    *,
    source: str,
    manifest: CollectionManifest,
    artifact_paths: list[Path],
    cursor_store: FileCollectionCursorStore,
) -> dict[str, Any]:
    manifest_path = artifact_paths[-1]
    published = _publish(artifact_paths)
    if manifest.status != "complete":
        return {
            "source": source,
            "status": manifest.status,
            "manifest_path": str(manifest_path),
            "cursor_advanced": False,
            "published": published,
        }
    cursor = cursor_store.advance(
        source=source,
        window_end=manifest.window.end,
        metadata={"manifest_path": str(manifest_path)},
    )
    cursor_path = cursor_store.path_for(source)
    cursor_publication = publish_artifact_if_enabled(cursor_path)
    if cursor_publication is not None:
        published.append(cursor_publication.to_dict())
    return {
        "source": source,
        "status": manifest.status,
        "manifest_path": str(manifest_path),
        "cursor": cursor,
        "cursor_advanced": True,
        "published": published,
    }


def collect_google_news_raw(
    *,
    project_root: Path,
    requested_window: CollectionWindow,
    session: Any | None = None,
) -> dict[str, Any]:
    cursor_store = FileCollectionCursorStore(
        project_root / "data/raw/current/_state"
    )
    window = _effective_window(
        requested_window, cursor=cursor_store.load("google_news")
    )
    result = collect_google_news_window(window, session=session)
    directory = _window_directory(project_root, "google-news", window)
    raw_paths: list[Path] = []
    for index, raw in enumerate(result.raw_responses, start=1):
        path = directory / f"response-{index:02d}.xml"
        _atomic_bytes(path, raw)
        raw_paths.append(path)
    events_path = directory / "events.jsonl"
    # Publish the complete artifact set only after raw, events and manifest all exist.
    write_jsonl(result.events, events_path, publish=False)
    manifest_path = directory / "manifest.json"
    _atomic_json(manifest_path, result.manifest.to_dict())
    finalized = _finalize_source(
        source="google_news",
        manifest=result.manifest,
        artifact_paths=[*raw_paths, events_path, manifest_path],
        cursor_store=cursor_store,
    )
    finalized.update(
        {
            "events_path": str(events_path),
            "normalized": result.manifest.normalized,
            "window": window.to_dict(),
        }
    )
    return finalized


def collect_reddit_raw(
    *,
    project_root: Path,
    requested_window: CollectionWindow,
    credentials: RedditCredentials | None = None,
    session: Any | None = None,
) -> dict[str, Any]:
    cursor_store = FileCollectionCursorStore(
        project_root / "data/raw/current/_state"
    )
    window = _effective_window(requested_window, cursor=cursor_store.load("reddit"))
    result = collect_reddit_window(
        window,
        credentials=credentials or RedditCredentials.from_env(),
        session=session,
    )
    directory = _window_directory(project_root, "community/provider=reddit", window)
    raw_path = directory / "response.jsonl"
    _atomic_raw_jsonl(raw_path, result.raw_rows)
    events_path = directory / "events.jsonl"
    # Publish the complete artifact set only after raw, events and manifest all exist.
    write_jsonl(result.events, events_path, publish=False)
    manifest_path = directory / "manifest.json"
    _atomic_json(manifest_path, result.manifest.to_dict())
    finalized = _finalize_source(
        source="reddit",
        manifest=result.manifest,
        artifact_paths=[raw_path, events_path, manifest_path],
        cursor_store=cursor_store,
    )
    finalized.update(
        {
            "events_path": str(events_path),
            "normalized": result.manifest.normalized,
            "window": window.to_dict(),
        }
    )
    return finalized
