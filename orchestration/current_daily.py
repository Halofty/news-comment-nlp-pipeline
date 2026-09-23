from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from core.events import parse_event_time, validate_event
from storage.collection_cursor import FileCollectionCursorStore
from storage.data_lake import publish_artifact_if_enabled
from storage.jsonl import write_jsonl

KST = ZoneInfo("Asia/Seoul")
CURRENT_SOURCES = ("google_news", "reddit")


def resolve_analysis_dates(
    *,
    data_interval_end: datetime,
    scheduled: bool,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[str]:
    """Resolve deterministic KST dates for scheduled runs and manual backfills."""
    if data_interval_end.tzinfo is None or data_interval_end.utcoffset() is None:
        raise ValueError("data_interval_end must include a timezone")
    if scheduled:
        return [(data_interval_end.astimezone(KST).date() - timedelta(days=1)).isoformat()]
    if bool(start_date) != bool(end_date):
        raise ValueError("start_date and end_date must be provided together")
    if not start_date:
        return [(data_interval_end.astimezone(KST).date() - timedelta(days=1)).isoformat()]
    try:
        first = date.fromisoformat(str(start_date))
        last = date.fromisoformat(str(end_date))
    except ValueError as error:
        raise ValueError("start_date and end_date must use YYYY-MM-DD") from error
    if last < first:
        raise ValueError("end_date must be on or after start_date")
    return [(first + timedelta(days=offset)).isoformat() for offset in range((last - first).days + 1)]


def _day_bounds(analysis_date: date) -> tuple[datetime, datetime]:
    start = datetime.combine(analysis_date, time.min, tzinfo=KST)
    return start, start + timedelta(days=1)


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_root(project_root: Path, source: str) -> Path:
    if source == "google_news":
        return project_root / "data/raw/current/google-news"
    if source == "reddit":
        return project_root / "data/raw/current/community/provider=reddit"
    raise ValueError(f"unsupported current source: {source}")


def _source_manifests(project_root: Path, source: str) -> list[Path]:
    return sorted(_source_root(project_root, source).glob("collected_date=*/window_start=*/manifest.json"))


def _manifest_overlaps_day(manifest: Mapping[str, Any], start: datetime, end: datetime) -> bool:
    window_start = datetime.fromisoformat(str(manifest["window_start"]))
    window_end = datetime.fromisoformat(str(manifest["window_end"]))
    if window_start.tzinfo is None or window_end.tzinfo is None:
        raise ValueError("manifest window must include a timezone")
    return window_start < end and window_end > start


def verify_daily_completeness(*, project_root: Path, analysis_date: date) -> dict[str, Any]:
    """Fail closed unless both source cursors and stored windows cover the KST day."""
    start, end = _day_bounds(analysis_date)
    cursor_store = FileCollectionCursorStore(project_root / "data/raw/current/_state")
    sources: dict[str, Any] = {}
    for source in CURRENT_SOURCES:
        cursor = cursor_store.load(source)
        if cursor is None:
            raise RuntimeError(f"{source} cursor is missing")
        cursor_end = datetime.fromisoformat(str(cursor["window_end"]).replace("Z", "+00:00"))
        if cursor_end < end:
            raise RuntimeError(f"{source} cursor has not reached {end.isoformat()}")
        selected: list[dict[str, Any]] = []
        coverage: list[tuple[datetime, datetime]] = []
        for path in _source_manifests(project_root, source):
            manifest = _read_json(path)
            if not _manifest_overlaps_day(manifest, start, end):
                continue
            if manifest.get("status") != "complete":
                raise RuntimeError(f"incomplete {source} collection window: {path}")
            events_path = path.with_name("events.jsonl")
            if not events_path.is_file():
                raise FileNotFoundError(events_path)
            window_start = datetime.fromisoformat(str(manifest["window_start"]))
            window_end = datetime.fromisoformat(str(manifest["window_end"]))
            coverage.append((max(window_start, start), min(window_end, end)))
            selected.append(
                {
                    "manifest_path": str(path.relative_to(project_root)),
                    "events_path": str(events_path.relative_to(project_root)),
                    "events_sha256": _sha256(events_path),
                    "normalized": int(manifest.get("normalized", 0)),
                }
            )
        if not selected:
            raise RuntimeError(f"no complete {source} windows overlap {analysis_date}")
        covered_until = start
        for window_start, window_end in sorted(coverage):
            if window_start > covered_until:
                raise RuntimeError(
                    f"{source} collection gap from {covered_until.isoformat()} "
                    f"to {window_start.isoformat()}"
                )
            if window_end > covered_until:
                covered_until = window_end
        if covered_until < end:
            raise RuntimeError(
                f"{source} collection gap from {covered_until.isoformat()} "
                f"to {end.isoformat()}"
            )
        sources[source] = {"cursor": cursor, "windows": selected}
    return {
        "analysis_date": analysis_date.isoformat(),
        "day_start": start.isoformat(),
        "day_end": end.isoformat(),
        "sources": sources,
    }


def _iter_daily_events(
    *, project_root: Path, gate: Mapping[str, Any], start: datetime, end: datetime
) -> Iterable[dict[str, Any]]:
    for source in CURRENT_SOURCES:
        for window in gate["sources"][source]["windows"]:
            path = project_root / str(window["events_path"])
            for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if not line.strip():
                    continue
                event = validate_event(json.loads(line), line_number=line_number)
                timestamp = parse_event_time(event["event_time"]).astimezone(KST)
                if start <= timestamp < end:
                    yield event


def finalize_current_day(*, project_root: Path, analysis_date: date) -> dict[str, Any]:
    gate = verify_daily_completeness(project_root=project_root, analysis_date=analysis_date)
    start, end = _day_bounds(analysis_date)
    unique: dict[str, dict[str, Any]] = {}
    source_rows = {"google_news": 0, "reddit": 0}
    input_rows = 0
    for event in _iter_daily_events(project_root=project_root, gate=gate, start=start, end=end):
        input_rows += 1
        source_rows["google_news" if event["source_name"] == "web_news" else "reddit"] += 1
        unique.setdefault(str(event["event_id"]), event)

    output_dir = project_root / "data/finalized/current" / f"date={analysis_date.isoformat()}"
    events_path = output_dir / "events.jsonl"
    # The finalized event file and manifest are uploaded together below.
    write_jsonl(unique.values(), events_path, publish=False)
    manifest = {
        **gate,
        "status": "finalized",
        "input_rows": input_rows,
        "stored_rows": len(unique),
        "duplicate_rows": input_rows - len(unique),
        "source_rows": source_rows,
        "events_path": str(events_path.relative_to(project_root)),
        "events_sha256": _sha256(events_path),
        "finalized_at": datetime.now(tz=KST).isoformat(),
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = manifest_path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(manifest_path)
    publications = []
    for path in (events_path, manifest_path):
        result = publish_artifact_if_enabled(path)
        if result is not None:
            publications.append(result.to_dict())
    return {**manifest, "manifest_path": str(manifest_path.relative_to(project_root)), "published": publications}
