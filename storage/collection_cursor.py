from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


def _parse_aware(value: str) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("cursor timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


class FileCollectionCursorStore:
    """Small atomic cursor store for one active Airflow ingestion run at a time."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def path_for(self, source: str) -> Path:
        safe = source.strip().casefold().replace("_", "-")
        if not safe or any(character not in "abcdefghijklmnopqrstuvwxyz0123456789-" for character in safe):
            raise ValueError("cursor source must contain only letters, digits, '-' or '_'")
        return self.root / f"{safe}.json"

    def load(self, source: str) -> dict[str, Any] | None:
        path = self.path_for(source)
        if not path.exists():
            return None
        value = json.loads(path.read_text(encoding="utf-8"))
        if value.get("source") != source:
            raise ValueError(f"cursor source mismatch in {path}")
        _parse_aware(str(value["window_end"]))
        return value

    def advance(
        self,
        *,
        source: str,
        window_end: datetime,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if window_end.tzinfo is None or window_end.utcoffset() is None:
            raise ValueError("window_end must include a timezone")
        target_end = window_end.astimezone(timezone.utc)
        previous = self.load(source)
        if previous is not None and target_end < _parse_aware(str(previous["window_end"])):
            raise ValueError("cursor must not move backwards")
        value = {
            "version": 1,
            "source": source,
            "window_end": target_end.isoformat().replace("+00:00", "Z"),
            "metadata": dict(metadata or {}),
        }
        path = self.path_for(source)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".json.tmp")
        try:
            temporary.write_text(
                json.dumps(value, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            temporary.replace(path)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
        return value

