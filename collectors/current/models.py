from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")


def _require_aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone")
    return value


@dataclass(frozen=True)
class CollectionWindow:
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        _require_aware(self.start, field_name="start")
        _require_aware(self.end, field_name="end")
        if self.end <= self.start:
            raise ValueError("collection window end must be after start")

    @property
    def start_utc(self) -> datetime:
        return self.start.astimezone(timezone.utc)

    @property
    def end_utc(self) -> datetime:
        return self.end.astimezone(timezone.utc)

    @property
    def window_id(self) -> str:
        return self.start.astimezone(KST).strftime("%Y-%m-%dT%H%M%S%z")

    @property
    def collected_date(self) -> str:
        return self.start.astimezone(KST).date().isoformat()

    def contains(self, value: datetime) -> bool:
        moment = _require_aware(value, field_name="value").astimezone(timezone.utc)
        return self.start_utc <= moment < self.end_utc

    def to_dict(self) -> dict[str, str]:
        return {
            "window_start": self.start.isoformat(),
            "window_end": self.end.isoformat(),
        }


@dataclass(frozen=True)
class CollectionManifest:
    source: str
    window: CollectionWindow
    requested: int
    received: int
    normalized: int
    duplicate_in_window: int
    rejected: int
    status: str = "complete"
    cursor_before: str | None = None
    cursor_after: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.source.strip():
            raise ValueError("manifest source must not be empty")
        if self.status not in {"complete", "partial", "failed"}:
            raise ValueError("manifest status must be complete, partial, or failed")
        counts = (
            self.requested,
            self.received,
            self.normalized,
            self.duplicate_in_window,
            self.rejected,
        )
        if any(type(value) is not int or value < 0 for value in counts):
            raise ValueError("manifest counts must be non-negative integers")
        if self.normalized + self.duplicate_in_window + self.rejected > self.received:
            raise ValueError("manifest row accounting exceeds received rows")

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result.pop("window")
        result.update(self.window.to_dict())
        return result

