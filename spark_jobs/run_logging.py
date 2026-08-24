from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class JsonlRunLogger:
    """Append-only operational log that intentionally excludes event payload data."""

    def __init__(self, path: Path, *, run_id: str | None = None) -> None:
        self.path = path
        self.run_id = run_id or str(uuid.uuid4())
        self.sequence = 0
        self.started = time.perf_counter()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    def emit(self, event: str, **fields: Any) -> dict[str, Any]:
        self.sequence += 1
        record = {
            "timestamp": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "run_id": self.run_id,
            "sequence": self.sequence,
            "event": event,
            "elapsed_seconds": round(time.perf_counter() - self.started, 3),
            **fields,
        }
        with self.path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        return record
