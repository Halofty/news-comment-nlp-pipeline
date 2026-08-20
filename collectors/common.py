from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def stable_event_id(source_name: str, source_id: str) -> str:
    value = f"{source_name}:{source_id}".encode("utf-8")
    return hashlib.sha256(value).hexdigest()


def write_jsonl(events: Iterable[dict[str, Any]], output: Path) -> int:
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    count = 0
    try:
        with temporary.open("w", encoding="utf-8") as file:
            for event in events:
                file.write(json.dumps(event, ensure_ascii=False) + "\n")
                count += 1
        temporary.replace(output)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return count
