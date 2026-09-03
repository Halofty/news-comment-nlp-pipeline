from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

from core.events import validate_event


def read_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    from storage.data_lake import materialize_artifact_if_enabled

    materialize_artifact_if_enabled(path)
    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"invalid JSON on line {line_number}: {error.msg}"
                ) from error
            yield validate_event(event, line_number=line_number)


def write_jsonl(events: Iterable[dict[str, Any]], output: Path) -> int:
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    count = 0
    try:
        with temporary.open("w", encoding="utf-8") as file:
            for event in events:
                validate_event(event)
                file.write(json.dumps(event, ensure_ascii=False) + "\n")
                count += 1
        temporary.replace(output)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    from storage.data_lake import publish_artifact_if_enabled

    publish_artifact_if_enabled(output)
    return count
