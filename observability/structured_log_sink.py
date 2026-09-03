from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from observability.models import (
    BatchObservation,
    GenerationObservation,
    ReconciliationResult,
    StageObservation,
)


class StructuredLogSink:
    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    def _write(self, event: str, values: Mapping[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        record = {"event": event, **values}
        with self._path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
            handle.write("\n")

    def record_batch(self, observation: BatchObservation) -> None:
        self._write("llm_batch_trace", observation.metadata())

    def record_stage(self, observation: StageObservation) -> None:
        self._write("llm_batch_stage", observation.metadata())

    def record_generation(self, observation: GenerationObservation) -> None:
        values = observation.metadata()
        if observation.usage:
            values.update(
                {
                    "input_tokens": observation.usage.input_tokens,
                    "output_tokens": observation.usage.output_tokens,
                    "total_tokens": observation.usage.total_tokens,
                    "cached_input_tokens": observation.usage.cached_input_tokens,
                    "reasoning_output_tokens": observation.usage.reasoning_output_tokens,
                }
            )
        if observation.cost:
            values.update(
                {
                    "input_cost_usd": str(observation.cost.input_cost),
                    "cached_input_cost_usd": str(
                        observation.cost.cached_input_cost
                    ),
                    "output_cost_usd": str(observation.cost.output_cost),
                    "total_cost_usd": str(observation.cost.total_cost),
                }
            )
        self._write("llm_generation", values)

    def record_reconciliation(self, result: ReconciliationResult) -> None:
        self._write("llm_usage_reconciliation", result.metadata())

    def flush(self) -> None:
        if not self._path.is_file():
            return None
        from storage.data_lake import publish_artifact_if_enabled

        publish_artifact_if_enabled(self._path)
        return None
