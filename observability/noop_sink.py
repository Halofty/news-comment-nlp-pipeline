from __future__ import annotations

from observability.models import (
    BatchObservation,
    GenerationObservation,
    ReconciliationResult,
    StageObservation,
)


class NoOpSink:
    def record_batch(self, observation: BatchObservation) -> None:
        return None

    def record_stage(self, observation: StageObservation) -> None:
        return None

    def record_generation(self, observation: GenerationObservation) -> None:
        return None

    def record_reconciliation(self, result: ReconciliationResult) -> None:
        return None

    def flush(self) -> None:
        return None

