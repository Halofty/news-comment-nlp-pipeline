from __future__ import annotations

import logging
from typing import Protocol

from observability.models import (
    BatchObservation,
    GenerationObservation,
    ReconciliationResult,
    StageObservation,
)


class ObservabilitySink(Protocol):
    def record_batch(self, observation: BatchObservation) -> None: ...

    def record_stage(self, observation: StageObservation) -> None: ...

    def record_generation(self, observation: GenerationObservation) -> None: ...

    def record_reconciliation(self, result: ReconciliationResult) -> None: ...

    def flush(self) -> None: ...


class FailSafeObservabilitySink:
    def __init__(
        self,
        primary: ObservabilitySink,
        fallback: ObservabilitySink,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._primary = primary
        self._fallback = fallback
        self._logger = logger or logging.getLogger(__name__)

    def _call(self, method: str, value: object | None = None) -> None:
        try:
            target = getattr(self._primary, method)
            target() if value is None else target(value)
        except Exception as error:  # observability must not break the batch workflow
            self._logger.warning(
                "observability_primary_failed method=%s error_type=%s",
                method,
                type(error).__name__,
            )
            target = getattr(self._fallback, method)
            target() if value is None else target(value)

    def record_batch(self, observation: BatchObservation) -> None:
        self._call("record_batch", observation)

    def record_stage(self, observation: StageObservation) -> None:
        self._call("record_stage", observation)

    def record_generation(self, observation: GenerationObservation) -> None:
        self._call("record_generation", observation)

    def record_reconciliation(self, result: ReconciliationResult) -> None:
        self._call("record_reconciliation", result)

    def flush(self) -> None:
        self._call("flush")

