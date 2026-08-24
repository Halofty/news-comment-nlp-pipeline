from __future__ import annotations

import os
from typing import Any

from observability.models import (
    BatchObservation,
    GenerationObservation,
    ReconciliationResult,
    StageObservation,
)


class LangfuseSink:
    def __init__(self, client: Any | None = None) -> None:
        if client is None:
            public_key = os.environ.get("LANGFUSE_PUBLIC_KEY", "").strip()
            secret_key = os.environ.get("LANGFUSE_SECRET_KEY", "").strip()
            if not public_key or not secret_key:
                raise ValueError(
                    "LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY are required"
                )
            try:
                from langfuse import Langfuse
            except ImportError as error:
                raise RuntimeError(
                    "Langfuse sink requires langfuse>=4,<5"
                ) from error
            client = Langfuse(
                public_key=public_key,
                secret_key=secret_key,
                base_url=os.environ.get(
                    "LANGFUSE_BASE_URL", "https://jp.cloud.langfuse.com"
                ),
                environment=os.environ.get(
                    "LANGFUSE_TRACING_ENVIRONMENT", "development"
                ),
                timeout=int(os.environ.get("LANGFUSE_TIMEOUT_SECONDS", "5")),
            )
        self._client = client

    def _trace_context(self, batch_id: str) -> dict[str, str]:
        return {
            "trace_id": self._client.create_trace_id(
                seed=f"news-pipeline-llm-batch:{batch_id}"
            )
        }

    def record_batch(self, observation: BatchObservation) -> None:
        span = self._client.start_observation(
            trace_context=self._trace_context(observation.batch_id),
            name="llm-batch",
            as_type="span",
            metadata=observation.metadata(),
        )
        span.end()

    def record_stage(self, observation: StageObservation) -> None:
        span = self._client.start_observation(
            trace_context=self._trace_context(observation.batch_id),
            name=observation.stage,
            as_type="span",
            metadata=observation.metadata(),
            level="ERROR" if observation.error_code else "DEFAULT",
            status_message=observation.error_code,
        )
        span.end()

    def record_generation(self, observation: GenerationObservation) -> None:
        generation = self._client.start_observation(
            trace_context=self._trace_context(observation.batch_id),
            name="llm-document-analysis",
            as_type="generation",
            metadata=observation.metadata(),
            version=observation.prompt_version,
            model=observation.model,
            usage_details=(
                observation.usage.as_usage_details() if observation.usage else None
            ),
            cost_details=(
                observation.cost.as_cost_details() if observation.cost else None
            ),
            level="ERROR" if observation.error_code else "DEFAULT",
            status_message=observation.error_code,
        )
        generation.end()

    def record_reconciliation(self, result: ReconciliationResult) -> None:
        span = self._client.start_observation(
            trace_context=self._trace_context(result.batch_id),
            name="reconcile-openai-batch-usage",
            as_type="span",
            metadata=result.metadata(),
            level="WARNING" if result.status == "mismatched" else "DEFAULT",
            status_message=(
                "TOKEN_USAGE_MISMATCH" if result.status == "mismatched" else None
            ),
        )
        span.end()

    def flush(self) -> None:
        self._client.flush()
