from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from observability.models import (
    BatchObservation,
    GenerationObservation,
    PriceSchedule,
    TokenUsage,
    calculate_cost,
)


def _read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    with Path(path).open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def _timestamp(value: int | float | str) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("sample timestamps must include a timezone")
    return parsed.astimezone(timezone.utc)


def token_usage_from_openai(value: dict[str, Any]) -> TokenUsage:
    input_tokens = value.get("input_tokens", value.get("prompt_tokens"))
    output_tokens = value.get("output_tokens", value.get("completion_tokens"))
    if input_tokens is None or output_tokens is None:
        raise ValueError("OpenAI usage must include input and output token counts")
    input_details = value.get("input_tokens_details") or value.get(
        "prompt_tokens_details"
    ) or {}
    output_details = value.get("output_tokens_details") or value.get(
        "completion_tokens_details"
    ) or {}
    total_tokens = value.get("total_tokens", input_tokens + output_tokens)
    return TokenUsage(
        input_tokens=int(input_tokens),
        output_tokens=int(output_tokens),
        total_tokens=int(total_tokens),
        cached_input_tokens=int(input_details.get("cached_tokens", 0)),
        reasoning_output_tokens=int(output_details.get("reasoning_tokens", 0)),
    )


@dataclass(frozen=True)
class SampleBatch:
    batch: BatchObservation
    generations: tuple[GenerationObservation, ...]
    batch_usage: TokenUsage | None
    expected_completed_count: int


def load_sample_batch(
    *,
    batch_path: str | Path,
    manifest_path: str | Path,
    results_path: str | Path,
    pricing: PriceSchedule,
) -> SampleBatch:
    batch_data = json.loads(Path(batch_path).read_text(encoding="utf-8"))
    manifest_rows = _read_jsonl(manifest_path)
    result_rows = _read_jsonl(results_path)
    manifest = {row["custom_id"]: row for row in manifest_rows}
    if len(manifest) != len(manifest_rows):
        raise ValueError("manifest custom_id values must be unique")

    batch_id = str(batch_data["metadata"]["llm_batch_id"])
    model = str(batch_data["metadata"]["model"])
    generations: list[GenerationObservation] = []
    seen_custom_ids: set[str] = set()
    for result in result_rows:
        custom_id = str(result["custom_id"])
        if custom_id in seen_custom_ids:
            raise ValueError(f"duplicate result custom_id: {custom_id}")
        seen_custom_ids.add(custom_id)
        request = manifest.get(custom_id)
        if request is None:
            raise ValueError(f"result is missing from manifest: {custom_id}")

        response = result.get("response")
        error = result.get("error")
        if response is not None:
            body = response["body"]
            usage = token_usage_from_openai(body["usage"])
            cost = calculate_cost(usage, pricing)
            status = "completed"
            error_code = None
            result_model = str(body.get("model", model))
        else:
            usage = None
            cost = None
            status = "failed"
            error_code = str((error or {}).get("code", "UNKNOWN_BATCH_ERROR"))
            result_model = model

        generations.append(
            GenerationObservation(
                batch_id=batch_id,
                event_id=str(request["event_id"]),
                custom_id=custom_id,
                prompt_version=str(request["prompt_version"]),
                schema_version=int(request["schema_version"]),
                model=result_model,
                attempt=int(request["attempt"]),
                status=status,
                validation_result=str(request["validation_result"]),
                usage=usage,
                cost=cost,
                submitted_at=_timestamp(request["submitted_at"]),
                completed_at=(
                    _timestamp(batch_data["completed_at"])
                    if batch_data.get("completed_at")
                    else None
                ),
                error_code=error_code,
            )
        )

    missing_results = set(manifest) - seen_custom_ids
    if missing_results:
        raise ValueError(
            "manifest requests are missing results: " + ", ".join(sorted(missing_results))
        )

    batch_usage_data = batch_data.get("usage")
    batch_usage = (
        token_usage_from_openai(batch_usage_data) if batch_usage_data else None
    )
    request_counts = batch_data.get("request_counts") or {}
    batch = BatchObservation(
        batch_id=batch_id,
        openai_batch_id=str(batch_data["id"]),
        model=model,
        document_count=int(request_counts.get("total", len(manifest))),
        status=str(batch_data["status"]),
        submitted_at=_timestamp(batch_data["created_at"]),
        completed_at=(
            _timestamp(batch_data["completed_at"])
            if batch_data.get("completed_at")
            else None
        ),
    )
    return SampleBatch(
        batch=batch,
        generations=tuple(generations),
        batch_usage=batch_usage,
        expected_completed_count=int(request_counts.get("completed", 0)),
    )


def total_cost(generations: Iterable[GenerationObservation]) -> Decimal:
    return sum(
        (record.cost.total_cost for record in generations if record.cost),
        start=Decimal("0"),
    )
