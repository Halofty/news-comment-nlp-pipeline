from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Iterable


def _require_text(name: str, value: str) -> None:
    if not value.strip():
        raise ValueError(f"{name} must not be empty")


def _require_non_negative(name: str, value: int) -> None:
    if value < 0:
        raise ValueError(f"{name} must not be negative")


def isoformat_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class TokenUsage:
    input_tokens: int
    output_tokens: int
    total_tokens: int
    cached_input_tokens: int = 0
    reasoning_output_tokens: int = 0

    def __post_init__(self) -> None:
        for name in (
            "input_tokens",
            "output_tokens",
            "total_tokens",
            "cached_input_tokens",
            "reasoning_output_tokens",
        ):
            _require_non_negative(name, getattr(self, name))
        if self.cached_input_tokens > self.input_tokens:
            raise ValueError("cached_input_tokens must not exceed input_tokens")
        if self.reasoning_output_tokens > self.output_tokens:
            raise ValueError("reasoning_output_tokens must not exceed output_tokens")
        if self.total_tokens != self.input_tokens + self.output_tokens:
            raise ValueError("total_tokens must equal input_tokens + output_tokens")

    def as_usage_details(self) -> dict[str, int]:
        details = {
            "input": self.input_tokens,
            "output": self.output_tokens,
            "total": self.total_tokens,
        }
        if self.cached_input_tokens:
            details["cache_read_input_tokens"] = self.cached_input_tokens
        if self.reasoning_output_tokens:
            details["reasoning_output_tokens"] = self.reasoning_output_tokens
        return details


@dataclass(frozen=True)
class PriceSchedule:
    version: str
    effective_date: str
    input_per_million: Decimal
    cached_input_per_million: Decimal
    output_per_million: Decimal
    currency: str = "USD"

    def __post_init__(self) -> None:
        _require_text("version", self.version)
        _require_text("effective_date", self.effective_date)
        if self.currency != "USD":
            raise ValueError("only USD pricing is supported")
        for name in (
            "input_per_million",
            "cached_input_per_million",
            "output_per_million",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must not be negative")


@dataclass(frozen=True)
class CostDetails:
    input_cost: Decimal
    cached_input_cost: Decimal
    output_cost: Decimal
    total_cost: Decimal
    pricing_version: str
    pricing_effective_date: str
    currency: str = "USD"

    def as_cost_details(self) -> dict[str, float]:
        details = {
            "input": float(self.input_cost),
            "output": float(self.output_cost),
            "total": float(self.total_cost),
        }
        if self.cached_input_cost:
            details["cache_read_input_tokens"] = float(self.cached_input_cost)
        return details


def calculate_cost(usage: TokenUsage, pricing: PriceSchedule) -> CostDetails:
    million = Decimal(1_000_000)
    uncached_input = usage.input_tokens - usage.cached_input_tokens
    input_cost = Decimal(uncached_input) * pricing.input_per_million / million
    cached_input_cost = (
        Decimal(usage.cached_input_tokens)
        * pricing.cached_input_per_million
        / million
    )
    output_cost = Decimal(usage.output_tokens) * pricing.output_per_million / million
    return CostDetails(
        input_cost=input_cost,
        cached_input_cost=cached_input_cost,
        output_cost=output_cost,
        total_cost=input_cost + cached_input_cost + output_cost,
        pricing_version=pricing.version,
        pricing_effective_date=pricing.effective_date,
        currency=pricing.currency,
    )


@dataclass(frozen=True)
class BatchObservation:
    batch_id: str
    openai_batch_id: str
    model: str
    document_count: int
    status: str
    submitted_at: datetime
    completed_at: datetime | None = None

    def __post_init__(self) -> None:
        for name in ("batch_id", "openai_batch_id", "model", "status"):
            _require_text(name, getattr(self, name))
        _require_non_negative("document_count", self.document_count)
        isoformat_utc(self.submitted_at)
        isoformat_utc(self.completed_at)

    def metadata(self) -> dict[str, object]:
        return {
            "llm_batch_id": self.batch_id,
            "openai_batch_id": self.openai_batch_id,
            "model": self.model,
            "document_count": self.document_count,
            "status": self.status,
            "submitted_at": isoformat_utc(self.submitted_at),
            "completed_at": isoformat_utc(self.completed_at),
        }


@dataclass(frozen=True)
class StageObservation:
    batch_id: str
    stage: str
    status: str
    started_at: datetime
    completed_at: datetime | None = None
    error_code: str | None = None

    def __post_init__(self) -> None:
        for name in ("batch_id", "stage", "status"):
            _require_text(name, getattr(self, name))
        if self.error_code is not None:
            _require_text("error_code", self.error_code)
        isoformat_utc(self.started_at)
        isoformat_utc(self.completed_at)

    def metadata(self) -> dict[str, object]:
        return {
            "llm_batch_id": self.batch_id,
            "stage": self.stage,
            "status": self.status,
            "error_code": self.error_code,
            "started_at": isoformat_utc(self.started_at),
            "completed_at": isoformat_utc(self.completed_at),
        }


@dataclass(frozen=True)
class GenerationObservation:
    batch_id: str
    event_id: str
    custom_id: str
    prompt_version: str
    schema_version: int
    model: str
    attempt: int
    status: str
    validation_result: str
    usage: TokenUsage | None
    cost: CostDetails | None
    submitted_at: datetime
    completed_at: datetime | None = None
    error_code: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "batch_id",
            "event_id",
            "custom_id",
            "prompt_version",
            "model",
            "status",
            "validation_result",
        ):
            _require_text(name, getattr(self, name))
        if self.schema_version < 1:
            raise ValueError("schema_version must be positive")
        if self.attempt < 1:
            raise ValueError("attempt must be positive")
        if (self.usage is None) != (self.cost is None):
            raise ValueError("usage and cost must both be present or both be absent")
        if self.error_code is not None:
            _require_text("error_code", self.error_code)
        isoformat_utc(self.submitted_at)
        isoformat_utc(self.completed_at)

    def metadata(self) -> dict[str, object]:
        values: dict[str, object] = {
            "llm_batch_id": self.batch_id,
            "event_id": self.event_id,
            "custom_id": self.custom_id,
            "prompt_version": self.prompt_version,
            "schema_version": self.schema_version,
            "model": self.model,
            "attempt": self.attempt,
            "status": self.status,
            "validation_result": self.validation_result,
            "usage_status": "available" if self.usage else "unavailable",
            "error_code": self.error_code,
            "submitted_at": isoformat_utc(self.submitted_at),
            "completed_at": isoformat_utc(self.completed_at),
        }
        if self.cost:
            values.update(
                {
                    "pricing_version": self.cost.pricing_version,
                    "pricing_effective_date": self.cost.pricing_effective_date,
                    "currency": self.cost.currency,
                }
            )
        return values


@dataclass(frozen=True)
class ReconciliationResult:
    batch_id: str
    status: str
    generation_count: int
    expected_completed_count: int
    generation_usage: TokenUsage | None
    batch_usage: TokenUsage | None

    def metadata(self) -> dict[str, object]:
        values: dict[str, object] = {
            "llm_batch_id": self.batch_id,
            "reconciliation_status": self.status,
            "generation_count": self.generation_count,
            "expected_completed_count": self.expected_completed_count,
        }
        for prefix, usage in (
            ("generation", self.generation_usage),
            ("batch", self.batch_usage),
        ):
            if usage:
                values.update(
                    {
                        f"{prefix}_input_tokens": usage.input_tokens,
                        f"{prefix}_output_tokens": usage.output_tokens,
                        f"{prefix}_total_tokens": usage.total_tokens,
                        f"{prefix}_cached_input_tokens": usage.cached_input_tokens,
                        f"{prefix}_reasoning_output_tokens": usage.reasoning_output_tokens,
                    }
                )
        return values


def sum_usage(values: Iterable[TokenUsage]) -> TokenUsage:
    rows = list(values)
    return TokenUsage(
        input_tokens=sum(row.input_tokens for row in rows),
        output_tokens=sum(row.output_tokens for row in rows),
        total_tokens=sum(row.total_tokens for row in rows),
        cached_input_tokens=sum(row.cached_input_tokens for row in rows),
        reasoning_output_tokens=sum(row.reasoning_output_tokens for row in rows),
    )


def reconcile_usage(
    *,
    batch_id: str,
    generations: Iterable[GenerationObservation],
    batch_usage: TokenUsage | None,
    expected_completed_count: int,
) -> ReconciliationResult:
    records = list(generations)
    available = [record.usage for record in records if record.usage is not None]
    generation_usage = sum_usage(available) if available else None
    if batch_usage is None or generation_usage is None:
        status = "not_available"
    elif len(available) != expected_completed_count:
        status = "mismatched"
    elif generation_usage == batch_usage:
        status = "matched"
    else:
        status = "mismatched"
    return ReconciliationResult(
        batch_id=batch_id,
        status=status,
        generation_count=len(records),
        expected_completed_count=expected_completed_count,
        generation_usage=generation_usage,
        batch_usage=batch_usage,
    )

