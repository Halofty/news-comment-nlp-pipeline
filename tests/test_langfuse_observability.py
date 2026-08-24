from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from jobs.verify_langfuse import build_parser, run
from observability import (
    BatchObservation,
    FailSafeObservabilitySink,
    GenerationObservation,
    LangfuseSink,
    PriceSchedule,
    StructuredLogSink,
    TokenUsage,
    calculate_cost,
    reconcile_usage,
)
from observability.openai_batch import load_sample_batch, token_usage_from_openai


SAMPLE_BATCH = Path("sample/llm-batch-object.json")
SAMPLE_MANIFEST = Path("sample/llm-batch-request-manifest.jsonl")
SAMPLE_RESULTS = Path("sample/llm-batch-results.jsonl")


def _pricing() -> PriceSchedule:
    return PriceSchedule(
        version="sample-batch-v1",
        effective_date="2026-08-24",
        input_per_million=Decimal("0.50"),
        cached_input_per_million=Decimal("0.25"),
        output_per_million=Decimal("2.00"),
    )


def _sample():
    return load_sample_batch(
        batch_path=SAMPLE_BATCH,
        manifest_path=SAMPLE_MANIFEST,
        results_path=SAMPLE_RESULTS,
        pricing=_pricing(),
    )


def test_openai_usage_supports_responses_and_chat_token_names() -> None:
    responses_usage = token_usage_from_openai(
        {"input_tokens": 10, "output_tokens": 2, "total_tokens": 12}
    )
    chat_usage = token_usage_from_openai(
        {"prompt_tokens": 8, "completion_tokens": 3, "total_tokens": 11}
    )

    assert responses_usage == TokenUsage(10, 2, 12)
    assert chat_usage == TokenUsage(8, 3, 11)


def test_token_usage_rejects_inconsistent_totals_and_breakdowns() -> None:
    with pytest.raises(ValueError, match="total_tokens"):
        TokenUsage(input_tokens=10, output_tokens=2, total_tokens=13)
    with pytest.raises(ValueError, match="cached_input_tokens"):
        TokenUsage(
            input_tokens=10,
            output_tokens=2,
            total_tokens=12,
            cached_input_tokens=11,
        )


def test_batch_price_separates_cached_input_and_output() -> None:
    cost = calculate_cost(
        TokenUsage(
            input_tokens=120,
            output_tokens=30,
            total_tokens=150,
            cached_input_tokens=20,
        ),
        _pricing(),
    )

    assert cost.input_cost == Decimal("0.00005")
    assert cost.cached_input_cost == Decimal("0.000005")
    assert cost.output_cost == Decimal("0.00006")
    assert cost.total_cost == Decimal("0.000115")


def test_sample_generation_usage_matches_batch_usage() -> None:
    sample = _sample()
    result = reconcile_usage(
        batch_id=sample.batch.batch_id,
        generations=sample.generations,
        batch_usage=sample.batch_usage,
        expected_completed_count=sample.expected_completed_count,
    )

    assert result.status == "matched"
    assert result.generation_count == 3
    assert result.generation_usage == TokenUsage(
        input_tokens=300,
        output_tokens=60,
        total_tokens=360,
        cached_input_tokens=20,
        reasoning_output_tokens=5,
    )
    assert sum(
        generation.cost.total_cost for generation in sample.generations
    ) == Decimal("0.000265")


def test_reconciliation_reports_mismatch_without_raising() -> None:
    sample = _sample()
    result = reconcile_usage(
        batch_id=sample.batch.batch_id,
        generations=sample.generations,
        batch_usage=TokenUsage(301, 60, 361, 20, 5),
        expected_completed_count=3,
    )

    assert result.status == "mismatched"


class _FakeObservation:
    def __init__(self) -> None:
        self.ended = False

    def end(self) -> None:
        self.ended = True


class _FakeLangfuseClient:
    def __init__(self) -> None:
        self.calls: list[tuple[dict, _FakeObservation]] = []
        self.flushed = False

    def create_trace_id(self, *, seed: str) -> str:
        return f"trace:{seed}"

    def start_observation(self, **kwargs):
        observation = _FakeObservation()
        self.calls.append((kwargs, observation))
        return observation

    def flush(self) -> None:
        self.flushed = True


def test_langfuse_adapter_records_usage_without_input_or_output() -> None:
    sample = _sample()
    client = _FakeLangfuseClient()
    sink = LangfuseSink(client)

    sink.record_batch(sample.batch)
    sink.record_generation(sample.generations[1])
    sink.flush()

    assert len(client.calls) == 2
    batch_call, generation_call = (item[0] for item in client.calls)
    assert batch_call["trace_context"] == generation_call["trace_context"]
    assert generation_call["as_type"] == "generation"
    assert generation_call["usage_details"] == {
        "input": 120,
        "output": 30,
        "total": 150,
        "cache_read_input_tokens": 20,
        "reasoning_output_tokens": 5,
    }
    assert generation_call["cost_details"]["total"] == 0.000115
    for call, observation in client.calls:
        assert "input" not in call
        assert "output" not in call
        assert observation.ended
    assert client.flushed


class _FailingSink:
    def record_batch(self, _observation) -> None:
        raise RuntimeError("secret-value-must-not-be-logged")

    def record_stage(self, _observation) -> None:
        raise RuntimeError("failure")

    def record_generation(self, _observation) -> None:
        raise RuntimeError("failure")

    def record_reconciliation(self, _result) -> None:
        raise RuntimeError("failure")

    def flush(self) -> None:
        raise RuntimeError("failure")


def test_langfuse_failure_falls_back_without_exposing_error_message(
    tmp_path, caplog
) -> None:
    sample = _sample()
    output = tmp_path / "fallback.jsonl"
    sink = FailSafeObservabilitySink(_FailingSink(), StructuredLogSink(output))

    sink.record_batch(sample.batch)
    sink.flush()

    record = json.loads(output.read_text(encoding="utf-8").splitlines()[0])
    assert record["event"] == "llm_batch_trace"
    assert "secret-value-must-not-be-logged" not in caplog.text
    assert "error_type=RuntimeError" in caplog.text


def test_verification_cli_writes_metadata_only_trace(tmp_path) -> None:
    output = tmp_path / "trace.jsonl"
    args = build_parser().parse_args(["--output", str(output)])

    summary = run(args)
    records = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]

    assert summary == {
        "batch_id": "llm-sample-20260824-001",
        "generation_count": 3,
        "reconciliation_status": "matched",
        "input_tokens": 300,
        "output_tokens": 60,
        "total_tokens": 360,
        "total_cost_usd": "0.000265",
        "sink": "structured-log",
        "output": str(output),
    }
    assert len(records) == 9
    assert records[-1]["reconciliation_status"] == "matched"
    forbidden_keys = {"input", "output", "text", "title", "url", "community", "author"}
    assert all(forbidden_keys.isdisjoint(record) for record in records)
    assert sum(record["attempt"] > 1 for record in records if "attempt" in record) == 1


def test_langfuse_sink_requires_keys_without_creating_client(monkeypatch) -> None:
    monkeypatch.delenv("LANGFUSE_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("LANGFUSE_SECRET_KEY", raising=False)

    with pytest.raises(ValueError, match="LANGFUSE_PUBLIC_KEY"):
        LangfuseSink()

