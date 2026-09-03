from __future__ import annotations

import json
import hashlib
import os
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Iterator

import requests
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from llm_analysis.contract import (
    ANALYSIS_SCHEMA,
    PROMPT_VERSION,
    RESULT_SCHEMA_VERSION,
    SYSTEM_INSTRUCTIONS,
)

DEFAULT_MODEL = "gpt-5.6-luna"
BATCH_INPUT_PRICE_PER_MILLION = Decimal("0.10")
BATCH_OUTPUT_PRICE_PER_MILLION = Decimal("0.60")


@dataclass(frozen=True)
class BatchBuildResult:
    input_rows: int
    request_rows: int
    skipped_rows: int
    estimated_input_tokens: int
    maximum_output_tokens: int
    estimated_max_cost_usd: Decimal
    budget_status: str
    request_path: Path
    manifest_path: Path
    report_path: Path

    def as_dict(self) -> dict[str, object]:
        return {
            "input_rows": self.input_rows,
            "request_rows": self.request_rows,
            "skipped_rows": self.skipped_rows,
            "estimated_input_tokens": self.estimated_input_tokens,
            "maximum_output_tokens": self.maximum_output_tokens,
            "estimated_max_cost_usd": str(self.estimated_max_cost_usd),
            "budget_status": self.budget_status,
            "request_path": str(self.request_path),
            "manifest_path": str(self.manifest_path),
            "report_path": str(self.report_path),
        }


def _iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError(f"line {line_number} is not a JSON object")
            yield value


def _iter_parquet(path: Path) -> Iterator[dict[str, Any]]:
    try:
        import pyarrow.dataset as ds
    except ImportError as error:
        raise RuntimeError("Parquet input requires pyarrow") from error
    dataset = ds.dataset(path, format="parquet", partitioning="hive")
    available = set(dataset.schema.names)
    required = {"event_id", "source_name"}
    if not required.issubset(available) or not ({"text", "title"} & available):
        raise ValueError("Parquet input must include event_id, source_name, and text/title")
    columns = [
        name
        for name in ("event_id", "source_name", "source_type", "title", "text")
        if name in available
    ]
    for batch in dataset.scanner(columns=columns).to_batches():
        yield from batch.to_pylist()


def iter_events(path: str | Path) -> Iterator[dict[str, Any]]:
    source = Path(path)
    if source.is_dir() or source.suffix == ".parquet":
        yield from _iter_parquet(source)
    else:
        yield from _iter_jsonl(source)


def _estimate_tokens(text: str) -> int:
    # Conservative preflight estimate only; API usage remains the billing source.
    return max(1, (len(text.encode("utf-8")) + 3) // 4)


def _budget_status(cost: Decimal, budget: Decimal | None) -> str:
    if budget is None:
        return "not_configured"
    if budget <= 0:
        return "blocked"
    ratio = cost / budget
    if ratio >= 1:
        return "blocked"
    if ratio >= Decimal("0.90"):
        return "critical"
    if ratio >= Decimal("0.70"):
        return "warning"
    return "ok"


def _request_body(*, model: str, text: str, max_output_tokens: int) -> dict[str, Any]:
    return {
        "model": model,
        "reasoning": {"effort": "low"},
        "max_output_tokens": max_output_tokens,
        "instructions": SYSTEM_INSTRUCTIONS,
        "input": text,
        "text": {
            "verbosity": "low",
            "format": {
                "type": "json_schema",
                "name": "news_comment_analysis",
                "strict": True,
                "schema": ANALYSIS_SCHEMA,
            },
        },
    }


def build_batch_file(
    *,
    input_path: str | Path,
    request_path: str | Path,
    manifest_path: str | Path,
    report_path: str | Path,
    model: str = DEFAULT_MODEL,
    limit: int = 100,
    max_text_bytes: int = 16_000,
    max_output_tokens: int = 300,
    daily_budget_usd: Decimal | None = None,
) -> BatchBuildResult:
    if limit < 1 or max_text_bytes < 1 or max_output_tokens < 1:
        raise ValueError("limit and token/byte limits must be positive")
    requests_out: list[dict[str, Any]] = []
    manifest_out: list[dict[str, Any]] = []
    seen: set[str] = set()
    input_rows = skipped_rows = estimated_input_tokens = 0
    for event in iter_events(input_path):
        input_rows += 1
        event_id = str(event.get("event_id") or "").strip()
        text = str(event.get("text") or event.get("title") or "").strip()
        encoded = text.encode("utf-8")
        if not event_id or not text or len(encoded) > max_text_bytes or event_id in seen:
            skipped_rows += 1
            continue
        seen.add(event_id)
        request_key = hashlib.sha256(
            f"{event_id}:{PROMPT_VERSION}:1".encode("utf-8")
        ).hexdigest()
        custom_id = f"evt-{request_key}-p1-a1"
        body = _request_body(
            model=model, text=text, max_output_tokens=max_output_tokens
        )
        requests_out.append(
            {
                "custom_id": custom_id,
                "method": "POST",
                "url": "/v1/responses",
                "body": body,
            }
        )
        tokens = _estimate_tokens(json.dumps(body, ensure_ascii=False))
        estimated_input_tokens += tokens
        manifest_out.append(
            {
                "custom_id": custom_id,
                "event_id": event_id,
                "source_name": str(event.get("source_name") or "unknown"),
                "prompt_version": PROMPT_VERSION,
                "schema_version": RESULT_SCHEMA_VERSION,
                "model": model,
                "attempt": 1,
                "estimated_input_tokens": tokens,
            }
        )
        if len(requests_out) >= limit:
            break
    max_outputs = len(requests_out) * max_output_tokens
    estimated_cost = (
        Decimal(estimated_input_tokens) * BATCH_INPUT_PRICE_PER_MILLION
        + Decimal(max_outputs) * BATCH_OUTPUT_PRICE_PER_MILLION
    ) / Decimal(1_000_000)
    status = _budget_status(estimated_cost, daily_budget_usd)
    result = BatchBuildResult(
        input_rows=input_rows,
        request_rows=len(requests_out),
        skipped_rows=skipped_rows,
        estimated_input_tokens=estimated_input_tokens,
        maximum_output_tokens=max_outputs,
        estimated_max_cost_usd=estimated_cost,
        budget_status=status,
        request_path=Path(request_path),
        manifest_path=Path(manifest_path),
        report_path=Path(report_path),
    )
    for path in (result.request_path, result.manifest_path, result.report_path):
        path.parent.mkdir(parents=True, exist_ok=True)
    with result.request_path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in requests_out:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    with result.manifest_path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in manifest_out:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    result.report_path.write_text(
        json.dumps(result.as_dict(), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    from storage.data_lake import publish_artifact_if_enabled

    for artifact_path in (result.request_path, result.manifest_path, result.report_path):
        publish_artifact_if_enabled(artifact_path)
    return result


class OpenAIBatchClient:
    def __init__(self, api_key: str | None = None, *, timeout: int = 60) -> None:
        key = (api_key or os.environ.get("OPENAI_API_KEY", "")).strip()
        if not key:
            raise ValueError("OPENAI_API_KEY is required")
        self._headers = {"Authorization": f"Bearer {key}"}
        self._timeout = timeout
        self._base_url = "https://api.openai.com/v1"

    def upload(self, path: str | Path) -> dict[str, Any]:
        with Path(path).open("rb") as handle:
            response = requests.post(
                f"{self._base_url}/files",
                headers=self._headers,
                data={"purpose": "batch"},
                files={"file": (Path(path).name, handle, "application/jsonl")},
                timeout=self._timeout,
            )
        response.raise_for_status()
        return response.json()

    def submit(self, input_file_id: str, *, metadata: dict[str, str]) -> dict[str, Any]:
        response = requests.post(
            f"{self._base_url}/batches",
            headers={**self._headers, "Content-Type": "application/json"},
            json={
                "input_file_id": input_file_id,
                "endpoint": "/v1/responses",
                "completion_window": "24h",
                "metadata": metadata,
            },
            timeout=self._timeout,
        )
        response.raise_for_status()
        return response.json()

    def retrieve(self, batch_id: str) -> dict[str, Any]:
        response = requests.get(
            f"{self._base_url}/batches/{batch_id}",
            headers=self._headers,
            timeout=self._timeout,
        )
        response.raise_for_status()
        return response.json()

    def download(self, file_id: str, output: str | Path) -> Path:
        response = requests.get(
            f"{self._base_url}/files/{file_id}/content",
            headers=self._headers,
            timeout=self._timeout,
        )
        response.raise_for_status()
        target = Path(output)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(response.content)
        from storage.data_lake import publish_artifact_if_enabled

        publish_artifact_if_enabled(target)
        return target


def _response_output_text(body: dict[str, Any]) -> str:
    if isinstance(body.get("output_text"), str):
        return body["output_text"]
    for item in body.get("output") or []:
        if item.get("type") != "message":
            continue
        for content in item.get("content") or []:
            if content.get("type") == "output_text" and isinstance(content.get("text"), str):
                return content["text"]
    raise ValueError("Responses API body has no output_text")


def validate_batch_results(
    *, result_path: str | Path, manifest_path: str | Path, output_path: str | Path
) -> dict[str, int]:
    manifest = {row["custom_id"]: row for row in _iter_jsonl(Path(manifest_path))}
    validator = Draft202012Validator(ANALYSIS_SCHEMA)
    output: list[dict[str, Any]] = []
    failed = 0
    seen: set[str] = set()
    for row in _iter_jsonl(Path(result_path)):
        custom_id = str(row.get("custom_id") or "")
        if custom_id in seen or custom_id not in manifest:
            failed += 1
            continue
        seen.add(custom_id)
        response = row.get("response")
        if not response or int(response.get("status_code", 0)) >= 400:
            failed += 1
            continue
        try:
            analysis = json.loads(_response_output_text(response["body"]))
            validator.validate(analysis)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError, ValidationError):
            failed += 1
            continue
        output.append(
            {
                "event_id": manifest[custom_id]["event_id"],
                "custom_id": custom_id,
                "model": response["body"].get("model", manifest[custom_id]["model"]),
                "prompt_version": manifest[custom_id]["prompt_version"],
                "schema_version": manifest[custom_id]["schema_version"],
                **analysis,
                "usage": response["body"].get("usage"),
            }
        )
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8", newline="\n") as handle:
        for row in output:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    from storage.data_lake import publish_artifact_if_enabled

    publish_artifact_if_enabled(target)
    return {
        "manifest_rows": len(manifest),
        "result_rows": len(seen),
        "validated_rows": len(output),
        "failed_rows": failed,
        "missing_rows": len(set(manifest) - seen),
    }
