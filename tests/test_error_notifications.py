from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from orchestration.error_notifications import (
    build_ingest_payload,
    notify_task_failure,
)


def test_build_ingest_payload_matches_error_alert_contract() -> None:
    payload = build_ingest_payload(
        dag_id="news_comment_end_to_end_pipeline",
        task_id="run_kafka_spark_batch",
        run_id="manual__2026-09-08T00:00:00+00:00",
        map_index=5,
        try_number=2,
        log_url="http://localhost:8082/dags/x/grid?task_id=run_kafka_spark_batch",
        error_type="OffsetOutOfRangeException",
        error_message=(
            "Offsets out of range with no configured reset policy for "
            "partitions: {raw-text-0=53}"
        ),
        occurred_at=datetime(2026, 9, 8, 3, 0, tzinfo=timezone.utc),
    )
    assert set(payload) == {"dag_id", "task_id", "error_type", "message", "occurred_at"}
    assert payload["dag_id"] == "news_comment_end_to_end_pipeline"
    assert payload["task_id"] == "run_kafka_spark_batch[5]"
    assert payload["error_type"] == "OffsetOutOfRangeException"
    assert "raw-text-0=53" in payload["message"]
    assert "run=manual__2026-09-08T00:00:00+00:00" in payload["message"]
    assert "try=2" in payload["message"]
    assert "log=http://localhost:8082" in payload["message"]
    assert payload["occurred_at"] == "2026-09-08T03:00:00+00:00"


def test_build_ingest_payload_omits_map_index_when_not_mapped() -> None:
    payload = build_ingest_payload(
        dag_id="news_comment_end_to_end_pipeline",
        task_id="prepare_kafka_topics",
        error_type="RuntimeError",
        error_message="Kafka topic metadata unavailable: raw-text",
    )
    assert payload["task_id"] == "prepare_kafka_topics"


def test_build_ingest_payload_truncates_long_errors() -> None:
    payload = build_ingest_payload(
        dag_id="dag",
        task_id="task",
        error_type="RuntimeError",
        error_message="x" * 5000,
    )
    assert "생략" in payload["message"]
    assert len(payload["message"]) < 1000


def test_build_ingest_payload_redacts_credentials() -> None:
    payload = build_ingest_payload(
        dag_id="dag",
        task_id="task",
        error_type="RuntimeError",
        error_message=(
            "OPENAI_API_KEY=sk-supersecretvalue "
            "password=hunter2 "
            "https://hooks.slack.com/services/T000/B000/SECRET"
        ),
    )
    rendered = str(payload)
    assert "supersecretvalue" not in rendered
    assert "hunter2" not in rendered
    assert "/T000/B000/SECRET" not in rendered
    assert "REDACTED" in rendered


def test_notify_task_failure_posts_to_ingest_endpoint(monkeypatch) -> None:
    monkeypatch.setenv("ALERT_SERVER_URL", "http://alert.test:8001")
    monkeypatch.setenv("ALERT_SERVER_TOKEN", "test-token")
    calls: dict = {}

    class _Response:
        def raise_for_status(self) -> None:
            return None

    def _fake_post(url, *, json, headers, timeout):
        calls.update(url=url, json=json, headers=headers, timeout=timeout)
        return _Response()

    task_instance = SimpleNamespace(
        task_id="capture_kafka_start_offsets",
        map_index=3,
        try_number=2,
        log_url="http://localhost:8082/log",
    )
    context = {
        "dag": SimpleNamespace(dag_id="news_comment_end_to_end_pipeline"),
        "run_id": "manual__test",
        "task_instance": task_instance,
        "exception": RuntimeError("Kafka topic metadata unavailable: raw-text"),
    }

    notify_task_failure(context, request_func=_fake_post)

    assert calls["url"] == "http://alert.test:8001/ingest"
    assert calls["headers"] == {"X-Alert-Token": "test-token"}
    assert calls["json"]["dag_id"] == "news_comment_end_to_end_pipeline"
    assert calls["json"]["task_id"] == "capture_kafka_start_offsets[3]"
    assert calls["json"]["error_type"] == "RuntimeError"
    assert "Kafka topic metadata unavailable" in calls["json"]["message"]


def test_notify_task_failure_strips_trailing_slash_from_server_url(monkeypatch) -> None:
    monkeypatch.setenv("ALERT_SERVER_URL", "http://alert.test:8001/")
    monkeypatch.setenv("ALERT_SERVER_TOKEN", "test-token")
    calls: dict = {}

    class _Response:
        def raise_for_status(self) -> None:
            return None

    def _fake_post(url, **kwargs):
        calls["url"] = url
        return _Response()

    notify_task_failure(
        {"task_instance": SimpleNamespace(task_id="run_spark"), "exception": ValueError("boom")},
        request_func=_fake_post,
    )

    assert calls["url"] == "http://alert.test:8001/ingest"


def test_notify_task_failure_skips_silently_without_config(monkeypatch) -> None:
    monkeypatch.delenv("ALERT_SERVER_URL", raising=False)
    monkeypatch.delenv("ALERT_SERVER_TOKEN", raising=False)
    calls: list = []

    def _fake_post(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("request_func must not be called without server config")

    notify_task_failure({}, request_func=_fake_post)
    assert calls == []


def test_notify_task_failure_skips_when_only_token_is_set(monkeypatch) -> None:
    monkeypatch.delenv("ALERT_SERVER_URL", raising=False)
    monkeypatch.setenv("ALERT_SERVER_TOKEN", "test-token")
    calls: list = []

    def _fake_post(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("request_func must not be called without a server URL")

    notify_task_failure({}, request_func=_fake_post)
    assert calls == []


def test_notify_task_failure_never_raises_on_http_error(monkeypatch) -> None:
    monkeypatch.setenv("ALERT_SERVER_URL", "http://alert.test:8001")
    monkeypatch.setenv("ALERT_SERVER_TOKEN", "test-token")

    class _FailingResponse:
        def raise_for_status(self) -> None:
            raise RuntimeError("401 Unauthorized")

    notify_task_failure(
        {"task_instance": SimpleNamespace(task_id="run_spark"), "exception": ValueError("boom")},
        request_func=lambda *a, **k: _FailingResponse(),
    )


def test_notify_task_failure_never_raises_on_connection_error(monkeypatch) -> None:
    monkeypatch.setenv("ALERT_SERVER_URL", "http://alert.test:8001")
    monkeypatch.setenv("ALERT_SERVER_TOKEN", "test-token")

    def _raise(*args, **kwargs):
        raise ConnectionError("network down")

    notify_task_failure(
        {"task_instance": SimpleNamespace(task_id="run_spark"), "exception": ValueError("boom")},
        request_func=_raise,
    )
