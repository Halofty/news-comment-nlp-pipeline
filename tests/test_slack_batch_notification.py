from __future__ import annotations

import json

from jobs.watch_openai_batch import (
    PreviewNotifier,
    aggregate_topics,
    build_notification,
    summarize_sentiments,
    watch_batch,
)
from notifications.slack import SlackWebhookNotifier


def _manifest(path) -> None:
    path.write_text(
        json.dumps(
            {
                "custom_id": "request-1",
                "event_id": "event-1",
                "model": "gpt-5.6-luna",
                "prompt_version": "news-comment-analysis-v1",
                "schema_version": 1,
                "period": "2012-01-01",
                "group": "economy-and-society",
            }
        )
        + "\n",
        encoding="utf-8",
    )


def _raw_result() -> str:
    analysis = {
        "sentiment": "neutral",
        "sentiment_score": 0,
        "topics": ["Economy", "Policy"],
        "keywords": ["market"],
        "summary": "A synthetic economy update.",
    }
    return json.dumps(
        {
            "custom_id": "request-1",
            "response": {
                "status_code": 200,
                "body": {
                    "model": "gpt-5.6-luna",
                    "output_text": json.dumps(analysis),
                    "usage": {"input_tokens": 20, "output_tokens": 10},
                },
            },
        }
    ) + "\n"


class _Client:
    def __init__(self) -> None:
        self.retrieves = 0

    def retrieve(self, batch_id):
        self.retrieves += 1
        status = "in_progress" if self.retrieves == 1 else "completed"
        return {
            "id": batch_id,
            "status": status,
            "output_file_id": "file-output" if status == "completed" else None,
            "in_progress_at": 1_000,
            "completed_at": 4_661 if status == "completed" else None,
            "request_counts": {"total": 1, "completed": int(status == "completed"), "failed": 0},
        }

    def download(self, _file_id, output):
        output.write_text(_raw_result(), encoding="utf-8")
        return output


class _Notifier:
    def __init__(self) -> None:
        self.messages = []

    def send(self, *, text, blocks=None):
        self.messages.append({"text": text, "blocks": blocks})


def test_completed_batch_validates_topics_and_notifies_once(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_STORAGE_BACKEND", "local")
    manifest = tmp_path / "manifest.jsonl"
    _manifest(manifest)
    client = _Client()
    notifier = _Notifier()
    arguments = {
        "client": client,
        "notifier": notifier,
        "batch_id": "batch-1",
        "manifest_path": manifest,
        "result_path": tmp_path / "result.jsonl",
        "validated_path": tmp_path / "validated.jsonl",
        "batch_state_path": tmp_path / "batch-state.json",
        "notification_state_path": tmp_path / "notification-state.json",
        "poll_interval_seconds": 1,
        "timeout_seconds": 10,
        "sleeper": lambda _seconds: None,
    }
    result = watch_batch(**arguments)
    assert result["notification_status"] == "sent"
    assert result["topics"][0] == {"topic": "economy", "count": 1}
    assert len(notifier.messages) == 1
    assert "2012-01-01" in notifier.messages[0]["text"]
    assert "사회·경제" in notifier.messages[0]["text"]
    assert "economy" in notifier.messages[0]["text"]
    assert "대표 감정" in notifier.messages[0]["text"]
    assert "중립" in notifier.messages[0]["text"]
    assert "1시간 1분 1초" in notifier.messages[0]["text"]
    completed_fields = notifier.messages[0]["blocks"][1]["fields"]
    assert any("시작 시각" in field["text"] for field in completed_fields)
    assert any("종료 시각" in field["text"] for field in completed_fields)
    assert any("소요 시간" in field["text"] for field in completed_fields)

    second = watch_batch(**arguments, once=True)
    assert second["notification_status"] == "already_sent"
    assert len(notifier.messages) == 1


def test_topic_aggregation_is_case_insensitive() -> None:
    assert aggregate_topics(
        [{"topics": ["Economy", "policy"]}, {"topics": ["economy"]}]
    ) == [("economy", 2), ("policy", 1)]


def test_sentiment_summary_keeps_distribution_and_polarization() -> None:
    result = summarize_sentiments(
        [
            {
                "event_id": "period:economy:2012-02-06",
                "dominant_sentiment": "negative",
                "sentiment_score": -0.42,
                "estimated_sentiment_distribution": {
                    "positive": 0.15,
                    "neutral": 0.30,
                    "negative": 0.55,
                },
                "polarization": "high",
                "polarization_score": 0.82,
                "positive_tones": [
                    {"tone": "hope", "share": 1.0, "drivers": ["recovery"]}
                ],
                "negative_tones": [
                    {
                        "tone": "anxiety",
                        "share": 0.6,
                        "drivers": ["job security", "living costs"],
                    },
                    {
                        "tone": "frustration",
                        "share": 0.4,
                        "drivers": ["economic policy"],
                    },
                ],
                "summary": "Economic discussion is anxious and frustrated.",
            }
        ]
    )
    assert result == [
        {
            "group": "사회·경제",
            "dominant_sentiment": "negative",
            "sentiment_score": -0.42,
            "distribution": {
                "positive": 0.15,
                "neutral": 0.30,
                "negative": 0.55,
            },
            "polarization": "high",
            "polarization_score": 0.82,
            "positive_tones": [
                {"tone": "hope", "share": 1.0, "drivers": ["recovery"]}
            ],
            "negative_tones": [
                {
                    "tone": "anxiety",
                    "share": 0.6,
                    "drivers": ["job security", "living costs"],
                },
                {
                    "tone": "frustration",
                    "share": 0.4,
                    "drivers": ["economic policy"],
                },
            ],
            "summary": "Economic discussion is anxious and frustrated.",
        }
    ]

    text, blocks = build_notification(
        batch={
            "id": "batch-v3",
            "status": "completed",
            "request_counts": {"total": 1, "completed": 1, "failed": 0},
            "in_progress_at": 1_000,
            "completed_at": 1_120,
        },
        topics=[("economy", 1)],
        validation={"validated_rows": 1},
        sentiments=result,
        context={"period": "2012-04-01", "group": "사회·경제"},
    )
    assert "불안 60% (job security, living costs)" in text
    assert "희망 100% (recovery)" in text
    assert "Economic discussion is anxious and frustrated." in text
    assert any("분석 요약" in block.get("text", {}).get("text", "") for block in blocks)


def test_preview_does_not_block_later_webhook_delivery(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_STORAGE_BACKEND", "local")
    manifest = tmp_path / "manifest.jsonl"
    _manifest(manifest)
    paths = {
        "batch_id": "batch-preview",
        "manifest_path": manifest,
        "result_path": tmp_path / "result.jsonl",
        "validated_path": tmp_path / "validated.jsonl",
        "batch_state_path": tmp_path / "batch-state.json",
        "notification_state_path": tmp_path / "notification-state.json",
        "poll_interval_seconds": 1,
        "timeout_seconds": 10,
        "sleeper": lambda _seconds: None,
    }
    preview = watch_batch(client=_Client(), notifier=PreviewNotifier(), **paths)
    assert preview["notification_status"] == "previewed"
    assert preview["delivery_mode"] == "preview"

    webhook = _Notifier()
    delivered = watch_batch(client=_Client(), notifier=webhook, **paths)
    assert delivered["notification_status"] == "sent"
    assert delivered["delivery_mode"] == "slack_webhook"
    assert len(webhook.messages) == 1


class _Response:
    def raise_for_status(self):
        return None


class _Session:
    def __init__(self) -> None:
        self.calls = []

    def post(self, url, *, json, timeout):
        self.calls.append((url, json, timeout))
        return _Response()


def test_slack_webhook_sends_blocks_without_exposing_url() -> None:
    session = _Session()
    notifier = SlackWebhookNotifier(
        "https://hooks.slack.test/services/example", session=session
    )
    notifier.send(text="completed", blocks=[{"type": "section"}])
    assert session.calls[0][1]["text"] == "completed"
    assert session.calls[0][1]["blocks"] == [{"type": "section"}]
