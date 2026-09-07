from __future__ import annotations

import argparse
import json
import os
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence
from zoneinfo import ZoneInfo

from llm_analysis import OpenAIBatchClient, validate_batch_results
from notifications import SlackWebhookNotifier

TERMINAL_STATUSES = {"completed", "failed", "expired", "cancelled"}
GROUP_LABELS = {
    "politics-and-international": "정치·국제",
    "economy-and-society": "사회·경제",
    "technology-and-digital": "기술·디지털",
    "environment-and-science": "환경·과학",
}
GROUP_KEYS = {
    "politics": "정치·국제",
    "economy": "사회·경제",
    "technology": "기술·디지털",
    "environment": "환경·과학",
}
SENTIMENT_LABELS = {
    "positive": "긍정",
    "neutral": "중립",
    "negative": "부정",
    "mixed": "혼합",
}
POLARIZATION_LABELS = {"low": "낮음", "medium": "보통", "high": "높음"}
TONE_LABELS = {
    "optimism": "낙관",
    "hope": "희망",
    "satisfaction": "만족",
    "trust": "신뢰",
    "enthusiasm": "열의",
    "gratitude": "감사",
    "relief": "안도",
    "anger": "분노",
    "anxiety": "불안",
    "frustration": "좌절",
    "distrust": "불신",
    "sadness": "슬픔",
    "cynicism": "냉소",
    "disappointment": "실망",
}


class BatchClient(Protocol):
    def retrieve(self, batch_id: str) -> dict[str, Any]: ...

    def download(self, file_id: str, output: str | Path) -> Path: ...


class Notifier(Protocol):
    def send(
        self,
        *,
        text: str,
        blocks: Sequence[Mapping[str, Any]] | None = None,
    ) -> None: ...


class PreviewNotifier:
    delivery_mode = "preview"

    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    def send(
        self,
        *,
        text: str,
        blocks: Sequence[Mapping[str, Any]] | None = None,
    ) -> None:
        self.messages.append({"text": text, "blocks": list(blocks or [])})
        print(json.dumps(self.messages[-1], ensure_ascii=False))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def aggregate_topics(rows: Sequence[Mapping[str, Any]], *, limit: int = 5) -> list[tuple[str, int]]:
    if limit < 1:
        raise ValueError("topic limit must be positive")
    counts: Counter[str] = Counter()
    for row in rows:
        for raw_topic in row.get("topics") or []:
            topic = str(raw_topic).strip().casefold()
            if topic:
                counts[topic] += 1
    return counts.most_common(limit)


def summarize_sentiments(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for row in rows:
        event_id = str(row.get("event_id") or "")
        parts = event_id.split(":")
        group_key = parts[1] if len(parts) >= 3 and parts[0] == "period" else ""
        distribution = row.get("estimated_sentiment_distribution")
        if not isinstance(distribution, Mapping):
            distribution = {}
        summary = {
                "group": GROUP_KEYS.get(group_key, group_key or "전체"),
                "dominant_sentiment": str(
                    row.get("dominant_sentiment") or row.get("sentiment") or "unknown"
                ),
                "sentiment_score": row.get("sentiment_score"),
                "distribution": {
                    key: distribution.get(key)
                    for key in ("positive", "neutral", "negative")
                    if distribution.get(key) is not None
                },
                "polarization": row.get("polarization"),
                "polarization_score": row.get("polarization_score"),
            }
        if isinstance(row.get("positive_tones"), list):
            summary["positive_tones"] = row["positive_tones"]
        if isinstance(row.get("negative_tones"), list):
            summary["negative_tones"] = row["negative_tones"]
        if str(row.get("summary") or "").strip():
            summary["summary"] = str(row["summary"]).strip()
        summaries.append(summary)
    return summaries


def _format_score(value: Any) -> str:
    return "확인 불가" if value is None else f"{float(value):.2f}"


def _format_sentiments(sentiments: Sequence[Mapping[str, Any]]) -> str:
    if not sentiments:
        return "없음"
    lines: list[str] = []
    for item in sentiments:
        dominant = str(item.get("dominant_sentiment") or "unknown")
        line = (
            f"*{item.get('group') or '전체'}*: 대표 감정 "
            f"*{SENTIMENT_LABELS.get(dominant, dominant)}* "
            f"(`{_format_score(item.get('sentiment_score'))}`)"
        )
        distribution = item.get("distribution")
        if isinstance(distribution, Mapping) and distribution:
            line += " · 분포 " + " / ".join(
                f"{SENTIMENT_LABELS[key]} {float(distribution[key]) * 100:.0f}%"
                for key in ("positive", "neutral", "negative")
                if key in distribution
            )
        polarization = item.get("polarization")
        if polarization is not None:
            polarization_text = POLARIZATION_LABELS.get(
                str(polarization), str(polarization)
            )
            line += (
                f" · 양극화 *{polarization_text}* "
                f"(`{_format_score(item.get('polarization_score'))}`)"
            )
        for field, heading in (
            ("negative_tones", "부정 세부"),
            ("positive_tones", "긍정 세부"),
        ):
            tones = item.get(field)
            if isinstance(tones, list) and tones:
                ranked = sorted(
                    (tone for tone in tones if isinstance(tone, Mapping)),
                    key=lambda tone: float(tone.get("share") or 0),
                    reverse=True,
                )[:3]
                details = []
                for tone in ranked:
                    name = TONE_LABELS.get(str(tone.get("tone")), str(tone.get("tone")))
                    drivers = ", ".join(str(value) for value in tone.get("drivers") or [])
                    detail = f"{name} {float(tone.get('share') or 0) * 100:.0f}%"
                    if drivers:
                        detail += f" ({drivers})"
                    details.append(detail)
                if details:
                    line += f"\n  · {heading}: " + " / ".join(details)
        lines.append(line)
    return "\n".join(lines)


def _format_analysis_summaries(
    sentiments: Sequence[Mapping[str, Any]],
) -> str:
    values = [
        f"*{item.get('group') or '전체'}*: {str(item['summary']).strip()}"
        for item in sentiments
        if str(item.get("summary") or "").strip()
    ]
    return "\n".join(values) or "없음"


def read_batch_context(manifest_path: Path) -> dict[str, str]:
    rows = _read_jsonl(manifest_path)
    periods = sorted({str(row.get("period") or "").strip() for row in rows} - {""})
    groups = sorted({str(row.get("group") or "").strip() for row in rows} - {""})
    if not periods:
        period = "확인 불가"
    elif len(periods) == 1:
        period = periods[0]
    else:
        period = f"{periods[0]} ~ {periods[-1]}"
    if not groups:
        group = "확인 불가"
    else:
        group = ", ".join(GROUP_LABELS.get(value, value) for value in groups)
    return {"period": period, "group": group}


def _format_batch_time(value: Any) -> str:
    if value is None:
        return "확인 불가"
    timestamp = datetime.fromtimestamp(float(value), tz=timezone.utc)
    return timestamp.astimezone(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S KST")


def _format_duration(started_at: Any, ended_at: Any) -> str:
    if started_at is None or ended_at is None:
        return "확인 불가"
    seconds = max(0, int(float(ended_at) - float(started_at)))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}시간 {minutes}분 {seconds}초"
    if minutes:
        return f"{minutes}분 {seconds}초"
    return f"{seconds}초"


def build_notification(
    *,
    batch: Mapping[str, Any],
    topics: Sequence[tuple[str, int]],
    validation: Mapping[str, int] | None,
    sentiments: Sequence[Mapping[str, Any]] | None = None,
    context: Mapping[str, str] | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    status = str(batch.get("status") or "unknown")
    counts = batch.get("request_counts") or {}
    batch_id = str(batch.get("id") or "unknown")
    topic_text = ", ".join(f"{name} ({count})" for name, count in topics) or "없음"
    sentiment_text = _format_sentiments(sentiments or [])
    analysis_summary_text = _format_analysis_summaries(sentiments or [])
    validated = int((validation or {}).get("validated_rows", 0))
    context = context or {"period": "확인 불가", "group": "확인 불가"}
    started_at = batch.get("in_progress_at") or batch.get("created_at")
    ended_at = (
        batch.get("completed_at")
        or batch.get("failed_at")
        or batch.get("expired_at")
        or batch.get("cancelled_at")
    )
    summary = (
        f"OpenAI Batch {batch_id} 상태: {status}. "
        f"데이터 날짜: {context['period']}, 대주제: {context['group']}. "
        f"완료 {int(counts.get('completed') or 0)}/{int(counts.get('total') or 0)}, "
        f"실패 {int(counts.get('failed') or 0)}, 검증 {validated}. "
        f"소요 시간: {_format_duration(started_at, ended_at)}. "
        f"감정 결과: {sentiment_text}. 분석 요약: {analysis_summary_text}. "
        f"주요 토픽: {topic_text}"
    )
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "LLM Batch 처리 결과"},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Batch*\n`{batch_id}`"},
                {"type": "mrkdwn", "text": f"*상태*\n{status}"},
                {"type": "mrkdwn", "text": f"*데이터 날짜*\n{context['period']}"},
                {"type": "mrkdwn", "text": f"*대주제*\n{context['group']}"},
                {
                    "type": "mrkdwn",
                    "text": f"*요청*\n{int(counts.get('completed') or 0)}/{int(counts.get('total') or 0)} 완료",
                },
                {"type": "mrkdwn", "text": f"*검증 결과*\n{validated}건"},
                {
                    "type": "mrkdwn",
                    "text": f"*시작 시각*\n{_format_batch_time(started_at)}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*종료 시각*\n{_format_batch_time(ended_at)}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*소요 시간*\n{_format_duration(started_at, ended_at)}",
                },
            ],
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*감정·양극화 분석*\n{sentiment_text}"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*분석 요약*\n{analysis_summary_text}"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*주요 토픽*\n{topic_text}"},
        },
    ]
    return summary, blocks


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    temporary.replace(path)
    from storage.data_lake import publish_artifact_if_enabled

    publish_artifact_if_enabled(path)


def watch_batch(
    *,
    client: BatchClient,
    notifier: Notifier,
    batch_id: str,
    manifest_path: Path,
    result_path: Path,
    validated_path: Path,
    batch_state_path: Path,
    notification_state_path: Path,
    poll_interval_seconds: int = 60,
    timeout_seconds: int = 90_000,
    once: bool = False,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    if poll_interval_seconds < 1 or timeout_seconds < 1:
        raise ValueError("poll interval and timeout must be positive")
    started = clock()
    context = read_batch_context(manifest_path)
    while True:
        batch = client.retrieve(batch_id)
        _write_json(batch_state_path, batch)
        status = str(batch.get("status") or "unknown")
        delivery_mode = str(getattr(notifier, "delivery_mode", "slack_webhook"))
        previous: dict[str, Any] = {}
        if notification_state_path.is_file():
            previous = json.loads(notification_state_path.read_text(encoding="utf-8"))

        # Preserve compatibility with the original one-notification state format.
        if (
            status in TERMINAL_STATUSES
            and previous.get("batch_id") == batch_id
            and previous.get("notified_status") == status
            and previous.get("delivery_mode") == delivery_mode
            and "notifications" not in previous
        ):
            return {**previous, "notification_status": "already_sent"}

        notifications = dict(previous.get("notifications") or {})

        if status in TERMINAL_STATUSES:
            terminal_key = f"{status}:{delivery_mode}"
            if terminal_key in notifications:
                return {**previous, "notification_status": "already_sent"}

            validation: dict[str, int] | None = None
            topics: list[tuple[str, int]] = []
            sentiments: list[dict[str, Any]] = []
            if status == "completed":
                output_file_id = str(batch.get("output_file_id") or "")
                if not output_file_id:
                    raise RuntimeError("completed Batch has no output_file_id")
                client.download(output_file_id, result_path)
                validation = validate_batch_results(
                    result_path=result_path,
                    manifest_path=manifest_path,
                    output_path=validated_path,
                )
                if (
                    int(validation["failed_rows"]) > 0
                    or int(validation["missing_rows"]) > 0
                    or int(validation["validated_rows"])
                    != int(validation["manifest_rows"])
                ):
                    raise RuntimeError(
                        "Batch result validation failed: "
                        f"validated={validation['validated_rows']}/"
                        f"{validation['manifest_rows']}, "
                        f"failed={validation['failed_rows']}, "
                        f"missing={validation['missing_rows']}"
                    )
                validated_rows = _read_jsonl(validated_path)
                topics = aggregate_topics(validated_rows)
                sentiments = summarize_sentiments(validated_rows)

            text, blocks = build_notification(
                batch=batch,
                topics=topics,
                validation=validation,
                sentiments=sentiments,
                context=context,
            )
            notifier.send(text=text, blocks=blocks)
            notifications[terminal_key] = {
                "event": status,
                "status": status,
                "delivery_mode": delivery_mode,
                "sent_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            }
            state = {
                "batch_id": batch_id,
                "notified_status": status,
                "delivery_mode": delivery_mode,
                "notification_status": (
                    "previewed" if delivery_mode == "preview" else "sent"
                ),
                "context": context,
                "notifications": notifications,
                "notified_at": datetime.now(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "topics": [{"topic": name, "count": count} for name, count in topics],
                "sentiments": sentiments,
                "validation": validation,
            }
            _write_json(notification_state_path, state)
            return state

        if once:
            return {
                "batch_id": batch_id,
                "batch_status": status,
                "notification_status": "waiting",
            }
        if clock() - started >= timeout_seconds:
            raise TimeoutError(f"Batch monitoring timed out with status={status}")
        sleeper(poll_interval_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Wait for an OpenAI Batch and send validated topics to Slack"
    )
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--result-output", type=Path, required=True)
    parser.add_argument("--validated-output", type=Path, required=True)
    parser.add_argument("--batch-state", type=Path, required=True)
    parser.add_argument("--notification-state", type=Path, required=True)
    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=int(os.getenv("OPENAI_BATCH_POLL_INTERVAL_SECONDS", "60")),
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=int(os.getenv("OPENAI_BATCH_MONITOR_TIMEOUT_SECONDS", "90000")),
    )
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run-slack", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.dry_run_slack:
        notifier: Notifier = PreviewNotifier()
    else:
        webhook_url = os.getenv("SLACK_WEBHOOK_URL", "").strip()
        if not webhook_url:
            raise ValueError("SLACK_WEBHOOK_URL is required unless --dry-run-slack is used")
        notifier = SlackWebhookNotifier(webhook_url)
    result = watch_batch(
        client=OpenAIBatchClient(),
        notifier=notifier,
        batch_id=args.batch_id,
        manifest_path=args.manifest,
        result_path=args.result_output,
        validated_path=args.validated_output,
        batch_state_path=args.batch_state,
        notification_state_path=args.notification_state,
        poll_interval_seconds=args.poll_interval_seconds,
        timeout_seconds=args.timeout_seconds,
        once=args.once,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
