from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

import requests

ALERT_SERVER_URL_ENV_VAR = "ALERT_SERVER_URL"
ALERT_SERVER_TOKEN_ENV_VAR = "ALERT_SERVER_TOKEN"
MAX_ERROR_MESSAGE_LENGTH = 800
REQUEST_TIMEOUT_SECONDS = 5

_SECRET_PATTERNS = (
    re.compile(r"https://hooks\.slack\.com/services/[^\s'\"<>]+", re.IGNORECASE),
    re.compile(
        r"(?i)(api[_-]?key|token|password|secret|authorization)\s*[:=]\s*([^\s,;]+)"
    ),
    re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b"),
)

logger = logging.getLogger(__name__)


def _truncate(text: str, *, limit: int = MAX_ERROR_MESSAGE_LENGTH) -> str:
    value = text.strip() or "에러 메시지가 없습니다."
    if len(value) <= limit:
        return value
    return value[:limit] + "... (생략)"


def _redact(text: str) -> str:
    """Remove common credentials before an exception reaches the alert server."""
    value = text
    value = _SECRET_PATTERNS[0].sub("[REDACTED_SLACK_WEBHOOK]", value)
    value = _SECRET_PATTERNS[1].sub(lambda match: f"{match.group(1)}=[REDACTED]", value)
    return _SECRET_PATTERNS[2].sub("[REDACTED_OPENAI_KEY]", value)


def build_ingest_payload(
    *,
    dag_id: str,
    task_id: str,
    error_type: str,
    error_message: str,
    map_index: int | None = None,
    run_id: str | None = None,
    try_number: int | None = None,
    log_url: str | None = None,
    occurred_at: datetime | None = None,
) -> dict[str, str]:
    """Build the Error-Alert `/ingest` request body.

    Matches https://github.com/Halofty/Error-Alert's contract exactly: only
    dag_id, task_id, error_type, message and occurred_at. Dedup, digesting
    and Slack delivery are the Error-Alert server's job, not this
    pipeline's, so run_id/try_number/log_url (which have no field of their
    own) are folded into the message text instead of dropped. Kept free of
    any Airflow import so it can be unit tested with plain values;
    ``notify_task_failure`` below is the Airflow-facing adapter.
    """
    occurred_at = occurred_at or datetime.now(timezone.utc)
    stage = task_id if map_index is None or map_index < 0 else f"{task_id}[{map_index}]"

    message = _redact(error_message).strip() or "에러 메시지가 없습니다."
    context_bits = [
        f"run={run_id}" if run_id else None,
        f"try={try_number}" if try_number else None,
        f"log={log_url}" if log_url else None,
    ]
    context_suffix = " ".join(bit for bit in context_bits if bit)
    if context_suffix:
        message = f"{message} ({context_suffix})"

    return {
        "dag_id": dag_id,
        "task_id": stage,
        "error_type": error_type,
        "message": _truncate(message),
        "occurred_at": occurred_at.isoformat(),
    }


def notify_task_failure(
    context: Mapping[str, Any],
    *,
    request_func: Callable[..., Any] = requests.post,
    server_url_env_var: str = ALERT_SERVER_URL_ENV_VAR,
    token_env_var: str = ALERT_SERVER_TOKEN_ENV_VAR,
) -> None:
    """Airflow ``on_failure_callback``: report the failure to the Error-Alert
    server's ``/ingest`` endpoint (https://github.com/Halofty/Error-Alert).

    Wired once via the DAG's ``default_args`` so every task (including each
    mapped instance) reports here when it finally fails, i.e. after retries
    are exhausted. This must never raise: a broken or unreachable alert
    server would otherwise mask the real task failure that triggered the
    callback in the first place, or fail the DAG a second time for an
    unrelated reason. Dedup, daily/weekly digesting and the actual Slack
    delivery all happen on the Error-Alert server, not here.
    """
    try:
        server_url = os.getenv(server_url_env_var, "").strip().rstrip("/")
        token = os.getenv(token_env_var, "").strip()
        if not server_url or not token:
            logger.warning(
                "%s and/or %s are not set; skipping error-alert ingest",
                server_url_env_var,
                token_env_var,
            )
            return

        task_instance = context.get("task_instance")
        dag = context.get("dag")
        exception = context.get("exception")
        map_index = getattr(task_instance, "map_index", None)

        payload = build_ingest_payload(
            dag_id=str(getattr(dag, "dag_id", None) or context.get("dag_id") or "unknown"),
            task_id=str(getattr(task_instance, "task_id", None) or context.get("task_id") or "unknown"),
            map_index=map_index if isinstance(map_index, int) and map_index >= 0 else None,
            run_id=str(context.get("run_id")) if context.get("run_id") else None,
            try_number=getattr(task_instance, "try_number", None),
            log_url=getattr(task_instance, "log_url", None),
            error_type=type(exception).__name__ if exception is not None else "UnknownError",
            error_message=str(exception) if exception is not None else "",
        )
        response = request_func(
            f"{server_url}/ingest",
            json=payload,
            headers={"X-Alert-Token": token},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except Exception:
        logger.exception("failed to report task failure to the Error-Alert server")
