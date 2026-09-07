from __future__ import annotations

from typing import Any, Mapping, Sequence
from urllib.parse import urlparse

import requests


class SlackWebhookNotifier:
    delivery_mode = "slack_webhook"

    def __init__(
        self,
        webhook_url: str,
        *,
        timeout: int = 10,
        session: Any | None = None,
    ) -> None:
        value = webhook_url.strip()
        parsed = urlparse(value)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("SLACK_WEBHOOK_URL must be a valid https URL")
        self._webhook_url = value
        self._timeout = timeout
        self._session = session or requests.Session()

    def send(
        self,
        *,
        text: str,
        blocks: Sequence[Mapping[str, Any]] | None = None,
    ) -> None:
        payload: dict[str, Any] = {"text": text}
        if blocks:
            payload["blocks"] = list(blocks)
        response = self._session.post(
            self._webhook_url,
            json=payload,
            timeout=self._timeout,
        )
        response.raise_for_status()
