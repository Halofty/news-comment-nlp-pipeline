from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

import requests

from collectors.current.models import CollectionManifest, CollectionWindow
from core.events import stable_event_id, utc_now_iso

DEFAULT_SUBREDDITS = ("Economics", "business", "TrueReddit", "changemyview")
TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
OAUTH_ROOT = "https://oauth.reddit.com"


@dataclass(frozen=True)
class RedditCredentials:
    client_id: str
    client_secret: str
    user_agent: str

    @classmethod
    def from_env(cls) -> "RedditCredentials":
        values = {
            "client_id": os.getenv("REDDIT_CLIENT_ID", "").strip(),
            "client_secret": os.getenv("REDDIT_CLIENT_SECRET", "").strip(),
            "user_agent": os.getenv("REDDIT_USER_AGENT", "").strip(),
        }
        missing = [name for name, value in values.items() if not value]
        if missing:
            raise ValueError(f"missing Reddit API settings: {', '.join(missing)}")
        return cls(**values)


@dataclass(frozen=True)
class RedditWindowResult:
    raw_rows: tuple[dict[str, Any], ...]
    events: tuple[dict[str, Any], ...]
    manifest: CollectionManifest


def _access_token(
    session: requests.Session,
    credentials: RedditCredentials,
    *,
    timeout: float,
) -> str:
    response = session.post(
        TOKEN_URL,
        auth=(credentials.client_id, credentials.client_secret),
        data={"grant_type": "client_credentials"},
        headers={"User-Agent": credentials.user_agent},
        timeout=timeout,
    )
    response.raise_for_status()
    token = str(response.json().get("access_token") or "").strip()
    if not token:
        raise RuntimeError("Reddit OAuth response did not contain access_token")
    return token


def _sanitized_raw(data: Mapping[str, Any]) -> dict[str, Any]:
    allowed = (
        "id",
        "body",
        "subreddit",
        "created_utc",
        "score",
        "permalink",
        "link_id",
        "controversiality",
        "edited",
    )
    return {key: data.get(key) for key in allowed}


def _to_event(data: Mapping[str, Any], *, collected_at: str) -> dict[str, Any] | None:
    comment_id = str(data.get("id") or "").strip()
    body = str(data.get("body") or "").strip()
    created = data.get("created_utc")
    if not comment_id or not body or body in {"[deleted]", "[removed]"} or created is None:
        return None
    event_time = datetime.fromtimestamp(float(created), tz=timezone.utc)
    permalink = str(data.get("permalink") or "").strip()
    return {
        "event_id": stable_event_id("reddit", comment_id),
        "source_type": "comment",
        "source_name": "reddit",
        "event_time": event_time.isoformat().replace("+00:00", "Z"),
        "collected_at": collected_at,
        "language": "unknown",
        "title": None,
        "text": body,
        "url": f"https://www.reddit.com{permalink}" if permalink else None,
        "community": str(data.get("subreddit") or "") or None,
        "engagement": int(data.get("score") or 0),
        "schema_version": 1,
        "metadata": {
            "link_id": data.get("link_id"),
            "controversiality": int(data.get("controversiality") or 0),
            "edited": bool(data.get("edited", False)),
            "collection_scope": "current_reddit_comments",
        },
    }


def collect_reddit_window(
    window: CollectionWindow,
    *,
    credentials: RedditCredentials,
    subreddits: Sequence[str] = DEFAULT_SUBREDDITS,
    session: requests.Session | None = None,
    timeout: float = 60,
    page_size: int = 100,
    max_pages_per_subreddit: int = 10,
) -> RedditWindowResult:
    if not subreddits or len({value.casefold() for value in subreddits}) != len(subreddits):
        raise ValueError("subreddits must be non-empty and unique")
    if not 1 <= page_size <= 100 or max_pages_per_subreddit < 1:
        raise ValueError("invalid Reddit pagination settings")
    client = session or requests.Session()
    token = _access_token(client, credentials, timeout=timeout)
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": credentials.user_agent,
    }
    collected_at = utc_now_iso()
    raw_rows: list[dict[str, Any]] = []
    unique: dict[str, dict[str, Any]] = {}
    received = rejected = duplicates = requests_made = 0
    truncated_subreddits: list[str] = []

    for subreddit in subreddits:
        after: str | None = None
        reached_start = False
        for page_number in range(max_pages_per_subreddit):
            params: dict[str, Any] = {"limit": page_size, "raw_json": 1}
            if after:
                params["after"] = after
            response = client.get(
                f"{OAUTH_ROOT}/r/{subreddit}/comments",
                params=params,
                headers=headers,
                timeout=timeout,
            )
            response.raise_for_status()
            requests_made += 1
            payload = response.json()
            children = payload.get("data", {}).get("children", [])
            if not isinstance(children, list):
                raise ValueError("Reddit listing children must be an array")
            if not children:
                reached_start = True
                break
            for child in children:
                data = child.get("data", {}) if isinstance(child, Mapping) else {}
                received += 1
                created = data.get("created_utc")
                if created is None:
                    rejected += 1
                    continue
                created_at = datetime.fromtimestamp(float(created), tz=timezone.utc)
                if created_at < window.start_utc:
                    reached_start = True
                    continue
                if created_at >= window.end_utc:
                    continue
                raw_rows.append(_sanitized_raw(data))
                event = _to_event(data, collected_at=collected_at)
                if event is None:
                    rejected += 1
                    continue
                event_id = str(event["event_id"])
                if event_id in unique:
                    duplicates += 1
                else:
                    unique[event_id] = event
            after = payload.get("data", {}).get("after")
            if reached_start or not after:
                reached_start = True
                break
        if not reached_start:
            truncated_subreddits.append(subreddit)

    status = "partial" if truncated_subreddits else "complete"
    manifest = CollectionManifest(
        source="reddit",
        window=window,
        requested=requests_made,
        received=received,
        normalized=len(unique),
        duplicate_in_window=duplicates,
        rejected=rejected,
        status=status,
        cursor_before=window.start_utc.isoformat(),
        cursor_after=window.end_utc.isoformat() if status == "complete" else None,
        metadata={
            "subreddits": list(subreddits),
            "truncated_subreddits": truncated_subreddits,
            "raw_rows_exclude_author_fields": True,
        },
    )
    return RedditWindowResult(tuple(raw_rows), tuple(unique.values()), manifest)

