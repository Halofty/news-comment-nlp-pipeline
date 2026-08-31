from __future__ import annotations

import html
import re
import unicodedata
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

WHITESPACE_PATTERN = re.compile(r"\s+")
TRACKING_PARAMETERS = {"fbclid", "gclid", "mc_cid", "mc_eid"}


def normalize_title(value: str) -> str:
    """Return a comparison/analysis title without modifying the stored original."""
    decoded = html.unescape(value)
    normalized = unicodedata.normalize("NFC", decoded)
    return WHITESPACE_PATTERN.sub(" ", normalized).strip()


def canonicalize_url(value: str) -> str:
    """Normalize a URL for stable IDs and remove common tracking parameters."""
    parts = urlsplit(value.strip())
    host = (parts.hostname or "").lower()
    port = parts.port
    if port and not ((parts.scheme.lower() == "http" and port == 80) or (parts.scheme.lower() == "https" and port == 443)):
        host = f"{host}:{port}"

    query = [
        (key, item)
        for key, item in parse_qsl(parts.query, keep_blank_values=True)
        if key.casefold() not in TRACKING_PARAMETERS
        and not key.casefold().startswith("utm_")
    ]
    return urlunsplit(
        (parts.scheme.lower(), host, parts.path or "/", urlencode(sorted(query)), "")
    )


def matching_keywords(title: str, keywords: tuple[str, ...]) -> tuple[str, ...]:
    folded = normalize_title(title).casefold()
    return tuple(keyword for keyword in keywords if keyword.casefold() in folded)
