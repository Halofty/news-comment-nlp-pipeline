from __future__ import annotations

import re
import unicodedata
import zlib
from dataclasses import asdict, dataclass
from typing import Any

SOFT_CHARACTER_LIMIT = 5_000
HARD_CHARACTER_LIMIT = 20_000
HARD_UTF8_BYTE_LIMIT = 64 * 1024
QUALITY_POLICY_VERSION = 1
MAX_COMBINING_MARK_RUN = 8
REPETITION_MIN_CHARACTERS = 100
REPETITION_RATIO_THRESHOLD = 0.80
URL_HEAVY_MIN_COUNT = 2
URL_RATIO_THRESHOLD = 0.50

ALLOWED_CONTROL_CHARACTERS = frozenset({"\t", "\n", "\r"})
ZERO_WIDTH_CHARACTERS = frozenset(
    {
        "\u200b",  # zero width space
        "\u2060",  # word joiner
        "\ufeff",  # zero width no-break space / BOM
    }
)
SOURCE_TOMBSTONES = frozenset({"[deleted]", "[removed]"})

URL_PATTERN = re.compile(r"(?:https?://|www\.)[^\s]+", re.IGNORECASE)
EMAIL_PATTERN = re.compile(
    r"(?<![\w.+-])[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}(?![\w.-])",
    re.IGNORECASE,
)
PHONE_PATTERN = re.compile(r"(?<!\d)(?:\+?\d[\d ()-]{7,}\d)(?!\d)")

QUALITY_STATUSES = frozenset({"accept", "flag", "quarantine", "reject"})
QUALITY_FLAGS = frozenset(
    {
        "SOURCE_TOMBSTONE",
        "EMPTY_AFTER_NORMALIZATION",
        "EXCESSIVE_LENGTH",
        "EXCESSIVE_UTF8_BYTES",
        "CONTROL_CHARACTERS",
        "ZERO_WIDTH_CHARACTERS",
        "EXCESSIVE_COMBINING_MARKS",
        "HIGH_REPETITION",
        "URL_HEAVY",
        "POSSIBLE_PII",
    }
)


@dataclass(frozen=True)
class TextQualityResult:
    quality_policy_version: int
    text_clean: str
    character_count: int
    utf8_byte_count: int
    control_character_count: int
    zero_width_count: int
    max_combining_mark_run: int
    url_count: int
    url_ratio: float
    repetition_ratio: float
    quality_status: str
    quality_flags: tuple[str, ...]
    exclusion_reason: str | None
    was_normalized: bool
    was_truncated: bool = False

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["quality_flags"] = list(self.quality_flags)
        return result


def _is_disallowed_control(character: str) -> bool:
    return (
        unicodedata.category(character) == "Cc"
        and character not in ALLOWED_CONTROL_CHARACTERS
    )


def _max_combining_mark_run(text: str) -> int:
    maximum = 0
    current = 0
    for character in text:
        if unicodedata.combining(character):
            current += 1
            maximum = max(maximum, current)
        else:
            current = 0
    return maximum


def _repetition_ratio(text: str) -> float:
    compact = "".join(text.split())
    if len(compact) < REPETITION_MIN_CHARACTERS:
        return 0.0

    character_counts: dict[str, int] = {}
    for character in compact:
        character_counts[character] = character_counts.get(character, 0) + 1
    dominant_character_ratio = max(character_counts.values()) / len(compact)

    encoded = compact.encode("utf-8")
    compression_savings = 1 - (len(zlib.compress(encoded)) / len(encoded))
    return round(max(dominant_character_ratio, compression_savings, 0.0), 4)


def _url_metrics(text: str) -> tuple[int, float]:
    matches = list(URL_PATTERN.finditer(text))
    if not text:
        return 0, 0.0
    covered = sum(match.end() - match.start() for match in matches)
    return len(matches), round(covered / len(text), 4)


def analyze_text_quality(text: str) -> TextQualityResult:
    if not isinstance(text, str):
        raise TypeError("text must be a string")

    character_count = len(text)
    utf8_byte_count = len(text.encode("utf-8"))
    control_character_count = sum(_is_disallowed_control(char) for char in text)
    zero_width_count = sum(char in ZERO_WIDTH_CHARACTERS for char in text)

    sanitized = "".join(
        char
        for char in text
        if not _is_disallowed_control(char) and char not in ZERO_WIDTH_CHARACTERS
    )
    text_clean = unicodedata.normalize("NFC", sanitized).strip()
    was_normalized = text_clean != text
    max_combining_mark_run = _max_combining_mark_run(text_clean)
    url_count, url_ratio = _url_metrics(text_clean)
    repetition_ratio = _repetition_ratio(text_clean)

    flags: list[str] = []
    stripped_casefolded = text.strip().casefold()
    if stripped_casefolded in SOURCE_TOMBSTONES:
        flags.append("SOURCE_TOMBSTONE")
    if not text_clean:
        flags.append("EMPTY_AFTER_NORMALIZATION")
    if character_count > SOFT_CHARACTER_LIMIT:
        flags.append("EXCESSIVE_LENGTH")
    if utf8_byte_count > HARD_UTF8_BYTE_LIMIT:
        flags.append("EXCESSIVE_UTF8_BYTES")
    if control_character_count:
        flags.append("CONTROL_CHARACTERS")
    if zero_width_count:
        flags.append("ZERO_WIDTH_CHARACTERS")
    if max_combining_mark_run > MAX_COMBINING_MARK_RUN:
        flags.append("EXCESSIVE_COMBINING_MARKS")
    if repetition_ratio >= REPETITION_RATIO_THRESHOLD:
        flags.append("HIGH_REPETITION")
    if url_count >= URL_HEAVY_MIN_COUNT and url_ratio >= URL_RATIO_THRESHOLD:
        flags.append("URL_HEAVY")
    if EMAIL_PATTERN.search(text_clean) or PHONE_PATTERN.search(text_clean):
        flags.append("POSSIBLE_PII")

    if "SOURCE_TOMBSTONE" in flags:
        status = "reject"
        exclusion_reason = "SOURCE_TOMBSTONE"
    elif "EMPTY_AFTER_NORMALIZATION" in flags:
        status = "reject"
        exclusion_reason = "EMPTY_AFTER_NORMALIZATION"
    else:
        quarantine_reasons = []
        if character_count > HARD_CHARACTER_LIMIT:
            quarantine_reasons.append("EXCESSIVE_LENGTH")
        if utf8_byte_count > HARD_UTF8_BYTE_LIMIT:
            quarantine_reasons.append("EXCESSIVE_UTF8_BYTES")
        if "EXCESSIVE_COMBINING_MARKS" in flags:
            quarantine_reasons.append("EXCESSIVE_COMBINING_MARKS")
        if "POSSIBLE_PII" in flags:
            quarantine_reasons.append("POSSIBLE_PII")

        if quarantine_reasons:
            status = "quarantine"
            exclusion_reason = quarantine_reasons[0]
        elif flags:
            status = "flag"
            exclusion_reason = None
        else:
            status = "accept"
            exclusion_reason = None

    if status not in QUALITY_STATUSES or not set(flags).issubset(QUALITY_FLAGS):
        raise AssertionError("quality policy produced an unknown status or flag")

    return TextQualityResult(
        quality_policy_version=QUALITY_POLICY_VERSION,
        text_clean=text_clean,
        character_count=character_count,
        utf8_byte_count=utf8_byte_count,
        control_character_count=control_character_count,
        zero_width_count=zero_width_count,
        max_combining_mark_run=max_combining_mark_run,
        url_count=url_count,
        url_ratio=url_ratio,
        repetition_ratio=repetition_ratio,
        quality_status=status,
        quality_flags=tuple(flags),
        exclusion_reason=exclusion_reason,
        was_normalized=was_normalized,
    )


def estimated_token_limit_exceeded(token_count: int, *, limit: int) -> bool:
    if token_count < 0:
        raise ValueError("token_count must not be negative")
    if limit < 1:
        raise ValueError("limit must be positive")
    return token_count > limit
