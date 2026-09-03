from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Any


QUALITY_GATE_VERSION = "llm-label-quality-v1"
_META_PHRASES = re.compile(
    r"\b(?:keep short|return only|requested json|do not infer|system prompt)\b",
    re.IGNORECASE,
)
_LONG_ALNUM_RUN = re.compile(r"[A-Za-z0-9]{24,}")
_ALLOWED_LABEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9 &'()+,./:-]*\Z")
_WORD = re.compile(r"[A-Za-z0-9]+(?:'[A-Za-z0-9]+)?")
_PUNCTUATION = str.maketrans(
    {
        "\u2018": "'",
        "\u2019": "'",
        "\u2013": "-",
        "\u2014": "-",
        "\u00a0": " ",
    }
)


def _normalize(value: str) -> tuple[str, bool]:
    normalized = unicodedata.normalize("NFKC", value).translate(_PUNCTUATION)
    without_controls = "".join(
        character
        for character in normalized
        if not unicodedata.category(character).startswith("C")
    )
    collapsed = " ".join(without_controls.split())
    return collapsed, collapsed != value


def clean_label(value: str, *, field: str) -> tuple[str | None, tuple[str, ...]]:
    if field not in {"topics", "keywords"}:
        raise ValueError("field must be topics or keywords")
    cleaned, changed = _normalize(value)
    reasons: list[str] = []
    if changed:
        reasons.append("unicode_or_whitespace_normalized")
    if not cleaned:
        return None, tuple(reasons + ["empty_after_normalization"])
    if any(ord(character) > 127 for character in cleaned):
        return None, tuple(reasons + ["non_ascii_character"])
    if not _ALLOWED_LABEL.fullmatch(cleaned):
        return None, tuple(reasons + ["disallowed_character"])
    if _META_PHRASES.search(cleaned):
        return None, tuple(reasons + ["prompt_meta_phrase"])
    if _LONG_ALNUM_RUN.search(cleaned):
        return None, tuple(reasons + ["suspicious_alphanumeric_run"])
    maximum_words = 8 if field == "topics" else 6
    if len(_WORD.findall(cleaned)) > maximum_words:
        return None, tuple(reasons + ["too_many_words"])
    return cleaned, tuple(reasons)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def apply_quality_gate(
    *, input_path: str | Path, output_path: str | Path, report_path: str | Path
) -> dict[str, Any]:
    source = Path(input_path)
    target = Path(output_path)
    report_target = Path(report_path)
    rows = [
        json.loads(line)
        for line in source.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    reason_counts: Counter[str] = Counter()
    input_labels = output_labels = removed_labels = modified_rows = 0
    output: list[dict[str, Any]] = []
    for row in rows:
        cleaned_row = dict(row)
        modified_fields: set[str] = set()
        row_removed = 0
        for field in ("topics", "keywords"):
            values = row.get(field)
            if not isinstance(values, list) or not all(
                isinstance(value, str) for value in values
            ):
                raise ValueError(f"{field} must be a list of strings")
            input_labels += len(values)
            cleaned_values: list[str] = []
            seen: set[str] = set()
            for value in values:
                cleaned, reasons = clean_label(value, field=field)
                reason_counts.update(reasons)
                if cleaned is None:
                    removed_labels += 1
                    row_removed += 1
                    modified_fields.add(field)
                    continue
                key = cleaned.casefold()
                if key in seen:
                    reason_counts["duplicate_after_normalization"] += 1
                    removed_labels += 1
                    row_removed += 1
                    modified_fields.add(field)
                    continue
                seen.add(key)
                cleaned_values.append(cleaned)
                if cleaned != value:
                    modified_fields.add(field)
            cleaned_row[field] = cleaned_values
            output_labels += len(cleaned_values)

        summary = row.get("summary")
        if not isinstance(summary, str):
            raise ValueError("summary must be a string")
        cleaned_summary, summary_changed = _normalize(summary)
        if not cleaned_summary:
            raise ValueError("summary is empty after normalization")
        if summary_changed:
            cleaned_row["summary"] = cleaned_summary
            modified_fields.add("summary")
            reason_counts["summary_unicode_or_whitespace_normalized"] += 1

        status = "modified" if modified_fields else "passed"
        if modified_fields:
            modified_rows += 1
        cleaned_row["quality_gate"] = {
            "version": QUALITY_GATE_VERSION,
            "status": status,
            "modified_fields": sorted(modified_fields),
            "removed_label_count": row_removed,
        }
        output.append(cleaned_row)

    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8", newline="\n") as handle:
        for row in output:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
            handle.write("\n")
    report = {
        "quality_gate_version": QUALITY_GATE_VERSION,
        "input_path": str(source),
        "output_path": str(target),
        "input_sha256": _sha256(source),
        "output_sha256": _sha256(target),
        "input_rows": len(rows),
        "output_rows": len(output),
        "modified_rows": modified_rows,
        "passed_rows": len(rows) - modified_rows,
        "input_labels": input_labels,
        "output_labels": output_labels,
        "removed_labels": removed_labels,
        "reason_counts": dict(sorted(reason_counts.items())),
    }
    report_target.parent.mkdir(parents=True, exist_ok=True)
    report_target.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return report

