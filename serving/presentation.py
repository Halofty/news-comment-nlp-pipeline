from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def format_tones(value: Any) -> str:
    """Render JSONB tone objects as compact text for table cells."""
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or not value:
        return "-"
    rendered: list[str] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        name = str(item.get("tone") or "unknown")
        try:
            share = f"{float(item.get('share') or 0) * 100:.0f}%"
        except (TypeError, ValueError):
            share = "-"
        drivers = item.get("drivers")
        driver_text = ""
        if isinstance(drivers, Sequence) and not isinstance(drivers, (str, bytes)):
            labels = [str(driver).strip() for driver in drivers if str(driver).strip()]
            if labels:
                driver_text = f" ({'; '.join(labels)})"
        rendered.append(f"{name} {share}{driver_text}")
    return " · ".join(rendered) if rendered else "-"


def format_analysis_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Prepare analysis records for Streamlit without changing repository output."""
    formatted: list[dict[str, Any]] = []
    for row in rows:
        display_row = dict(row)
        display_row["positive_tones"] = format_tones(row.get("positive_tones"))
        display_row["negative_tones"] = format_tones(row.get("negative_tones"))
        formatted.append(display_row)
    return formatted
