from __future__ import annotations

from decimal import Decimal

from observability.models import PriceSchedule


LONG_CONTEXT_THRESHOLD = 272_000


def gpt_5_6_luna_batch_pricing(input_tokens: int) -> PriceSchedule:
    """Return the pricing snapshot used for this project's Batch observations."""
    if input_tokens < 0:
        raise ValueError("input_tokens must not be negative")
    long_context = input_tokens > LONG_CONTEXT_THRESHOLD
    return PriceSchedule(
        version=(
            "gpt-5.6-luna-batch-long-context-2026-09-03"
            if long_context
            else "gpt-5.6-luna-batch-2026-09-03"
        ),
        effective_date="2026-09-03",
        input_per_million=Decimal("0.20" if long_context else "0.10"),
        cached_input_per_million=Decimal("0.02" if long_context else "0.01"),
        output_per_million=Decimal("0.90" if long_context else "0.60"),
    )
