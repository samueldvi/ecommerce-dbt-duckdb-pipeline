"""Small presentation transforms with no business-rule calculations."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Mapping, Sequence


def format_currency(value: object) -> str:
    """Format a DuckDB decimal as an Italian-style euro amount."""

    amount = value if isinstance(value, Decimal) else Decimal(str(value or 0))
    formatted = f"{amount:,.2f}"
    return f"€ {formatted.replace(',', '_').replace('.', ',').replace('_', '.')}"


def format_integer(value: object) -> str:
    """Format a whole number with an Italian thousands separator."""

    return f"{int(value or 0):,}".replace(",", ".")


def chart_records(
    records: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Convert exact SQL types only at the visualization boundary."""

    normalized: list[dict[str, object]] = []
    for record in records:
        normalized.append(
            {
                key: (
                    float(value)
                    if isinstance(value, Decimal)
                    else value.isoformat()
                    if isinstance(value, (date, datetime))
                    else value
                )
                for key, value in record.items()
            }
        )
    return normalized
