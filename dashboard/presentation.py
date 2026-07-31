"""Small presentation transforms with no business-rule calculations."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Mapping, Sequence


def _localized_decimal(value: object, language: str, decimals: int) -> str:
    number = value if isinstance(value, Decimal) else Decimal(str(value or 0))
    formatted = f"{number:,.{decimals}f}"
    if language == "it":
        return formatted.replace(",", "_").replace(".", ",").replace("_", ".")
    if language == "en":
        return formatted
    raise ValueError(f"Unsupported language: {language}")


def format_currency(value: object, language: str = "it") -> str:
    """Format a DuckDB decimal without changing its exact query value."""

    amount = _localized_decimal(value, language, 2)
    return f"€ {amount}" if language == "it" else f"€{amount}"


def format_integer(value: object, language: str = "it") -> str:
    """Format a whole number with locale-appropriate grouping."""

    return _localized_decimal(int(value or 0), language, 0)


def format_date(value: date | datetime, language: str = "it") -> str:
    """Format a calendar date for the selected dashboard language."""

    if language == "it":
        return value.strftime("%d/%m/%Y")
    if language == "en":
        return value.strftime("%b %d, %Y")
    raise ValueError(f"Unsupported language: {language}")


def format_datetime(value: datetime | None, language: str = "it") -> str:
    """Format a real ingestion timestamp, preserving its timezone label."""

    if value is None:
        return "—"
    timezone = value.tzname() or ""
    formatted_date = format_date(value, language)
    return f"{formatted_date} · {value:%H:%M} {timezone}".rstrip()


def format_period(
    start: date | None,
    end: date | None,
    language: str = "it",
) -> str:
    """Format a bounded date range or a neutral empty marker."""

    if start is None or end is None:
        return "—"
    return f"{format_date(start, language)} – {format_date(end, language)}"


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
