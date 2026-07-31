"""Pure Vega-Lite specifications for the dashboard visual layer."""

from __future__ import annotations

from typing import Mapping, Sequence

from .presentation import chart_records
from .theme import ThemeTokens


def _base_spec(
    records: Sequence[Mapping[str, object]],
    *,
    description: str,
    tokens: ThemeTokens,
) -> dict[str, object]:
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
        "description": description,
        "data": {"values": chart_records(records)},
        "background": tokens.surface,
        "config": {
            "view": {"stroke": None},
            "axis": {
                "domainColor": tokens.border,
                "gridColor": tokens.grid,
                "labelColor": tokens.muted,
                "titleColor": tokens.text,
                "titleFontWeight": 600,
            },
            "legend": {
                "labelColor": tokens.muted,
                "titleColor": tokens.text,
            },
        },
    }


def revenue_trend_spec(
    records: Sequence[Mapping[str, object]],
    *,
    description: str,
    date_label: str,
    revenue_label: str,
    tokens: ThemeTokens,
) -> dict[str, object]:
    spec = _base_spec(records, description=description, tokens=tokens)
    spec.update(
        {
            "height": 300,
            "mark": {
                "type": "line",
                "color": tokens.primary,
                "point": {"filled": True, "size": 48},
                "strokeWidth": 2.5,
            },
            "encoding": {
                "x": {
                    "field": "order_date",
                    "type": "temporal",
                    "title": date_label,
                },
                "y": {
                    "field": "total_revenue",
                    "type": "quantitative",
                    "title": revenue_label,
                    "scale": {"zero": True},
                },
                "tooltip": [
                    {"field": "order_date", "type": "temporal", "title": date_label},
                    {
                        "field": "total_revenue",
                        "type": "quantitative",
                        "title": revenue_label,
                        "format": ",.2f",
                    },
                ],
            },
        }
    )
    return spec


def category_revenue_spec(
    records: Sequence[Mapping[str, object]],
    *,
    description: str,
    revenue_label: str,
    category_label: str,
    tokens: ThemeTokens,
) -> dict[str, object]:
    spec = _base_spec(records, description=description, tokens=tokens)
    spec.update(
        {
            "height": 300,
            "mark": {"type": "bar", "cornerRadiusEnd": 3},
            "encoding": {
                "x": {
                    "aggregate": "sum",
                    "field": "total_revenue",
                    "type": "quantitative",
                    "title": revenue_label,
                    "scale": {"zero": True},
                },
                "y": {
                    "field": "category",
                    "type": "nominal",
                    "title": category_label,
                    "sort": "-x",
                },
                "color": {
                    "field": "category",
                    "type": "nominal",
                    "legend": None,
                    "scale": {"range": list(tokens.category_palette)},
                },
                "tooltip": [
                    {"field": "category", "type": "nominal", "title": category_label},
                    {
                        "aggregate": "sum",
                        "field": "total_revenue",
                        "type": "quantitative",
                        "title": revenue_label,
                        "format": ",.2f",
                    },
                ],
            },
        }
    )
    return spec


def horizontal_bar_spec(
    records: Sequence[Mapping[str, object]],
    *,
    category_field: str,
    value_field: str,
    category_label: str,
    value_label: str,
    description: str,
    tokens: ThemeTokens,
) -> dict[str, object]:
    spec = _base_spec(records, description=description, tokens=tokens)
    spec.update(
        {
            "height": 300,
            "mark": {
                "type": "bar",
                "color": tokens.primary,
                "cornerRadiusEnd": 3,
            },
            "encoding": {
                "x": {
                    "field": value_field,
                    "type": "quantitative",
                    "title": value_label,
                    "scale": {"zero": True},
                },
                "y": {
                    "field": category_field,
                    "type": "nominal",
                    "title": category_label,
                    "sort": "-x",
                },
                "tooltip": [
                    {
                        "field": category_field,
                        "type": "nominal",
                        "title": category_label,
                    },
                    {
                        "field": value_field,
                        "type": "quantitative",
                        "title": value_label,
                        "format": ",.2f",
                    },
                ],
            },
        }
    )
    return spec


def weekly_region_spec(
    records: Sequence[Mapping[str, object]],
    *,
    description: str,
    week_label: str,
    region_label: str,
    orders_label: str,
    tokens: ThemeTokens,
) -> dict[str, object]:
    spec = _base_spec(records, description=description, tokens=tokens)
    spec.update(
        {
            "height": 320,
            "mark": {"type": "line", "point": True, "strokeWidth": 2},
            "encoding": {
                "x": {
                    "field": "year_week",
                    "type": "ordinal",
                    "title": week_label,
                },
                "y": {
                    "field": "completed_order_count",
                    "type": "quantitative",
                    "title": orders_label,
                    "scale": {"zero": True},
                },
                "color": {
                    "field": "customer_region",
                    "type": "nominal",
                    "title": region_label,
                    "scale": {"range": list(tokens.category_palette)},
                },
                "tooltip": [
                    {"field": "year_week", "type": "ordinal", "title": week_label},
                    {
                        "field": "customer_region",
                        "type": "nominal",
                        "title": region_label,
                    },
                    {
                        "field": "completed_order_count",
                        "type": "quantitative",
                        "title": orders_label,
                    },
                ],
            },
        }
    )
    return spec
