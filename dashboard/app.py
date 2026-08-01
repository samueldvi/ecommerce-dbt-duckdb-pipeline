"""Business-first Streamlit interface backed by read-only DuckDB queries."""

from __future__ import annotations

from html import escape
from pathlib import Path

import streamlit as st

from .charts import (
    category_revenue_spec,
    horizontal_bar_spec,
    revenue_trend_spec,
    weekly_region_spec,
)
from .config import DashboardConfig, DashboardConfigurationError, load_dashboard_config
from .data import (
    DashboardDataError,
    DashboardFilters,
    DashboardMetadata,
    DashboardSnapshot,
    DatabaseFingerprint,
    DatabaseIncompleteError,
    database_fingerprint,
    load_dashboard_metadata,
    load_dashboard_snapshot,
)
from .i18n import SUPPORTED_LANGUAGES, Translator
from .presentation import (
    format_currency,
    format_datetime,
    format_integer,
    format_period,
)
from .theme import THEMES, ThemeTokens, theme_css, theme_tokens

SETUP_COMMANDS = """python scripts/generate_synthetic_data.py
python scripts/ingest_raw_data.py load-all
dbt build --profiles-dir .
streamlit run streamlit_app.py"""
ALL_FILTER_VALUES = "__all__"


@st.cache_data(show_spinner=False)
def _load_cached_metadata(
    database_path: str,
    dbt_schema: str,
    project_root: str,
    fingerprint: DatabaseFingerprint,
) -> DashboardMetadata:
    """Cache metadata and invalidate it when the DuckDB file changes."""

    del fingerprint
    return load_dashboard_metadata(
        DashboardConfig(
            database_path=Path(database_path),
            dbt_schema=dbt_schema,
            project_root=Path(project_root),
        )
    )


@st.cache_data(show_spinner=False)
def _load_cached_snapshot(
    database_path: str,
    dbt_schema: str,
    project_root: str,
    fingerprint: DatabaseFingerprint,
    filters: DashboardFilters,
) -> DashboardSnapshot:
    """Cache bounded aggregates by database fingerprint and active filters."""

    del fingerprint
    return load_dashboard_snapshot(
        DashboardConfig(
            database_path=Path(database_path),
            dbt_schema=dbt_schema,
            project_root=Path(project_root),
        ),
        filters,
    )


def _initialize_preferences() -> tuple[str, str]:
    st.session_state.setdefault("dashboard_language", "it")
    st.session_state.setdefault("dashboard_theme", "light")
    language = st.session_state["dashboard_language"]
    theme = st.session_state["dashboard_theme"]
    if language not in SUPPORTED_LANGUAGES:
        language = "it"
        st.session_state["dashboard_language"] = language
    if theme not in THEMES:
        theme = "light"
        st.session_state["dashboard_theme"] = theme
    return language, theme


def _store_widget_value(widget_key: str, state_key: str) -> None:
    st.session_state[state_key] = st.session_state[widget_key]


def _store_filter_widget_value(widget_key: str, state_key: str) -> None:
    value = st.session_state[widget_key]
    st.session_state[state_key] = None if value == ALL_FILTER_VALUES else value


def _render_preferences(translator: Translator) -> None:
    spacer, language_column, theme_column = st.columns([6, 1.4, 1.4])
    del spacer
    with language_column:
        st.selectbox(
            translator("language"),
            SUPPORTED_LANGUAGES,
            format_func=lambda value: translator(f"language_{value}"),
            key="dashboard_language",
        )
    with theme_column:
        theme_widget_key = f"dashboard_theme_widget_{translator.language}"
        st.session_state[theme_widget_key] = st.session_state["dashboard_theme"]
        st.selectbox(
            translator("theme"),
            tuple(THEMES),
            format_func=lambda value: translator(f"theme_{value}"),
            key=theme_widget_key,
            on_change=_store_widget_value,
            args=(theme_widget_key, "dashboard_theme"),
        )


def _render_data_error(
    config: DashboardConfig | None,
    error: Exception,
    translator: Translator,
) -> None:
    st.error(translator("error_title"))
    if isinstance(error, DatabaseIncompleteError):
        st.write(translator("error_incomplete"))
    elif isinstance(error, DashboardConfigurationError):
        st.write(translator("error_query"))
    else:
        st.write(translator("error_missing"))
    st.write(translator("error_action"))
    st.code(SETUP_COMMANDS, language="bash")
    if config is not None:
        with st.expander(translator("project_info")):
            st.write(
                translator(
                    "project_config",
                    database=escape(config.database_label),
                    schema=escape(config.dbt_schema),
                )
            )


def _initialize_filter_state(metadata: DashboardMetadata) -> None:
    if metadata.period_start is None or metadata.period_end is None:
        return
    current = st.session_state.get("dashboard_date_range")
    if not (
        isinstance(current, (tuple, list))
        and len(current) == 2
        and metadata.period_start <= current[0] <= current[1] <= metadata.period_end
    ):
        st.session_state["dashboard_date_range"] = (
            metadata.period_start,
            metadata.period_end,
        )
    if st.session_state.get("dashboard_category") not in (
        None,
        *metadata.categories,
    ):
        st.session_state["dashboard_category"] = None
    if st.session_state.get("dashboard_region") not in (None, *metadata.regions):
        st.session_state["dashboard_region"] = None


def _reset_filters(metadata: DashboardMetadata, language: str) -> None:
    st.session_state["dashboard_date_range"] = (
        metadata.period_start,
        metadata.period_end,
    )
    st.session_state["dashboard_category"] = None
    st.session_state["dashboard_region"] = None
    st.session_state[f"dashboard_category_widget_{language}"] = ALL_FILTER_VALUES
    st.session_state[f"dashboard_region_widget_{language}"] = ALL_FILTER_VALUES


def _render_filters(
    metadata: DashboardMetadata,
    translator: Translator,
) -> DashboardFilters | None:
    if metadata.period_start is None or metadata.period_end is None:
        return None
    st.subheader(translator("filters"))
    st.caption(translator("filter_help"))
    date_column, category_column, region_column, action_column = st.columns(
        [2.2, 1.4, 1.4, 1]
    )
    with date_column:
        selected_dates = st.date_input(
            translator("date_range"),
            min_value=metadata.period_start,
            max_value=metadata.period_end,
            key="dashboard_date_range",
        )
    with category_column:
        category_widget_key = (
            f"dashboard_category_widget_{translator.language}"
        )
        category_options = (ALL_FILTER_VALUES, *metadata.categories)
        st.session_state[category_widget_key] = (
            st.session_state.get("dashboard_category") or ALL_FILTER_VALUES
        )
        selected_category = st.selectbox(
            translator("category"),
            category_options,
            format_func=lambda value: (
                translator("all_categories")
                if value == ALL_FILTER_VALUES
                else value
            ),
            key=category_widget_key,
            on_change=_store_filter_widget_value,
            args=(category_widget_key, "dashboard_category"),
        )
    with region_column:
        region_widget_key = f"dashboard_region_widget_{translator.language}"
        region_options = (ALL_FILTER_VALUES, *metadata.regions)
        st.session_state[region_widget_key] = (
            st.session_state.get("dashboard_region") or ALL_FILTER_VALUES
        )
        selected_region = st.selectbox(
            translator("region"),
            region_options,
            format_func=lambda value: (
                translator("all_regions") if value == ALL_FILTER_VALUES else value
            ),
            key=region_widget_key,
            on_change=_store_filter_widget_value,
            args=(region_widget_key, "dashboard_region"),
        )
    with action_column:
        st.write("")
        st.button(
            translator("reset_filters"),
            on_click=_reset_filters,
            args=(metadata, translator.language),
            width="stretch",
        )
    if len(selected_dates) != 2:
        return None
    return DashboardFilters(
        start_date=selected_dates[0],
        end_date=selected_dates[1],
        category=(
            None if selected_category == ALL_FILTER_VALUES else selected_category
        ),
        region=None if selected_region == ALL_FILTER_VALUES else selected_region,
    )


def _render_header(
    metadata: DashboardMetadata,
    filters: DashboardFilters | None,
    translator: Translator,
) -> None:
    period_start = filters.start_date if filters else metadata.period_start
    period_end = filters.end_date if filters else metadata.period_end
    st.html(
        f"""
        <section class="dashboard-hero">
          <div class="dashboard-eyebrow">{escape(translator('overview_eyebrow'))}</div>
          <h1>{escape(translator('title'))}</h1>
          <p>{escape(translator('subtitle'))}</p>
          <div class="dashboard-meta">
            <span><strong>{escape(translator('period'))}:</strong>
              {escape(format_period(period_start, period_end, translator.language))}</span>
            <span><strong>{escape(translator('last_update'))}:</strong>
              {escape(format_datetime(metadata.last_ingested_at, translator.language))}</span>
          </div>
        </section>
        """
    )


def _render_kpis(snapshot: DashboardSnapshot, translator: Translator) -> bool:
    if not snapshot.kpis.rows:
        st.info(translator("empty_data"))
        return False
    kpis = snapshot.kpis.records()[0]
    if kpis["order_lines"] == 0:
        st.info(translator("empty_chart"))
    cards = (
        ("kpi_revenue", format_currency(kpis["total_revenue"], translator.language)),
        ("kpi_orders", format_integer(kpis["distinct_orders"], translator.language)),
        (
            "kpi_average_order",
            format_currency(
                kpis["average_revenue_order_value"], translator.language
            ),
        ),
        ("kpi_customers", format_integer(kpis["known_customers"], translator.language)),
    )
    card_html = "".join(
        "<article class='kpi-card'>"
        f"<div class='kpi-label'>{escape(translator(label))}</div>"
        f"<div class='kpi-value'>{escape(value)}</div>"
        "</article>"
        for label, value in cards
    )
    st.html(f"<section class='kpi-grid'>{card_html}</section>")
    st.caption(translator("kpi_note"))
    return kpis["order_lines"] > 0


def _chart_or_empty(
    spec: dict[str, object],
    has_rows: bool,
    translator: Translator,
) -> None:
    if not has_rows:
        st.info(translator("empty_chart"))
        return
    st.vega_lite_chart(spec, theme=None, width="stretch")


def _render_sales(
    snapshot: DashboardSnapshot,
    translator: Translator,
    tokens: ThemeTokens,
) -> None:
    st.header(translator("sales_section"))
    st.subheader(translator("sales_trend_title"))
    st.caption(translator("sales_trend_caption"))
    trend_records = snapshot.revenue_trend.records()
    _chart_or_empty(
        revenue_trend_spec(
            trend_records,
            description=translator("sales_trend_caption"),
            date_label=translator("axis_date"),
            revenue_label=translator("axis_revenue"),
            tokens=tokens,
        ),
        bool(trend_records),
        translator,
    )

    st.subheader(translator("category_title"))
    st.caption(translator("category_caption"))
    category_records = snapshot.revenue_by_category_month.records()
    _chart_or_empty(
        category_revenue_spec(
            category_records,
            description=translator("category_caption"),
            revenue_label=translator("axis_revenue"),
            category_label=translator("axis_category"),
            tokens=tokens,
        ),
        bool(category_records),
        translator,
    )
    with st.expander(translator("category_ranking")):
        ranking = [
            {
                translator("month"): record["revenue_month"],
                translator("ranking_position"): record["revenue_position"],
                translator("category"): record["category"],
                translator("revenue"): record["total_revenue"],
            }
            for record in snapshot.category_ranking.records()
        ]
        if ranking:
            st.dataframe(ranking, hide_index=True, width="stretch")
        else:
            st.info(translator("empty_chart"))


def _render_customers_regions(
    snapshot: DashboardSnapshot,
    translator: Translator,
    tokens: ThemeTokens,
) -> None:
    st.header(translator("customers_territory"))
    customer_column, region_column = st.columns(2)
    with customer_column:
        st.subheader(translator("top_customers_title"))
        st.caption(translator("top_customers_caption"))
        customers = snapshot.top_customers.records()
        _chart_or_empty(
            horizontal_bar_spec(
                customers,
                category_field="customer_name",
                value_field="total_spend",
                category_label=translator("axis_customer"),
                value_label=translator("axis_revenue"),
                description=translator("top_customers_caption"),
                tokens=tokens,
            ),
            bool(customers),
            translator,
        )
    with region_column:
        st.subheader(translator("regions_title"))
        st.caption(translator("regions_caption"))
        regions = snapshot.region_summary.records()
        _chart_or_empty(
            horizontal_bar_spec(
                regions,
                category_field="customer_region",
                value_field="completed_order_count",
                category_label=translator("axis_region"),
                value_label=translator("axis_orders"),
                description=translator("regions_caption"),
                tokens=tokens,
            ),
            bool(regions),
            translator,
        )
    with st.expander(translator("weekly_detail")):
        weekly = snapshot.orders_by_region_week.records()
        for record in weekly:
            record["year_week"] = (
                f"{record['order_year']}-W{int(record['iso_week_number']):02d}"
            )
        _chart_or_empty(
            weekly_region_spec(
                weekly,
                description=translator("weekly_detail"),
                week_label=translator("axis_date"),
                region_label=translator("axis_region"),
                orders_label=translator("axis_orders"),
                tokens=tokens,
            ),
            bool(weekly),
            translator,
        )


def _quality_rows(
    snapshot: DashboardSnapshot,
    translator: Translator,
) -> list[dict[str, object]]:
    if not snapshot.data_quality.rows:
        return []
    quality = snapshot.data_quality.records()[0]
    mappings = (
        ("duplicate_keys", "duplicate_current_keys"),
        ("invalid_channels", "invalid_sales_channels"),
        ("invalid_statuses", "invalid_order_statuses"),
        ("null_prices", "null_unit_prices"),
        ("negative_prices", "negative_unit_prices"),
        ("non_positive_quantities", "non_positive_quantities"),
        ("cancelled_lines", "cancelled_lines"),
        ("unknown_customers", "unknown_customers"),
        ("unknown_products", "unknown_products"),
        ("invalid_economic_lines", "economically_invalid_lines"),
    )
    return [
        {
            translator("quality_metric"): translator(label),
            translator("quality_records"): quality[column],
        }
        for label, column in mappings
    ]


def _render_data_status(
    snapshot: DashboardSnapshot,
    metadata: DashboardMetadata,
    translator: Translator,
) -> None:
    st.header(translator("data_status"))
    quality = snapshot.data_quality.records()[0] if snapshot.data_quality.rows else {}
    structural_ok = bool(quality) and (
        quality["duplicate_current_keys"] == 0
        and quality["invalid_sales_channels"] == 0
    )
    anomaly_columns = (
        "invalid_order_statuses",
        "null_unit_prices",
        "negative_unit_prices",
        "non_positive_quantities",
        "cancelled_lines",
        "unknown_customers",
        "unknown_products",
    )
    anomaly_count = sum(bool(quality.get(column, 0)) for column in anomaly_columns)
    status_class = "status-ready" if structural_ok else "status-attention"
    status_label = "status_ready" if structural_ok else "status_attention"
    status_cards = (
        (
            translator("data_status"),
            translator(status_label),
            status_class,
        ),
        (
            translator("checks_passed"),
            "2 / 2" if structural_ok else "0 / 2",
            status_class,
        ),
        (
            translator("anomaly_types"),
            format_integer(anomaly_count, translator.language),
            "",
        ),
        (
            translator("last_update"),
            format_datetime(metadata.last_ingested_at, translator.language),
            "",
        ),
    )
    cards = "".join(
        "<article class='status-card'>"
        f"<div class='status-label'>{escape(label)}</div>"
        f"<div class='status-value {css_class}'>{escape(value)}</div>"
        "</article>"
        for label, value, css_class in status_cards
    )
    st.html(f"<section class='status-grid'>{cards}</section>")
    st.caption(translator("data_status_note"))

    with st.expander(translator("load_history")):
        batch_rows = []
        for record in snapshot.incremental_loads.records():
            sequence = int(record["batch_sequence"])
            label_key = (
                "batch_late" if sequence == 4 else f"batch_run_{sequence}"
            )
            batch_rows.append(
                {
                    translator("batch"): translator(label_key),
                    translator("processed_records"): record["received_rows"],
                    translator("new_keys"): record["new_keys"],
                    translator("updated_records"): record["corrections"],
                    translator("cumulative_keys"): record["cumulative_keys"],
                }
            )
        if batch_rows:
            st.dataframe(batch_rows, hide_index=True, width="stretch")
        else:
            st.info(translator("batch_empty"))
    with st.expander(translator("quality_details")):
        rows = _quality_rows(snapshot, translator)
        if rows:
            st.dataframe(rows, hide_index=True, width="stretch")
        else:
            st.info(translator("quality_empty"))


def _render_project_info(config: DashboardConfig, translator: Translator) -> None:
    with st.expander(translator("project_info")):
        st.write(translator("project_architecture"))
        st.write(translator("project_incremental"))
        st.write(translator("project_limit"))
        st.write(translator("project_transfer"))
        st.caption(
            translator(
                "project_config",
                database=config.database_label,
                schema=config.dbt_schema,
            )
        )


def run() -> None:
    """Render the dashboard or a controlled, localized data error."""

    st.set_page_config(
        page_title="E-commerce analytics",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    language, theme = _initialize_preferences()
    translator = Translator(language)
    tokens = theme_tokens(theme)
    st.html(theme_css(tokens))
    _render_preferences(translator)

    config: DashboardConfig | None = None
    try:
        config = load_dashboard_config()
        fingerprint = database_fingerprint(config.database_path)
        metadata = _load_cached_metadata(
            str(config.database_path),
            config.dbt_schema,
            str(config.project_root),
            fingerprint,
        )
        _initialize_filter_state(metadata)
        selected_range = st.session_state.get("dashboard_date_range", ())
        active_filters = (
            DashboardFilters(
                selected_range[0],
                selected_range[1],
                st.session_state.get("dashboard_category"),
                st.session_state.get("dashboard_region"),
            )
            if len(selected_range) == 2
            else None
        )
        _render_header(metadata, active_filters, translator)
        filters = _render_filters(metadata, translator)
        if filters is None:
            st.info(translator("empty_data"))
            return
        with st.spinner(translator("loading")):
            snapshot = _load_cached_snapshot(
                str(config.database_path),
                config.dbt_schema,
                str(config.project_root),
                fingerprint,
                filters,
            )
    except (DashboardConfigurationError, DashboardDataError) as error:
        _render_data_error(config, error, translator)
        return
    except Exception:
        st.error(translator("error_title"))
        st.write(translator("error_unexpected"))
        return

    has_data = _render_kpis(snapshot, translator)
    if has_data:
        _render_sales(snapshot, translator, tokens)
        _render_customers_regions(snapshot, translator, tokens)
    _render_data_status(snapshot, metadata, translator)
    _render_project_info(config, translator)
