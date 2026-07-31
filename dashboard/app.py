"""Single-page Streamlit interface backed by read-only DuckDB queries."""

from __future__ import annotations

from pathlib import Path

import streamlit as st

from .config import DashboardConfig, DashboardConfigurationError, load_dashboard_config
from .data import (
    DashboardDataError,
    DashboardSnapshot,
    DatabaseFingerprint,
    DatabaseIncompleteError,
    database_fingerprint,
    load_dashboard_snapshot,
)
from .presentation import chart_records, format_currency, format_integer

BOOTSTRAP_COMMANDS = """python scripts/generate_synthetic_data.py
python scripts/ingest_raw_data.py load-all
dbt build --profiles-dir .
streamlit run streamlit_app.py"""


@st.cache_data(show_spinner=False)
def _load_cached_snapshot(
    database_path: str,
    dbt_schema: str,
    project_root: str,
    fingerprint: DatabaseFingerprint,
) -> DashboardSnapshot:
    """Cache aggregates and invalidate them with the database fingerprint."""

    del fingerprint
    return load_dashboard_snapshot(
        DashboardConfig(
            database_path=Path(database_path),
            dbt_schema=dbt_schema,
            project_root=Path(project_root),
        )
    )


def _render_bootstrap_error(
    config: DashboardConfig | None,
    error: Exception,
) -> None:
    st.error("La demo non può leggere un database analitico completo.")
    if config is not None:
        st.write(f"Risorsa configurata: `{config.database_label}`")
    if isinstance(error, DatabaseIncompleteError):
        st.write("Risorse mancanti: " + ", ".join(error.missing_resources))
    else:
        st.write(
            "Il file manca, è illeggibile oppure non è un database DuckDB "
            "valido."
        )
    st.write("Dalla root del repository eseguire:")
    st.code(BOOTSTRAP_COMMANDS, language="bash")


def _render_overview(config: DashboardConfig) -> None:
    st.title("E-commerce Incremental Analytics")
    st.write(
        "Una demo read-only della pipeline che acquisisce batch CSV, conserva "
        "le versioni raw e pubblica uno star schema analitico con dbt."
    )
    st.markdown(
        "**CSV batch → Raw transazionale → DuckDB → dbt staging / "
        "intermediate / marts → analisi → Streamlit**"
    )
    st.caption(
        "Python 3.12 · DuckDB · dbt Core · Streamlit · "
        f"database: {config.database_label} · schema: {config.dbt_schema}"
    )
    with st.expander("Come funziona l'incrementalità"):
        st.write(
            "Run 1, Run 2 e Run 3 aggiungono chiavi nuove su tre finestre "
            "commerciali. Il batch late-arriving contiene fatti storici "
            "arrivati dopo e correction prevalenti. Il manifest rende il "
            "replay dello stesso file un no-op; dbt mantiene una sola versione "
            "corrente per order_line_id."
        )


def _render_kpis(snapshot: DashboardSnapshot) -> None:
    st.header("KPI principali")
    if not snapshot.kpis.rows:
        st.info("Il mart è disponibile ma non contiene ancora righe analitiche.")
        return
    kpis = snapshot.kpis.records()[0]
    primary_columns = st.columns(3)
    primary_columns[0].metric(
        "Fatturato valido", format_currency(kpis["total_revenue"])
    )
    primary_columns[1].metric(
        "Ordini distinti", format_integer(kpis["distinct_orders"])
    )
    primary_columns[2].metric(
        "Valore medio ordine valido",
        format_currency(kpis["average_revenue_order_value"]),
        help="Fatturato valido diviso per gli ordini con almeno una riga contribuente.",
    )
    secondary_columns = st.columns(2)
    secondary_columns[0].metric(
        "Clienti noti", format_integer(kpis["known_customers"])
    )
    secondary_columns[1].metric(
        "Righe correnti", format_integer(kpis["order_lines"])
    )
    st.caption(
        "Il fatturato usa esclusivamente `fct_order_lines.revenue`; la demo "
        "non ricostruisce le regole di validità economica."
    )


def _chart_or_empty(
    records: list[dict[str, object]],
    *,
    x: str,
    y: str,
    x_label: str,
    y_label: str,
    color: str | None = None,
    horizontal: bool = False,
) -> None:
    if not records:
        st.info("Nessun dato disponibile per questa analisi.")
        return
    st.bar_chart(
        chart_records(records),
        x=x,
        y=y,
        x_label=x_label,
        y_label=y_label,
        color=color,
        horizontal=horizontal,
        sort=False,
        stack=False,
        width="stretch",
        height=340,
    )


def _render_analytics(snapshot: DashboardSnapshot) -> None:
    st.header("Analisi Gold")
    left, right = st.columns(2)
    with left:
        st.subheader("Fatturato mensile per categoria")
        st.caption("Euro, aggregati per mese commerciale e categoria prodotto.")
        _chart_or_empty(
            snapshot.revenue_by_category_month.records(),
            x="revenue_month",
            y="total_revenue",
            x_label="Mese",
            y_label="Fatturato (€)",
            color="category",
        )
    with right:
        st.subheader("Top 10 clienti per spesa")
        st.caption("Euro di fatturato valido sull'intero periodo.")
        _chart_or_empty(
            snapshot.top_customers.records(),
            x="total_spend",
            y="customer_name",
            x_label="Spesa totale (€)",
            y_label="Cliente",
            horizontal=True,
        )

    left, right = st.columns(2)
    with left:
        st.subheader("Ordini completed per regione e settimana")
        st.caption("Conteggio distinto degli ordini con ricavo valido.")
        weekly_records = snapshot.orders_by_region_week.records()
        for record in weekly_records:
            record["year_week"] = (
                f"{record['order_year']}-W{int(record['iso_week_number']):02d}"
            )
        _chart_or_empty(
            weekly_records,
            x="year_week",
            y="completed_order_count",
            x_label="Settimana ISO",
            y_label="Ordini distinti",
            color="customer_region",
        )
    with right:
        st.subheader("Ranking mensile delle categorie")
        st.caption("Dense rank per fatturato valido; posizione 1 = maggiore ricavo.")
        ranking_records = snapshot.category_ranking.records()
        for record in ranking_records:
            record["ranked_category"] = (
                f"#{record['revenue_position']} · {record['category']}"
            )
        _chart_or_empty(
            ranking_records,
            x="total_revenue",
            y="ranked_category",
            x_label="Fatturato (€)",
            y_label="Posizione e categoria",
            horizontal=True,
        )


def _render_incremental(snapshot: DashboardSnapshot) -> None:
    st.header("Caricamenti incrementali")
    records = snapshot.incremental_loads.records()
    if not records:
        st.info("Il manifest non contiene ancora batch di righe ordine.")
    else:
        table_records = []
        for record in records:
            table_records.append(
                {
                    "Batch": record["batch_label"],
                    "Righe ricevute": record["received_rows"],
                    "Nuove chiavi": record["new_keys"],
                    "Correction": record["corrections"],
                    "Chiavi cumulative": record["cumulative_keys"],
                }
            )
        st.dataframe(table_records, hide_index=True, width="stretch")
    st.write(
        "I late-arriving facts hanno una data commerciale storica ma arrivano "
        "in un batch successivo. Le correction sono versioni più recenti di "
        "chiavi Run 1 e non aumentano il numero di chiavi correnti. I numeri "
        "sopra descrivono lo stato persistito, non un monitoraggio live."
    )


def _render_data_quality(snapshot: DashboardSnapshot) -> None:
    st.header("Data Quality")
    if not snapshot.data_quality.rows:
        st.info("Nessuna metrica di qualità disponibile.")
        return
    quality = snapshot.data_quality.records()[0]
    structural_ok = (
        quality["duplicate_current_keys"] == 0
        and quality["invalid_sales_channels"] == 0
    )
    if structural_ok:
        st.success(
            "Grain corrente e dominio sales_channel conformi; le anomalie "
            "funzionali restano osservabili sotto."
        )
    else:
        st.warning("Sono presenti violazioni strutturali nel mart corrente.")

    structural, observed = st.columns(2)
    with structural:
        st.subheader("Vincoli correnti")
        st.metric("Chiavi duplicate", format_integer(quality["duplicate_current_keys"]))
        st.metric(
            "Canali fuori dominio",
            format_integer(quality["invalid_sales_channels"]),
        )
        st.metric(
            "Stati ordine fuori dominio",
            format_integer(quality["invalid_order_statuses"]),
        )
    with observed:
        st.subheader("Anomalie conservate")
        anomaly_rows = [
            {"Metrica": "Prezzi nulli", "Righe": quality["null_unit_prices"]},
            {"Metrica": "Prezzi negativi", "Righe": quality["negative_unit_prices"]},
            {
                "Metrica": "Quantità non positive",
                "Righe": quality["non_positive_quantities"],
            },
            {"Metrica": "Righe cancellate", "Righe": quality["cancelled_lines"]},
            {"Metrica": "Cliente Unknown", "Righe": quality["unknown_customers"]},
            {"Metrica": "Prodotto Unknown", "Righe": quality["unknown_products"]},
            {
                "Metrica": "Non valide economicamente",
                "Righe": quality["economically_invalid_lines"],
            },
        ]
        st.dataframe(anomaly_rows, hide_index=True, width="stretch")

    st.caption(
        "Queste metriche sono query descrittive sul mart. I controlli di "
        "schema/checksum appartengono all'ingestion; i test dbt vengono "
        "eseguiti con `dbt build` e non sono presentati come stato live."
    )


def _render_limits() -> None:
    st.header("Limiti dichiarati")
    st.info(
        "DuckDB è utilizzato come motore analitico locale, leggero e "
        "production-like per questa implementazione. Non sostituisce una "
        "piattaforma enterprise distribuita o multiwriter."
    )
    st.write(
        "Il repository include un piano concettuale verso Databricks e Delta "
        "Lake; non rappresenta un deployment cloud già realizzato."
    )


def run() -> None:
    """Render the complete dashboard or a controlled bootstrap message."""

    st.set_page_config(
        page_title="E-commerce Analytics",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    config: DashboardConfig | None = None
    try:
        config = load_dashboard_config()
        fingerprint = database_fingerprint(config.database_path)
        snapshot = _load_cached_snapshot(
            str(config.database_path),
            config.dbt_schema,
            str(config.project_root),
            fingerprint,
        )
    except (DashboardConfigurationError, DashboardDataError) as error:
        _render_bootstrap_error(config, error)
        return

    _render_overview(config)
    _render_kpis(snapshot)
    _render_analytics(snapshot)
    _render_incremental(snapshot)
    _render_data_quality(snapshot)
    _render_limits()
