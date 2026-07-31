"""Centralized Italian and English copy for the dashboard."""

from __future__ import annotations

from dataclasses import dataclass

SUPPORTED_LANGUAGES = ("it", "en")

TRANSLATIONS: dict[str, dict[str, str]] = {
    "it": {
        "language": "Lingua",
        "theme": "Tema",
        "language_it": "Italiano",
        "language_en": "English",
        "theme_light": "Chiaro",
        "theme_dark": "Scuro",
        "overview_eyebrow": "E-commerce analytics",
        "title": "Panoramica vendite e-commerce",
        "subtitle": "Performance commerciali, clienti e affidabilità dei dati in un'unica vista.",
        "period": "Periodo",
        "last_update": "Ultimo aggiornamento",
        "filters": "Filtri",
        "date_range": "Intervallo",
        "category": "Categoria",
        "region": "Regione",
        "all_categories": "Tutte le categorie",
        "all_regions": "Tutte le regioni",
        "reset_filters": "Ripristina filtri",
        "filter_help": "Restringi tutte le metriche e le analisi commerciali.",
        "kpi_revenue": "Fatturato valido",
        "kpi_orders": "Ordini",
        "kpi_average_order": "Valore medio ordine",
        "kpi_customers": "Clienti",
        "kpi_note": "Le metriche riutilizzano il fatturato già validato nella fact.",
        "sales_section": "Andamento delle vendite",
        "sales_trend_title": "Fatturato giornaliero",
        "sales_trend_caption": "Evoluzione del fatturato valido nel periodo selezionato.",
        "category_title": "Fatturato per categoria",
        "category_caption": "Contributo delle categorie al fatturato selezionato.",
        "category_ranking": "Dettaglio del ranking categorie",
        "customers_territory": "Clienti e territorio",
        "top_customers_title": "Top 10 clienti per spesa",
        "top_customers_caption": "Clienti con il maggiore fatturato valido nel periodo.",
        "regions_title": "Ordini per regione",
        "regions_caption": "Ordini distinti con ricavo valido per area geografica.",
        "weekly_detail": "Andamento settimanale per regione",
        "data_status": "Stato dei dati",
        "status_ready": "Dati verificati",
        "status_attention": "Verifica richiesta",
        "checks_passed": "Controlli chiave",
        "anomaly_types": "Tipi di anomalia osservati",
        "load_history": "Dettaglio dei caricamenti",
        "quality_details": "Dettaglio delle anomalie",
        "batch_empty": "Nessun caricamento di righe ordine registrato.",
        "quality_empty": "Nessuna metrica di qualità disponibile.",
        "data_status_note": (
            "Lo stato descrive i dati persistiti, non un monitoraggio in "
            "tempo reale."
        ),
        "project_info": "Informazioni sul progetto",
        "project_architecture": (
            "I dati sintetici vengono acquisiti in modo transazionale e "
            "trasformati in un modello analitico dimensionale."
        ),
        "project_limit": (
            "DuckDB è il motore analitico locale del progetto. Non sostituisce "
            "una piattaforma distribuita o progettata per scritture concorrenti."
        ),
        "project_config": "Database: {database} · schema: {schema}",
        "project_incremental": (
            "I caricamenti ordinari aggiungono nuove chiavi; il batch "
            "late-arriving applica 10 inserimenti e 5 correzioni in modo "
            "idempotente."
        ),
        "project_transfer": (
            "Il repository include un percorso concettuale verso Databricks "
            "e Delta Lake; non rappresenta un deployment cloud già attivo."
        ),
        "empty_chart": "Nessun dato disponibile con i filtri selezionati.",
        "empty_data": "La struttura dati è disponibile, ma non contiene record analitici.",
        "loading": "Caricamento dei dati…",
        "error_title": "La dashboard non è disponibile.",
        "error_missing": "Il database non è presente o non può essere aperto.",
        "error_incomplete": "Il database non contiene tutte le risorse necessarie.",
        "error_query": "Non è stato possibile completare l'analisi.",
        "error_unexpected": "Si è verificato un errore inatteso durante il caricamento.",
        "error_action": "Dalla root del repository esegui questi comandi:",
        "batch": "Caricamento",
        "processed_records": "Record elaborati",
        "new_keys": "Nuovi record",
        "updated_records": "Record aggiornati",
        "cumulative_keys": "Record correnti",
        "batch_run_1": "Primo caricamento",
        "batch_run_2": "Secondo caricamento",
        "batch_run_3": "Terzo caricamento",
        "batch_late": "Record arrivati in ritardo",
        "quality_metric": "Controllo",
        "quality_records": "Record",
        "duplicate_keys": "Chiavi duplicate",
        "invalid_channels": "Canali non validi",
        "invalid_statuses": "Stati ordine non validi",
        "null_prices": "Prezzi mancanti",
        "negative_prices": "Prezzi negativi",
        "non_positive_quantities": "Quantità non positive",
        "cancelled_lines": "Righe annullate",
        "unknown_customers": "Clienti non riconosciuti",
        "unknown_products": "Prodotti non riconosciuti",
        "invalid_economic_lines": "Righe escluse dal fatturato",
        "ranking_position": "Posizione",
        "revenue": "Fatturato",
        "axis_revenue": "Fatturato (€)",
        "axis_date": "Data",
        "axis_category": "Categoria",
        "axis_customer": "Cliente",
        "axis_region": "Regione",
        "axis_orders": "Ordini distinti",
        "month": "Mese",
    },
    "en": {
        "language": "Language",
        "theme": "Theme",
        "language_it": "Italiano",
        "language_en": "English",
        "theme_light": "Light",
        "theme_dark": "Dark",
        "overview_eyebrow": "E-commerce analytics",
        "title": "E-commerce sales overview",
        "subtitle": "Sales performance, customer value, and data reliability in one view.",
        "period": "Period",
        "last_update": "Last updated",
        "filters": "Filters",
        "date_range": "Date range",
        "category": "Category",
        "region": "Region",
        "all_categories": "All categories",
        "all_regions": "All regions",
        "reset_filters": "Reset filters",
        "filter_help": "Narrow all business metrics and analyses.",
        "kpi_revenue": "Valid revenue",
        "kpi_orders": "Orders",
        "kpi_average_order": "Average order value",
        "kpi_customers": "Customers",
        "kpi_note": "Metrics reuse the revenue already validated in the fact table.",
        "sales_section": "Sales performance",
        "sales_trend_title": "Daily revenue",
        "sales_trend_caption": "Valid revenue trend across the selected period.",
        "category_title": "Revenue by category",
        "category_caption": "Each category's contribution to selected revenue.",
        "category_ranking": "Category ranking details",
        "customers_territory": "Customers and regions",
        "top_customers_title": "Top 10 customers by spend",
        "top_customers_caption": "Customers generating the most valid revenue in the period.",
        "regions_title": "Orders by region",
        "regions_caption": "Distinct orders with valid revenue by geographic area.",
        "weekly_detail": "Weekly trend by region",
        "data_status": "Data status",
        "status_ready": "Data checks passed",
        "status_attention": "Review required",
        "checks_passed": "Key checks",
        "anomaly_types": "Observed anomaly types",
        "load_history": "Load history details",
        "quality_details": "Anomaly details",
        "batch_empty": "No order-line load has been recorded.",
        "quality_empty": "No data-quality metrics are available.",
        "data_status_note": "This status describes persisted data, not real-time monitoring.",
        "project_info": "About this project",
        "project_architecture": (
            "Synthetic data is ingested transactionally and transformed into "
            "a dimensional analytics model."
        ),
        "project_limit": (
            "DuckDB is the project's local analytics engine. It does not "
            "replace a distributed platform or one designed for concurrent "
            "writers."
        ),
        "project_config": "Database: {database} · schema: {schema}",
        "project_incremental": (
            "Ordinary loads add new keys; the late-arriving batch applies 10 "
            "inserts and 5 corrections idempotently."
        ),
        "project_transfer": (
            "The repository includes a conceptual path to Databricks and "
            "Delta Lake; it is not an active cloud deployment."
        ),
        "empty_chart": "No data matches the selected filters.",
        "empty_data": "The data structure is available, but it contains no analytics records.",
        "loading": "Loading data…",
        "error_title": "The dashboard is unavailable.",
        "error_missing": "The database is missing or cannot be opened.",
        "error_incomplete": "The database does not contain all required resources.",
        "error_query": "The analysis could not be completed.",
        "error_unexpected": "An unexpected error occurred while loading the dashboard.",
        "error_action": "Run these commands from the repository root:",
        "batch": "Load",
        "processed_records": "Records processed",
        "new_keys": "New records",
        "updated_records": "Updated records",
        "cumulative_keys": "Current records",
        "batch_run_1": "Initial load",
        "batch_run_2": "Second load",
        "batch_run_3": "Third load",
        "batch_late": "Late records",
        "quality_metric": "Check",
        "quality_records": "Records",
        "duplicate_keys": "Duplicate keys",
        "invalid_channels": "Invalid channels",
        "invalid_statuses": "Invalid order statuses",
        "null_prices": "Missing prices",
        "negative_prices": "Negative prices",
        "non_positive_quantities": "Non-positive quantities",
        "cancelled_lines": "Cancelled lines",
        "unknown_customers": "Unmatched customers",
        "unknown_products": "Unmatched products",
        "invalid_economic_lines": "Lines excluded from revenue",
        "ranking_position": "Rank",
        "revenue": "Revenue",
        "axis_revenue": "Revenue (€)",
        "axis_date": "Date",
        "axis_category": "Category",
        "axis_customer": "Customer",
        "axis_region": "Region",
        "axis_orders": "Distinct orders",
        "month": "Month",
    },
}


@dataclass(frozen=True)
class Translator:
    """Resolve all visible dashboard copy from one language catalog."""

    language: str

    def __post_init__(self) -> None:
        if self.language not in SUPPORTED_LANGUAGES:
            raise ValueError(f"Unsupported language: {self.language}")

    def __call__(self, key: str, **values: object) -> str:
        return TRANSLATIONS[self.language][key].format(**values)


def translation_keys(language: str) -> set[str]:
    """Expose catalog keys for completeness tests."""

    return set(TRANSLATIONS[language])
