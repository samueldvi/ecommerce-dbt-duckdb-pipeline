# E-commerce dbt DuckDB Pipeline

[![CI](https://github.com/samueldvi/ecommerce-dbt-duckdb-pipeline/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/samueldvi/ecommerce-dbt-duckdb-pipeline/actions/workflows/ci.yml)

Pipeline analitica incrementale per dati e-commerce, sviluppata con dbt e DuckDB.

Il progetto costruisce un modello dimensionale e simula più caricamenti per gestire dati incrementali, late-arriving data, correzioni di record già acquisiti ed evoluzione dello schema.

I contratti dei dataset e le regole funzionali sono descritti nella
[documentazione dei contratti](docs/data_contracts.md).

## TL;DR — Il progetto in 2 minuti

Questa pipeline risolve un caso e-commerce incrementale riproducibile: genera
dati sintetici, li acquisisce transazionalmente e costruisce uno star schema
pronto per analisi, mantenendo osservabili anomalie e versioni sorgente.

- **Architettura:** Python genera e carica i CSV; DuckDB conserva raw data e
  manifest; dbt organizza staging, intermediate e mart dimensionali; le query
  in `analyses/` consumano esclusivamente il livello Gold.
- **Incrementalità:** tre finestre ordinarie coprono 30 giorni. Il batch
  separato late-arriving applica 10 insert storici e 5 correction prevalenti;
  il replay è idempotente.
- **Schema evolution:** `sales_channel` compare soltanto nel Run 3 ordinario;
  Run 1, Run 2 e late-arriving restano compatibili con valore normalizzato a
  `NULL`.
- **Risultato verificato:** 5.010 `order_line_id` correnti, 10 insert tardivi,
  5 update e zero duplicati.
- **Analisi:** fatturato per categoria/mese, top 10 clienti, ordini completed
  per regione/settimana e ranking mensile delle categorie con window function.
- **Qualità:** test automatici verificano contratti, anomalie, membri
  `Unknown`, revenue, atomicità, determinismo, idempotenza e query analitiche.
- **Evoluzione:** DuckDB è adatto alla soluzione locale ma non offre
  orchestrazione distribuita; il
  [piano Databricks](docs/TRASFERIMENTO_DATABRICKS.md) trasferisce il disegno su
  object storage, Delta Lake, livelli Bronze/Silver/Gold e Lakeflow Jobs.

Esecuzione e verifica minime:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp profiles.yml.example profiles.yml
python scripts/generate_synthetic_data.py
python scripts/ingest_raw_data.py init
python scripts/ingest_raw_data.py load-all
dbt build --profiles-dir .
python -m unittest discover -s tests -v
```

## Configurazione locale

Creare e attivare l'ambiente virtuale, quindi installare le dipendenze:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Copiare il profilo di esempio nella root del progetto e verificare la configurazione:

```bash
cp profiles.yml.example profiles.yml
dbt debug --profiles-dir .
```

## Generazione dei dati sintetici

Il generatore crea CSV deterministici conformi ai
[contratti dati](docs/data_contracts.md), usando esclusivamente dati sintetici.
Il seed predefinito è `20250730` e la destinazione predefinita è `data/raw/`:

```bash
python scripts/generate_synthetic_data.py
```

Sono prodotti `customers.csv`, `products.csv` e `orders.csv` sotto `run_1/`,
oltre a `orders.csv` sotto `run_2/`, `run_3/` e `run_3/late_arriving/`.
Quest'ultimo file contiene il batch late-arriving, separato dal Run 3
ordinario. I valori nulli sono serializzati come campi CSV vuoti. Le opzioni
principali sono:

```bash
python scripts/generate_synthetic_data.py \
  --seed 20250730 \
  --output-dir data/raw \
  --force
```

`--force` sostituisce soltanto i sei CSV gestiti dal generatore. A parità di
seed, schema, ordine e contenuto dei file sono byte-identici.

Per eseguire i test automatici:

```bash
python -m unittest discover -s tests -v
```

## Ingestion raw

Inizializzare lo schema DuckDB e caricare transazionalmente i sei CSV:

```bash
python scripts/ingest_raw_data.py init
python scripts/ingest_raw_data.py load-all
```

Per caricare o riprodurre un singolo batch o file:

```bash
python scripts/ingest_raw_data.py load-batch run_1
python scripts/ingest_raw_data.py load-file run_2/orders.csv
```

Il replay di un file invariato è un no-op. Se un percorso già acquisito
presenta un checksum differente, l'operazione fallisce senza modifiche
parziali a dati raw o manifest.

## Pipeline incrementale dbt

Caricare in sequenza le tre finestre ordinarie e infine il batch late-arriving,
eseguendo `dbt build --profiles-dir .` dopo ogni comando:

```bash
python scripts/ingest_raw_data.py load-batch run_1
python scripts/ingest_raw_data.py load-batch run_2
python scripts/ingest_raw_data.py load-batch run_3
python scripts/ingest_raw_data.py load-batch run_3_late_arriving
```

Algoritmo di upsert, idempotenza, correction, schema evolution e backfill sono
descritti in [Gestione incrementale](docs/GESTIONE_INCREMENTALE.md).

## Demo Streamlit

La demo interroga in sola lettura i mart DuckDB e presenta KPI, quattro analisi,
stato storico dei batch e metriche trasparenti di data quality. Non ricostruisce
la logica economica in Python: il fatturato proviene dalla misura `revenue`
calcolata nella fact dbt.

Da un clone pulito, dopo avere configurato l'ambiente come indicato sopra,
preparare il database e avviare la demo:

```bash
cp profiles.yml.example profiles.yml
python scripts/generate_synthetic_data.py
python scripts/ingest_raw_data.py load-all
dbt build --profiles-dir .
streamlit run streamlit_app.py
```

L'interfaccia è disponibile normalmente su `http://localhost:8501`. Se il
database manca, è corrotto o non contiene raw e mart richiesti, la pagina mostra
le risorse mancanti e gli stessi comandi di preparazione senza esporre uno stack
trace e senza usare dati fittizi.

Il percorso predefinito è `data/warehouse/ecommerce.duckdb`, risolto rispetto
alla root del progetto e indipendente dalla directory corrente. Per un database
locale alternativo si può impostare `ECOMMERCE_DB_PATH`; se dbt pubblica i mart
in uno schema diverso da `analytics`, usare `ECOMMERCE_DBT_SCHEMA`. Lo schema
predefinito della demo è `analytics`. La struttura
essenziale è composta dall'entry point `streamlit_app.py` e dal package
`dashboard/`, che separa configurazione, query, accesso read-only e
presentazione.

DuckDB è utilizzato come motore analitico locale, leggero e production-like per
questa implementazione. Non sostituisce una piattaforma enterprise distribuita
o multiwriter.

## Query analitiche

Dopo avere popolato il database ed eseguito `dbt build --profiles-dir .`, le
quattro analisi possono essere eseguite direttamente con dbt:

```bash
dbt show --profiles-dir . --limit 100 --inline "$(cat analyses/revenue_by_category_month.sql)"
dbt show --profiles-dir . --limit 100 --inline "$(cat analyses/top_10_customers_by_spend.sql)"
dbt show --profiles-dir . --limit 100 --inline "$(cat analyses/completed_orders_by_region_week.sql)"
dbt show --profiles-dir . --limit 100 --inline "$(cat analyses/monthly_category_revenue_ranking.sql)"
```

La migrazione concettuale della soluzione è descritta nel
[piano di trasferimento verso Databricks](docs/TRASFERIMENTO_DATABRICKS.md).
