# E-commerce dbt DuckDB Pipeline

Pipeline analitica incrementale per dati e-commerce, sviluppata con dbt e DuckDB.

Il progetto costruisce un modello dimensionale e simula più caricamenti per gestire dati incrementali, late-arriving data, correzioni di record già acquisiti ed evoluzione dello schema.

I contratti dei dataset e le regole funzionali sono descritti nella
[documentazione dei contratti](docs/data_contracts.md).

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
oltre a `orders.csv` sotto `run_2/` e `run_3/`. I valori nulli sono serializzati
come campi CSV vuoti. Le opzioni principali sono:

```bash
python scripts/generate_synthetic_data.py \
  --seed 20250730 \
  --output-dir data/raw \
  --force
```

`--force` sostituisce soltanto i cinque CSV gestiti dal generatore. A parità di
seed, schema, ordine e contenuto dei file sono byte-identici.

Per eseguire i test automatici:

```bash
python -m unittest discover -s tests -v
```
