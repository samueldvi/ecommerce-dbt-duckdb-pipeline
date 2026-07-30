# E-commerce dbt DuckDB Pipeline

Pipeline analitica incrementale per dati e-commerce, sviluppata con dbt e DuckDB.

Il progetto costruisce un modello dimensionale e simula più caricamenti per gestire dati incrementali, late-arriving data, correzioni di record già acquisiti ed evoluzione dello schema.

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
