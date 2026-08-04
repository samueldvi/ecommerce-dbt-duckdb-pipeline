# Contribuire al progetto

## Setup locale

Usare Python 3.12 e lavorare su un branch dedicato:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt -r requirements-dev.txt
cp profiles.yml.example profiles.yml
```

`profiles.yml`, database ricostruiti, CSV, log e cache devono restare locali.

## Verifiche

Prima di aprire una pull request eseguire almeno:

```bash
python -m pip check
python -m ruff check scripts dashboard tests streamlit_app.py
python -m ruff format --check scripts dashboard tests streamlit_app.py
python -m pytest -q
dbt build --profiles-dir .
dbt test --profiles-dir .
git diff --check
```

Per modifiche limitate è possibile eseguire prima i test mirati, ma la suite
completa deve essere verde prima della revisione.

## Stile e ambito

- codice, SQL e identificatori tecnici sono in inglese;
- la documentazione descrittiva è in italiano;
- le regole normative non vengono dedotte dal codice esistente;
- le modifiche restano piccole, motivate e prive di artefatti generati;
- nuove dipendenze richiedono una motivazione tecnica e una versione esplicita.

## Pull request

La pull request deve descrivere scopo, file principali, verifiche eseguite e
limiti residui. Non deve includere segreti, configurazioni personali o dati non
sintetici. Evitare modifiche non correlate e attendere l'esito dei check CI
prima del merge.
