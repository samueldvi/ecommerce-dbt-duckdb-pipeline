# Gestione incrementale

## Architettura del caricamento

Il flusso separa acquisizione e trasformazione:

1. il generatore produce tre finestre ordinarie e un batch late-arriving
   separato;
2. `RawIngestor` valida e registra ogni file nel raw layer insieme al manifest,
   usando una transazione DuckDB esplicita;
3. i modelli staging espongono i record raw senza eliminare anomalie;
4. le dimensioni risolvono deterministicamente clienti e prodotti e includono
   i membri tecnici `Unknown`;
5. `fct_order_lines` applica l'upsert incrementale e conserva una sola versione
   corrente per `order_line_id`.

Le finestre ordinarie attualmente generate contengono 3.000, 1.000 e 1.000
righe. Questa ripartizione è deterministica ma non è un requisito normativo: il
vincolo è il totale di 5.000 chiavi distinte sulle tre finestre.

## Sequenza operativa

Dopo avere configurato `profiles.yml`, generare i dati e inizializzare il raw
layer:

```bash
python scripts/generate_synthetic_data.py
python scripts/ingest_raw_data.py init
```

Eseguire ogni finestra e costruire il modello dopo ciascun caricamento:

```bash
python scripts/ingest_raw_data.py load-batch run_1
dbt build --profiles-dir .

python scripts/ingest_raw_data.py load-batch run_2
dbt build --profiles-dir .

python scripts/ingest_raw_data.py load-batch run_3
dbt build --profiles-dir .

python scripts/ingest_raw_data.py load-batch run_3_late_arriving
dbt build --profiles-dir .
```

Il quarto comando carica il batch tardivo associato al Run 3, non una quarta
finestra commerciale.

## `order_date` e `updated_at`

`order_date` (`data_ordine` nella descrizione funzionale) rappresenta la data
commerciale dell'ordine. `updated_at`
rappresenta invece l'istante della versione sorgente e determina quale payload
è corrente.

Un filtro basato su `max(order_date)` non è sufficiente: i dieci nuovi fatti
tardivi hanno date comprese tra il 1° e il 10 gennaio, mentre le cinque
correzioni aggiornano chiavi della stessa finestra storica. Entrambi arrivano
dopo il Run 2 e verrebbero esclusi da un watermark sulla sola data commerciale.

Il watermark tecnico usa `source_ingested_at`, cioè l'istante transazionale con
cui la versione è entrata nel raw layer. Il confronto usa `>=` per non perdere
batch acquisiti allo stesso timestamp; record già valutati vengono comunque
scartati come no-op dal confronto di prevalenza.

## Upsert e deduplicazione

La fact è materializzata da dbt come modello incrementale con:

- `unique_key = order_line_id`;
- strategia DuckDB `delete+insert`;
- `on_schema_change = sync_all_columns`;
- una sola riga candidata per chiave in ogni esecuzione.

Le versioni candidate sono ordinate per `updated_at` decrescente. A parità di
timestamp viene confrontato, nell'ordine contrattuale, il payload canonico di
tutte le colonne diverse da `order_line_id`. Metadati di sorgente stabili
risolvono soltanto eventuali duplicati completamente identici e non cambiano il
payload di business.

Durante un'esecuzione incrementale una candidata viene emessa soltanto se:

- la chiave non esiste ancora; oppure
- `updated_at` è maggiore di quello corrente; oppure
- il timestamp è uguale e il payload canonico è maggiore.

Una versione identica o non prevalente non raggiunge il `delete+insert`. Il
modello non usa mai `order_date` per decidere se una versione è nuova.

## Late-arriving facts e correction

Il file `run_3/late_arriving/orders.csv` contiene 15 record completi senza
`sales_channel`:

- 10 nuove `order_line_id` con `order_date` nella finestra del Run 1;
- 5 versioni più recenti di chiavi già caricate nel Run 1.

Le correction preservano `order_id`, riferimenti dimensionali, stato e data
commerciale. Cambiano soltanto `quantity` oppure `unit_price` e hanno sempre
`updated_at` successivo. L'upsert produce quindi dieci insert e cinque update,
portando il risultato da 5.000 a 5.010 chiavi senza duplicati.

## Schema evolution

La colonna nullable `sales_channel` è assente fisicamente nei file dei Run 1 e
2 e nel batch tardivo. Compare soltanto nel file ordinario del Run 3 con valori
`web`, `mobile`, `marketplace` o `NULL`.

Il raw layer usa fin dall'inizio lo schema evoluto e normalizza a `NULL` la
colonna assente. dbt sincronizza esplicitamente lo schema della fact; nessun CSV
storico viene riscritto e tutti i record non provenienti dal Run 3 ordinario
mantengono `sales_channel = NULL`.

## Qualità e membri Unknown

La fact conserva record annullati, stati fuori dominio, quantità non positive e
prezzi nulli o negativi. `is_economically_valid` è vero soltanto per righe
`completed`, con quantità positiva e prezzo non nullo e non negativo. `revenue`
è valorizzato soltanto in questo caso; un prezzo uguale a zero resta valido e
produce ricavo zero.

I riferimenti cliente o prodotto non risolti vengono associati rispettivamente
a `customer|unknown` e `product|unknown`. Le business key sorgente restano nella
fact per rendere l'anomalia osservabile.

## Idempotenza e reprocessing

Il manifest raw identifica ogni file mediante percorso e checksum. Il replay di
un file invariato non inserisce righe né un nuovo manifest; lo stesso percorso
con contenuto diverso viene rifiutato.

Sul livello dbt, il watermark tecnico limita le candidate e il confronto di
prevalenza impedisce aggiornamenti artificiali. Rieseguire un batch seguito da
`dbt build` mantiene invariati:

- numero e insieme delle chiavi;
- payload corrente;
- conteggi raw e dimensionali;
- assegnazione ai membri `Unknown`;
- ricavi e flag di validità.

Un reprocessing completo dei sei file è quindi un no-op. `dbt --full-refresh`
rimane disponibile per ricostruire deterministicamente il modello quando
cambia la logica di trasformazione, non per il normale replay dei dati.

## Errori a metà run

Raw rows e manifest vengono scritti nella stessa transazione. Un errore di
schema, tipo, checksum o consistenza provoca `ROLLBACK`, senza rendere visibili
stati parziali.

dbt usa la transazione della materializzazione per il singolo modello
incrementale: se l'upsert della fact fallisce, la versione precedentemente
committata resta disponibile. L'intero DAG dbt non costituisce però un'unica
transazione globale; dopo un errore si corregge la causa e si riesegue
`dbt build`, sfruttando l'idempotenza. Questa è una limitazione esplicita della
soluzione locale.

## Backfill

Un backfill dati deve essere pubblicato come nuovo file gestito e nuovo evento
di manifest, senza sostituire silenziosamente un percorso già acquisito. Le
righe possono avere `order_date` storica: vengono considerate perché il
watermark è tecnico e la prevalenza usa `updated_at`.

Quando il backfill deriva da una modifica della logica dbt, la procedura è:

1. conservare invariato il raw layer;
2. eseguire i test e `dbt build --full-refresh` in un database controllato;
3. confrontare grain, chiavi, correction, anomalie e metriche;
4. sostituire il database locale soltanto dopo la verifica.

## Limiti di DuckDB locale

- DuckDB privilegia un singolo writer e non sostituisce un orchestratore
  distribuito;
- il manifest conosce soltanto i percorsi gestiti dall'ingestion corrente;
- non esiste un transaction log distribuito con time travel;
- l'atomicità è garantita per ingestion raw e singola materializzazione dbt,
  non per l'intero DAG;
- scheduling, retry e osservabilità operativa devono essere aggiunti fuori da
  questo progetto locale.

## Trasferimento a Delta Lake

In Delta Lake i CSV diventerebbero una tabella Bronze append-only con metadati
di file e batch. Una tabella Silver deduplicherebbe le versioni; dimensioni e
fact Gold manterrebbero lo stesso grain e le stesse regole di qualità.

L'upsert della fact diventerebbe concettualmente:

```sql
MERGE INTO gold.fct_order_lines AS target
USING prevalent_order_line_changes AS source
ON target.order_line_id = source.order_line_id
WHEN MATCHED AND (
  source.updated_at > target.updated_at
  OR (
    source.updated_at = target.updated_at
    AND source.canonical_payload > target.canonical_payload
  )
) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

Delta fornirebbe transaction log, concorrenza ottimistica, versionamento,
schema evolution controllata e time travel. Watermark, payload canonico,
gestione dei membri `Unknown` e test di idempotenza resterebbero invariati.
