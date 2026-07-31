# Trasferimento concettuale verso Databricks

## Scopo

Questo documento descrive come trasferire su Databricks la pipeline locale
basata su Python, DuckDB e dbt. Non rappresenta un deployment già disponibile:
definisce una destinazione progettuale che conserva grain, contratti,
deduplicazione, qualità e idempotenza della soluzione corrente.

I riferimenti normativi del progetto restano, nell'ordine,
`clarifications.md`, `requirements.md`, `data_contracts.md` ed
`expected_run_metrics.yml`. Il trasferimento non modifica i loro requisiti.

## Mappatura dell'architettura

| Componente locale | Destinazione Databricks | Responsabilità |
|---|---|---|
| CSV sotto `data/raw/` | Landing zone su cloud object storage | Conservazione immutabile dei sei file e della struttura dei run |
| `raw.ingestion_manifest` | `bronze.ingestion_manifest` in Delta | Idempotenza per percorso e checksum, conteggi e stato del batch |
| `raw.customers`, `raw.products`, `raw.order_lines` | Tabelle Delta Bronze append-only | Payload sorgente tipizzato e metadati di provenienza |
| Modelli dbt staging | Viste o tabelle Silver di normalizzazione | Tipizzazione, allineamento dello schema e osservabilità delle anomalie |
| Modelli intermediate | Tabelle o viste Silver correnti | Deduplicazione deterministica delle business key |
| `dim_customers`, `dim_products`, `dim_date` | Tabelle Delta Gold | Dimensioni governate, incluse le righe tecniche `Unknown` |
| `fct_order_lines` | Tabella Delta Gold | Una versione corrente per `order_line_id` e misure analitiche |
| File in `analyses/` | Query dbt o Databricks SQL | Consumo del modello Gold senza duplicare regole economiche |
| Comandi manuali locali | Lakeflow Jobs | Orchestrazione, dipendenze, retry e scheduling |

La struttura Bronze/Silver/Gold segue il pattern medallion: Bronze conserva la
sorgente, Silver applica validazione e risoluzione tecnica, Gold espone il
modello dimensionale. Il pattern è descritto nella
[documentazione Databricks sulla medallion architecture](https://docs.databricks.com/aws/en/lakehouse/medallion).

## Storage e landing zone

I CSV vengono depositati in uno storage a oggetti scelto per il cloud del
workspace, per esempio Amazon S3, Azure Data Lake Storage o Google Cloud
Storage. Un layout possibile, indipendente dal provider, è:

```text
landing/ecommerce/run_1/customers.csv
landing/ecommerce/run_1/products.csv
landing/ecommerce/run_1/orders.csv
landing/ecommerce/run_2/orders.csv
landing/ecommerce/run_3/orders.csv
landing/ecommerce/run_3/late_arriving/orders.csv
```

I file pubblicati sono immutabili. Un reprocessing non sovrascrive un oggetto
già acquisito con bytes differenti: usa un nuovo percorso versionato o viene
rifiutato. Bucket/container, credenziali e external location sono configurati
per ambiente e non inseriti nel repository.

Unity Catalog governa external location, storage credential, cataloghi e
schemi. Non si usano mount o percorsi personali. Per questo volume di dati è
sufficiente un caricamento batch; Auto Loader diventerebbe utile soltanto con
arrivi continui o numerosi file.

## Livelli Delta

### Bronze

Le tabelle Bronze sono append-only e mantengono:

- tutte le versioni sorgente, incluse correction e anomalie funzionali;
- `source_file`, `source_run`, `source_row_number`, checksum e hash di riga;
- un identificatore deterministico di ingestion;
- `source_ingested_at` assegnato al commit Bronze;
- il numero di righe e lo stato del file nel manifest.

La validazione preliminare controlla intestazioni, tipi, nullability
strutturale, checksum e conteggi. Errori strutturali bloccano il file; prezzi
nulli o negativi, quantità non positive, stati fuori dominio e chiavi orfane
rimangono osservabili.

### Silver

Silver allinea gli schemi per nome e produce una rappresentazione tipizzata:

- clienti: massimo `updated_at`, poi payload canonico decrescente;
- prodotti: payload canonico decrescente;
- righe ordine: massimo `updated_at`, poi payload canonico decrescente;
- duplicati identici: una sola candidata, senza dipendere dall'ordine fisico;
- anomalie funzionali: conservate con indicatori espliciti;
- chiavi dimensionali non risolte: ancora visibili per la risoluzione Gold.

Prima di ogni `MERGE`, Silver deve produrre al massimo una riga sorgente per
business key. Delta segnala come ambiguo un `MERGE` nel quale più righe
sorgente aggiornano la stessa riga target; la deduplicazione è quindi un
pre-requisito applicativo, non un effetto collaterale dell'upsert. Si veda la
[documentazione ufficiale di `MERGE`](https://docs.databricks.com/aws/en/delta/merge).

### Gold

Gold conserva lo star schema corrente:

- `dim_customers`: SCD Type 1 e membro `customer|unknown`;
- `dim_products`: una riga corrente per prodotto e membro `product|unknown`;
- `dim_date`: calendario analitico;
- `fct_order_lines`: una riga corrente per `order_line_id`.

La fact mantiene business key, surrogate key, misure, stato, date, canale,
metadati di prevalenza, `is_economically_valid` e `revenue`. Le analisi usano
la misura `revenue` già calcolata: non ricostruiscono le condizioni economiche.

## I tre tempi del dato

I tre timestamp hanno responsabilità diverse:

| Campo | Significato | Uso |
|---|---|---|
| `order_date` | Data commerciale | Calendario, mese e settimana; mai watermark tecnico |
| `updated_at` | Versione del payload sorgente | Prevalenza di insert e correction |
| `source_ingested_at` | Commit della versione in Bronze | Incrementalità tecnica e osservabilità del ritardo |

I dieci late insert hanno `order_date` nella finestra del Run 1 ma arrivano
dopo il Run 2. Le cinque correction aggiornano chiavi del Run 1 con
`updated_at` successivo. Un filtro basato su `order_date` perderebbe entrambi;
il caricamento seleziona invece nuovi commit Bronze e applica la prevalenza su
`updated_at` e payload canonico.

## Upsert Delta e idempotenza

La candidata Silver è deduplicata prima del `MERGE`. La semantica concettuale
della fact è:

```sql
MERGE INTO gold.fct_order_lines AS target
USING silver.prevalent_order_line_changes AS source
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

Il batch late-arriving produce dieci insert e cinque update. Una versione
uguale o non prevalente è un no-op. Il replay dello stesso file non raggiunge
il `MERGE` se percorso e checksum sono già nel manifest; anche se rivalutato,
il confronto di prevalenza mantiene invariato il target.

Manifest, righe Bronze e stato del batch devono diventare visibili insieme.
Quando un singolo statement Delta non basta a coordinare più tabelle, il job
usa stati espliciti (`started`, `committed`, `failed`) e rende consumabili
soltanto batch `committed`. Il manifest non deve dichiarare completato un file
prima che la relativa tabella Bronze sia stata committata.

Delta fornisce snapshot coerenti e serializzazione dei commit sulle singole
tabelle; lettori e writer concorrenti lavorano su snapshot consistenti. Le
modifiche multi-tabella non costituiscono però automaticamente una singola
transazione globale. La differenza è documentata nelle
[regole di isolamento e conflitto di Delta](https://docs.databricks.com/aws/en/optimizations/isolation).

## Schema evolution di `sales_channel`

Bronze registra lo schema fisico osservato:

- Run 1 e Run 2: colonna assente;
- batch late-arriving: colonna assente;
- righe ordinarie del Run 3: colonna nullable presente.

Silver allinea per nome e valorizza `sales_channel = NULL` dove la colonna era
assente. I valori non nulli ammessi restano `web`, `mobile` e `marketplace`.
L'evoluzione è autorizzata in modo controllato per questo campo; non si abilita
un'accettazione indiscriminata di colonne inattese. Una modifica di schema è
separata dai writer concorrenti perché le modifiche ai metadati possono
confliggere con altre scritture. La panoramica ufficiale è disponibile in
[Schema evolution in Databricks](https://docs.databricks.com/aws/en/data-engineering/schema-evolution).

## Orchestrazione con Lakeflow Jobs

Un Lakeflow Job può rappresentare il DAG seguente:

1. rilevare e validare i file attesi;
2. acquisire il lock logico del batch nel manifest;
3. caricare Bronze e committare il manifest;
4. costruire Silver e deduplicare le candidate;
5. eseguire i `MERGE` Gold;
6. eseguire test dbt e controlli quantitativi;
7. pubblicare metriche e stato finale.

Le dipendenze impediscono a Gold di partire se Bronze o Silver falliscono.
Retry automatici sono sicuri soltanto per task idempotenti. Un timeout, una
policy di retry con backoff e notifiche di fallimento vengono configurati per
task. Run 1 precede Run 2; il batch late-arriving può essere acquisito dopo Run
2, mentre il Run 3 ordinario resta la terza finestra commerciale.

Checkpoint e manifest hanno ruoli distinti: il checkpoint conserva lo stato
tecnico di un eventuale flusso incrementale; il manifest registra l'identità
funzionale del file, checksum, conteggi e risultato. Entrambi risiedono in
storage persistente governato, non sul disco effimero del compute.

## dbt e controlli di qualità

Il progetto può continuare a usare dbt sostituendo l'adapter DuckDB con quello
Databricks e mappando target e schemi per ambiente. I test restano parte del
DAG:

- unique e not-null sulle chiavi;
- relationships verso le dimensioni, inclusi i membri `Unknown`;
- domini di `sales_channel`;
- una riga corrente per `order_line_id`;
- 5.010 chiavi finali e cinque correction prevalenti;
- fatturato nullo per righe non economicamente valide;
- idempotenza del replay.

I test strutturali bloccano la promozione. Le anomalie funzionali previste
producono metriche e alert, non cancellazioni. Gli oracoli quantitativi restano
versionati nel repository e non sono ricavati dall'output da validare.

## Unity Catalog, sicurezza e lineage

Una separazione possibile usa cataloghi per ambiente e schemi per livello:

```text
ecommerce_dev.bronze
ecommerce_dev.silver
ecommerce_dev.gold
ecommerce_staging.bronze
ecommerce_staging.silver
ecommerce_staging.gold
ecommerce_prod.bronze
ecommerce_prod.silver
ecommerce_prod.gold
```

Gruppi e service principal ricevono il minimo privilegio necessario:
`USE CATALOG`, `USE SCHEMA`, lettura della landing zone, scrittura nel proprio
livello e sola lettura sui livelli precedenti. La produzione non è modificabile
da identità personali. Ownership e grant vengono gestiti come configurazione,
non tramite comandi manuali non tracciati. Le possibilità di controllo accessi
sono riepilogate nella
[documentazione Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/).

Le esecuzioni su oggetti Unity Catalog alimentano il lineage, utile per seguire
file, trasformazioni, tabelle e consumer; la visibilità del lineage rispetta i
privilegi sugli oggetti. Si veda
[Lineage in Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage).

Credenziali e token non entrano in Git, notebook o parametri in chiaro. Per
CI/CD si preferisce un'identità workload/service principal con credenziali a
breve durata; per altri segreti si usano secret scope o oggetti governati con
ACL minime. La gestione è descritta in
[Secret management](https://docs.databricks.com/aws/en/security/secrets/).

## Configurazione per ambiente

Configurazioni non sensibili includono catalogo, schema, landing path, policy
di compute e tag di costo. Vengono parametrizzate per `dev`, `staging` e
`production`. I segreti sono referenziati per nome e risolti durante il run.

Le differenze tra ambienti non cambiano contratti o SQL funzionale. Dev usa un
dataset isolato; staging esegue il replay completo e confronta le metriche;
production riceve soltanto una revisione già validata.

## Osservabilità e alert

Ogni run pubblica almeno:

- batch e file acquisiti, saltati o falliti;
- righe lette, committate, deduplicate, inserite e aggiornate;
- checksum e conteggi confrontati con il manifest;
- durata e costo dei task;
- numero di chiavi correnti e duplicati;
- anomalie per regola DQ;
- ritardo tra `order_date`, `updated_at` e `source_ingested_at`;
- esito dei test dbt e degli oracoli quantitativi.

Log strutturati includono run ID, task ID, source file e ingestion ID, senza
payload sensibili. Alert distinguono errore strutturale, conflitto di checksum,
violazione del grain, fallimento del `MERGE` e ritardo operativo. Dashboard e
retention vengono dimensionate sul volume reale, evitando telemetria superflua.

## Backfill e recovery

Un backfill usa un nuovo batch identificabile e file immutabili. La procedura è:

1. registrare motivo, intervallo e versione del codice;
2. acquisire i file in Bronze senza riscrivere la storia;
3. ricostruire Silver per le chiavi coinvolte;
4. applicare il `MERGE` con le stesse regole di prevalenza;
5. verificare conteggi, checksum, correction, Unknown e ricavi;
6. rendere il batch consumabile soltanto dopo i controlli.

Per errori di logica, il transaction log Delta permette di identificare la
versione precedente; il recovery applicativo preferisce una correzione
versionata e un nuovo run verificabile. Il time travel aiuta analisi e
confronto, ma non sostituisce retention, backup e una procedura di ripristino
provata. Se fallisce una fase multi-tabella, il job riparte dal primo stato non
committato oppure ricostruisce Silver/Gold dalla Bronze immutabile.

## CI/CD

La CI locale resta il primo gate: installazione riproducibile, `pip check`,
compilazione Python, suite completa, `dbt parse`, `dbt compile` e test. La
promozione Databricks aggiungerebbe:

1. validazione della configurazione di deployment;
2. deploy isolato in dev;
3. replay completo e test dbt;
4. deploy in staging con smoke test e confronto metriche;
5. approvazione protetta;
6. deploy in production usando lo stesso commit.

Databricks indica i Declarative Automation Bundles, già noti come Asset
Bundles, per definire e distribuire risorse come Lakeflow Jobs e pipeline. La
configurazione del bundle rimane versionata e parametrizzata per ambiente; il
repository attuale non la implementa. Riferimenti:
[Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/)
e [CI/CD su Databricks](https://docs.databricks.com/aws/en/dev-tools/ci-cd/flows).

## Compute e costi

Il dataset corrente è piccolo: non giustifica cluster persistenti o tuning
distribuito complesso. La prima scelta è compute job effimero e dimensionato al
minimo, oppure serverless quando disponibile e compatibile con governance e
budget. Il job termina dopo l'esecuzione; auto-termination, limiti di timeout,
tag di costo e budget alert sono obbligatori.

SQL warehouse e Photon sono valutati soltanto se frequenza e latenza delle
query lo giustificano. Partizionamento e ottimizzazioni fisiche si introducono
dopo misure concrete: con circa cinquemila fatti, l'eccesso di partizioni crea
più overhead che beneficio. Dev può usare risorse ridotte e isolate;
production usa policy che impediscono configurazioni sovradimensionate.

## Limiti ed evoluzioni

- Il documento non crea workspace, storage, cataloghi o job.
- La transazione atomica multi-tabella dell'ingestion locale deve essere
  sostituita da un protocollo applicativo esplicito basato su manifest e stati.
- L'eventuale uso streaming richiederebbe checkpoint, gestione dei restart e
  test aggiuntivi; non è necessario per i sei file correnti.
- La strategia di retention e disaster recovery dipende dal cloud e dagli
  obiettivi RPO/RTO, non specificati nel progetto.
- Il modello SCD Type 1 dei clienti resta invariato; una futura SCD Type 2
  richiederebbe un nuovo requisito e una nuova modellazione.
- Il dimensionamento finale deve basarsi su volumi, frequenza, SLA e costi
  misurati, non sulla sola architettura proposta.
