# Contratti dati e regole funzionali

## Scope and principles

Questi contratti definiscono struttura, semantica e risultati attesi dei dataset
`customers.csv`, `products.csv` e `orders.csv`. Costituiscono il riferimento per
le successive milestone di generazione, ingestion e modellazione, che non sono
ancora implementate.

Le responsabilità sono separate come segue:

- Python genererà i CSV sintetici e li caricherà nel raw layer senza applicare
  logica dimensionale o scartare anomalie funzionali;
- il raw layer conserverà i valori sorgente e la provenienza del run, rendendo
  disponibili tutte le versioni ricevute;
- dbt applicherà tipizzazione, controlli di qualità, deduplicazione,
  trasformazioni e modello dimensionale.

Principi:

- **determinismo**: gli stessi input producono gli stessi record prevalenti e
  gli stessi conteggi;
- **idempotenza**: rieseguire uno stesso run non modifica il risultato già
  ottenuto;
- **tracciabilità**: ogni record raw deve essere riconducibile a dataset, file e
  run di origine;
- **conservazione degli errori**: le anomalie funzionali restano osservabili;
  soltanto errori realmente strutturali, come una chiave obbligatoria assente o
  un campo obbligatorio non interpretabile, bloccano il caricamento;
- **tempo**: le date usano ISO 8601 `YYYY-MM-DD`; i timestamp usano UTC,
  precisione al microsecondo e rappresentazione ISO 8601
  `YYYY-MM-DDTHH:MM:SS.ffffffZ`;
- **importi**: i valori monetari usano `DECIMAL(12,2)`, mai tipi floating-point.

Decisioni adottate per rendere la specifica non ambigua:

- il periodo di riferimento degli ordini è dal `2025-01-01` al `2025-01-20`;
- Run 1 contiene 3.000 righe ordine e Run 2 ne contiene 2.000;
- a parità del timestamp di aggiornamento prevale deterministicamente il
  payload con rappresentazione canonica lessicograficamente maggiore;
- `sales_channel` è l'unica evoluzione di schema e compare dal Run 2.

## Dataset overview

| Dataset | Grain fisico | Business key | Caricamento | Run | Deduplicazione | Destinazione raw prevista |
|---|---|---|---|---|---|---|
| `customers.csv` | Una versione osservata di un cliente per riga; più versioni della stessa chiave sono ammesse | `customer_id` | Snapshot completo, una sola volta | Run 1 | Massimo `updated_at`, poi payload canonico | `data/raw/run_1/customers.csv` |
| `products.csv` | Una riga sorgente di prodotto | `product_id` | Snapshot completo, una sola volta | Run 1 | Payload canonico massimo per chiave | `data/raw/run_1/products.csv` |
| `orders.csv` | Una versione di una order line per riga | `order_line_id` | Incrementale con upsert | Run 1, Run 2, Run 3 | Massimo `updated_at`, poi payload canonico | `data/raw/run_<n>/orders.csv` |

Il payload canonico è la concatenazione ordinata delle colonne esplicitamente
indicate nella regola di deduplicazione di ciascun dataset, con valori null
rappresentati da un token costante e date, timestamp e decimali nel formato
canonico indicato in questo documento. Il confronto è lessicografico UTF-8,
case-sensitive. Righe completamente identiche collassano in una sola versione.

## `customers.csv` contract

Grain fisico: una riga rappresenta una versione del cliente presente nello
snapshot. Lo snapshot può contenere più righe con lo stesso `customer_id`.

Business key: `customer_id`.

| Colonna | Tipo logico | Null | Descrizione | Regola di validità | Esempio |
|---|---|---:|---|---|---|
| `customer_id` | `VARCHAR` | No | Identificatore naturale del cliente | Stringa non vuota dopo trim | `CUS_000123` |
| `customer_name` | `VARCHAR` | No | Nome visualizzato del cliente | Stringa non vuota dopo trim | `Mario Rossi` |
| `email` | `VARCHAR` | Sì | Recapito e-mail | Se valorizzato, una sola `@` e nessuno spazio | `mario.rossi@example.com` |
| `country_code` | `CHAR(2)` | Sì | Codice paese ISO 3166-1 alpha-2 | Se valorizzato, due lettere maiuscole ASCII | `IT` |
| `updated_at` | `TIMESTAMP(6)` UTC | No | Istante di validità della versione sorgente | Timestamp ISO 8601 valido con suffisso `Z` | `2025-01-01T09:30:00.000000Z` |

Per ogni `customer_id` è corrente la riga con `updated_at` maggiore. In caso di
parità, prevale il payload canonico lessicograficamente maggiore calcolato su
`customer_name`, `email` e `country_code`, in quest'ordine. Duplicati esatti
vengono collassati. Questa regola è intenzionalmente tecnica: evita che l'ordine
fisico del CSV influenzi il risultato. La dimensione finale conserva soltanto
la versione corrente, con comportamento equivalente a SCD Type 1.

## `products.csv` contract

Grain fisico: una riga rappresenta un prodotto nello snapshot immutabile.

Business key: `product_id`.

| Colonna | Tipo logico | Null | Descrizione | Regola di validità | Esempio |
|---|---|---:|---|---|---|
| `product_id` | `VARCHAR` | No | Identificatore naturale del prodotto | Stringa non vuota dopo trim | `PRD_000042` |
| `product_name` | `VARCHAR` | No | Nome del prodotto | Stringa non vuota dopo trim | `Wireless Mouse` |
| `category` | `VARCHAR` | No | Categoria analitica | Stringa non vuota dopo trim | `Accessories` |

Lo snapshot viene caricato una sola volta e non evolve. Se contiene più righe
con lo stesso `product_id`, prevale il payload canonico lessicograficamente
maggiore calcolato su `product_name` e `category`; duplicati esatti collassano.
La scelta è deterministica e non dipende dalla posizione nel file.

## `orders.csv` contract

Grain fisico: una riga rappresenta una versione di una order line. Il risultato
dell'upsert contiene una sola riga corrente per `order_line_id`.

- chiave univoca della riga: `order_line_id`;
- chiave dell'ordine: `order_id`, non necessariamente univoca perché un ordine
  può contenere più righe;
- riferimenti dimensionali: `customer_id` e `product_id`.

| Colonna | Tipo logico | Null | Presenza | Descrizione | Regola di validità | Esempio |
|---|---|---:|---|---|---|---|
| `order_line_id` | `VARCHAR` | No | Tutti i run | Identificatore naturale della riga | Stringa non vuota dopo trim | `OL_00001234` |
| `order_id` | `VARCHAR` | No | Tutti i run | Identificatore dell'ordine | Stringa non vuota dopo trim | `ORD_00000987` |
| `customer_id` | `VARCHAR` | No | Tutti i run | Riferimento al cliente | Stringa non vuota; può essere orfana | `CUS_000123` |
| `product_id` | `VARCHAR` | No | Tutti i run | Riferimento al prodotto | Stringa non vuota; può essere orfana | `PRD_000042` |
| `quantity` | `INTEGER` | No | Tutti i run | Quantità ordinata | Valore maggiore di zero | `2` |
| `unit_price` | `DECIMAL(12,2)` | Sì | Tutti i run | Prezzo unitario applicato | Valido economicamente solo se maggiore di zero | `19.90` |
| `order_status` | `VARCHAR` | No | Tutti i run | Stato della riga d'ordine | Uno tra `completed`, `cancelled` | `completed` |
| `order_date` | `DATE` | No | Tutti i run | Data commerciale dell'ordine | Data ISO 8601 nel periodo previsto dal run | `2025-01-08` |
| `updated_at` | `TIMESTAMP(6)` UTC | No | Tutti i run | Istante dell'ultima versione | Timestamp ISO 8601 valido con suffisso `Z` | `2025-01-08T14:05:10.123456Z` |
| `sales_channel` | `VARCHAR` | Sì | Dal Run 2 | Canale di acquisizione | Se valorizzato: `web`, `mobile` o `marketplace` | `mobile` |

Il ricavo lordo di riga è `quantity * unit_price`. Una riga contribuisce alle
metriche economiche soltanto quando `order_status = 'completed'`,
`quantity > 0` e `unit_price > 0`. Le righe annullate o economicamente non
valide rimangono nella fact con ricavo metrico escluso.

### Prevalenza e classificazione

Per ogni `order_line_id` prevale la versione con `updated_at` maggiore. A parità
di timestamp prevale il payload canonico lessicograficamente maggiore calcolato
su tutte le colonne diverse da `order_line_id`; duplicati esatti sono no-op.

La classificazione non richiede una colonna aggiuntiva nel CSV:

- **insert**: `order_line_id` non presente nello stato cumulativo precedente;
- **correction**: chiave già presente nel Run 1 e payload prevalente diverso,
  con almeno `quantity` o `unit_price` modificato;
- **no-op**: la versione ricevuta non prevale oppure coincide con quella
  corrente.

## Run specification

### Run 1 — Giorno 1

- file: `customers.csv`, `products.csv`, `orders.csv`;
- caricamento: snapshot una tantum per anagrafiche, primo incremento ordini;
- intervallo `order_date`: dal `2025-01-01` al `2025-01-10`, estremi inclusi;
- `orders.csv`: esattamente 3.000 righe e 3.000 `order_line_id` distinti;
- `customers.csv` e `products.csv`: almeno una riga; il loro conteggio esatto
  non è fissato, ma ogni riferimento non orfano degli ordini deve essere
  risolvibile;
- risultato: 3.000 order line correnti distinte.

### Run 2 — Giorno 2 incrementale

- file: soltanto `orders.csv`;
- caricamento: incremento di sole nuove chiavi;
- intervallo `order_date`: dal `2025-01-11` al `2025-01-20`, estremi inclusi;
- esattamente 2.000 righe e 2.000 nuovi `order_line_id`, disgiunti dal Run 1;
- introduce la colonna nullable `sales_channel`;
- risultato cumulativo: esattamente 5.000 order line distinte, senza update.

### Run 3 — Late-arriving data

- file: soltanto `orders.csv`;
- caricamento: upsert di insert tardivi e correction;
- esattamente 15 righe:
  - 10 insert con chiavi mai apparse, `order_date` compresa tra `2025-01-01` e
    `2025-01-10` e `updated_at` successivo all'istante di caricamento logico del
    Run 2; possono avere `sales_channel` nullo oppure valorizzato con `web`,
    `mobile` o `marketplace`;
  - 5 correction riferite a cinque `order_line_id` distinti del Run 1, con
    `updated_at` maggiore della versione precedente e modifica di almeno uno
    tra `quantity` e `unit_price`; sono record completi e, poiché correggono
    righe del Run 1, hanno `sales_channel` uguale a `NULL`;
- risultato: 10 insert, 5 update, 5.010 chiavi distinte e nessun duplicato.

Questi sono late-arriving facts: la data commerciale è storica, mentre la
versione arriva nel terzo caricamento. Non rappresentano dimensioni tardive.

## Data-quality rules

| ID | Dataset | Condizione | Severità | Comportamento atteso | Impatto sulle metriche |
|---|---|---|---|---|---|
| `DQ_CUS_001` | customers | `customer_id` nullo/vuoto o `updated_at` non interpretabile | Errore strutturale bloccante | Rifiutare il file/run e segnalare l'errore | Nessuna elaborazione parziale |
| `DQ_CUS_002` | customers | Più versioni dello stesso `customer_id` | Risoluzione deterministica | Conservare raw; scegliere massimo timestamp e payload canonico | Una sola riga corrente |
| `DQ_CUS_003` | customers | Attributo non conforme alle regole dichiarate | Warning | Conservare e marcare non conforme | Nessun impatto automatico sul fatturato |
| `DQ_PRD_001` | products | `product_id` nullo/vuoto | Errore strutturale bloccante | Rifiutare il file/run | Nessuna elaborazione parziale |
| `DQ_PRD_002` | products | Più righe dello stesso `product_id` | Risoluzione deterministica | Conservare raw; scegliere payload canonico | Una sola riga dimensionale |
| `DQ_ORD_001` | orders | `order_line_id` nullo/vuoto o campo obbligatorio non interpretabile | Errore strutturale bloccante | Rifiutare il file/run | Nessuna elaborazione parziale |
| `DQ_ORD_002` | orders | `unit_price` nullo o minore/uguale a zero | Record conservato, non valido economicamente | Mantenere nella fact e impostare il flag di validità economica a falso | Escludere il ricavo |
| `DQ_ORD_003` | orders | `quantity` minore/uguale a zero | Record conservato, non valido economicamente | Mantenere nella fact e impostare il flag di validità economica a falso | Escludere il ricavo |
| `DQ_ORD_004` | orders | `order_status = 'cancelled'` | Record conservato, non contribuente | Mantenere nella fact | Escludere il ricavo |
| `DQ_ORD_005` | orders | `customer_id` non risolto | Chiave orfana | Associare il membro tecnico cliente `Unknown` | Fatto conservato |
| `DQ_ORD_006` | orders | `product_id` non risolto | Chiave orfana | Associare il membro tecnico prodotto `Unknown` | Fatto conservato |
| `DQ_ORD_007` | orders | Più versioni dello stesso `order_line_id` | Risoluzione deterministica | Applicare prevalenza; conservare le versioni raw | Una sola riga corrente |
| `DQ_ORD_008` | orders | `order_status` fuori dominio | Record conservato, non valido economicamente | Mantenere nella fact e impostare il flag di validità economica a falso; non bloccare il file/run | Escludere il ricavo |

Gli scenari sintetici devono includere almeno un ordine annullato, almeno un
prezzo nullo o non positivo e almeno una chiave orfana cliente o prodotto. I
conteggi esatti di queste anomalie non sono fissati.

## Dimensional behavior

### `dim_customers`

Una riga per `customer_id` dopo deduplicazione. Usa una surrogate key stabile e
conserva soltanto lo stato corrente: comportamento SCD Type 1, non SCD Type 2.
Include un membro tecnico `Unknown` con surrogate key riservata e business key
tecnica distinta dalle chiavi sorgente.

### `dim_products`

Una riga per `product_id` dopo deduplicazione deterministica dello snapshot.
Usa una surrogate key stabile e include un membro tecnico `Unknown`.

### `dim_date`

Una riga per data del calendario necessaria a coprire almeno il periodo degli
ordini. La business key è la data ISO e la surrogate key è stabile.

### `fct_order_lines`

Grain: una riga corrente per `order_line_id`. Contiene le surrogate key di
cliente, prodotto e data, oltre alle misure e agli attributi necessari del
contratto ordini. I join verso le dimensioni preservano sempre il fatto:
le business key non risolte usano la surrogate key `Unknown`, senza inner join
che eliminino righe.

La fact conserva righe annullate, prezzi nulli o non positivi e quantità non
positive. Espone un indicatore di validità economica; il ricavo utilizzabile
nelle metriche è valorizzato soltanto per righe completate e valide. Gli errori
restano quindi analizzabili senza alterare il grain.

## Incremental and upsert semantics

`order_line_id` è la unique key. Per ogni chiave, il confronto usa prima
`updated_at` e poi il payload canonico:

- chiave assente: insert;
- chiave presente e versione candidata prevalente con payload diverso: update;
- versione uguale o non prevalente: no-op.

Le parità di `updated_at` sono indipendenti dall'ordine di arrivo perché il
payload canonico determina sempre lo stesso vincitore. L'upsert produce al
massimo una riga corrente per chiave. Rieseguendo un run invariato, tutte le
righe diventano no-op: conteggi, payload e numero di chiavi non cambiano.

Dopo Run 3 sono attesi 5.010 insert distinti complessivi, dei quali 10 tardivi,
e cinque chiavi aggiornate una volta rispetto allo stato dopo Run 2.

## Schema evolution

Run 2 aggiunge a `orders.csv` la colonna `sales_channel`, utile per analizzare
ricavi e volumi per canale. La colonna:

- è nullable e non modifica il grain né alcuna chiave;
- ammette `web`, `mobile` e `marketplace`;
- è assente fisicamente nel file del Run 1;
- viene normalizzata a `NULL` per tutte le righe precedenti;
- rimane disponibile nel Run 3;
- le cinque correction sono record completi ma, riferendosi a righe del Run 1,
  mantengono `sales_channel` uguale a `NULL`;
- i dieci insert tardivi possono avere `sales_channel` nullo oppure uno dei
  valori ammessi.

L'unione degli schemi deve allineare le colonne per nome e rappresentare
l'assenza storica con `NULL`. Nessun file precedente deve essere riscritto.

## Acceptance criteria

- [ ] Run 1 produce esattamente 3.000 `order_line_id` distinti.
- [ ] Run 1 + Run 2 producono esattamente 5.000 chiavi distinte.
- [ ] Run 3 contiene esattamente 10 insert e 5 correction di chiavi del Run 1.
- [ ] Lo stato finale contiene esattamente 5.010 chiavi distinte.
- [ ] Sono applicati esattamente 5 update rispetto allo stato dopo Run 2.
- [ ] Non esistono duplicati finali di `order_line_id`.
- [ ] Ogni correction modifica `quantity` o `unit_price` ed è prevalente.
- [ ] Rieseguire ciascun run non modifica lo stato risultante.
- [ ] Gli ordini annullati sono conservati ma esclusi dal fatturato.
- [ ] Prezzi nulli o non positivi sono conservati ma esclusi dalle metriche.
- [ ] Le chiavi cliente o prodotto orfane sono associate a `Unknown`.
- [ ] La selezione del cliente corrente è deterministica.
- [ ] Il Run 1 senza `sales_channel` resta compatibile con lo schema evoluto.
- [ ] Grain, business key e conteggi coincidono con
  `expected_run_metrics.yml`.
