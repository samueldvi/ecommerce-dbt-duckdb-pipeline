# Requisiti del progetto

## Scopo

Il progetto realizza una pipeline analitica locale per uno scenario e-commerce.
La soluzione deve generare dati esclusivamente sintetici, caricarli in modo
riproducibile e trasformarli con dbt in un modello dimensionale interrogabile.

Questo documento presenta esclusivamente i requisiti tecnici in forma
anonimizzata.

## Terminologia canonica

Quando una descrizione funzionale usa un termine equivalente, nel repository si
applicano i nomi canonici definiti in `docs/data_contracts.md`.

| Concetto funzionale | Nome canonico |
|---|---|
| Clienti | `customers.csv`, `customer_id` |
| Prodotti | `products.csv`, `product_id` |
| Righe d'ordine | `orders.csv`, `order_line_id` |
| Ordine | `order_id` |
| Quantità | `quantity` |
| Prezzo unitario | `unit_price` |
| Stato ordine | `order_status` |
| Data ordine | `order_date` |
| Data di aggiornamento | `updated_at` |
| Canale di vendita | `sales_channel` |

## Dataset sintetici

La base dati richiesta comprende:

- esattamente 200 `customer_id` distinti;
- esattamente 80 `product_id` distinti;
- esattamente 5.000 `order_line_id` iniziali distinti, distribuiti su 30
  giorni;
- versioni multiple per 3 o 4 clienti già incluse nello snapshot iniziale
  `customers.csv`, senza aumentare il numero di `customer_id` distinti;
- almeno il 15% degli `order_id` distinti composto da più righe;
- relazioni tra righe ordine, clienti e prodotti tramite le rispettive business
  key.

Per il requisito sugli ordini multi-riga, il numeratore è il numero di
`order_id` distinti associati ad almeno due `order_line_id` distinti; il
denominatore è il numero totale di `order_id` distinti nelle 5.000 righe
iniziali. Il rapporto tra numeratore e denominatore deve essere almeno `0,15`.

I dati devono essere interamente sintetici: non sono ammessi dati personali,
commerciali o aziendali reali. La generazione deve essere riproducibile e
adeguata a esercitare i casi funzionali e di qualità previsti.

Il dataset deve includere intenzionalmente:

- `customer_id` non presenti nell'anagrafica clienti;
- `product_id` non presenti nell'anagrafica prodotti;
- `unit_price` nulli o negativi.

Queste anomalie servono a verificare la qualità e la conservazione dei dati; il
loro trattamento analitico è definito dai chiarimenti e dai contratti tecnici.

## Architettura e tecnologie

La soluzione deve usare:

- Python per generare e caricare i dati raw;
- `dbt-core` con adapter DuckDB per trasformazioni e controlli;
- DuckDB come database analitico locale;
- modelli dbt organizzati almeno nei livelli staging e marts;
- uno star schema con una fact al grain della singola riga ordine;
- dimensioni analitiche per cliente e prodotto.

La modellazione deve rendere espliciti grain, business key, surrogate key,
relazioni dimensionali, regole di deduplicazione e trattamento delle anomalie.

## Caricamenti incrementali

Il comportamento incrementale deve essere dimostrato attraverso tre run riferiti
a finestre di 10 giorni.

La simulazione deve comprendere:

- Run 1 con le righe iniziali dei primi 10 giorni;
- Run 2 con le righe iniziali dei successivi 10 giorni;
- Run 3 con le righe iniziali degli ultimi 10 giorni;
- esattamente 5.000 righe iniziali complessive nelle tre finestre, senza
  imporre una ripartizione numerica per run non specificata;
- 15 late-arriving data ricevuti dopo il Run 2 e aggiuntivi alle 5.000 righe
  iniziali;
- inserimento di nuove righe tardive e correzione di righe già acquisite;
- idempotenza delle riesecuzioni;
- evoluzione dello schema nel Run 3 mediante il concetto canonico
  `sales_channel`: la colonna è assente nei Run 1 e 2 e compare per la prima
  volta nelle righe iniziali del Run 3;
- mantenimento dello schema precedente per le 15 righe late-arriving ricevute
  dopo il Run 2, che non contengono `sales_channel`.

I conteggi operativi, la collocazione delle correction e la semantica degli
upsert sono precisati in `docs/clarifications.md`,
`docs/data_contracts.md` e `docs/expected_run_metrics.yml`.

## Modello analitico

Il risultato deve adottare uno star schema in cui:

- la fact conserva una riga corrente per `order_line_id`;
- la fact mantiene le misure e gli attributi necessari per analizzare quantità,
  prezzi, stato, data e canale;
- le dimensioni cliente e prodotto espongono lo stato analitico previsto;
- le chiavi dimensionali non risolte non causano la perdita dei fatti;
- le regole di validità economica rendono distinguibili le righe utilizzabili
  nelle metriche da quelle conservate per osservabilità.

## Test automatici

Devono essere presenti test automatici sufficienti a verificare:

- struttura, grain e business key dei dataset e dei modelli;
- qualità dei dati e trattamento delle anomalie richieste;
- correttezza dei caricamenti incrementali e delle correction;
- idempotenza;
- gestione dei late-arriving data;
- compatibilità con l'evoluzione dello schema;
- coerenza dei risultati quantitativi con gli oracoli approvati;
- determinismo e riproducibilità dove richiesti.

## Query analitiche

I deliverable devono includere almeno queste query sul modello dimensionale:

- fatturato totale per categoria prodotto e mese;
- top 10 clienti per spesa totale nel periodo;
- numero di ordini per regione e settimana;
- almeno una query con window function, per esempio un ranking o un running
  total.

## Trasferimento concettuale

Deve essere prodotto un documento che descriva come trasferire concettualmente
la soluzione locale verso Databricks e Delta Lake. Il documento deve mettere in
relazione sorgenti, livelli di trasformazione, caricamenti incrementali,
controlli di qualità e modello analitico, senza richiedere l'implementazione
effettiva della piattaforma cloud.

## Documentazione e tracciabilità del lavoro

La consegna deve comprendere:

- documentazione delle scelte architetturali e di modellazione;
- istruzioni riproducibili per configurazione, esecuzione e verifica;
- descrizione dei contratti e delle regole di qualità;
- log delle ore impiegate;
- eventuali note di revisione;
- limitazioni, assunzioni e decisioni rilevanti.

## Deliverable finali

Sono richiesti almeno:

- codice Python per generazione e caricamento raw;
- progetto dbt con staging, marts, test e documentazione;
- database o procedura riproducibile per costruirlo;
- query analitiche richieste;
- documentazione di trasferimento concettuale a Databricks/Delta Lake;
- documentazione delle scelte, istruzioni operative e log delle ore;
- eventuali review notes applicabili.

Non devono essere versionati dati generati, segreti, configurazioni personali o
artefatti temporanei.

## Criteri di verifica

La soluzione è verificabile quando:

- usa soltanto dati sintetici;
- è riproducibile a partire dal repository e dalle dipendenze dichiarate;
- rispetta grain, chiavi, schemi e regole normative approvate;
- dimostra i tre run incrementali, i late-arriving data e la schema evolution;
- conserva e rende osservabili le anomalie intenzionali;
- produce uno star schema coerente;
- supera i test automatici pertinenti;
- espone risultati analitici documentati;
- non contiene informazioni identificative o materiale interno;
- include documentazione sufficiente alla revisione e al trasferimento
  concettuale.
