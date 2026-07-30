# Chiarimenti normativi

Data delle decisioni: **2026-07-30**.

Le decisioni seguenti rendono espliciti i comportamenti richiesti e devono
essere applicate a progettazione, implementazione, test e revisione.

## 1. Customer versioning

- `customers.csv` contiene `updated_at`.
- La versione corrente di ogni `customer_id` è quella con `updated_at` più
  recente.
- In caso di parità si applica il tie-break deterministico definito nel
  contratto tecnico.
- Non si devono usare indicatori arbitrari contenuti nel nome del cliente o in
  altri valori testuali.

## 2. Master data loading

- Soltanto `orders.csv` è suddiviso nei tre run.
- `customers.csv` e `products.csv` sono snapshot completi caricati una sola
  volta con il Run 1.
- Lo snapshot clienti contiene esattamente 200 `customer_id` distinti.
- Lo snapshot prodotti contiene esattamente 80 `product_id` distinti.
- Tre o quattro `customer_id` hanno versioni multiple già presenti nello
  snapshot clienti; tali versioni non aumentano il conteggio dei clienti
  distinti.
- Le versioni duplicate dei clienti sono già presenti nello snapshot iniziale.
- Non devono essere creati run successivi delle anagrafiche per simulare una
  Slowly Changing Dimension.

## 3. Order runs and schema evolution

- Le 5.000 righe iniziali hanno `order_line_id` distinti e sono distribuite su
  30 giorni.
- Run 1 carica le righe iniziali dei primi 10 giorni.
- Run 2 carica le righe iniziali dei successivi 10 giorni.
- Run 3 carica le righe iniziali degli ultimi 10 giorni.
- Non è fissato un conteggio esatto per ciascuna finestra; il totale iniziale
  verificabile sulle tre finestre è esattamente 5.000.
- `sales_channel`, nome canonico equivalente a `canale_vendita`, compare per la
  prima volta nelle righe iniziali del Run 3.
- Run 1 e Run 2 non contengono la colonna `sales_channel`.
- Le 15 righe late-arriving ricevute dopo il Run 2 mantengono lo schema
  precedente e non contengono `sales_channel`.

## 4. Multi-line orders

- Il requisito minimo è soddisfatto quando almeno il 15% degli `order_id`
  distinti nelle 5.000 righe iniziali ha almeno due `order_line_id` distinti.
- Il numeratore è il numero di `order_id` distinti con almeno due
  `order_line_id` distinti.
- Il denominatore è il numero totale di `order_id` distinti nelle 5.000 righe
  iniziali.

## 5. Late-arriving data

- Le 15 righe arrivano dopo il Run 2.
- Dieci righe hanno `order_line_id` mai caricati prima e `order_date` compresa
  nei primi 10 giorni.
- Cinque righe correggono `order_line_id` già caricati nel Run 1.
- Una correction modifica `quantity` oppure `unit_price`.
- Il suo `updated_at` è successivo a quello della versione originale.
- Tutti gli altri campi di business devono rimanere uguali a quelli della riga
  originale.
- Le 15 righe sono aggiuntive rispetto alle 5.000 iniziali.
- Dopo l'upsert il risultato contiene 5.010 `order_line_id` distinti.
- Le cinque correction aggiornano righe esistenti e non aumentano il conteggio
  distinto.
- La pipeline deve essere idempotente.

## 6. Revenue validity

- Gli ordini annullati sono esclusi dal fatturato.
- Le righe con `unit_price` nullo o negativo sono conservate.
- `unit_price` uguale a zero non è classificato come non valido da questa
  regola.
- Le righe con `unit_price` nullo o negativo vengono segnalate come non valide
  economicamente.
- Le righe economicamente non valide sono escluse dalle metriche economiche.
- Nessuna di queste righe deve essere eliminata silenziosamente.

## 7. Unknown dimension members

- I fatti con `customer_id` o `product_id` non presenti nelle anagrafiche devono
  essere conservati.
- Tali fatti devono essere collegati a membri dimensionali surrogate `Unknown`.
- I fatti non devono essere persi tramite inner join.
- L'anomalia deve rimanere osservabile.

## 8. Repository visibility

- Il repository resta privato durante la revisione.
- Un'eventuale pubblicazione avverrà soltanto dopo la conclusione della
  revisione.
- Il repository non deve contenere la traccia originale, nomi, indirizzi e-mail
  o riferimenti interni.
- Anche la cronologia Git deve restare sicura per una futura pubblicazione.

## Precedence

In caso di conflitto si applica il seguente ordine di precedenza:

1. `docs/clarifications.md`
2. `docs/requirements.md`
3. `docs/data_contracts.md`
4. `docs/expected_run_metrics.yml`

`docs/data_contracts.md` traduce i requisiti in vincoli tecnici verificabili.
`docs/expected_run_metrics.yml` contiene gli oracoli quantitativi
machine-readable. Nessuno dei due documenti può introdurre comportamenti
contrari ai chiarimenti espliciti o ai requisiti di livello superiore.

Qualsiasi conflitto o ambiguità deve essere segnalato prima di modificare il
codice. Non si devono inventare requisiti mancanti né dedurre nuove regole
normative dall'implementazione esistente.
