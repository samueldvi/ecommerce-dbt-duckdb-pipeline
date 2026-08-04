# Politica di sicurezza

## Segnalazione responsabile

Non pubblicare vulnerabilità, credenziali o dettagli sfruttabili nelle issue.
Usare una segnalazione privata tramite la sezione **Security** del repository e
includere impatto, versione o commit interessato, passi di riproduzione e una
possibile mitigazione. Se la segnalazione privata di GitHub non è disponibile,
contattare il maintainer attraverso un canale privato indicato nel suo profilo
GitHub.

Non inserire mai token, password, chiavi o dati personali nella segnalazione se
non sono indispensabili; eventuali valori sensibili devono essere oscurati.

## Versioni supportate

Durante lo sviluppo è supportato il contenuto del branch `main`. Le correzioni
di sicurezza vengono preparate su un branch dedicato, verificate dalla CI e
integrate dopo revisione. Non sono previste patch per snapshot storici del
repository.

## Controlli automatici

La CI esegue una scansione delle dipendenze Python con `pip-audit` e una
scansione dei segreti con Gitleaks. Questi controlli non sostituiscono la
revisione manuale né la rotazione immediata di una credenziale eventualmente
esposta.
