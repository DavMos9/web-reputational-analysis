# Contributing

Grazie per l'interesse nel progetto. Ecco le linee guida per contribuire in modo coerente con l'architettura esistente.

---

## Prerequisiti

- Python 3.11+
- Ambiente virtuale attivato (`python -m venv .venv && source .venv/bin/activate`)
- Dipendenze installate (`pip install -r requirements.txt`)
- File `.env` configurato con le API key necessarie

---

## Flusso di contribuzione

1. **Apri una issue** prima di iniziare lavori significativi — usa i template in `.github/ISSUE_TEMPLATE/`.
2. **Crea un branch** dal `main` con un nome descrittivo (es. `feat/reddit-collector`, `fix/gdelt-timeout`).
3. **Implementa la modifica** rispettando le regole di architettura sotto.
4. **Aggiorna la documentazione** se hai aggiunto o modificato comportamenti.
5. **Apri una pull request** verso `main` con una descrizione chiara delle modifiche.

---

## Regole di architettura

Il progetto segue una pipeline modulare con responsabilità separate. **Non mescolare le responsabilità dei moduli.**

### Aggiungere un nuovo collector

1. Crea `collectors/{nome}_collector.py`.
2. Eredita da `BaseCollector` e definisci `source_id`.
3. Implementa `collect(target, query, max_results)` → `list[RawRecord]`.
4. Il collector **non normalizza, non pulisce, non salva** — restituisce solo `RawRecord` con il payload grezzo.
5. Gestisci tutti gli errori API internamente con `try/except` e logging.
6. Aggiungi il mapping nel normalizer (`pipeline/normalizer.py`) per convertire `RawRecord` → `Record`.
7. Registra il collector in `collectors/__init__.py` (o dove è definito il `REGISTRY`).
8. Aggiorna la wiki [Collectors](../../wiki/Collectors) e [Schema-Dati](../../wiki/Schema-Dati) con il nuovo mapping.

### Modificare lo schema dati

Lo schema è definito in `models/record.py`. Qualsiasi modifica deve:

- Essere retrocompatibile con i dati già esportati.
- Aggiornare il Data Contract PDF in `docs/`.
- Aggiornare la wiki [Schema-Dati](../../wiki/Schema-Dati).
- Aggiornare `Record._EXPORT_FIELDS` se si aggiungono campi all'export CSV.

### Modificare la pipeline

I passi della pipeline sono in `pipeline/`. L'ordine è definito in `pipeline/runner.py`:

```
collect → normalize → clean → deduplicate → export
```

Non aggiungere logica di business in `runner.py` — mantienila nei moduli specifici.

---

## Stile del codice

- **Una responsabilità per funzione** — funzioni brevi e focalizzate.
- **Nessun hardcoding** — usa `config.py` e variabili d'ambiente.
- **Logging** con i livelli corretti: `info` per flusso normale, `warning` per anomalie, `error` per fallimenti.
- **Type hints** su tutte le funzioni pubbliche.
- **Docstring** sui metodi pubblici (formato Google style o NumPy style).

---

## Test

Ogni nuovo collector o modifica ai processor deve essere coperta da test unitari in `tests/`.

```bash
# Esegui tutti i test
python -m pytest tests/

# Esegui un file specifico
python -m pytest tests/test_normalizer.py -v
```

I test non devono effettuare chiamate API reali — usa mock per le risposte delle fonti esterne.

---

## Segnalare un bug

Usa il template [`bug_report`](.github/ISSUE_TEMPLATE/bug_report.md). Includi sempre il log completo dell'esecuzione e il comando usato.

## Richiedere una funzionalità

Usa il template [`feature_request`](.github/ISSUE_TEMPLATE/feature_request.md). Se si tratta di un nuovo collector, specifica fonte, documentazione API, API key necessaria e limite del piano gratuito.
