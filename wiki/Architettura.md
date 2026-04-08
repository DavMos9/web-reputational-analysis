# Architettura

## Pipeline logica

```
Input (target + queries)
        │
        ▼
  ┌─────────────────────────────────┐
  │         COLLECTORS              │
  │  news · gdelt · wikipedia       │
  │  youtube · guardian · nyt       │
  └─────────────────────────────────┘
        │
        ▼
  Persistenza raw  →  data/raw/
        │
        ▼
  ┌─────────────────┐
  │   PROCESSORS    │
  │  1. Normalizer  │  date ISO 8601, URL, domini
  │  2. Cleaner     │  stringhe, null, encoding
  │  3. Deduplicator│  URL + titolo+dominio
  │  4. Validator   │  campi obbligatori
  └─────────────────┘
        │
        ├── data/processed/  ← snapshot intermedio
        │
        ▼
  ┌─────────────────┐
  │   EXPORTERS     │
  │  JSON · CSV     │
  └─────────────────┘
        │
        ▼
  data/final/  →  IBM Cloud Pak for Data
```

## Moduli

### collectors/
Un file per fonte. Ogni collector espone `collect(target_entity, query, **kwargs) -> list[dict]`. Gli errori vengono gestiti internamente: se una fonte fallisce, la pipeline continua con le altre.

### processors/
Applicati in sequenza a tutta la lista di record:
- **normalizer.py** — uniforma tipi e formati
- **cleaner.py** — pulisce il testo
- **deduplicator.py** — rimuove duplicati a due livelli (URL esatto, poi titolo+dominio)
- **validator.py** — scarta record senza campi obbligatori

### main.py
Orchestratore. Accetta argomenti CLI, chiama ogni collector per ogni query, esegue la pipeline di processing e salva gli output.

## Fault tolerance

Ogni collector è avvolto in un `try/except`: un errore (rate limit, timeout, chiave mancante) produce un log di warning e una lista vuota, senza interrompere l'esecuzione degli altri collector.
