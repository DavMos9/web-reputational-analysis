---
name: Bug Report
about: Segnala un problema o un comportamento inatteso nella pipeline
title: '[BUG] '
labels: bug
assignees: ''
---

## Fonte o modulo coinvolto

Indica dove si è verificato il problema:

- [ ] `news` — NewsAPI
- [ ] `gdelt` — GDELT DOC 2.0
- [ ] `wikipedia` — Wikipedia
- [ ] `youtube` — YouTube Data API
- [ ] `guardian` — The Guardian
- [ ] `nyt` — New York Times
- [ ] `normalizer.py`
- [ ] `cleaner.py`
- [ ] `deduplicator.py`
- [ ] `validator.py`
- [ ] `main.py` (orchestrazione generale)

## Comando eseguito

```bash
python main.py --target "..." --queries "..." --sources ... --max-results ...
```

## Output del terminale

Incolla il log completo prodotto dalla pipeline (quello che inizia con `=== Avvio pipeline ===`):

```
2026-XX-XXT... [INFO] === Avvio pipeline ===
2026-XX-XXT... [INFO] Target: ...
...
```

## Comportamento osservato

Descrivi cosa è successo: es. "Il collector GDELT restituisce 0 record senza errori", "La pipeline termina con `KeyError` nel normalizer", "Il file CSV finale è vuoto nonostante 90 record processati".

## Comportamento atteso

Descrivi cosa dovrebbe succedere: es. "GDELT dovrebbe raccogliere almeno 20 articoli", "Il normalizer dovrebbe convertire la data in ISO 8601 senza sollevare eccezioni".

## Ambiente

- OS: 
- Python: 
- Commit / branch: 

## Chiave API configurata?

- [ ] Sì, la chiave per questa fonte è presente nel `.env`
- [ ] No / Non so
- [ ] La fonte non richiede chiave (GDELT, Wikipedia)
