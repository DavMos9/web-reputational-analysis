# Web Reputational Analysis

Pipeline Python per la raccolta, normalizzazione ed esportazione di dati da fonti web eterogenee, finalizzata ad attività di web reputational analysis.

## Overview

Il progetto consente di raccogliere informazioni su un'entità (persona, brand, organizzazione) da più fonti online, uniformarle in uno schema dati coerente ed esportarle in formato strutturato.

I dati prodotti sono pensati per essere utilizzati in pipeline di analisi successive, in particolare su IBM Cloud Pak for Data.

## Obiettivi

- Aggregare dati da fonti multiple
- Uniformare dati eterogenei
- Ridurre duplicati
- Generare dataset strutturati
- Supportare pipeline ETL downstream

## Fonti dati

- NewsAPI
- GDELT DOC 2.0
- Wikipedia
- YouTube Data API
- Reddit (PRAW)

Nota: Google Search API è stata valutata ma esclusa per assenza di piano gratuito adeguato.

## Architettura

Pipeline logica:

1. Input (target e query)
2. Collectors (interrogazione API)
3. Raccolta dati grezzi
4. Normalizzazione
5. Pulizia dati
6. Deduplicazione
7. Export (JSON / CSV)
8. Integrazione con IBM Cloud Pak for Data

## Struttura del progetto

web-reputational-analysis/

- main.py
- config.py
- requirements.txt
- README.md

- collectors/
  - news_collector.py
  - gdelt_collector.py
  - wikipedia_collector.py
  - youtube_collector.py
  - reddit_collector.py

- processors/
- exporters/
- utils/

- data/
  - raw/
  - processed/
  - final/

- tests/
- docs/

## Schema dati

Tutti i dati vengono convertiti in uno schema unificato.

Esempio:

```json
{
  "source_type": "news",
  "target_entity": "Nome Cognome",
  "query": "Nome Cognome scandalo",
  "title": "Titolo risultato",
  "url": "https://example.com",
  "domain": "example.com",
  "published_at": null,
  "retrieved_at": "2026-04-04T15:00:00Z"
}