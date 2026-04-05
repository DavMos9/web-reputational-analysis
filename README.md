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

- Google Search API
- YouTube Data API
- NewsAPI
- Wikipedia
- GDELT DOC 2.0

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
  - google_collector.py

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

{
  "source_type": "google",
  "target_entity": "Nome Cognome",
  "query": "Nome Cognome scandalo",
  "title": "Titolo risultato",
  "url": "https://example.com",
  "domain": "example.com",
  "published_at": null,
  "retrieved_at": "2026-04-04T15:00:00Z"
}

Schema completo disponibile in:
docs/data_schema.tex

## Configurazione

Le API key non sono incluse nel repository.

Creare un file `.env` oppure usare variabili ambiente:

GOOGLE_API_KEY=
GOOGLE_CX=
YOUTUBE_API_KEY=
NEWS_API_KEY=

## Installazione

git clone https://github.com/DavMos9/web-reputational-analysis.git  
cd web-reputational-analysis  

python3 -m venv venv  
source venv/bin/activate  

pip install -r requirements.txt  

## Utilizzo

python main.py

## Stato del progetto

- Architettura definita
- Documentazione completata
- Implementazione in corso

## Note

- I dati nella cartella `data/` non vengono versionati
- Le API possono avere limiti di quota
- Deduplicazione:
  - base in Python
  - avanzata in ETL (IBM)

## Roadmap

- Implementazione collectors
- Normalizzazione dati
- Deduplicazione
- Export JSON/CSV
- Integrazione ETL

## Licenza

Da definire