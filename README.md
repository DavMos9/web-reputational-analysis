# Web Reputational Analysis

> Pipeline Python per la raccolta, normalizzazione, arricchimento ed esportazione di dati da fonti web eterogenee, finalizzata ad attività di web reputational analysis.

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-in%20sviluppo-yellow)

---

## Indice

- [Panoramica](#panoramica)
- [Fonti dati](#fonti-dati)
- [Architettura](#architettura)
- [Requisiti](#requisiti)
- [Installazione](#installazione)
- [Configurazione](#configurazione)
- [Utilizzo](#utilizzo)
- [Schema dati](#schema-dati)
- [Limiti noti](#limiti-noti)
- [Struttura del progetto](#struttura-del-progetto)
- [Documentazione](#documentazione)
- [License](#license)

---

## Panoramica

Il progetto consente di raccogliere informazioni su un'entità (persona, brand, organizzazione) da più fonti online, uniformarle in uno schema dati coerente, arricchirle con language detection e sentiment analysis multilingue, ed esportarle in formato JSON e CSV.

I dati prodotti sono pensati per essere utilizzati in pipeline di analisi successive, in particolare su **IBM Cloud Pak for Data**.

---

## Fonti dati

| Fonte | Tipo | API Key | Limite gratuito |
|---|---|---|---|
| [NewsAPI](https://newsapi.org) | Articoli di news | Sì | 100 req/giorno |
| [GDELT DOC 2.0](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/) | Media globale | No | Nessuno (rate limit variabile) |
| [Wikipedia](https://www.mediawiki.org/wiki/API:Main_page) | Contesto enciclopedico | No | Nessuno |
| [YouTube Data API v3](https://developers.google.com/youtube/v3) | Video e canali | Sì | 10.000 unità/giorno |
| [The Guardian](https://open-platform.theguardian.com) | Articoli giornalistici | Sì | 5.000 req/giorno |
| [New York Times](https://developer.nytimes.com) | Articoli giornalistici | Sì | 4.000 req/giorno |

> **Nota:** Google Search API è stata valutata ma esclusa per assenza di piano gratuito adeguato.

---

## Architettura

```
Input (target + queries)
        │
        ▼
  Collectors (API)
        │
        ▼
  Persistenza raw  →  data/raw/
        │
        ▼
   Normalizer
        │
        ▼
    Cleaner
        │
        ▼
  Deduplicator
        │
        ▼
    Enricher  ←  language detection + sentiment analysis (NLP opzionale)
        │
        ▼
   Exporters (JSON + CSV)  →  data/final/
        │
        ▼
IBM Cloud Pak for Data
```

La pipeline è modulare e ogni step è indipendente. L'enrichment (language detection + sentiment) è opzionale: se le dipendenze NLP non sono installate, i campi `language` e `sentiment` rimangono `null` senza interrompere la pipeline.

---

## Requisiti

- Python 3.11+
- pip

Dipendenze core: `requests`, `python-dotenv`, `python-dateutil`, `wikipedia-api`

Dipendenze NLP (opzionali): `langdetect`, `transformers`, `torch`, `sentencepiece`, `protobuf`, `tiktoken`

---

## Installazione

```bash
# Clona la repository
git clone https://github.com/DavMos9/web-reputational-analysis.git
cd web-reputational-analysis

# Crea e attiva un ambiente virtuale
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
.venv\Scripts\activate     # Windows

# Installa le dipendenze core
pip install -e .

# Installa anche le dipendenze NLP (consigliato per enrichment completo)
pip install -e ".[nlp]"
```

> **Nota:** il modello XLM-RoBERTa per il sentiment (~1.1 GB) viene scaricato automaticamente da HuggingFace alla prima esecuzione e cachato localmente.

---

## Configurazione

Copia il file di esempio e inserisci le tue API key:

```bash
cp .env.example .env
```

Modifica `.env`:

```env
YOUTUBE_API_KEY=la_tua_chiave
NEWS_API_KEY=la_tua_chiave
GUARDIAN_API_KEY=la_tua_chiave
NYT_API_KEY=la_tua_chiave
```

Le fonti senza API key (GDELT, Wikipedia) funzionano senza configurazione aggiuntiva.

---

## Utilizzo

### Esecuzione base

```bash
python main.py --target "Giorgia Meloni" --queries "Giorgia Meloni"
```

Output atteso:

```
2026-04-08T18:12:18 [INFO] === Pipeline avviata: target='Giorgia Meloni', fonti=[...] ===
[news] Raccolti 20 record per query: 'Giorgia Meloni'
[gdelt] Raccolti 20 record per query: 'Giorgia Meloni'
[wikipedia] Raccolti 1 record per query: 'Giorgia Meloni'
[youtube] Raccolti 20 record per query: 'Giorgia Meloni'
[guardian] Raccolti 20 record per query: 'Giorgia Meloni'
[nyt] Raccolti 10 record per query: 'Giorgia Meloni'
2026-04-08T18:12:32 [INFO] Deduplicati: 0 rimossi, 91 record unici.
2026-04-08T18:12:32 [INFO] Enrichment: 87/91 record con language, 74/91 con sentiment.
2026-04-08T18:12:32 [INFO] === Pipeline completata: 91 record finali. ===
```

### Query multiple

```bash
python main.py --target "Elon Musk" --queries "Elon Musk Tesla" "Elon Musk SpaceX"
```

### Selezione fonti specifiche

```bash
python main.py --target "Apple" --queries "Apple" --sources news gdelt guardian
```

### Lingua NewsAPI

NewsAPI supporta il filtraggio per lingua. Il default è `en` (inglese):

```bash
# Risultati in italiano
python main.py --target "Giorgia Meloni" --queries "Giorgia Meloni" --news-language it

# Risultati in francese
python main.py --target "Emmanuel Macron" --queries "Macron" --news-language fr
```

> **Attenzione:** NewsAPI nel piano gratuito supporta un sottoinsieme limitato di lingue. Verificare la disponibilità su [newsapi.org/docs](https://newsapi.org/docs).

### Opzioni disponibili

| Parametro | Descrizione | Default |
|---|---|---|
| `--target` | Entità da analizzare | obbligatorio |
| `--queries` | Una o più query di ricerca | obbligatorio |
| `--sources` | Fonti da interrogare | tutte |
| `--max-results` | Risultati massimi per fonte/query | 20 |
| `--news-language` | Lingua per NewsAPI (ISO 639-1) | `en` |
| `--no-raw` | Non salvare i payload grezzi | False |

### Output

I file vengono salvati in:

```
data/
├── raw/    # Payload grezzi per debug e audit
└── final/  # Dataset finale (JSON + CSV)
```

---

## Schema dati

Ogni record rispetta il seguente schema unificato, definito in `models/record.py`:

```json
{
  "source":         "news",
  "query":          "Giorgia Meloni",
  "target":         "Giorgia Meloni",
  "title":          "Titolo articolo",
  "text":           "Corpo del testo o estratto",
  "date":           "2026-04-08",
  "url":            "https://example.com/article",
  "author":         "Nome Autore",
  "language":       "it",
  "domain":         "example.com",
  "retrieved_at":   "2026-04-08T16:12:18+00:00",
  "views_count":    null,
  "likes_count":    null,
  "comments_count": null,
  "sentiment":      0.312456
}
```

**Campi obbligatori:** `source`, `query`, `target`, `title`, `text`, `url`

**Campi opzionali:** tutti gli altri (`null` se non disponibili)

**`language`:** codice ISO 639-1 (es. `"en"`, `"it"`). Per sorgenti che non espongono la lingua nel payload (Guardian, NYT), il valore viene rilevato automaticamente dall'enricher tramite langdetect.

**`sentiment`:** float in `[-1.0, 1.0]`. Calcolato come `P(positive) - P(negative)` tramite XLM-RoBERTa multilingue. Supportato per: ar, en, fr, de, hi, it, pt, es. `null` per lingue non supportate o testo troppo breve.

**`raw_payload`:** presente nel modello interno, escluso dall'export finale.

**Date:** formato `YYYY-MM-DD` (solo data, no orario).

Per la specifica completa vedere [`docs/Web_Reputational_Analysis_Data_Contract.pdf`](docs/Web_Reputational_Analysis_Data_Contract.pdf).

---

## Limiti noti

**GDELT:** applica rate limiting variabile. In caso di errori 429, il collector esegue retry automatico con backoff esponenziale (fino a 3 tentativi, delay iniziale 3s). Se GDELT è sovraccarico, alcune query potrebbero restituire 0 risultati. Query con token molto corti (< 3 caratteri) vengono sanitizzate automaticamente.

**NewsAPI:** il piano gratuito è limitato a 100 richieste/giorno e agli articoli degli ultimi 30 giorni. La lingua è configurabile via `--news-language` ma non tutte le lingue hanno la stessa copertura.

**Enrichment NLP:** richiede `pip install -e ".[nlp]"`. Il modello XLM-RoBERTa (~1.1 GB) viene scaricato alla prima esecuzione. Senza le dipendenze NLP, i campi `language` e `sentiment` rimangono `null` ma la pipeline procede normalmente.

**Wikipedia:** restituisce sempre 1 pagina per target indipendentemente da `--max-results`. La ricerca avviene sul nome dell'entità (`target`), non sulla query tematica.

---

## Struttura del progetto

```
web-reputational-analysis/
├── main.py                  # Entry point e orchestrazione CLI
├── config.py                # Caricamento variabili d'ambiente
├── pyproject.toml           # Dipendenze e configurazione progetto
├── .env.example
│
├── models/                  # Tipi di dato della pipeline
│   └── record.py            # RawRecord (grezzo) e Record (normalizzato)
│
├── collectors/              # Un file per ogni fonte dati
│   ├── base.py              # Classe base astratta (BaseCollector)
│   ├── news_collector.py
│   ├── gdelt_collector.py
│   ├── wikipedia_collector.py
│   ├── youtube_collector.py
│   ├── guardian_collector.py
│   └── nyt_collector.py
│
├── pipeline/                # Passi di trasformazione
│   ├── runner.py            # PipelineRunner — orchestratore
│   ├── normalizer.py        # RawRecord → Record (date YYYY-MM-DD, URL, domini)
│   ├── cleaner.py           # Pulizia stringhe e null
│   ├── deduplicator.py      # Rimozione duplicati (URL + titolo+dominio)
│   └── enricher.py          # Language detection + sentiment analysis (NLP)
│
├── storage/                 # Persistenza
│   └── raw_store.py         # Salvataggio RawRecord grezzi in data/raw/
│
├── exporters/               # Export finale
│   ├── json_exporter.py     # Serializzazione JSON
│   └── csv_exporter.py      # Serializzazione CSV
│
├── utils/                   # Utility condivise
│   └── slugify.py           # target_slug() e now_timestamp()
│
├── data/
│   ├── raw/                 # Payload grezzi
│   └── final/               # Output finale
│
└── tests/                   # Test unitari
    ├── test_collector.py
    ├── test_normalizer.py
    ├── test_cleaner.py
    ├── test_deduplicator.py
    ├── test_exporters.py
    ├── test_raw_store.py
    ├── test_runner.py
    └── test_slugify.py
```

---

## Documentazione

- [Documentazione di progetto](docs/Web_Reputational_Analysis_Project.pdf)
- [Data Contract](docs/Web_Reputational_Analysis_Data_Contract.pdf)
- [Wiki](../../wiki) — installazione dettagliata, architettura, riferimento collector

---

## License

Distribuito sotto licenza MIT. Vedere [LICENSE](LICENSE) per i dettagli.
