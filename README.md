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

### News e media

| Fonte | Tipo | API Key | Limite gratuito |
|---|---|---|---|
| [NewsAPI](https://newsapi.org) | Articoli di news | Sì | 100 req/giorno |
| [GDELT DOC 2.0](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/) | Media globale | No | Nessuno (rate limit variabile) |
| [The Guardian](https://open-platform.theguardian.com) | Articoli giornalistici | Sì | 5.000 req/giorno |
| [New York Times](https://developer.nytimes.com) | Articoli giornalistici | Sì | 4.000 req/giorno |

### Social e contenuti generati dagli utenti (UGC)

| Fonte | Tipo | API Key | Limite gratuito |
|---|---|---|---|
| [YouTube Data API v3](https://developers.google.com/youtube/v3) | Video e commenti | Sì | 10.000 unità/giorno |
| [Bluesky](https://docs.bsky.app) | Post social | No | ~30 req/min |
| [Mastodon](https://docs.joinmastodon.org/methods/search/) | Post social (fediverse) | Opzionale | 300 req/5 min per IP |
| [Lemmy](https://join-lemmy.org/docs/contributors/04-api.html) | Post e commenti (forum) | No | ~60 req/min |
| [Stack Exchange](https://api.stackexchange.com/docs) | Domande e risposte | Opzionale | 300 req/giorno (10k con key) |

### Riferimenti enciclopedici

| Fonte | Tipo | API Key | Limite gratuito |
|---|---|---|---|
| [Wikipedia](https://www.mediawiki.org/wiki/API:Main_page) | Contesto enciclopedico | No | Nessuno |
| [Wikipedia Talk Pages](https://www.mediawiki.org/wiki/API:Parsing_wikitext) | Discussioni editoriali | No | Nessuno |

### Web search generalista

| Fonte | Tipo | API Key | Limite gratuito |
|---|---|---|---|
| [Brave Search](https://api-dashboard.search.brave.com/) | Web search (indice indipendente) | Sì | 2.000 query/mese, 1 query/sec |

> **Nota:** Google Search API è stata valutata ma esclusa per assenza di piano gratuito adeguato. Reddit API è stata esclusa definitivamente: il modello di pricing/accesso attuale e i vincoli dei Termini di Servizio (uso commerciale/ricerca, moderazione) la rendono inadatta a una pipeline di reputation analysis riproducibile. La copertura di contenuti in stile forum/community è coperta da Lemmy, Mastodon e Bluesky. SearXNG è stata valutata come alternativa self-hosted ma scartata per fragilità infrastrutturale (blocchi IP dei motori upstream, affidabilità bassa): Brave offre copertura comparabile con un indice proprio, API documentata e ToS chiari — requisito essenziale per una pipeline riproducibile in contesto accademico.

---

## Architettura

```
Input (target + queries)
        │
        ▼
  Collectors (12 fonti API)
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
   Aggregator  ←  reputation score, trend, metriche entity-level
        │
        ▼
   Exporters (JSON + CSV + Summary)  →  data/final/
        │
        ▼
IBM Cloud Pak for Data
```

La pipeline è modulare e ogni step è indipendente. L'enrichment (language detection + sentiment) è opzionale: se le dipendenze NLP non sono installate, i campi `language` e `sentiment` rimangono `null` senza interrompere la pipeline. L'aggregator produce un reputation score composito e un'analisi del trend a partire dai record arricchiti.

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

Copia il file di esempio e inserisci le tue chiavi:

```bash
cp .env.example .env
```

Modifica `.env`:

```env
# API key delle fonti dati (obbligatorie per le rispettive fonti)
YOUTUBE_API_KEY=la_tua_chiave
NEWS_API_KEY=la_tua_chiave
GUARDIAN_API_KEY=la_tua_chiave
NYT_API_KEY=la_tua_chiave
BRAVE_API_KEY=la_tua_chiave

# Opzionali — aumentano i limiti delle rispettive fonti
STACKEXCHANGE_API_KEY=la_tua_chiave
MASTODON_ACCESS_TOKEN=il_tuo_token
MASTODON_TOKEN_INSTANCE=mastodon.social

# HuggingFace token (opzionale, consigliato per l'enrichment NLP)
HF_TOKEN=hf_xxxxxxxxxxxxxxxxxxxx
```

Le fonti senza API key (GDELT, Wikipedia, WikiTalk, Lemmy, Bluesky) funzionano senza configurazione aggiuntiva. Stack Exchange e Mastodon funzionano anche senza key, con limiti ridotti.

**`HF_TOKEN`** — opzionale ma consigliato se si usa l'enrichment NLP. Senza token, HuggingFace applica un rate limit ridotto durante il download del modello XLM-RoBERTa e logga un warning sulle richieste non autenticate. Con il token il download è più stabile e il warning sparisce. Il token va creato su [huggingface.co/settings/tokens](https://huggingface.co/settings/tokens) (tipo **Read**, piano gratuito sufficiente). Non serve per eseguire la pipeline se il modello è già stato scaricato e cachato localmente.

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
2026-04-08T18:12:32 [INFO] Aggregazione completata per 'Giorgia Meloni': 91 record, reputation=0.5842, trend=stable
2026-04-08T18:12:32 [INFO] === Pipeline completata: 91 record finali, reputation=0.5842 ===

Risultato: 91 record finali esportati in data/final/

--- Reputation Summary: Giorgia Meloni ---
  Reputation Score: 0.5842
  Sentiment (avg):  0.0523
  Trend:            stable
  Sources:          91 record da 6 fonti
  Date range:       ('2026-03-10', '2026-04-08')
```

### Query multiple

```bash
python main.py --target "Elon Musk" --queries "Elon Musk Tesla" "Elon Musk SpaceX"
```

### Selezione fonti specifiche

```bash
python main.py --target "Apple" --queries "Apple" --sources news gdelt guardian mastodon lemmy
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
| `--sources` | Fonti da interrogare (es. `news gdelt mastodon lemmy`) | tutte |
| `--max-results` | Risultati massimi per fonte/query | 20 |
| `--news-language` | Lingua per NewsAPI (ISO 639-1) | `en` |
| `--no-raw` | Non salvare i payload grezzi | False |

### Output

I file vengono salvati in:

```
data/
├── raw/    # Payload grezzi per debug e audit
└── final/  # Dataset finale
    ├── {target}_{timestamp}_final.json      # Record individuali (JSON)
    ├── {target}_{timestamp}_final.csv       # Record individuali (CSV)
    └── {target}_{timestamp}_summary.json    # Analisi aggregata entity-level
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

### Reputation summary (entity-level)

Oltre ai record individuali, la pipeline produce un file `*_summary.json` con l'analisi aggregata per entità. Lo schema è definito in `pipeline/aggregator.py` (`EntitySummary`):

```json
{
  "entity": "Giorgia Meloni",
  "record_count": 91,
  "records_with_sentiment": 74,
  "source_distribution": {
    "news": 20,
    "gdelt": 20,
    "youtube": 20,
    "guardian": 20,
    "nyt": 10,
    "wikipedia": 1
  },
  "sentiment_avg": 0.0523,
  "sentiment_std": 0.4218,
  "source_trust_avg": 0.8142,
  "recency_score": 0.7856,
  "volume_score": 0.8503,
  "reputation_score": 0.5842,
  "trend": "stable",
  "date_range": {
    "from": "2026-03-10",
    "to": "2026-04-08"
  },
  "computed_at": "2026-04-08T18:12:32Z"
}
```

**Reputation score** — composito in `[0.0, 1.0]`, calcolato come media pesata di quattro componenti (pesi configurabili in `config.py`):

| Componente | Peso | Descrizione |
|---|---|---|
| Sentiment | 0.40 | Media pesata del sentiment dei record (pesata per autorevolezza della fonte) |
| Source trust | 0.30 | Media dei pesi di autorevolezza delle sorgenti (`SOURCE_WEIGHTS`) |
| Recency | 0.20 | Quanto sono recenti i record (decay esponenziale, half-life 30 giorni) |
| Volume | 0.10 | Volume normalizzato con log-scaling |

**Trend** — direzione del sentiment nel tempo, calcolata con regressione lineare: `"up"` (miglioramento), `"down"` (peggioramento), `"stable"`, `"unknown"` (dati insufficienti, < 3 record con data e sentiment).

**`sentiment_std`** — deviazione standard del sentiment. Un valore alto indica opinioni polarizzate (es. entità controversa), un valore basso indica consenso.

### Esempio di analisi reale

Un'esecuzione su `"OpenAI"` con query `"OpenAI GPT"` e `"OpenAI lawsuits"` potrebbe produrre:

```json
{
  "entity": "OpenAI",
  "record_count": 127,
  "records_with_sentiment": 98,
  "source_distribution": {
    "news": 35,
    "gdelt": 28,
    "guardian": 18,
    "youtube": 16,
    "bluesky": 12,
    "mastodon": 8,
    "nyt": 6,
    "stackexchange": 3,
    "wikipedia": 1
  },
  "sentiment_avg": -0.0842,
  "sentiment_std": 0.5123,
  "source_trust_avg": 0.7890,
  "recency_score": 0.9012,
  "volume_score": 0.9350,
  "reputation_score": 0.5234,
  "trend": "down",
  "date_range": {
    "from": "2026-03-05",
    "to": "2026-04-13"
  },
  "computed_at": "2026-04-13T10:30:00Z"
}
```

In questo caso: il sentiment medio è leggermente negativo (-0.08) con alta dispersione (0.51), indicando opinioni polarizzate. Il trend è `"down"`, suggerendo un peggioramento nel periodo analizzato. L'alto recency score (0.90) indica che la maggior parte dei contenuti è recente. Il reputation score complessivo (0.52) riflette una reputazione attorno alla neutralità, trascinata verso il basso dal sentiment negativo.

---

## Limiti noti

**GDELT:** applica rate limiting variabile. In caso di errori 429, il collector esegue retry automatico con backoff esponenziale (fino a 3 tentativi, delay iniziale 3s). Se GDELT è sovraccarico, alcune query potrebbero restituire 0 risultati. Query con token molto corti (< 3 caratteri) vengono sanitizzate automaticamente.

**NewsAPI:** il piano gratuito è limitato a 100 richieste/giorno e agli articoli degli ultimi 30 giorni. La lingua è configurabile via `--news-language` ma non tutte le lingue hanno la stessa copertura.

**Enrichment NLP:** richiede `pip install -e ".[nlp]"`. Il modello XLM-RoBERTa (~1.1 GB) viene scaricato alla prima esecuzione. Senza le dipendenze NLP, i campi `language` e `sentiment` rimangono `null` ma la pipeline procede normalmente.

**Wikipedia / WikiTalk:** restituiscono rispettivamente 1 pagina enciclopedica e le sezioni della relativa talk page. La ricerca avviene sul nome dell'entità (`target`), non sulla query tematica.

**Mastodon:** il token è specifico dell'istanza dove è stato creato (es. mastodon.social). Su istanze senza token, il collector usa automaticamente il fallback sulla timeline hashtag pubblica. La ricerca full-text sugli statuses richiede ElasticSearch attivo sull'istanza.

**Lemmy / Stack Exchange:** la ricerca cross-istanza può produrre duplicati (es. lo stesso post federato su più istanze Lemmy). Il deduplicator li rimuove automaticamente via URL canonico.

**Brave Search:** piano gratuito limitato a 20 risultati per query e 1 query/sec (rate limit applicato a livello API, non dal collector). Il campo `page_age` non è sempre presente nei risultati: quando assente, `date` resta `None` e il record contribuisce meno al `recency_score` dell'aggregator. `author` non è esposto in forma strutturata. Il peso `SOURCE_WEIGHTS["brave"]` è intenzionalmente basso (0.55) perché i risultati includono contenuti SEO eterogenei di qualità variabile.

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
│   ├── youtube_comments_collector.py
│   ├── guardian_collector.py
│   ├── nyt_collector.py
│   ├── bluesky_collector.py
│   ├── stackexchange_collector.py
│   ├── mastodon_collector.py
│   ├── lemmy_collector.py
│   ├── wikitalk_collector.py
│   └── brave_collector.py
│
├── pipeline/                # Passi di trasformazione
│   ├── runner.py            # PipelineRunner — orchestratore
│   ├── normalizer.py        # RawRecord → Record (date YYYY-MM-DD, URL, domini)
│   ├── cleaner.py           # Pulizia stringhe e null
│   ├── deduplicator.py      # Rimozione duplicati (URL + titolo+dominio)
│   ├── enricher.py          # Language detection + sentiment analysis (NLP)
│   └── aggregator.py        # Aggregazione entity-level e reputation score
│
├── storage/                 # Persistenza
│   └── raw_store.py         # Salvataggio RawRecord grezzi in data/raw/
│
├── exporters/               # Export finale
│   ├── json_exporter.py     # Serializzazione JSON (record-level)
│   ├── csv_exporter.py      # Serializzazione CSV (record-level)
│   └── summary_json_exporter.py  # Serializzazione summary (entity-level)
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
    ├── test_aggregator.py
    ├── test_summary_json_exporter.py
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
