# Web Reputational Analysis

> Pipeline Python per la raccolta, normalizzazione ed esportazione di dati da fonti web eterogenee, finalizzata ad attività di web reputational analysis.

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
- [Struttura del progetto](#struttura-del-progetto)
- [Documentazione](#documentazione)
- [Contributing](#contributing)
- [License](#license)

---

## Panoramica

Il progetto consente di raccogliere informazioni su un'entità (persona, brand, organizzazione) da più fonti online, uniformarle in uno schema dati coerente ed esportarle in formato JSON e CSV.

I dati prodotti sono pensati per essere utilizzati in pipeline di analisi successive, in particolare su **IBM Cloud Pak for Data**.

---

## Fonti dati

| Fonte | Tipo | API Key | Limite gratuito |
|---|---|---|---|
| [NewsAPI](https://newsapi.org) | Articoli di news | Sì | 100 req/giorno |
| [GDELT DOC 2.0](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/) | Media globale | No | Nessuno |
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
  Persistenza raw
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
   Validator
        │
        ▼
   Exporters (JSON + CSV)
        │
        ▼
IBM Cloud Pak for Data
```

---

## Requisiti

- Python 3.11+
- pip
- Dipendenze principali: `requests`, `python-dotenv`, `python-dateutil`, `pandas`, `wikipedia-api`, `praw`

Tutte le dipendenze sono elencate in `requirements.txt` e installate con un singolo comando.

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

# Installa le dipendenze
pip install -r requirements.txt
```

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
2026-04-08T18:12:18 [INFO] === Avvio pipeline ===
2026-04-08T18:12:18 [INFO] Target:  Giorgia Meloni
2026-04-08T18:12:18 [INFO] Fonti:   ['news', 'gdelt', 'wikipedia', 'youtube', 'guardian', 'nyt']
[NewsAPI] Raccolti 20 articoli per query: 'Giorgia Meloni'
[GDELT] Raccolti 20 articoli per query: 'Giorgia Meloni'
[Wikipedia] Pagina trovata: 'Giorgia Meloni' (lingua: it)
[YouTube] Raccolti 20 video per query: 'Giorgia Meloni'
[Guardian] Raccolti 20 articoli per query: 'Giorgia Meloni'
[NYT] Raccolti 10 articoli per query: 'Giorgia Meloni'
2026-04-08T18:12:32 [INFO] Deduplicazione: 0 duplicati rimossi, 91 record unici
2026-04-08T18:12:32 [INFO] Record finali validi: 91
2026-04-08T18:12:32 [INFO] === Pipeline completata ===

Risultato: 91 record finali esportati in data/final/
```

### Query multiple

```bash
python main.py --target "Elon Musk" --queries "Elon Musk Tesla" "Elon Musk SpaceX"
```

### Selezione fonti specifiche

```bash
python main.py --target "Apple" --queries "Apple" --sources news gdelt guardian
```

### Opzioni disponibili

| Parametro | Descrizione | Default |
|---|---|---|
| `--target` | Entità da analizzare | obbligatorio |
| `--queries` | Una o più query di ricerca | obbligatorio |
| `--sources` | Fonti da interrogare | tutte |
| `--max-results` | Risultati massimi per fonte/query | 20 |
| `--no-raw` | Non salvare i payload grezzi | False |

### Output

I file vengono salvati in:

```
data/
├── raw/        # Payload grezzi per debug e audit
├── processed/  # Record normalizzati e puliti
└── final/      # Dataset finale (JSON + CSV)
```

---

## Schema dati

Ogni record rispetta il seguente schema unificato:

```json
{
  "source_type": "news",
  "source_name": "NewsAPI",
  "target_entity": "Giorgia Meloni",
  "query": "Giorgia Meloni",
  "title": "Titolo articolo",
  "snippet": "Estratto...",
  "content": "Testo completo se disponibile",
  "url": "https://example.com/article",
  "domain": "example.com",
  "author": "Nome Autore",
  "published_at": "2026-04-08T10:00:00+00:00",
  "retrieved_at": "2026-04-08T16:12:18+00:00",
  "language": "it",
  "country": "IT",
  "rank": null,
  "views_count": null,
  "likes_count": null,
  "comments_count": null,
  "engagement_score": null,
  "keywords_found": [],
  "sentiment_stub": null,
  "raw_payload": {}
}
```

Per la specifica completa vedere [`docs/Web_Reputational_Analysis_Data_Contract.pdf`](docs/Web_Reputational_Analysis_Data_Contract.pdf).

---

## Struttura del progetto

```
web-reputational-analysis/
├── main.py                  # Entry point e orchestrazione pipeline
├── config.py                # Caricamento variabili d'ambiente
├── requirements.txt
├── .env.example
│
├── collectors/              # Un file per ogni fonte dati
│   ├── news_collector.py
│   ├── gdelt_collector.py
│   ├── wikipedia_collector.py
│   ├── youtube_collector.py
│   ├── guardian_collector.py
│   └── nyt_collector.py
│
├── processors/              # Trasformazione dei dati
│   ├── normalizer.py        # Date ISO 8601, URL, domini
│   ├── cleaner.py           # Pulizia stringhe e null
│   ├── deduplicator.py      # Rimozione duplicati (URL + titolo+dominio)
│   └── validator.py         # Verifica campi obbligatori
│
├── exporters/               # Export dedicato (JSON Lines, Parquet) — previsto
├── utils/                   # Logger centralizzato e helper date — previsto
│
├── data/
│   ├── raw/                 # Payload grezzi
│   ├── processed/           # Record normalizzati
│   └── final/               # Output finale
│
├── tests/                   # Test unitari — previsti
├── wiki/                    # Sorgenti pagine GitHub Wiki
└── docs/                    # Documentazione PDF
```

---

## Documentazione

- [Documentazione di progetto](docs/Web_Reputational_Analysis_Project.pdf)
- [Data Contract](docs/Web_Reputational_Analysis_Data_Contract.pdf)
- [Wiki](../../wiki) — installazione dettagliata, architettura, riferimento collector

---

## Contributing

Contributi, segnalazioni di bug e suggerimenti sono benvenuti.
Leggi [CONTRIBUTING.md](CONTRIBUTING.md) prima di aprire una pull request.

---

## License

Distribuito sotto licenza MIT. Vedere [LICENSE](LICENSE) per i dettagli.
