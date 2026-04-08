# Collectors

## Interfaccia comune

Ogni collector espone una funzione:

```python
def collect(target_entity: str, query: str, **kwargs) -> list[dict]:
    ...
```

Restituisce una lista di record conformi al [Data Contract](Schema-Dati).

---

## NewsAPI (`news_collector.py`)

**Fonte:** [newsapi.org](https://newsapi.org)
**API Key:** `NEWS_API_KEY`
**Limite:** 100 req/giorno (piano Developer)

```python
from collectors.news_collector import collect
records = collect("Giorgia Meloni", "Giorgia Meloni governo", page_size=20)
```

---

## GDELT DOC 2.0 (`gdelt_collector.py`)

**Fonte:** [gdeltproject.org](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/)
**API Key:** non richiesta
**Limite:** nessuno (rate limiting gestito con retry automatico)

```python
from collectors.gdelt_collector import collect
records = collect("Giorgia Meloni", "Giorgia Meloni", max_records=75)
```

---

## Wikipedia (`wikipedia_collector.py`)

**Fonte:** [Wikipedia API](https://www.mediawiki.org/wiki/API:Main_page)
**API Key:** non richiesta
**Strategia:** usa `opensearch` per trovare la pagina più rilevante per `target_entity`, poi ne scarica il contenuto. Deduplica per titolo nella stessa sessione.

```python
from collectors.wikipedia_collector import collect
records = collect("Giorgia Meloni", "Giorgia Meloni politica", lang="it")
```

---

## YouTube Data API v3 (`youtube_collector.py`)

**Fonte:** [YouTube Data API](https://developers.google.com/youtube/v3)
**API Key:** `YOUTUBE_API_KEY`
**Limite:** 10.000 unità/giorno (1 ricerca = 100 unità)

```python
from collectors.youtube_collector import collect
records = collect("Giorgia Meloni", "Giorgia Meloni discorso", max_results=20)
```

---

## The Guardian (`guardian_collector.py`)

**Fonte:** [The Guardian Open Platform](https://open-platform.theguardian.com)
**API Key:** `GUARDIAN_API_KEY`
**Limite:** 5.000 req/giorno

```python
from collectors.guardian_collector import collect
records = collect("Giorgia Meloni", "Giorgia Meloni Italy", page_size=20)
```

---

## New York Times (`nyt_collector.py`)

**Fonte:** [NYT Article Search API](https://developer.nytimes.com/docs/articlesearch-product/1/overview)
**API Key:** `NYT_API_KEY`
**Limite:** 4.000 req/giorno, 10 req/minuto

```python
from collectors.nyt_collector import collect
records = collect("Giorgia Meloni", "Giorgia Meloni", max_results=10)
```
