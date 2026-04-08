"""
NewsAPI Collector
Recupera articoli di news tramite NewsAPI (https://newsapi.org).
Piano gratuito: 100 richieste/giorno, notizie degli ultimi 30 giorni.
"""

import requests
from datetime import datetime, timezone
from urllib.parse import urlparse
from config import NEWS_API_KEY

BASE_URL = "https://newsapi.org/v2/everything"


def _extract_domain(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return ""


def _empty_record() -> dict:
    return {
        "source_type": "news",
        "source_name": "NewsAPI",
        "target_entity": "",
        "query": "",
        "title": "",
        "snippet": "",
        "content": "",
        "url": "",
        "domain": "",
        "author": "",
        "published_at": None,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "language": "",
        "country": "",
        "rank": None,
        "views_count": None,
        "likes_count": None,
        "comments_count": None,
        "engagement_score": None,
        "keywords_found": [],
        "sentiment_stub": None,
        "raw_payload": {},
    }


def collect(target_entity: str, query: str, page_size: int = 20) -> list[dict]:
    """
    Recupera articoli da NewsAPI per la query indicata.

    Args:
        target_entity: entità oggetto dell'analisi (es. "Mario Rossi")
        query: stringa di ricerca (es. "Mario Rossi scandalo")
        page_size: numero massimo di risultati (max 100 per il piano gratuito)

    Returns:
        Lista di record normalizzati secondo il data contract.
    """
    if not NEWS_API_KEY:
        print("[NewsAPI] NEWS_API_KEY non configurata. Salto la raccolta.")
        return []

    params = {
        "q": query,
        "apiKey": NEWS_API_KEY,
        "pageSize": min(page_size, 100),
        "sortBy": "relevancy",
        "language": "it",
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        print(f"[NewsAPI] Errore nella richiesta: {e}")
        return []

    articles = data.get("articles", [])
    records = []

    for article in articles:
        url = article.get("url", "")
        if not url:
            continue

        record = _empty_record()
        record["target_entity"] = target_entity
        record["query"] = query
        record["title"] = article.get("title") or ""
        record["snippet"] = article.get("description") or ""
        record["content"] = article.get("content") or ""
        record["url"] = url
        record["domain"] = _extract_domain(url)
        record["author"] = article.get("author") or ""
        record["published_at"] = article.get("publishedAt") or None
        record["source_name"] = article.get("source", {}).get("name") or "NewsAPI"
        record["raw_payload"] = article

        records.append(record)

    print(f"[NewsAPI] Raccolti {len(records)} articoli per query: '{query}'")
    return records
