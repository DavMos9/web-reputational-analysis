"""
The Guardian API Collector
Recupera articoli giornalistici tramite The Guardian Open Platform.
Piano gratuito: 5.000 richieste/giorno, archivio dal 1999.
Registrazione: https://open-platform.theguardian.com/access/
"""

import requests
from datetime import datetime, timezone
from urllib.parse import urlparse
from config import GUARDIAN_API_KEY

BASE_URL = "https://content.guardianapis.com/search"


def _extract_domain(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return ""


def _empty_record() -> dict:
    return {
        "source_type": "news",
        "source_name": "The Guardian",
        "target_entity": "",
        "query": "",
        "title": "",
        "snippet": "",
        "content": "",
        "url": "",
        "domain": "theguardian.com",
        "author": "",
        "published_at": None,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "language": "en",
        "country": "GB",
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
    Recupera articoli da The Guardian per la query indicata.

    Args:
        target_entity: entità oggetto dell'analisi (es. "Mario Rossi")
        query: stringa di ricerca (es. "Mario Rossi scandal")
        page_size: numero massimo di risultati (max 200 per richiesta)

    Returns:
        Lista di record normalizzati secondo il data contract.
    """
    if not GUARDIAN_API_KEY:
        print("[Guardian] GUARDIAN_API_KEY non configurata. Salto la raccolta.")
        return []

    params = {
        "q": query,
        "api-key": GUARDIAN_API_KEY,
        "page-size": min(page_size, 200),
        "order-by": "relevance",
        "show-fields": "headline,trailText,bodyText,byline,shortUrl",
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        print(f"[Guardian] Errore nella richiesta: {e}")
        return []

    results = data.get("response", {}).get("results", [])
    records = []

    for rank, article in enumerate(results, start=1):
        fields = article.get("fields", {})
        url = fields.get("shortUrl") or article.get("webUrl", "")
        if not url:
            continue

        record = _empty_record()
        record["target_entity"] = target_entity
        record["query"] = query
        record["title"] = fields.get("headline") or article.get("webTitle") or ""
        record["snippet"] = fields.get("trailText") or ""
        record["content"] = fields.get("bodyText") or ""
        record["url"] = url
        record["domain"] = _extract_domain(url)
        record["author"] = fields.get("byline") or ""
        record["published_at"] = article.get("webPublicationDate") or None
        record["rank"] = rank
        record["raw_payload"] = article

        records.append(record)

    print(f"[Guardian] Raccolti {len(records)} articoli per query: '{query}'")
    return records
