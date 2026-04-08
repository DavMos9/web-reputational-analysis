"""
New York Times Article Search API Collector
Recupera articoli tramite NYT Article Search API.
Piano gratuito: 10 richieste/minuto, 4.000 richieste/giorno. Archivio dal 1851.
Registrazione: https://developer.nytimes.com/accounts/create
"""

import requests
from datetime import datetime, timezone
from urllib.parse import urlparse
from config import NYT_API_KEY

BASE_URL = "https://api.nytimes.com/svc/search/v2/articlesearch.json"


def _extract_domain(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return ""


def _empty_record() -> dict:
    return {
        "source_type": "news",
        "source_name": "The New York Times",
        "target_entity": "",
        "query": "",
        "title": "",
        "snippet": "",
        "content": "",
        "url": "",
        "domain": "nytimes.com",
        "author": "",
        "published_at": None,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "language": "en",
        "country": "US",
        "rank": None,
        "views_count": None,
        "likes_count": None,
        "comments_count": None,
        "engagement_score": None,
        "keywords_found": [],
        "sentiment_stub": None,
        "raw_payload": {},
    }


def collect(target_entity: str, query: str, max_results: int = 10) -> list[dict]:
    """
    Recupera articoli dal New York Times per la query indicata.

    Nota: l'API restituisce 10 risultati per pagina. max_results > 10
    richiede più chiamate (rispettare il limite di 10 req/min).

    Args:
        target_entity: entità oggetto dell'analisi (es. "Mario Rossi")
        query: stringa di ricerca (es. "Mario Rossi")
        max_results: numero massimo di risultati (10 per pagina)

    Returns:
        Lista di record normalizzati secondo il data contract.
    """
    if not NYT_API_KEY:
        print("[NYT] NYT_API_KEY non configurata. Salto la raccolta.")
        return []

    params = {
        "q": query,
        "api-key": NYT_API_KEY,
        "sort": "relevance",
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        print(f"[NYT] Errore nella richiesta: {e}")
        return []

    docs = data.get("response", {}).get("docs", [])
    records = []

    for rank, doc in enumerate(docs[:max_results], start=1):
        # L'URL principale è nel campo "web_url"
        url = doc.get("web_url", "")
        if not url:
            continue

        # Estrai autore dalla lista byline
        byline = doc.get("byline", {})
        author = byline.get("original", "") if isinstance(byline, dict) else ""

        # Estrai keywords dal campo "keywords"
        keywords = [
            kw.get("value", "")
            for kw in doc.get("keywords", [])
            if kw.get("value")
        ]

        record = _empty_record()
        record["target_entity"] = target_entity
        record["query"] = query
        record["title"] = doc.get("headline", {}).get("main") or ""
        record["snippet"] = doc.get("abstract") or doc.get("lead_paragraph") or ""
        record["url"] = url
        record["domain"] = _extract_domain(url)
        record["author"] = author.replace("By ", "").strip() if author else ""
        record["published_at"] = doc.get("pub_date") or None
        record["rank"] = rank
        record["keywords_found"] = keywords
        record["source_name"] = (
            doc.get("source") or "The New York Times"
        )
        record["raw_payload"] = doc

        records.append(record)

    print(f"[NYT] Raccolti {len(records)} articoli per query: '{query}'")
    return records
