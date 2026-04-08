"""
GDELT DOC 2.0 Collector
Recupera articoli media globali tramite GDELT DOC 2.0 (https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/).
Completamente gratuito, nessuna API key richiesta.
Aggiornato ogni 15 minuti, copertura in 65+ lingue.

Nota: GDELT applica rate limiting su chiamate ravvicinate.
Il collector include retry con backoff esponenziale per gestirlo automaticamente.
"""

import time
import requests
from datetime import datetime, timezone
from urllib.parse import urlparse

BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

# Secondi di attesa prima di ogni chiamata (evita 429 su query multiple)
_REQUEST_DELAY = 2.0
# Numero massimo di tentativi in caso di 429
_MAX_RETRIES = 3


def _extract_domain(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return ""


def _empty_record() -> dict:
    return {
        "source_type": "gdelt",
        "source_name": "GDELT DOC 2.0",
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


def _request_with_retry(params: dict) -> dict | None:
    """
    Esegue la richiesta a GDELT con retry esponenziale in caso di 429.
    Attende _REQUEST_DELAY secondi prima di ogni chiamata.
    """
    time.sleep(_REQUEST_DELAY)

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            response = requests.get(BASE_URL, params=params, timeout=15)

            if response.status_code == 429:
                wait = _REQUEST_DELAY * (2 ** attempt)  # 4s, 8s, 16s
                print(f"[GDELT] Rate limit (429), attendo {wait:.0f}s prima del tentativo {attempt + 1}/{_MAX_RETRIES}...")
                time.sleep(wait)
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            print(f"[GDELT] Errore HTTP al tentativo {attempt}: {e}")
            if attempt == _MAX_RETRIES:
                return None
        except requests.RequestException as e:
            print(f"[GDELT] Errore di rete al tentativo {attempt}: {e}")
            if attempt == _MAX_RETRIES:
                return None
        except ValueError as e:
            print(f"[GDELT] Errore nel parsing JSON: {e}")
            return None

    return None


def collect(target_entity: str, query: str, max_records: int = 75) -> list[dict]:
    """
    Recupera articoli da GDELT DOC 2.0 per la query indicata.

    Args:
        target_entity: entità oggetto dell'analisi (es. "Mario Rossi")
        query: stringa di ricerca (es. "Mario Rossi")
        max_records: numero massimo di risultati (max 250)

    Returns:
        Lista di record normalizzati secondo il data contract.
    """
    params = {
        "query": query,
        "mode": "artlist",
        "maxrecords": min(max_records, 250),
        "format": "json",
        "sort": "datedesc",
    }

    data = _request_with_retry(params)
    if data is None:
        print(f"[GDELT] Impossibile recuperare dati per query: '{query}'")
        return []

    articles = data.get("articles", [])
    records = []

    for article in articles:
        url = article.get("url", "")
        if not url:
            continue

        # GDELT restituisce seendate come stringa es. "20260408T120000Z"
        raw_date = article.get("seendate", "")
        published_at = None
        if raw_date:
            try:
                published_at = datetime.strptime(raw_date, "%Y%m%dT%H%M%SZ").replace(
                    tzinfo=timezone.utc
                ).isoformat()
            except ValueError:
                published_at = raw_date

        record = _empty_record()
        record["target_entity"] = target_entity
        record["query"] = query
        record["title"] = article.get("title") or ""
        record["url"] = url
        record["domain"] = _extract_domain(url)
        record["published_at"] = published_at
        record["language"] = article.get("language") or ""
        record["country"] = article.get("sourcecountry") or ""
        record["source_name"] = article.get("domain") or "GDELT DOC 2.0"
        record["raw_payload"] = article

        records.append(record)

    print(f"[GDELT] Raccolti {len(records)} articoli per query: '{query}'")
    return records
