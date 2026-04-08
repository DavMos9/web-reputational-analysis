"""
Wikipedia Collector
Recupera contesto enciclopedico tramite la libreria wikipediaapi.
Completamente gratuito, nessuna API key richiesta.
"""

import wikipediaapi
from datetime import datetime, timezone

WIKI_USER_AGENT = "web-reputational-analysis/1.0"


def _empty_record() -> dict:
    return {
        "source_type": "wikipedia",
        "source_name": "Wikipedia",
        "target_entity": "",
        "query": "",
        "title": "",
        "snippet": "",
        "content": "",
        "url": "",
        "domain": "wikipedia.org",
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


def _fetch_page(wiki, title: str) -> dict | None:
    """Recupera una singola pagina Wikipedia."""
    page = wiki.page(title)
    if not page.exists():
        return None
    return page


def collect(target_entity: str, query: str, lang: str = "it") -> list[dict]:
    """
    Recupera la pagina Wikipedia corrispondente alla query.
    Prova prima in italiano, poi in inglese se non trovata.

    Args:
        target_entity: entità oggetto dell'analisi (es. "Mario Rossi")
        query: termine di ricerca (di solito il nome dell'entità)
        lang: lingua preferita ("it" o "en")

    Returns:
        Lista con 0 o 1 record normalizzati secondo il data contract.
    """
    records = []
    languages = [lang] if lang == "en" else [lang, "en"]

    for language in languages:
        wiki = wikipediaapi.Wikipedia(
            user_agent=WIKI_USER_AGENT,
            language=language,
        )
        page = _fetch_page(wiki, query)

        if page is None:
            print(f"[Wikipedia] Nessuna pagina trovata per '{query}' in lingua '{language}'")
            continue

        # Usa il summary come snippet e le prime 2000 char come content
        summary = page.summary or ""
        full_text = page.text or ""

        record = _empty_record()
        record["target_entity"] = target_entity
        record["query"] = query
        record["title"] = page.title
        record["snippet"] = summary[:500] if summary else ""
        record["content"] = full_text[:5000] if full_text else ""
        record["url"] = page.fullurl
        record["language"] = language
        record["raw_payload"] = {
            "title": page.title,
            "summary": summary,
            "url": page.fullurl,
            "language": language,
        }

        records.append(record)
        print(f"[Wikipedia] Pagina trovata: '{page.title}' (lingua: {language})")
        break  # Trovata la prima pagina valida, non cercare in altre lingue

    return records
