"""
Wikipedia Collector
Recupera contesto enciclopedico tramite Wikipedia API.
Completamente gratuito, nessuna API key richiesta.

Strategia:
1. Usa l'API opensearch di Wikipedia per trovare il titolo della pagina
   più rilevante per la query (funziona come un motore di ricerca interno).
2. Recupera il contenuto della pagina trovata con wikipediaapi.
3. Deduplica per titolo: se query diverse portano alla stessa pagina,
   la pagina viene scaricata una sola volta.

Esempio: query "Elon Musk Tesla" → opensearch trova "Elon Musk" → pagina scaricata.
         query "Elon Musk SpaceX" → opensearch trova "Elon Musk" → già vista, saltata.
"""

import requests
import wikipediaapi
from datetime import datetime, timezone

WIKI_USER_AGENT = "web-reputational-analysis/1.0"
OPENSEARCH_URL = "https://{lang}.wikipedia.org/w/api.php"

# Cache globale per titoli già scaricati nella sessione corrente
_fetched_titles: set[str] = set()


def _reset_cache():
    """Svuota la cache dei titoli (utile nei test)."""
    global _fetched_titles
    _fetched_titles = set()


def _opensearch(query: str, lang: str) -> str | None:
    """
    Usa l'API opensearch di Wikipedia per trovare il titolo della pagina
    più rilevante per una query in linguaggio naturale.

    Returns:
        Titolo della prima pagina trovata, o None.
    """
    params = {
        "action": "opensearch",
        "search": query,
        "limit": 1,
        "namespace": 0,
        "format": "json",
    }
    try:
        response = requests.get(
            OPENSEARCH_URL.format(lang=lang),
            params=params,
            headers={"User-Agent": WIKI_USER_AGENT},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        # Formato risposta: [query, [titoli], [descrizioni], [url]]
        titles = data[1] if len(data) > 1 else []
        return titles[0] if titles else None
    except Exception as e:
        print(f"[Wikipedia] Opensearch fallita per '{query}' ({lang}): {e}")
        return None


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


def collect(target_entity: str, query: str, lang: str = "it") -> list[dict]:
    """
    Recupera la pagina Wikipedia più rilevante per la query.

    Usa opensearch per trovare il titolo corretto, poi scarica la pagina.
    Se la stessa pagina è già stata scaricata in una chiamata precedente
    (per una query diversa), restituisce lista vuota per evitare duplicati.

    Args:
        target_entity: entità oggetto dell'analisi (es. "Elon Musk")
        query: query di ricerca (es. "Elon Musk Tesla", "Elon Musk SpaceX")
        lang: lingua preferita ("it" o "en")

    Returns:
        Lista con 0 o 1 record normalizzati secondo il data contract.
    """
    languages = [lang] if lang == "en" else [lang, "en"]

    for language in languages:
        # Fase 1: cerca l'entità (non la query composta) su Wikipedia.
        # Wikipedia è contesto enciclopedico sul target — le query composte
        # come "Elon Musk Tesla" non corrispondono a pagine reali.
        page_title = _opensearch(target_entity, language)
        if not page_title:
            print(f"[Wikipedia] Nessun risultato opensearch per '{query}' ({language})")
            continue

        # Chiave cache: lingua + titolo normalizzato
        cache_key = f"{language}:{page_title.lower()}"
        if cache_key in _fetched_titles:
            print(f"[Wikipedia] Pagina '{page_title}' già scaricata, salto (query: '{query}')")
            return []

        # Fase 2: scarica il contenuto della pagina
        wiki = wikipediaapi.Wikipedia(
            user_agent=WIKI_USER_AGENT,
            language=language,
        )
        page = wiki.page(page_title)

        if not page.exists():
            print(f"[Wikipedia] Pagina '{page_title}' non trovata ({language})")
            continue

        # Registra in cache e costruisci il record
        _fetched_titles.add(cache_key)

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

        print(f"[Wikipedia] Pagina trovata: '{page.title}' (lingua: {language}, query: '{query}')")
        return [record]

    return []
