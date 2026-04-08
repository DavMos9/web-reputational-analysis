"""
collectors/wikipedia_collector.py

Collector per Wikipedia API.
Gratuito, senza API key.

Strategia:
1. opensearch per trovare il titolo della pagina più rilevante per il target.
2. Scarica la pagina con wikipediaapi.
3. Cache per titolo: se query diverse portano alla stessa pagina,
   la pagina viene scaricata una sola volta per sessione.
"""

import requests
import wikipediaapi
from models import RawRecord
from collectors.base import BaseCollector

WIKI_USER_AGENT  = "web-reputational-analysis/1.0"
OPENSEARCH_URL   = "https://{lang}.wikipedia.org/w/api.php"


class WikipediaCollector(BaseCollector):
    source_id = "wikipedia"

    def __init__(self) -> None:
        # Cache per titoli già scaricati in questa istanza
        self._fetched: set[str] = set()

    def collect(self, target: str, query: str, lang: str = "it") -> list[RawRecord]:
        """
        Args:
            target: entità analizzata (usata per la ricerca su Wikipedia).
            query:  query originale (inclusa nel RawRecord per tracciabilità).
            lang:   lingua preferita ("it" o "en"). Fallback automatico su "en".
        """
        languages = [lang] if lang == "en" else [lang, "en"]

        for language in languages:
            page_title = self._opensearch(target, language)
            if not page_title:
                self._log_skip(f"opensearch senza risultati per '{target}' ({language})")
                continue

            cache_key = f"{language}:{page_title.lower()}"
            if cache_key in self._fetched:
                self._log_skip(f"pagina '{page_title}' già scaricata (query: '{query}')")
                return []

            wiki = wikipediaapi.Wikipedia(
                user_agent=WIKI_USER_AGENT,
                language=language,
            )
            page = wiki.page(page_title)

            if not page.exists():
                self._log_skip(f"pagina '{page_title}' non trovata ({language})")
                continue

            self._fetched.add(cache_key)

            payload = {
                "title":    page.title,
                "summary":  page.summary or "",
                "text":     page.text or "",
                "url":      page.fullurl,
                "language": language,
            }

            self._log_collected(query, 1)
            return [self._make_raw(target, query, payload)]

        return []

    # ------------------------------------------------------------------

    def _opensearch(self, query: str, lang: str) -> str | None:
        """Restituisce il titolo della pagina Wikipedia più rilevante per la query."""
        params = {
            "action":    "opensearch",
            "search":    query,
            "limit":     1,
            "namespace": 0,
            "format":    "json",
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
            titles = data[1] if len(data) > 1 else []
            return titles[0] if titles else None
        except Exception as e:
            self._log_error(query, e)
            return None

    def reset_cache(self) -> None:
        """Svuota la cache dei titoli. Utile nei test."""
        self._fetched.clear()
