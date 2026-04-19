"""collectors/wikipedia_collector.py — Wikipedia API (gratuita, senza key)."""

import requests
import wikipediaapi
from models import RawRecord
from collectors.base import BaseCollector

WIKI_USER_AGENT  = "web-reputational-analysis/1.0"
OPENSEARCH_URL   = "https://{lang}.wikipedia.org/w/api.php"


class WikipediaCollector(BaseCollector):
    source_id = "wikipedia"

    def __init__(self) -> None:
        self._fetched: set[str] = set()  # evita di scaricare la stessa pagina più volte

    def collect(self, target: str, query: str, max_results: int = 1, **kwargs: object) -> list[RawRecord]:
        """
        max_results ignorato: restituisce sempre 1 pagina per target.
        kwargs: lang (str, default "it") con fallback a "en".
        La ricerca usa `target`, non `query` (Wikipedia è entity-based).
        """
        lang: str = str(kwargs.get("lang", "it"))

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

    def _opensearch(self, target: str, lang: str) -> str | None:
        """Restituisce il titolo della pagina Wikipedia più rilevante."""
        params = {
            "action":    "opensearch",
            "search":    target,
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
            self._log_error(target, e)
            return None

    def reset_cache(self) -> None:
        """Svuota la cache dei titoli. Utile nei test."""
        self._fetched.clear()
