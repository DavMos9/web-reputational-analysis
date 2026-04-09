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

    def collect(self, target: str, query: str, max_results: int = 1, **kwargs: object) -> list[RawRecord]:
        """
        max_results è ignorato: Wikipedia restituisce sempre 1 pagina per target.
        kwargs supporta: lang (str, default "it") — lingua preferita con fallback a "en".

        Nota sul parametro `query`:
            Wikipedia opera su entità enciclopediche, non su query tematiche.
            La ricerca viene effettuata usando `target` (es. "Elon Musk"),
            non `query` (es. "Elon Musk Tesla controversie"). Il parametro
            `query` è ricevuto per rispettare l'interfaccia BaseCollector
            ed è usato solo per i log (tracciabilità dei RawRecord).
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
        """
        Restituisce il titolo della pagina Wikipedia più rilevante per il target.

        Il parametro si chiama `target` (non `query`) per chiarire che la
        ricerca è sull'entità principale, non su un tema libero.
        """
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
