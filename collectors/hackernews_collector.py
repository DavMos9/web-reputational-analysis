"""
collectors/hackernews_collector.py

Collector per Hacker News tramite Algolia Search API.

Documentazione:
    https://hn.algolia.com/api

Limiti:
    - Nessuna API key richiesta.
    - Nessuna quota ufficiale documentata; uso ragionevole per ricerca accademica.
    - Restituisce storie (stories) ordinate per rilevanza.

Note sulla copertura:
    Hacker News non è solo tech: la community discute attivamente di politica,
    economia, scienza, cultura e personaggi pubblici di rilievo internazionale.
    Tuttavia resta più densa su target tech/startup. Per questo motivo è
    classificato come sorgente opt-in in main.py (richiede --sources esplicito).
"""

from __future__ import annotations

import logging

import requests
from collectors.base import BaseCollector
from models import RawRecord

log = logging.getLogger(__name__)

_BASE_URL = "https://hn.algolia.com/api/v1/search"
_MAX_RESULTS_CAP = 50  # limite ragionevole per singola richiesta


class HackerNewsCollector(BaseCollector):
    source_id = "hackernews"

    def collect(
        self,
        target: str,
        query: str,
        max_results: int = 20,
        **kwargs: object,
    ) -> list[RawRecord]:
        """
        Args:
            target:      entità analizzata.
            query:       stringa di ricerca.
            max_results: numero massimo di risultati (cap a 50).
        """
        params = {
            "query":       query,
            "tags":        "story",
            "hitsPerPage": min(max_results, _MAX_RESULTS_CAP),
        }

        try:
            response = requests.get(_BASE_URL, params=params, timeout=10)

            if response.status_code == 429:
                log.warning(
                    "[HackerNewsCollector] Rate limit raggiunto (HTTP 429). "
                    "Riprova tra qualche istante."
                )
                return []

            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            self._log_error(query, e)
            return []

        hits = data.get("hits", [])

        records = [
            self._make_raw(target, query, hit)
            for hit in hits
        ]

        self._log_collected(query, len(records))
        return records
