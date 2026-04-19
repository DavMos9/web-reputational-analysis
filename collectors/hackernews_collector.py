"""
collectors/hackernews_collector.py

Collector per Hacker News tramite Algolia Search API.

Documentazione:
    https://hn.algolia.com/api

Endpoint disponibili:
    /search          — ordinamento per rilevanza (default Algolia)
    /search_by_date  — ordinamento cronologico decrescente

Limiti:
    - Nessuna API key richiesta.
    - Nessuna quota ufficiale documentata; uso ragionevole per ricerca accademica.

Parametro search_by_date:
    Quando True, usa l'endpoint /search_by_date che ordina i risultati per data
    decrescente (più recenti prima). Consigliato in combinazione con --since per
    evitare che il filtro temporale scarti la maggior parte dei risultati.
    Default: False (compatibilità con comportamento originale).

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
from collectors.retry import http_get_with_retry
from models import RawRecord

log = logging.getLogger(__name__)

_BASE_URL_RELEVANCE = "https://hn.algolia.com/api/v1/search"
_BASE_URL_DATE      = "https://hn.algolia.com/api/v1/search_by_date"
_MAX_RESULTS_CAP    = 50  # limite ragionevole per singola richiesta


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
            target:         entità analizzata.
            query:          stringa di ricerca.
            max_results:    numero massimo di risultati (cap a 50).
            kwargs:
                search_by_date (bool): se True, usa l'endpoint /search_by_date
                    (ordinamento cronologico) invece di /search (ordinamento per
                    rilevanza). Default False. Consigliato con --since per
                    massimizzare i risultati recenti.
        """
        search_by_date: bool = bool(kwargs.get("search_by_date", False))
        url = _BASE_URL_DATE if search_by_date else _BASE_URL_RELEVANCE

        params = {
            "query":       query,
            "tags":        "story",
            "hitsPerPage": min(max_results, _MAX_RESULTS_CAP),
        }

        try:
            response = http_get_with_retry(
                url, params=params, timeout=10, source_id=self.source_id
            )

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
