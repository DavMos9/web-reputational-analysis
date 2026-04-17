"""
collectors/guardian_collector.py

Collector per The Guardian Open Platform.
Piano gratuito: 5.000 richieste/giorno, archivio dal 1999.
Registrazione: https://open-platform.theguardian.com/access/
"""

import logging

import requests
from config import GUARDIAN_API_KEY
from models import RawRecord
from collectors.base import BaseCollector

log = logging.getLogger(__name__)

BASE_URL = "https://content.guardianapis.com/search"


class GuardianCollector(BaseCollector):
    source_id = "guardian"

    def collect(self, target: str, query: str, max_results: int = 20) -> list[RawRecord]:
        """
        Args:
            target:      entità analizzata.
            query:       stringa di ricerca.
            max_results: numero massimo di risultati (max 200 per richiesta).
        """
        if not GUARDIAN_API_KEY:
            self._log_skip("GUARDIAN_API_KEY non configurata")
            return []

        params = {
            "q":           query,
            "api-key":     GUARDIAN_API_KEY,
            "page-size":   min(max_results, 200),
            "order-by":    "relevance",
            "show-fields": "headline,trailText,bodyText,byline,shortUrl",
        }

        try:
            response = requests.get(BASE_URL, params=params, timeout=10)

            if response.status_code == 429:
                log.warning(
                    "[GuardianCollector] Limite giornaliero raggiunto (HTTP 429). "
                    "Piano gratuito: 5.000 req/giorno. Riprova domani o verifica il tuo piano."
                )
                return []

            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            self._log_error(query, e)
            return []

        results = data.get("response", {}).get("results", [])

        records = [
            self._make_raw(target, query, {**article, "_rank": rank})
            for rank, article in enumerate(results, start=1)
        ]

        self._log_collected(query, len(records))
        return records
