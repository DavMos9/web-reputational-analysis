"""
collectors/gdelt_collector.py

Collector per GDELT DOC 2.0 (https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/).
Gratuito, senza API key. Aggiornato ogni 15 minuti, 65+ lingue.

Nota: GDELT applica rate limiting su chiamate ravvicinate.
Incluso retry con backoff esponenziale per gestirlo automaticamente.
"""

import time
import requests
from models import RawRecord
from collectors.base import BaseCollector

BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

_REQUEST_DELAY = 2.0   # secondi minimi tra le chiamate
_MAX_RETRIES   = 3


class GdeltCollector(BaseCollector):
    source_id = "gdelt"

    def collect(self, target: str, query: str, max_records: int = 75) -> list[RawRecord]:
        """
        Args:
            target:      entità analizzata.
            query:       stringa di ricerca.
            max_records: numero massimo di risultati (max 250).
        """
        params = {
            "query":      query,
            "mode":       "artlist",
            "maxrecords": min(max_records, 250),
            "format":     "json",
            "sort":       "datedesc",
        }

        data = self._request_with_retry(params, query)
        if data is None:
            return []

        records = [
            self._make_raw(target, query, article)
            for article in data.get("articles", [])
            if article.get("url")
        ]

        self._log_collected(query, len(records))
        return records

    # ------------------------------------------------------------------

    def _request_with_retry(self, params: dict, query: str) -> dict | None:
        """Esegue la richiesta con retry esponenziale in caso di HTTP 429."""
        time.sleep(_REQUEST_DELAY)

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                response = requests.get(BASE_URL, params=params, timeout=15)

                if response.status_code == 429:
                    wait = _REQUEST_DELAY * (2 ** attempt)
                    self._log_skip(
                        f"rate limit (429), attendo {wait:.0f}s "
                        f"(tentativo {attempt}/{_MAX_RETRIES})"
                    )
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                return response.json()

            except requests.exceptions.HTTPError as e:
                self._log_error(query, e)
                if attempt == _MAX_RETRIES:
                    return None
            except requests.RequestException as e:
                self._log_error(query, e)
                if attempt == _MAX_RETRIES:
                    return None
            except ValueError as e:
                self._log_error(query, e)
                return None

        return None
