"""collectors/brave_collector.py — Brave Search API. Piano gratuito: 2.000 query/mese."""

from __future__ import annotations

import requests
from config import BRAVE_API_KEY
from models import RawRecord
from collectors.base import BaseCollector
from collectors.retry import http_get_with_retry

BASE_URL = "https://api.search.brave.com/res/v1/web/search"

MAX_RESULTS_PER_REQUEST = 20


class BraveCollector(BaseCollector):
    source_id = "brave"

    def collect(
        self,
        target: str,
        query: str,
        max_results: int = 20,
        **kwargs: object,
    ) -> list[RawRecord]:
        """kwargs: country, search_lang, freshness ("pd"|"pw"|"pm"|"py"), safesearch."""
        if not BRAVE_API_KEY:
            self._log_skip("BRAVE_API_KEY non configurata")
            return []

        params: dict[str, object] = {
            "q":     query,
            "count": min(max_results, MAX_RESULTS_PER_REQUEST),
        }

        # Parametri opzionali solo se presenti: valori vuoti/None attiverebbero filtri indesiderati.
        for optional in ("country", "search_lang", "freshness", "safesearch"):
            value = kwargs.get(optional)
            if value:
                params[optional] = str(value)

        headers = {
            "Accept":                "application/json",
            "Accept-Encoding":       "gzip",
            "X-Subscription-Token":  BRAVE_API_KEY,
        }

        try:
            response = http_get_with_retry(
                BASE_URL, params=params, headers=headers, timeout=10, source_id=self.source_id
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            self._log_error(query, e)
            return []

        results = data.get("web", {}).get("results", [])

        records = [
            self._make_raw(target, query, {**item, "_rank": rank})
            for rank, item in enumerate(results, start=1)
            if item.get("url")
        ]

        self._log_collected(query, len(records))
        return records
