"""
collectors/reddit_collector.py — Reddit /search.json non autenticato.

Limiti: ~30 req/min, max 100 risultati. User-Agent obbligatorio: Reddit blocca
richieste con UA generico. `url` canonico è sempre il permalink (non il link
esterno) per garantire unicità nella deduplicazione.
"""

from __future__ import annotations

import logging

import requests
from collectors.base import BaseCollector
from collectors.retry import http_get_with_retry
from config import APP_USER_AGENT
from models import RawRecord

log = logging.getLogger(__name__)

_BASE_URL        = "https://www.reddit.com/search.json"
_MAX_RESULTS_CAP = 100

_USER_AGENT = APP_USER_AGENT


class RedditCollector(BaseCollector):
    source_id = "reddit"

    def collect(
        self,
        target: str,
        query: str,
        max_results: int = 20,
        **kwargs: object,
    ) -> list[RawRecord]:
        """kwargs: sort ("relevance"|"new"|"hot"|"top"), time ("all"|"year"|"month"|...)."""
        sort: str        = str(kwargs.get("sort", "relevance"))
        time_filter: str = str(kwargs.get("time", "all"))

        params = {
            "q":      query,
            "sort":   sort,
            "t":      time_filter,
            "limit":  min(max_results, _MAX_RESULTS_CAP),
            "type":   "link",     # solo post, non commenti
        }

        headers = {"User-Agent": _USER_AGENT}

        try:
            response = http_get_with_retry(
                _BASE_URL,
                params=params,
                headers=headers,
                timeout=15,
                source_id=self.source_id,
            )

            # 403: subreddit privato o blocco UA — non recuperabile con retry.
            if response.status_code == 403:
                log.warning(
                    "[RedditCollector] Accesso negato (HTTP 403). "
                    "Possibile blocco User-Agent o subreddit privato."
                )
                return []

            if response.status_code == 429:
                log.warning(
                    "[RedditCollector] Rate limit (HTTP 429) ancora attivo dopo retry. "
                    "Sorgente skippata per questa esecuzione."
                )
                return []

            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            self._log_error(query, e)
            return []

        children = data.get("data", {}).get("children", [])

        records = [
            self._make_raw(target, query, child.get("data", {}))
            for child in children
            if child.get("data", {}).get("permalink")
        ]

        self._log_collected(query, len(records))
        return records
