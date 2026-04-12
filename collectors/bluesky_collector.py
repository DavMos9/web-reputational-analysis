"""
collectors/bluesky_collector.py

Collector per Bluesky Social tramite AT Protocol Public API.

Nessuna autenticazione richiesta: l'endpoint di ricerca è pubblico.
Documentazione: https://docs.bsky.app/docs/api/app-bsky-feed-search-posts

Rate limit: non documentato ufficialmente; si consiglia di non superare
le 30 richieste/minuto per rispettare un comportamento corretto.
"""

import requests

from collectors.base import BaseCollector
from models import RawRecord

SEARCH_URL = "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts"

# Numero massimo di post per singola richiesta (limite imposto dall'API)
_MAX_LIMIT = 100


class BlueskyCollector(BaseCollector):
    source_id = "bluesky"

    def collect(
        self,
        target: str,
        query: str,
        max_results: int = 50,
        sort: str = "latest",
        **kwargs,
    ) -> list[RawRecord]:
        """
        Args:
            target:      entità analizzata.
            query:       stringa di ricerca.
            max_results: numero massimo di post da raccogliere (max 100).
            sort:        "latest" (cronologico, default) o "top" (per engagement).
        """
        params = {
            "q":     query,
            "limit": min(max_results, _MAX_LIMIT),
            "sort":  sort,
        }

        try:
            response = requests.get(SEARCH_URL, params=params, timeout=10)
            response.raise_for_status()
            posts = response.json().get("posts", [])
        except requests.RequestException as e:
            self._log_error(query, e)
            return []

        if not posts:
            self._log_collected(query, 0)
            return []

        records = [self._make_raw(target, query, post) for post in posts]
        self._log_collected(query, len(records))
        return records
