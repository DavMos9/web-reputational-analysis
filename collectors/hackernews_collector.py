"""collectors/hackernews_collector.py — Algolia Search API per Hacker News. Nessuna API key."""

from __future__ import annotations

import logging

import requests
from collectors.base import BaseCollector
from collectors.retry import http_get_with_retry
from models import RawRecord

log = logging.getLogger(__name__)

_BASE_URL_RELEVANCE = "https://hn.algolia.com/api/v1/search"
_BASE_URL_DATE      = "https://hn.algolia.com/api/v1/search_by_date"
_MAX_RESULTS_CAP    = 50


class HackerNewsCollector(BaseCollector):
    source_id = "hackernews"

    def collect(
        self,
        target: str,
        query: str,
        max_results: int = 20,
        **kwargs: object,
    ) -> list[RawRecord]:
        """kwargs: search_by_date (bool) — usa /search_by_date (cronologico) invece di /search (rilevanza)."""
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
                log.warning("[HackerNewsCollector] Rate limit raggiunto (HTTP 429).")
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
