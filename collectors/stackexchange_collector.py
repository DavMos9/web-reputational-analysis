"""
collectors/stackexchange_collector.py — Stack Exchange API v2.3 (/search/excerpts).

Senza API key: 300 req/giorno per IP. Con key: 10.000 req/giorno.
Le risposte includono backoff e quota_remaining gestiti internamente.
"""

from __future__ import annotations

import logging
import time

import requests

from collectors.base import BaseCollector
from collectors.retry import http_get_with_retry
from config import STACKEXCHANGE_API_KEY
from models import RawRecord

log = logging.getLogger(__name__)

_BASE_URL = "https://api.stackexchange.com/2.3"

_MAX_PAGESIZE = 100
_DEFAULT_SITES = ("stackoverflow",)
_INTER_REQUEST_DELAY = 0.5


class StackExchangeCollector(BaseCollector):
    source_id = "stackexchange"

    def collect(
        self,
        target: str,
        query: str,
        max_results: int = 30,
        sites: tuple[str, ...] | None = None,
        sort: str = "relevance",
        **kwargs: object,
    ) -> list[RawRecord]:
        """sites: siti da interrogare (default: ("stackoverflow",)); sort: "relevance"|"votes"."""
        sites = sites or _DEFAULT_SITES
        pagesize = min(max_results, _MAX_PAGESIZE)

        all_records: list[RawRecord] = []

        for site in sites:
            records = self._search_site(target, query, site, pagesize, sort)
            all_records.extend(records)

            if len(sites) > 1:
                time.sleep(_INTER_REQUEST_DELAY)

        self._log_collected(query, len(all_records))
        return all_records

    def _search_site(
        self,
        target: str,
        query: str,
        site: str,
        pagesize: int,
        sort: str,
    ) -> list[RawRecord]:
        """Esegue la ricerca su un singolo sito Stack Exchange."""
        params: dict[str, object] = {
            "q": query,
            "site": site,
            "pagesize": pagesize,
            "sort": sort,
            "order": "desc",
        }

        if STACKEXCHANGE_API_KEY:
            params["key"] = STACKEXCHANGE_API_KEY

        try:
            response = http_get_with_retry(
                f"{_BASE_URL}/search/excerpts",
                params=params,
                timeout=15,
                source_id=self.source_id,
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            self._log_error(query, e)
            return []

        quota_remaining = data.get("quota_remaining")
        if quota_remaining is not None and quota_remaining < 20:
            log.warning(
                "[%s] Quota quasi esaurita: %d richieste rimanenti",
                self.source_id,
                quota_remaining,
            )

        backoff = data.get("backoff")
        if backoff:
            log.info("[%s] Backoff richiesto dal server: %d secondi", self.source_id, backoff)
            time.sleep(backoff)

        if "error_id" in data:
            log.error(
                "[%s] Errore API (site=%s): %s — %s",
                self.source_id,
                site,
                data.get("error_name", "unknown"),
                data.get("error_message", ""),
            )
            return []

        items = data.get("items", [])
        if not items:
            log.info("[%s] Nessun risultato su '%s' per query: '%s'", self.source_id, site, query)
            return []

        records = []
        for item in items:
            item["_site"] = site
            records.append(self._make_raw(target, query, item))

        log.info(
            "[%s] Raccolti %d risultati da '%s' per query: '%s'",
            self.source_id, len(records), site, query,
        )
        return records
