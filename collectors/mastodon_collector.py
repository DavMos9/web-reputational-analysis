"""
collectors/mastodon_collector.py — Mastodon API v2/v1.

Strategia: prima ricerca full-text (/api/v2/search, richiede ElasticSearch attivo),
poi fallback su timeline hashtag (/api/v1/timelines/tag/:hashtag).
Il token è incluso solo sull'istanza per cui è stato creato (MASTODON_TOKEN_INSTANCE).
"""

from __future__ import annotations

import logging
import re
import time

import requests

from collectors.base import BaseCollector
from collectors.retry import http_get_with_retry
from config import MASTODON_ACCESS_TOKEN, MASTODON_TOKEN_INSTANCE, MASTODON_INSTANCES
from models import RawRecord

log = logging.getLogger(__name__)

_SEARCH_LIMIT = 40        # max per tipo imposto dall'API
_TIMELINE_LIMIT = 40
_INTER_INSTANCE_DELAY = 0.5


class MastodonCollector(BaseCollector):
    source_id = "mastodon"

    def collect(
        self,
        target: str,
        query: str,
        max_results: int = 30,
        instances: tuple[str, ...] | None = None,
        **kwargs: object,
    ) -> list[RawRecord]:
        """instances: tuple di istanze (default da config.MASTODON_INSTANCES)."""
        instances = instances or MASTODON_INSTANCES
        limit = min(max_results, _SEARCH_LIMIT)

        all_records: list[RawRecord] = []

        for instance in instances:
            records = self._collect_from_instance(target, query, instance, limit)
            all_records.extend(records)

            if len(instances) > 1:
                time.sleep(_INTER_INSTANCE_DELAY)

        self._log_collected(query, len(all_records))
        return all_records

    def _collect_from_instance(
        self,
        target: str,
        query: str,
        instance: str,
        limit: int,
    ) -> list[RawRecord]:
        """
        Raccoglie post da una singola istanza Mastodon.
        Prova prima la ricerca full-text, poi fallback su hashtag timeline.
        """
        base_url = f"https://{instance}"

        # Strategia 1: ricerca full-text sugli statuses
        records = self._search_statuses(target, query, instance, base_url, limit)
        if records:
            return records

        # Strategia 2: fallback su hashtag timeline
        log.info(
            "[%s] Ricerca su '%s' non ha restituito statuses, provo timeline hashtag",
            self.source_id, instance,
        )
        return self._hashtag_timeline(target, query, instance, base_url, limit)

    def _build_headers(self, instance: str) -> dict[str, str]:
        """Il token è incluso solo sull'istanza per cui è stato creato."""
        headers: dict[str, str] = {
            "Accept": "application/json",
        }
        if MASTODON_ACCESS_TOKEN and instance == MASTODON_TOKEN_INSTANCE:
            headers["Authorization"] = f"Bearer {MASTODON_ACCESS_TOKEN}"
        return headers

    def _search_statuses(
        self,
        target: str,
        query: str,
        instance: str,
        base_url: str,
        limit: int,
    ) -> list[RawRecord]:
        """Ricerca full-text via /api/v2/search."""
        params: dict[str, object] = {
            "q": query,
            "type": "statuses",
            "limit": limit,
        }

        try:
            response = http_get_with_retry(
                f"{base_url}/api/v2/search",
                params=params,
                headers=self._build_headers(instance),
                timeout=15,
                source_id=self.source_id,
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            self._log_error(query, e)
            return []

        statuses = data.get("statuses", [])
        if not statuses:
            return []

        records = []
        for status in statuses:
            status["_instance"] = instance
            records.append(self._make_raw(target, query, status))

        log.info(
            "[%s] Ricerca su '%s': %d statuses per query '%s'",
            self.source_id, instance, len(records), query,
        )
        return records

    def _hashtag_timeline(
        self,
        target: str,
        query: str,
        instance: str,
        base_url: str,
        limit: int,
    ) -> list[RawRecord]:
        """Fallback su timeline pubblica hashtag."""
        hashtag = self._query_to_hashtag(query)
        if not hashtag:
            log.warning(
                "[%s] Impossibile derivare un hashtag dalla query '%s'",
                self.source_id, query,
            )
            return []

        params: dict[str, object] = {
            "limit": min(limit, _TIMELINE_LIMIT),
        }

        try:
            response = http_get_with_retry(
                f"{base_url}/api/v1/timelines/tag/{hashtag}",
                params=params,
                headers=self._build_headers(instance),
                timeout=15,
                source_id=self.source_id,
            )
            response.raise_for_status()
            statuses = response.json()
        except requests.RequestException as e:
            self._log_error(query, e)
            return []

        if not statuses or not isinstance(statuses, list):
            log.info(
                "[%s] Nessun post per hashtag '#%s' su '%s'",
                self.source_id, hashtag, instance,
            )
            return []

        records = []
        for status in statuses:
            status["_instance"] = instance
            status["_hashtag_fallback"] = hashtag
            records.append(self._make_raw(target, query, status))

        log.info(
            "[%s] Timeline hashtag '#%s' su '%s': %d statuses",
            self.source_id, hashtag, instance, len(records),
        )
        return records

    @staticmethod
    def _query_to_hashtag(query: str) -> str:
        """Converte query in hashtag CamelCase: "Elon Musk" → "ElonMusk"."""
        cleaned = re.sub(r"[^\w\s]", "", query)
        words = cleaned.split()
        if not words:
            return ""
        return "".join(w.capitalize() for w in words)
