"""collectors/lemmy_collector.py — Lemmy API v3 (/api/v3/search). Nessuna auth richiesta."""

from __future__ import annotations

import logging
import time

import requests

from collectors.base import BaseCollector
from collectors.retry import http_get_with_retry
from config import LEMMY_INSTANCES
from models import RawRecord

log = logging.getLogger(__name__)

_MAX_LIMIT = 50
_INTER_INSTANCE_DELAY = 0.5

_CONTENT_TYPES = ("Posts", "Comments")


class LemmyCollector(BaseCollector):
    source_id = "lemmy"

    def collect(
        self,
        target: str,
        query: str,
        max_results: int = 30,
        instances: tuple[str, ...] | None = None,
        sort: str = "TopAll",
        content_types: tuple[str, ...] = _CONTENT_TYPES,
        **kwargs: object,
    ) -> list[RawRecord]:
        """sort: "TopAll"|"New"|"Hot"|"Old". instances: default da config.LEMMY_INSTANCES."""
        instances = instances or LEMMY_INSTANCES
        limit = min(max_results, _MAX_LIMIT)

        all_records: list[RawRecord] = []

        for instance in instances:
            for content_type in content_types:
                records = self._search(target, query, instance, limit, sort, content_type)
                all_records.extend(records)

            if len(instances) > 1:
                time.sleep(_INTER_INSTANCE_DELAY)

        self._log_collected(query, len(all_records))
        return all_records

    def _search(
        self,
        target: str,
        query: str,
        instance: str,
        limit: int,
        sort: str,
        content_type: str,
    ) -> list[RawRecord]:
        """Esegue la ricerca su una singola istanza per un tipo di contenuto."""
        params: dict[str, object] = {
            "q": query,
            "type_": content_type,
            "limit": limit,
            "sort": sort,
        }

        try:
            response = http_get_with_retry(
                f"https://{instance}/api/v3/search",
                params=params,
                timeout=15,
                source_id=self.source_id,
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            self._log_error(query, e)
            return []

        if content_type == "Posts":
            items = data.get("posts", [])
        elif content_type == "Comments":
            items = data.get("comments", [])
        else:
            items = []

        if not items:
            log.info(
                "[%s] Nessun risultato %s su '%s' per query: '%s'",
                self.source_id, content_type, instance, query,
            )
            return []

        records = []
        for item in items:
            item["_instance"] = instance
            item["_content_type"] = content_type
            records.append(self._make_raw(target, query, item))

        log.info(
            "[%s] %d %s da '%s' per query: '%s'",
            self.source_id, len(records), content_type.lower(), instance, query,
        )
        return records
