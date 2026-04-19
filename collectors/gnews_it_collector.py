"""
collectors/gnews_it_collector.py — Google News RSS (lingua/paese Italia). Nessuna API key.

Le URL del feed sono redirect Google: vengono risolte in parallelo via HEAD request.
consent.google.com viene ignorato come destinazione finale (fallback all'URL originale).
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from collectors.base import BaseCollector
from collectors.retry import http_get_with_retry
from config import APP_USER_AGENT
from models import RawRecord

log = logging.getLogger(__name__)

_BASE_URL = "https://news.google.com/rss/search"
_MAX_RESULTS_CAP = 100


class GNewsItCollector(BaseCollector):
    source_id = "gnews_it"

    def collect(
        self,
        target: str,
        query: str,
        max_results: int = 20,
        **kwargs: object,
    ) -> list[RawRecord]:
        """max_results applicato in post-processing (RSS non accetta parametro di limite)."""
        params = {
            "q":    query,
            "hl":   "it",
            "gl":   "IT",
            "ceid": "IT:it",
        }

        try:
            response = http_get_with_retry(
                _BASE_URL,
                params=params,
                timeout=15,
                headers={"User-Agent": APP_USER_AGENT},
                source_id=self.source_id,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            self._log_error(query, e)
            return []

        items = self._parse_rss(response.text)
        items = items[: min(max_results, _MAX_RESULTS_CAP)]

        items = self._resolve_redirects(items)

        records = [
            self._make_raw(target, query, item)
            for item in items
        ]

        self._log_collected(query, len(records))
        return records

    # ------------------------------------------------------------------

    def _resolve_redirects(self, items: list[dict], timeout: int = 5) -> list[dict]:
        """Risolve i redirect Google News in parallelo (max 8 worker)."""
        def resolve_one(item: dict) -> dict:
            original = item.get("link") or ""
            if not original:
                return item
            try:
                resp = requests.head(
                    original,
                    allow_redirects=True,
                    timeout=timeout,
                    headers={"User-Agent": APP_USER_AGENT},
                )
                resolved = resp.url
                # consent.google.com: gate GDPR su target anglofoni nel feed IT.
                # In quel caso manteniamo l'URL originale per evitare che il cleaner scarti il record.
                if resolved and not resolved.startswith("https://consent.google.com"):
                    if resolved != original:
                        return {**item, "link": resolved}
            except Exception:
                pass  # fallback: mantieni URL originale
            return item

        max_workers = min(8, len(items))
        if max_workers == 0:
            return items

        resolved: dict[int, dict] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(resolve_one, item): i for i, item in enumerate(items)}
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    resolved[idx] = future.result()
                except Exception:
                    resolved[idx] = items[idx]

        return [resolved[i] for i in range(len(items))]

    # ------------------------------------------------------------------

    @staticmethod
    def _parse_rss(xml_text: str) -> list[dict]:
        """Parsa il feed RSS e restituisce una lista di dizionari per il normalizer."""
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            log.warning("[GNewsItCollector] Errore parsing RSS: %s", e)
            return []

        channel = root.find("channel")
        if channel is None:
            return []

        items: list[dict] = []
        for item in channel.findall("item"):
            source_el = item.find("source")
            items.append({
                "title":       _text(item, "title"),
                "link":        _text(item, "link"),
                "pubDate":     _text(item, "pubDate"),
                "description": _text(item, "description"),
                "source_name": source_el.text.strip() if source_el is not None and source_el.text else None,
                "source_url":  source_el.get("url") if source_el is not None else None,
            })
        return items


def _text(element: ET.Element, tag: str) -> str | None:
    """Restituisce il testo di un sotto-elemento, None se assente."""
    child = element.find(tag)
    return child.text.strip() if child is not None and child.text else None
