"""collectors/bbc_collector.py — BBC News RSS. Nessuna API key.

I feed BBC non includono dc:creator, quindi author è sempre None nel normalizer.
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

_RSS_FEEDS: list[str] = [
    "https://feeds.bbci.co.uk/news/world/rss.xml",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://feeds.bbci.co.uk/news/technology/rss.xml",
    "https://feeds.bbci.co.uk/news/politics/rss.xml",
]

_HEADERS = {
    "User-Agent": APP_USER_AGENT,
}


class BbcCollector(BaseCollector):
    source_id = "bbc"

    def collect(
        self,
        target: str,
        query: str,
        max_results: int = 20,
        **kwargs: object,
    ) -> list[RawRecord]:
        items = self._fetch_all_feeds(timeout=15)

        terms = [t.lower() for t in query.split() if len(t) > 2]
        if terms:
            items = [
                item for item in items
                if self._is_relevant(item, terms)
            ]

        seen_urls: set[str] = set()
        unique: list[dict] = []
        for item in items:
            url = item.get("link") or ""
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique.append(item)

        unique.sort(key=lambda x: x.get("pubDate") or "", reverse=True)
        unique = unique[:max_results]

        records = [self._make_raw(target, query, item) for item in unique]
        self._log_collected(query, len(records))
        return records

    def _fetch_all_feeds(self, timeout: int = 15) -> list[dict]:
        """Scarica tutti i feed RSS in parallelo."""
        all_items: list[dict] = []

        def fetch_one(url: str) -> list[dict]:
            try:
                resp = http_get_with_retry(url, headers=_HEADERS, timeout=timeout, source_id=self.source_id)
                resp.raise_for_status()
                return self._parse_rss(resp.text)
            except requests.RequestException as e:
                log.warning("[BbcCollector] Errore fetch %s: %s", url, e)
                return []

        with ThreadPoolExecutor(max_workers=len(_RSS_FEEDS)) as executor:
            futures = {executor.submit(fetch_one, url): url for url in _RSS_FEEDS}
            for future in as_completed(futures):
                try:
                    all_items.extend(future.result())
                except Exception as e:
                    log.warning("[BbcCollector] Future fallita: %s", e)

        return all_items

    @staticmethod
    def _parse_rss(xml_text: str) -> list[dict]:
        """Parsa un feed RSS e restituisce lista di dizionari."""
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            log.warning("[BbcCollector] Errore parsing RSS: %s", e)
            return []

        channel = root.find("channel")
        if channel is None:
            return []

        items: list[dict] = []
        for item in channel.findall("item"):
            items.append({
                "title":       _text(item, "title"),
                "link":        _text(item, "link"),
                "pubDate":     _text(item, "pubDate"),
                "description": _text(item, "description"),
            })
        return items

    @staticmethod
    def _is_relevant(item: dict, terms: list[str]) -> bool:
        """True se almeno un termine appare in titolo o descrizione."""
        haystack = " ".join(filter(None, [
            item.get("title") or "",
            item.get("description") or "",
        ])).lower()
        return any(term in haystack for term in terms)


def _text(element: ET.Element, tag: str) -> str | None:
    """Testo di un sotto-elemento, None se assente."""
    child = element.find(tag)
    return child.text.strip() if child is not None and child.text else None
