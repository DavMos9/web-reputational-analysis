"""
collectors/bbc_collector.py

Collector per BBC News tramite feed RSS pubblici.

Endpoint (nessuna API key richiesta):
    https://feeds.bbci.co.uk/news/world/rss.xml        — notizie mondiali
    https://feeds.bbci.co.uk/news/business/rss.xml     — economia e business
    https://feeds.bbci.co.uk/news/technology/rss.xml   — tecnologia
    https://feeds.bbci.co.uk/news/politics/rss.xml     — politica UK/Europa

BBC News è una delle principali fonti giornalistiche internazionali. I feed RSS
coprono le principali categorie editoriali e vengono aggiornati ogni 15 minuti.

Strategia di raccolta (identica al pattern RSS del progetto):
    - Scarica i tre feed in parallelo.
    - Filtra gli item il cui titolo o descrizione contiene almeno uno dei termini
      della query (case-insensitive, split su spazi, termini con ≥ 3 caratteri).
    - Deduplicazione per URL prima di restituire i record.
    - Restituisce fino a max_results record ordinati per data (più recenti prima).

Limiti:
    - Copertura limitata agli articoli presenti nei feed al momento della raccolta
      (tipicamente 30 item per feed, aggiornati ogni 15 minuti).
    - Il filtro per query è client-side: per query molto specifiche il volume
      può essere basso o zero.
    - I feed BBC non includono il campo dc:creator: author è sempre None.
    - Language è impostata a None nel normalizer: l'enricher la rileva via langdetect
      (BBC pubblica principalmente in inglese ma occasionalmente in altre lingue).
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from collectors.base import BaseCollector
from models import RawRecord

log = logging.getLogger(__name__)

# Feed RSS BBC News pubblici — copertura notizie mondiali, business, tech e politica.
_RSS_FEEDS: list[str] = [
    "https://feeds.bbci.co.uk/news/world/rss.xml",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://feeds.bbci.co.uk/news/technology/rss.xml",
    "https://feeds.bbci.co.uk/news/politics/rss.xml",  # UK/Europa — utile per target politici
]

_HEADERS = {
    "User-Agent": "web-reputational-analysis/0.4.0 (academic research pipeline)",
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
        """
        Args:
            target:      entità analizzata.
            query:       stringa di ricerca usata per filtrare gli item RSS.
            max_results: numero massimo di record da restituire.
        """
        items = self._fetch_all_feeds(timeout=15)

        # Filtro rilevanza: almeno un termine (>= 3 caratteri) in titolo o descrizione.
        terms = [t.lower() for t in query.split() if len(t) > 2]
        if terms:
            items = [
                item for item in items
                if self._is_relevant(item, terms)
            ]

        # Deduplicazione per URL
        seen_urls: set[str] = set()
        unique: list[dict] = []
        for item in items:
            url = item.get("link") or ""
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique.append(item)

        # Ordina per data decrescente e tronca
        unique.sort(key=lambda x: x.get("pubDate") or "", reverse=True)
        unique = unique[:max_results]

        records = [self._make_raw(target, query, item) for item in unique]
        self._log_collected(query, len(records))
        return records

    # ------------------------------------------------------------------

    def _fetch_all_feeds(self, timeout: int = 15) -> list[dict]:
        """Scarica tutti i feed RSS BBC in parallelo."""
        all_items: list[dict] = []

        def fetch_one(url: str) -> list[dict]:
            try:
                resp = requests.get(url, headers=_HEADERS, timeout=timeout)
                if resp.status_code == 429:
                    log.warning(
                        "[BbcCollector] Rate limit raggiunto (HTTP 429) su %s.", url
                    )
                    return []
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
        """
        Parsa un feed RSS BBC e restituisce lista di dizionari.

        I feed BBC usano CDATA per title e description, gestiti correttamente
        da xml.etree.ElementTree. Non includono dc:creator: author resta None.
        """
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
        """
        Verifica se almeno un termine di ricerca appare nel titolo
        o nella descrizione (case-insensitive).
        """
        haystack = " ".join(filter(None, [
            item.get("title") or "",
            item.get("description") or "",
        ])).lower()
        return any(term in haystack for term in terms)


def _text(element: ET.Element, tag: str) -> str | None:
    """Restituisce il testo di un sotto-elemento, None se assente."""
    child = element.find(tag)
    return child.text.strip() if child is not None and child.text else None
