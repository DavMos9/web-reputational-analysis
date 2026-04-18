"""
collectors/ansa_collector.py

Collector per ANSA (Agenzia Nazionale Stampa Associata) tramite feed RSS pubblici.

Endpoint (nessuna API key richiesta):
    https://www.ansa.it/sito/notizie/mondo/mondo_rss.xml             — esteri
    https://www.ansa.it/sito/notizie/politica/politica_rss.xml       — politica
    https://www.ansa.it/sito/notizie/economia/economia_rss.xml       — economia
    https://www.ansa.it/sito/notizie/cronaca/cronaca_rss.xml         — cronaca
    https://www.ansa.it/sito/notizie/cultura/cultura_rss.xml         — cultura e spettacolo
    https://www.ansa.it/sito/notizie/tecnologia/tecnologia_rss.xml   — tecnologia
    https://www.ansa.it/sito/notizie/sport/sport_rss.xml             — sport

ANSA è la principale agenzia di stampa italiana. I feed RSS coprono le
principali categorie editoriali e vengono aggiornati in tempo reale.
I testi sono in italiano; il normalizer imposta `language="it"` direttamente.

Strategia di raccolta:
    - Scarica tutti i feed in parallelo.
    - Filtra gli item il cui titolo o descrizione contiene almeno uno dei
      termini della query (case-insensitive, split su spazi, minimo 2 caratteri).
    - Deduplicazione per URL prima di restituire i record.
    - Restituisce fino a max_results record ordinati per data (più recenti prima).

Limiti:
    - Copertura limitata agli item presenti nei feed al momento della raccolta
      (tipicamente 20-40 item per feed, aggiornati frequentemente).
    - Il filtro per query è client-side: query molto specifiche possono
      restituire pochi o zero risultati.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from collectors.base import BaseCollector
from models import RawRecord

log = logging.getLogger(__name__)

# Feed RSS ANSA — copertura tematica ampia per reputation analysis generalista.
# I feed cultura, tecnologia e sport estendono la copertura a celebrity, personaggi
# dello spettacolo, sportivi e aziende tech — categorie assenti dai 4 feed originali.
_RSS_FEEDS: list[str] = [
    "https://www.ansa.it/sito/notizie/mondo/mondo_rss.xml",
    "https://www.ansa.it/sito/notizie/politica/politica_rss.xml",
    "https://www.ansa.it/sito/notizie/economia/economia_rss.xml",
    "https://www.ansa.it/sito/notizie/cronaca/cronaca_rss.xml",
    "https://www.ansa.it/sito/notizie/cultura/cultura_rss.xml",         # spettacolo, celebrity
    "https://www.ansa.it/sito/notizie/tecnologia/tecnologia_rss.xml",   # aziende tech, prodotti
    "https://www.ansa.it/sito/notizie/sport/sport_rss.xml",             # sportivi, club
]

_HEADERS = {
    "User-Agent": "web-reputational-analysis/0.4.0 (academic research pipeline)",
}


class AnsaCollector(BaseCollector):
    source_id = "ansa"

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

        # Filtro rilevanza: almeno un termine della query in titolo o descrizione.
        # Termini di 1 carattere ignorati (articoli, preposizioni brevi).
        terms = [t.lower() for t in query.split() if len(t) > 1]
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
        """Scarica tutti i feed RSS ANSA in parallelo."""
        all_items: list[dict] = []

        def fetch_one(url: str) -> list[dict]:
            try:
                resp = requests.get(url, headers=_HEADERS, timeout=timeout)
                if resp.status_code == 429:
                    log.warning(
                        "[AnsaCollector] Rate limit raggiunto (HTTP 429) su %s.", url
                    )
                    return []
                resp.raise_for_status()
                return self._parse_rss(resp.text)
            except requests.RequestException as e:
                log.warning("[AnsaCollector] Errore fetch %s: %s", url, e)
                return []

        with ThreadPoolExecutor(max_workers=len(_RSS_FEEDS)) as executor:
            futures = {executor.submit(fetch_one, url): url for url in _RSS_FEEDS}
            for future in as_completed(futures):
                try:
                    all_items.extend(future.result())
                except Exception as e:
                    log.warning("[AnsaCollector] Future fallita: %s", e)

        return all_items

    @staticmethod
    def _parse_rss(xml_text: str) -> list[dict]:
        """
        Parsa un feed RSS ANSA e restituisce lista di dizionari.

        Il feed ANSA include namespace media per le immagini e Dublin Core
        per i metadati. Vengono estratti solo i campi testuali utili.
        """
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            log.warning("[AnsaCollector] Errore parsing RSS: %s", e)
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
