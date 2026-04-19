"""
collectors/gnews_it_collector.py

Collector per Google News RSS filtrato su lingua/paese Italia.

Endpoint (nessuna API key richiesta):
    https://news.google.com/rss/search?q={query}&hl=it&gl=IT&ceid=IT:it

Questo endpoint restituisce un feed RSS 2.0 con gli articoli più recenti
e rilevanti indicizzati da Google News per la query fornita, filtrati per
lingua italiana e paese Italia. Copre le principali testate nazionali
(Corriere della Sera, Repubblica, ANSA, Il Sole 24 Ore, La Stampa, ecc.)
senza richiedere API key specifiche per ciascuna.

Risoluzione redirect:
    Le URL nel feed RSS sono redirect Google (news.google.com/rss/articles/...).
    Il collector le risolve tramite HEAD request per ottenere il permalink
    canonico dell'articolo. Il dominio reale viene comunque estratto anche
    dall'elemento <source url="..."> nel feed (più affidabile, non richiede
    una richiesta aggiuntiva). La risoluzione redirect è parallela tramite
    ThreadPoolExecutor per non appesantire il tempo totale di raccolta.
    In caso di errore la URL originale viene mantenuta come fallback.

Limiti:
    - Nessuna quota ufficiale documentata; usare con rate limite conservativo.
    - Il campo `text` da RSS è solitamente breve (snippet/titoli aggregati):
      il normalizer lo pulisce; il titolo porta l'informazione principale.
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
_MAX_RESULTS_CAP = 100  # Google News RSS restituisce al massimo ~100 item


class GNewsItCollector(BaseCollector):
    source_id = "gnews_it"

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
            query:       stringa di ricerca.
            max_results: numero di risultati desiderati (cap a 100).
                         Google News RSS non accetta un parametro di limite:
                         restituisce tutti gli item disponibili (di solito 10-100).
                         Il cap viene applicato in post-processing.
        """
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

        # Risolvi i redirect Google in parallelo per ottenere URL canonici.
        items = self._resolve_redirects(items)

        records = [
            self._make_raw(target, query, item)
            for item in items
        ]

        self._log_collected(query, len(records))
        return records

    # ------------------------------------------------------------------

    def _resolve_redirects(self, items: list[dict], timeout: int = 5) -> list[dict]:
        """
        Risolve in parallelo i redirect Google News per ogni item.

        Usa ThreadPoolExecutor con un pool limitato (max 8 worker) per non
        saturare le connessioni. In caso di errore su un singolo item, la URL
        originale viene mantenuta senza interrompere gli altri.

        Args:
            items:   lista di dizionari prodotta da _parse_rss.
            timeout: secondi di timeout per ogni HEAD request.

        Returns:
            Stessa lista con campo 'link' aggiornato alla URL finale.
        """
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
                # Se Google ha interposto la consent page (es. target anglofoni
                # sul feed IT), il redirect non porta all'articolo ma alla
                # pagina GDPR. In quel caso manteniamo l'URL originale: è
                # comunque un redirect funzionante verso l'articolo ed evita
                # che il cleaner scarti il record per dominio bloccato.
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
        """
        Parsa il feed RSS e restituisce una lista di dizionari con i campi
        rilevanti per il normalizer.

        Struttura attesa per ogni <item>:
            <title>...</title>
            <link>...</link>         ← redirect Google
            <pubDate>...</pubDate>
            <description>...</description>
            <source url="https://...">Nome testata</source>
        """
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
