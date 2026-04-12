"""
collectors/wikitalk_collector.py

Collector per le Talk Pages di Wikipedia (pagine di discussione).

Le talk pages contengono opinioni, dibattiti e dispute editoriali
riguardo le voci enciclopediche — dati preziosi per la reputation analysis
perché riflettono controversie e percezioni degli editor.

Strategia:
1. Opensearch su namespace 0 (articolo) per trovare il titolo canonico.
2. Richiesta a Talk:{titolo} via action=parse per ottenere sezioni e wikitext.
3. Ogni sezione di discussione diventa un RawRecord separato.
4. Sezioni troppo corte o puramente template vengono scartate.

API: MediaWiki Action API (gratuita, nessuna key).
Documentazione: https://www.mediawiki.org/wiki/API:Parsing_wikitext
Rate limit: raccomandato < 200 req/sec con User-Agent valido.
"""

from __future__ import annotations

import logging
import re

import requests

from collectors.base import BaseCollector
from models import RawRecord

log = logging.getLogger(__name__)

_USER_AGENT = "web-reputational-analysis/1.0"
_API_URL = "https://{lang}.wikipedia.org/w/api.php"

# Soglia minima di caratteri per considerare una sezione come discussione reale
# (esclude sezioni vuote, redirect, o con solo template)
_MIN_SECTION_LENGTH = 50

# Pattern per identificare contenuto prevalentemente template (non discussione)
_TEMPLATE_HEAVY_RE = re.compile(r"^\s*\{\{[^}]+\}\}\s*$", re.DOTALL)


class WikiTalkCollector(BaseCollector):
    source_id = "wikitalk"

    def __init__(self) -> None:
        self._fetched: set[str] = set()

    def collect(
        self,
        target: str,
        query: str,
        max_results: int = 20,
        **kwargs,
    ) -> list[RawRecord]:
        """
        Args:
            target:      entità analizzata (es. "Elon Musk").
            query:       stringa di ricerca (usata per tracciabilità).
            max_results: numero massimo di sezioni da raccogliere.
            kwargs:      lang (str, default "en") — lingua Wikipedia.

        Nota: la ricerca usa `target`, non `query`, perché le talk pages
        sono associate a un'entità enciclopedica specifica.
        """
        lang: str = str(kwargs.get("lang", "en"))

        # Trova il titolo canonico dell'articolo
        page_title = self._opensearch(target, lang)
        if not page_title:
            self._log_skip(f"opensearch senza risultati per '{target}' ({lang})")
            return []

        cache_key = f"{lang}:Talk:{page_title.lower()}"
        if cache_key in self._fetched:
            self._log_skip(f"talk page '{page_title}' già raccolta (query: '{query}')")
            return []

        # Fetch talk page
        sections = self._fetch_talk_sections(page_title, lang)
        if not sections:
            self._log_skip(f"talk page per '{page_title}' vuota o inesistente ({lang})")
            return []

        self._fetched.add(cache_key)

        records = []
        talk_url_base = f"https://{lang}.wikipedia.org/wiki/Talk:{page_title.replace(' ', '_')}"

        for section in sections[:max_results]:
            # Costruisci anchor per la sezione
            anchor = section["anchor"]
            section_url = f"{talk_url_base}#{anchor}" if anchor else talk_url_base

            payload = {
                "page_title": page_title,
                "section_title": section["title"],
                "section_index": section["index"],
                "section_level": section["level"],
                "wikitext": section["wikitext"],
                "url": section_url,
                "language": lang,
            }
            records.append(self._make_raw(target, query, payload))

        self._log_collected(query, len(records))
        return records

    def _opensearch(self, target: str, lang: str) -> str | None:
        """Trova il titolo canonico della pagina Wikipedia per il target."""
        params = {
            "action": "opensearch",
            "search": target,
            "limit": 1,
            "namespace": 0,
            "format": "json",
        }
        try:
            response = requests.get(
                _API_URL.format(lang=lang),
                params=params,
                headers={"User-Agent": _USER_AGENT},
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            titles = data[1] if len(data) > 1 else []
            return titles[0] if titles else None
        except Exception as e:
            self._log_error(target, e)
            return None

    def _fetch_talk_sections(
        self,
        page_title: str,
        lang: str,
    ) -> list[dict]:
        """
        Scarica la talk page e la suddivide in sezioni di discussione.

        Returns:
            Lista di dict con title, index, level, anchor, wikitext.
            Solo sezioni con contenuto sufficiente (sopra _MIN_SECTION_LENGTH).
        """
        talk_page = f"Talk:{page_title}"

        # Fetch sezioni + wikitext intero
        params = {
            "action": "parse",
            "page": talk_page,
            "prop": "sections|wikitext",
            "format": "json",
        }

        try:
            response = requests.get(
                _API_URL.format(lang=lang),
                params=params,
                headers={"User-Agent": _USER_AGENT},
                timeout=15,
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            self._log_error(page_title, e)
            return []

        if "error" in data:
            log.info(
                "[%s] Talk page '%s' non trovata: %s",
                self.source_id, talk_page, data["error"].get("info", ""),
            )
            return []

        parse = data.get("parse", {})
        sections_meta = parse.get("sections", [])
        full_wikitext = parse.get("wikitext", {}).get("*", "")

        if not sections_meta or not full_wikitext:
            return []

        # Split del wikitext per sezioni usando gli header
        return self._split_sections(sections_meta, full_wikitext)

    def _split_sections(
        self,
        sections_meta: list[dict],
        full_wikitext: str,
    ) -> list[dict]:
        """
        Suddivide il wikitext in sezioni basandosi sui metadati.

        Filtra sezioni troppo corte o composte solo da template.
        """
        # Trova le posizioni degli header nel wikitext
        # Header format: == Title == (level 2), === Title === (level 3), ecc.
        header_pattern = re.compile(r"^(={2,})\s*(.+?)\s*\1\s*$", re.MULTILINE)
        headers = list(header_pattern.finditer(full_wikitext))

        result = []
        for i, meta in enumerate(sections_meta):
            title = meta.get("line", "")
            level = int(meta.get("level", 2))
            anchor = meta.get("anchor", title.replace(" ", "_"))

            # Trova il wikitext della sezione
            section_text = self._extract_section_text(headers, i, full_wikitext)

            # Filtra sezioni vuote o solo-template
            cleaned = self._clean_wikitext(section_text)
            if len(cleaned) < _MIN_SECTION_LENGTH:
                continue
            if _TEMPLATE_HEAVY_RE.match(section_text):
                continue

            result.append({
                "title": title,
                "index": meta.get("index", str(i)),
                "level": level,
                "anchor": anchor,
                "wikitext": cleaned,
            })

        return result

    @staticmethod
    def _extract_section_text(
        headers: list[re.Match],
        section_idx: int,
        full_wikitext: str,
    ) -> str:
        """Estrae il testo di una sezione dato il suo indice."""
        if section_idx >= len(headers):
            return ""

        start = headers[section_idx].end()
        end = headers[section_idx + 1].start() if section_idx + 1 < len(headers) else len(full_wikitext)
        return full_wikitext[start:end].strip()

    @staticmethod
    def _strip_templates(text: str) -> str:
        """
        Rimuove tutti i template MediaWiki {{ ... }}, inclusi quelli
        multi-linea e annidati, usando un contatore di parentesi graffe.
        """
        result = []
        depth = 0
        i = 0
        while i < len(text):
            if text[i:i + 2] == "{{":
                depth += 1
                i += 2
            elif text[i:i + 2] == "}}" and depth > 0:
                depth -= 1
                i += 2
            elif depth == 0:
                result.append(text[i])
                i += 1
            else:
                i += 1
        return "".join(result)

    @classmethod
    def _clean_wikitext(cls, text: str) -> str:
        """
        Pulizia del wikitext per estrarre testo leggibile.

        Rimuove template (anche annidati/multi-linea), link interni,
        formattazione wiki e markup delle talk page.
        """
        if not text:
            return ""

        # Rimuovi template di firma e timestamp (~~~~)
        cleaned = re.sub(r"~{3,5}", "", text)
        # Rimuovi tutti i template (inclusi multi-linea e annidati)
        cleaned = cls._strip_templates(cleaned)
        # Rimuovi link interni: [[target|display]] → display, [[target]] → target
        cleaned = re.sub(r"\[\[[^|\]]*\|([^\]]+)\]\]", r"\1", cleaned)
        cleaned = re.sub(r"\[\[([^\]]+)\]\]", r"\1", cleaned)
        # Rimuovi link esterni: [url text] → text
        cleaned = re.sub(r"\[https?://\S+\s+([^\]]+)\]", r"\1", cleaned)
        # Rimuovi bold/italic wiki markup
        cleaned = re.sub(r"'{2,3}", "", cleaned)
        # Rimuovi indent/outdent (: all'inizio riga)
        cleaned = re.sub(r"^[:*#]+\s?", "", cleaned, flags=re.MULTILINE)
        # Normalizza whitespace
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned.strip()

    def reset_cache(self) -> None:
        """Svuota la cache dei titoli. Utile nei test."""
        self._fetched.clear()
