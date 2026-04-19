"""
collectors/wikitalk_collector.py — Wikipedia Talk Pages (MediaWiki Action API).

Ogni sezione di discussione diventa un RawRecord separato.
Sezioni troppo corte o composte solo da template vengono scartate.
"""

from __future__ import annotations

import logging
import re

import requests

from collectors.base import BaseCollector
from collectors.retry import http_get_with_retry
from models import RawRecord

log = logging.getLogger(__name__)

_USER_AGENT = "web-reputational-analysis/1.0"
_API_URL = "https://{lang}.wikipedia.org/w/api.php"

_MIN_SECTION_LENGTH = 50  # esclude sezioni vuote o con solo template

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
        **kwargs: object,
    ) -> list[RawRecord]:
        """kwargs: lang (str, default "en"). La ricerca usa `target`, non `query`."""
        lang: str = str(kwargs.get("lang", "en"))

        page_title = self._opensearch(target, lang)
        if not page_title:
            self._log_skip(f"opensearch senza risultati per '{target}' ({lang})")
            return []

        cache_key = f"{lang}:Talk:{page_title.lower()}"
        if cache_key in self._fetched:
            self._log_skip(f"talk page '{page_title}' già raccolta (query: '{query}')")
            return []

        sections = self._fetch_talk_sections(page_title, lang)
        if not sections:
            self._log_skip(f"talk page per '{page_title}' vuota o inesistente ({lang})")
            return []

        self._fetched.add(cache_key)

        records = []
        talk_url_base = f"https://{lang}.wikipedia.org/wiki/Talk:{page_title.replace(' ', '_')}"

        for section in sections[:max_results]:
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
            response = http_get_with_retry(
                _API_URL.format(lang=lang),
                params=params,
                headers={"User-Agent": _USER_AGENT},
                timeout=10,
                source_id=self.source_id,
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
        """Scarica la talk page e la suddivide in sezioni di discussione."""
        talk_page = f"Talk:{page_title}"

        params = {
            "action": "parse",
            "page": talk_page,
            "prop": "sections|wikitext",
            "format": "json",
        }

        try:
            response = http_get_with_retry(
                _API_URL.format(lang=lang),
                params=params,
                headers={"User-Agent": _USER_AGENT},
                timeout=15,
                source_id=self.source_id,
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

        return self._split_sections(sections_meta, full_wikitext)

    def _split_sections(
        self,
        sections_meta: list[dict],
        full_wikitext: str,
    ) -> list[dict]:
        """Suddivide il wikitext in sezioni. Filtra sezioni vuote o solo-template."""
        header_pattern = re.compile(r"^(={2,})\s*(.+?)\s*\1\s*$", re.MULTILINE)
        headers = list(header_pattern.finditer(full_wikitext))

        result = []
        for i, meta in enumerate(sections_meta):
            title = meta.get("line", "")
            level = int(meta.get("level", 2))
            anchor = meta.get("anchor", title.replace(" ", "_"))

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
        """Rimuove template MediaWiki {{ ... }} anche multi-linea e annidati."""
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
        """Rimuove template, link e markup wiki per estrarre testo leggibile."""
        if not text:
            return ""
        cleaned = re.sub(r"~{3,5}", "", text)          # firma e timestamp
        cleaned = cls._strip_templates(cleaned)
        cleaned = re.sub(r"\[\[[^|\]]*\|([^\]]+)\]\]", r"\1", cleaned)
        cleaned = re.sub(r"\[\[([^\]]+)\]\]", r"\1", cleaned)
        cleaned = re.sub(r"\[https?://\S+\s+([^\]]+)\]", r"\1", cleaned)
        cleaned = re.sub(r"'{2,3}", "", cleaned)
        cleaned = re.sub(r"^[:*#]+\s?", "", cleaned, flags=re.MULTILINE)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned.strip()

    def reset_cache(self) -> None:
        """Svuota la cache dei titoli. Utile nei test."""
        self._fetched.clear()
