"""
pipeline/deduplicator.py — Deduplicazione a due livelli: URL canonico e titolo+dominio.

Fuzzy dedup (Jaccard/LSH) non implementato: O(n) con nessuna dipendenza è sufficiente.
Sorgenti parent-child (youtube_comments, wikitalk) saltano il livello titolo+dominio.
"""

from __future__ import annotations

import logging
import re
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

from models import Record

log = logging.getLogger(__name__)

_TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
    "ref", "referer", "referrer", "source", "fbclid", "gclid", "msclkid",
    "mc_eid", "mc_cid", "_ga",
}

# wikitalk: il fragment (#Section) identifica la conversazione; non va scartato.
_FRAGMENT_PRESERVING_SOURCES = frozenset({"wikitalk"})

# Sorgenti in cui il title è ereditato dal contenitore (youtube_comments: titolo video;
# wikitalk: titolo di sezione generico). Il dedup title+domain creerebbe falsi positivi.
_TITLE_DEDUP_EXCLUDED_SOURCES = frozenset({"youtube_comments", "wikitalk"})


def _canonical_url(url: str, *, preserve_fragment: bool = False) -> str:
    """Normalizza URL: lowercase, rimuove tracking params e trailing slash. Preserva fragment se richiesto."""
    if not url:
        return ""
    try:
        parsed = urlparse(url.lower().strip())
        params = parse_qs(parsed.query, keep_blank_values=True)
        clean_params = {k: v for k, v in params.items() if k not in _TRACKING_PARAMS}
        clean_query = urlencode(clean_params, doseq=True)
        return urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path.rstrip("/"),
            parsed.params,
            clean_query,
            parsed.fragment if preserve_fragment else "",
        ))
    except ValueError:
        return url.lower().strip()


def _canonical_title(title: str) -> str:
    """Normalizza titolo: lowercase, rimuove punteggiatura e spazi multipli."""
    if not title:
        return ""
    t = re.sub(r"[^\w\s]", "", title.lower())
    return re.sub(r"\s+", " ", t).strip()


def deduplicate(records: list[Record]) -> tuple[list[Record], int]:
    """Rimuove duplicati. Ritorna (record_unici, n_rimossi)."""
    seen_urls: set[str] = set()
    seen_title_domain: set[tuple[str, str]] = set()

    unique: list[Record] = []
    removed = 0

    for record in records:
        preserve_fragment = record.source in _FRAGMENT_PRESERVING_SOURCES
        url_key = _canonical_url(record.url, preserve_fragment=preserve_fragment)
        title_key = _canonical_title(record.title)
        domain_key = (record.domain or "").lower().strip()
        skip_title_dedup = record.source in _TITLE_DEDUP_EXCLUDED_SOURCES

        if url_key and url_key in seen_urls:
            removed += 1
            continue

        if not skip_title_dedup:
            td_key = (title_key, domain_key)
            if title_key and domain_key and td_key in seen_title_domain:
                removed += 1
                continue
            if title_key and domain_key:
                seen_title_domain.add(td_key)

        if url_key:
            seen_urls.add(url_key)

        unique.append(record)

    log.info("Deduplicazione: %d rimossi, %d unici.", removed, len(unique))
    return unique, removed
