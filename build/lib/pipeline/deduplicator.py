"""
pipeline/deduplicator.py

Rimuove Record duplicati su due livelli:

Livello 1 — URL identico (stesso articolo, sorgenti o URL tracking diversi)
Livello 2 — Titolo + dominio identici (stesso articolo con URL lievemente diverso:
            parametri di tracking, redirect, versioni AMP, ecc.)
"""

from __future__ import annotations

import logging
import re
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

from models import Record

log = logging.getLogger(__name__)

# Parametri di tracking da ignorare nella comparazione degli URL
_TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
    "ref", "referer", "referrer", "source", "fbclid", "gclid", "msclkid",
    "mc_eid", "mc_cid", "_ga",
}


def _canonical_url(url: str) -> str:
    """
    Normalizza un URL per la comparazione:
    - lowercase scheme e host
    - rimuove parametri di tracking
    - rimuove trailing slash e fragment
    """
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
            "",  # ignora fragment
        ))
    except Exception:
        return url.lower().strip()


def _canonical_title(title: str) -> str:
    """
    Normalizza un titolo per la comparazione:
    - lowercase
    - rimuove punteggiatura e spazi multipli
    """
    if not title:
        return ""
    t = re.sub(r"[^\w\s]", "", title.lower())
    return re.sub(r"\s+", " ", t).strip()


def deduplicate(records: list[Record]) -> tuple[list[Record], int]:
    """
    Rimuove duplicati da una lista di Record.

    Args:
        records: lista di Record puliti e normalizzati.

    Returns:
        Tupla (record_unici, numero_duplicati_rimossi).
    """
    seen_urls: set[str] = set()
    seen_title_domain: set[tuple[str, str]] = set()

    unique: list[Record] = []
    removed = 0

    for record in records:
        url_key = _canonical_url(record.url)
        title_key = _canonical_title(record.title)
        domain_key = (record.domain or "").lower().strip()

        # Livello 1: URL canonico identico
        if url_key and url_key in seen_urls:
            removed += 1
            continue

        # Livello 2: titolo + dominio identici
        td_key = (title_key, domain_key)
        if title_key and domain_key and td_key in seen_title_domain:
            removed += 1
            continue

        if url_key:
            seen_urls.add(url_key)
        if title_key and domain_key:
            seen_title_domain.add(td_key)

        unique.append(record)

    log.info("Deduplicazione: %d rimossi, %d unici.", removed, len(unique))
    return unique, removed
