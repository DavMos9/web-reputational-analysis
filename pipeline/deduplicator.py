"""
pipeline/deduplicator.py

Rimuove Record duplicati su due livelli:

Livello 1 — URL identico (stesso articolo, sorgenti o URL tracking diversi)
Livello 2 — Titolo + dominio identici (stesso articolo con URL lievemente diverso:
            parametri di tracking, redirect, versioni AMP, ecc.)

Il livello 2 viene SALTATO per sorgenti "parent-child" dove molti record
distinti condividono lo stesso `title` del contenitore (es. commenti YouTube,
che ereditano il titolo del video). Per quelle fonti l'identità va cercata
altrove (URL distinto, testo del singolo commento).

--- Approcci non implementati ---
Fuzzy deduplication (similarità Jaccard/coseno sul testo) è stata valutata
ma non implementata: i due livelli URL + titolo/dominio coprono la stragrande
maggioranza dei duplicati pratici con costo O(n) e nessuna dipendenza esterna.
Fuzzy dedup richiederebbe O(n²) confronti o indicizzazione LSH, e introduce
falsi positivi su record semanticamente distinti ma lessicalmente simili
(es. aggiornamenti di breaking news). Da valutare se il dataset cresce
significativamente o se emergono duplicati residui sistematici.
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

# Sorgenti per cui il fragment URL è semanticamente rilevante e NON va scartato
# nella canonicalizzazione. Esempio: Wikipedia Talk Pages, dove ogni sezione
# (#Section_title) è una conversazione distinta — tutte sulla stessa pagina,
# ma ognuna è un record autonomo.
_FRAGMENT_PRESERVING_SOURCES = frozenset({"wikitalk"})

# Sorgenti "parent-child" dove molti record distinti condividono lo stesso
# `title` (ereditato dal contenitore): per loro il dedup di livello 2
# (title + domain) collasserebbe erroneamente tutti i figli in uno solo.
# Per queste fonti applichiamo solo il livello 1 (URL canonico).
#   - youtube_comments: ogni commento ha title = titolo del video parent;
#     l'identità è nel query-param `lc=<comment_id>` dell'URL.
_TITLE_DEDUP_EXCLUDED_SOURCES = frozenset({"youtube_comments"})


def _canonical_url(url: str, *, preserve_fragment: bool = False) -> str:
    """
    Normalizza un URL per la comparazione:
    - lowercase scheme e host
    - rimuove parametri di tracking
    - rimuove trailing slash
    - scarta il fragment, eccetto quando `preserve_fragment=True` (es. wikitalk:
      `#Section` identifica la conversazione).
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
            parsed.fragment if preserve_fragment else "",
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
        preserve_fragment = record.source in _FRAGMENT_PRESERVING_SOURCES
        url_key = _canonical_url(record.url, preserve_fragment=preserve_fragment)
        title_key = _canonical_title(record.title)
        domain_key = (record.domain or "").lower().strip()
        skip_title_dedup = record.source in _TITLE_DEDUP_EXCLUDED_SOURCES

        # Livello 1: URL canonico identico
        if url_key and url_key in seen_urls:
            removed += 1
            continue

        # Livello 2: titolo + dominio identici (skippato per sorgenti parent-child).
        # Per quelle fonti il title non è discriminante — più record distinti
        # condividono lo stesso titolo, quindi non va né controllato né indicizzato.
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
