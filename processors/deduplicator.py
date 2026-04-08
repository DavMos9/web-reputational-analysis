"""
Deduplicator
Rimuove record duplicati secondo due livelli (data contract §7):

Livello 1 — URL identico (stesso contenuto, fonti diverse o stessa fonte)
Livello 2 — Titolo + dominio identici (stesso articolo con URL leggermente diverso,
            es. parametri di tracking, redirect, versioni AMP)
"""

import re
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode


# Parametri URL da ignorare nella comparazione (tracking, analytics, ecc.)
_TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
    "ref", "referer", "referrer", "source", "fbclid", "gclid", "msclkid",
    "mc_eid", "mc_cid", "_ga",
}


def _normalize_url_for_dedup(url: str) -> str:
    """
    Normalizza un URL per la comparazione:
    - lowercase scheme e host
    - rimuove parametri di tracking
    - rimuove trailing slash
    """
    if not url:
        return ""
    try:
        parsed = urlparse(url.lower().strip())
        # Rimuovi parametri di tracking dai query params
        params = parse_qs(parsed.query, keep_blank_values=True)
        clean_params = {k: v for k, v in params.items() if k not in _TRACKING_PARAMS}
        clean_query = urlencode(clean_params, doseq=True)
        normalized = urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path.rstrip("/"),
            parsed.params,
            clean_query,
            "",  # ignora fragment
        ))
        return normalized
    except Exception:
        return url.lower().strip()


def _normalize_title_for_dedup(title: str) -> str:
    """
    Normalizza un titolo per la comparazione:
    - lowercase
    - rimuove punteggiatura e spazi multipli
    """
    if not title:
        return ""
    t = title.lower()
    t = re.sub(r"[^\w\s]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def deduplicate(records: list[dict]) -> tuple[list[dict], int]:
    """
    Rimuove duplicati da una lista di record.

    Args:
        records: lista di record puliti e normalizzati

    Returns:
        Tupla (records_unici, numero_duplicati_rimossi)
    """
    seen_urls: set[str] = set()
    seen_title_domain: set[tuple[str, str]] = set()

    unique: list[dict] = []
    removed = 0

    for record in records:
        url_key = _normalize_url_for_dedup(record.get("url", ""))
        title_key = _normalize_title_for_dedup(record.get("title", ""))
        domain_key = (record.get("domain") or "").lower().strip()

        # Livello 1: URL identico
        if url_key and url_key in seen_urls:
            removed += 1
            continue

        # Livello 2: titolo + dominio identici
        td_key = (title_key, domain_key)
        if title_key and domain_key and td_key in seen_title_domain:
            removed += 1
            continue

        # Record unico: aggiungi e registra le chiavi
        if url_key:
            seen_urls.add(url_key)
        if title_key and domain_key:
            seen_title_domain.add(td_key)

        unique.append(record)

    return unique, removed
