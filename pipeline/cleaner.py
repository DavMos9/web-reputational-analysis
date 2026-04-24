"""pipeline/cleaner.py — Pulizia testuale (clean) e filtro qualità (filter_quality) dei Record."""

from __future__ import annotations

import html
import logging
import re
import unicodedata
from dataclasses import replace

from config import MIN_TEXT_LENGTH, MIN_TITLE_LENGTH, BLOCKED_DOMAINS, MAX_TEXT_LENGTH
from models import Record

log = logging.getLogger(__name__)

_REQUIRED_STR = ("source", "title", "text", "url", "query", "target", "domain")
_OPTIONAL_STR = ("author", "language")  # stringa vuota → None


def _clean_str(value: str | None) -> str:
    """html.unescape + rimozione ctrl chars (preserva \t\n\r) + collasso spazi + NFC."""
    if value is None:
        return ""
    raw = html.unescape(str(value).strip())
    # C0/C1 control chars + U+2028 LINE SEPARATOR + U+2029 PARAGRAPH SEPARATOR.
    raw = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f\u2028\u2029]", "", raw)
    # \xa0 (non-breaking space di Google News) collassato come spazio normale.
    raw = re.sub(r"[ \t\xa0]+", " ", raw)
    return unicodedata.normalize("NFC", raw.strip())


def _truncate_text(text: str) -> str:
    """Tronca `text` a MAX_TEXT_LENGTH caratteri al confine di frase più vicino.

    Strategia a tre livelli (dalla più preferibile alla più grezza):
      1. Ultimo punto fermo/esclamativo/interrogativo entro il limite
         → testo semanticamente completo, ideale per NLP ed export.
      2. Ultimo spazio entro il limite (fallback se nessun '.' abbastanza vicino)
         → almeno non spezza una parola a metà.
      3. Taglio netto a MAX_TEXT_LENGTH (ultimo fallback per testi senza spazi).

    La soglia "abbastanza vicino" è MAX_TEXT_LENGTH // 2: evita di restituire
    uno spezzone troppo corto quando la prima frase è già molto lunga.
    Se MAX_TEXT_LENGTH è 0 la funzione è no-op.
    """
    if not MAX_TEXT_LENGTH or len(text) <= MAX_TEXT_LENGTH:
        return text
    truncated = text[:MAX_TEXT_LENGTH]
    floor = MAX_TEXT_LENGTH // 2  # posizione minima accettabile per il taglio

    # Livello 1 — confine di frase
    last_sentence = max(
        truncated.rfind("."),
        truncated.rfind("!"),
        truncated.rfind("?"),
    )
    if last_sentence > floor:
        return truncated[:last_sentence + 1]

    # Livello 2 — confine di parola
    last_space = truncated.rfind(" ")
    if last_space > floor:
        return truncated[:last_space]

    # Livello 3 — taglio netto (testi senza spazi o frasi lunghissime)
    return truncated


def clean(record: Record) -> Record:
    """Pulisce i valori testuali del Record. Il Record originale non viene modificato."""
    updates: dict = {}

    for f in _REQUIRED_STR:
        cleaned = _clean_str(getattr(record, f, ""))
        if f == "text":
            cleaned = _truncate_text(cleaned)
        if cleaned != getattr(record, f, ""):
            updates[f] = cleaned

    for f in _OPTIONAL_STR:
        val = _clean_str(getattr(record, f, None))
        result = val if val else None
        if result != getattr(record, f, None):
            updates[f] = result

    return replace(record, **updates) if updates else record


def clean_all(records: list[Record]) -> list[Record]:
    """Pulisce una lista di Record. Non filtra — restituisce sempre lo stesso numero."""
    return [clean(r) for r in records]


def filter_quality_all(records: list[Record]) -> list[Record]:
    """Variante di filter_quality senza contatore. Ritorna solo i record validi."""
    valid, _ = filter_quality(records)
    return valid


def filter_quality(records: list[Record]) -> tuple[list[Record], int]:
    """
    Scarta Record con text E title sotto soglia.

    Un record con solo titolo (es. GDELT) passa se il titolo è abbastanza lungo.
    Soglie in config.py (MIN_TEXT_LENGTH, MIN_TITLE_LENGTH).
    """
    valid: list[Record] = []
    skipped = 0

    for r in records:
        if r.domain and r.domain in BLOCKED_DOMAINS:
            log.warning(
                "[cleaner] Scartato per dominio bloccato [source=%s, domain=%s]: '%s'",
                r.source, r.domain, (r.title or "")[:80],
            )
            skipped += 1
            continue

        text_len  = len(r.text  or "")
        title_len = len(r.title or "")

        if text_len < MIN_TEXT_LENGTH and title_len < MIN_TITLE_LENGTH:
            log.debug(
                "[cleaner] Scartato per qualità [source=%s, title_len=%d, text_len=%d]: '%s'",
                r.source, title_len, text_len,
                (r.title or "")[:60],
            )
            skipped += 1
        else:
            valid.append(r)

    if skipped:
        log.info("[cleaner] Scartati %d record per qualità insufficiente.", skipped)

    return valid, skipped
