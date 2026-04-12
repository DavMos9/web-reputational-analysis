"""
pipeline/cleaner.py

Due responsabilità distinte, due funzioni distinte:

1. clean() / clean_all()
   Pulisce i valori testuali di un Record:
   - strip e normalizzazione Unicode NFC
   - campi stringa obbligatori: mai None
   - campi stringa opzionali: stringa vuota → None

2. filter_quality() / filter_quality_all()
   Scarta Record che non soddisfano la soglia minima di qualità.
   Un record viene scartato solo se ENTRAMBI title e text sono sotto soglia
   (un articolo con solo titolo, come GDELT, è ancora valido).
   Le soglie sono lette da config.py — non sono hardcoded qui.

Separare clean e filter permette di:
- testare le due responsabilità indipendentemente
- usare solo clean senza filter (es. in unit test di normalizer)
- applicare soglie diverse per sorgente in futuro senza toccare clean
"""

from __future__ import annotations

import logging
import unicodedata
from dataclasses import replace

from config import MIN_TEXT_LENGTH, MIN_TITLE_LENGTH
from models import Record

log = logging.getLogger(__name__)

# Campi stringa che non possono essere None nel Record finale
_REQUIRED_STR = ("source", "title", "text", "url", "query", "target", "domain")

# Campi stringa opzionali: "" → None
_OPTIONAL_STR = ("author", "language")


def _clean_str(value: str | None) -> str:
    """Strip + normalizzazione Unicode NFC."""
    if value is None:
        return ""
    return unicodedata.normalize("NFC", str(value).strip())


# ---------------------------------------------------------------------------
# Pulizia testuale
# ---------------------------------------------------------------------------

def clean(record: Record) -> Record:
    """
    Pulisce i valori testuali di un singolo Record.

    Usa dataclasses.replace per produrre un nuovo Record immutato
    (il Record originale non viene modificato).
    """
    updates: dict = {}

    for f in _REQUIRED_STR:
        cleaned = _clean_str(getattr(record, f, ""))
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


# ---------------------------------------------------------------------------
# Filtro qualità
# ---------------------------------------------------------------------------

def filter_quality(records: list[Record]) -> tuple[list[Record], int]:
    """
    Scarta i Record che non soddisfano la soglia minima di qualità.

    Criterio di scarto: text troppo corto E title troppo corto.
    Un record con solo il titolo (es. GDELT, senza body) viene conservato
    se il titolo è sufficientemente lungo.

    Le soglie MIN_TEXT_LENGTH e MIN_TITLE_LENGTH vengono lette da config.py.

    Args:
        records: lista di Record già puliti.

    Returns:
        Tupla (record_validi, numero_scartati).
    """
    valid: list[Record] = []
    skipped = 0

    for r in records:
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
