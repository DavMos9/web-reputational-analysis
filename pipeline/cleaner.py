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

import html
import logging
import re
import unicodedata
from dataclasses import replace

from config import MIN_TEXT_LENGTH, MIN_TITLE_LENGTH, BLOCKED_DOMAINS
from models import Record

log = logging.getLogger(__name__)

# Campi stringa che non possono essere None nel Record finale
_REQUIRED_STR = ("source", "title", "text", "url", "query", "target", "domain")

# Campi stringa opzionali: "" → None
_OPTIONAL_STR = ("author", "language")


def _clean_str(value: str | None) -> str:
    """Strip, decodifica HTML entities, normalizza whitespace orizzontale, NFC.

    Operazioni in ordine:
    1. html.unescape(): &quot;→"  &amp;→&  &nbsp;→\xa0  &#N;→char  ecc.
    2. Rimozione caratteri di controllo Unicode: \x00–\x08, \x0b, \x0c,
       \x0e–\x1f, \x7f–\x9f. Questi possono provenire da alcune API (null
       bytes, BOM residui, ecc.) e corromperebbero silenziosamente il JSON/CSV
       di output. \t (\x09), \n (\x0a) e \r (\x0d) vengono preservati
       perché sono whitespace legittimo nel contenuto testuale.
    3. Collasso whitespace orizzontale: ogni sequenza di spazi, tab o \xa0
       (non-breaking space, usato da Google News come separatore) diventa
       un singolo spazio. I newline (\n, \r) NON vengono toccati: sono
       contenuto reale nei post social, commenti e testo Wikipedia.
    4. strip() finale per rimuovere spazi ai bordi.
    5. unicodedata.normalize("NFC"): forma canonica composta.
    """
    if value is None:
        return ""
    raw = html.unescape(str(value).strip())
    # Rimuove caratteri di controllo (preserva \t=\x09, \n=\x0a, \r=\x0d)
    raw = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]", "", raw)
    raw = re.sub(r"[ \t\xa0]+", " ", raw)
    return unicodedata.normalize("NFC", raw.strip())


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


def filter_quality_all(records: list[Record]) -> list[Record]:
    """
    Scarta i Record che non soddisfano la soglia minima di qualità.

    Variante semplificata di filter_quality() che restituisce solo la lista
    dei record validi, senza il contatore dei rimossi. Comoda nei contesti
    (test, notebook) in cui il numero di scartati non interessa.

    Args:
        records: lista di Record già puliti.

    Returns:
        Lista di Record validi (stessa semantica di filter_quality()[0]).
    """
    valid, _ = filter_quality(records)
    return valid


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
        # --- Filtro 1: dominio bloccato (consent pages, redirect gate, ecc.) ---
        if r.domain and r.domain in BLOCKED_DOMAINS:
            log.warning(
                "[cleaner] Scartato per dominio bloccato [source=%s, domain=%s]: '%s'",
                r.source, r.domain, (r.title or "")[:80],
            )
            skipped += 1
            continue

        # --- Filtro 2: qualità del contenuto (testo e titolo sotto soglia) ---
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
