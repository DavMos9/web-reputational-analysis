"""
pipeline/cleaner.py

Pulisce i valori testuali di un Record:
- strip e normalizzazione Unicode NFC
- campi stringa obbligatori: mai None
- campi stringa opzionali: stringa vuota → None
"""

from __future__ import annotations

import unicodedata
from dataclasses import replace

from models import Record

# Campi stringa che non possono essere None nel Record finale
_REQUIRED = ("source", "title", "text", "url", "query", "target", "domain")

# Campi stringa opzionali: "" → None
_OPTIONAL = ("author", "language")


def _clean_str(value: str | None) -> str:
    """Strip + normalizzazione Unicode NFC."""
    if value is None:
        return ""
    return unicodedata.normalize("NFC", str(value).strip())


def clean(record: Record) -> Record:
    """
    Pulisce un singolo Record.

    Usa dataclasses.replace per produrre un nuovo Record immutato
    (il Record originale non viene modificato).
    """
    updates: dict = {}

    for field in _REQUIRED:
        cleaned = _clean_str(getattr(record, field, ""))
        if cleaned != getattr(record, field, ""):
            updates[field] = cleaned

    for field in _OPTIONAL:
        val = _clean_str(getattr(record, field, None))
        result = val if val else None
        if result != getattr(record, field, None):
            updates[field] = result

    return replace(record, **updates) if updates else record


def clean_all(records: list[Record]) -> list[Record]:
    """Pulisce una lista di Record."""
    return [clean(r) for r in records]
