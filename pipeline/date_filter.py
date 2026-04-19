"""pipeline/date_filter.py — Filtro temporale opzionale per i Record.

Record con date=None vengono sempre mantenuti: scartarli penalizzerebbe
fonti senza data (Wikipedia, wikitalk, alcuni risultati Brave/search).
"""

from __future__ import annotations

import logging
from datetime import date

from models import Record

log = logging.getLogger(__name__)


def parse_since(value: str) -> str:
    """Valida il formato 'YYYY-MM-DD' e restituisce la stessa stringa. Lancia ValueError se invalido."""
    try:
        date.fromisoformat(value)
    except (TypeError, ValueError) as e:
        raise ValueError(f"--since: formato atteso 'YYYY-MM-DD', ricevuto {value!r}") from e
    return value


def filter_by_date(records: list[Record], since: str | None) -> tuple[list[Record], int]:
    """Scarta record con date < since. Ritorna (record_mantenuti, n_scartati)."""
    if not since:
        return records, 0

    since_date = date.fromisoformat(since)

    kept: list[Record] = []
    dropped = 0
    for r in records:
        if not r.date:
            kept.append(r)
            continue
        try:
            record_date = date.fromisoformat(r.date)
        except (ValueError, TypeError):
            log.debug(
                "[date_filter] Data malformata '%s' in record [source=%s], mantenuto.",
                r.date, r.source,
            )
            kept.append(r)
            continue
        if record_date >= since_date:
            kept.append(r)
        else:
            dropped += 1

    if dropped:
        log.info("[date_filter] Scartati %d record anteriori a %s.", dropped, since)
    return kept, dropped
