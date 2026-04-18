"""
pipeline/date_filter.py

Filtro temporale opzionale. Scarta i record con data anteriore a `since`.

Semantica delle date:
- `record.date` in formato "YYYY-MM-DD" (garantito dal normalizer).
- `since` in formato "YYYY-MM-DD".
- I record con `date=None` vengono MANTENUTI: non possiamo giudicarli,
  e scartarli penalizzerebbe fonti enciclopediche prive di data
  (Wikipedia, molte sezioni wikitalk, alcuni risultati Brave/search).
  Se in futuro servisse escluderli, va fatto con una flag esplicita.

La funzione è idempotente e non mutabile: restituisce una nuova lista.
Separata dal cleaner perché:
- è opzionale (il cleaner è sempre attivo),
- opera su una dimensione ortogonale (tempo, non qualità testuale),
- ha validazione dedicata del formato data.
"""

from __future__ import annotations

import logging
from datetime import date

from models import Record

log = logging.getLogger(__name__)


def parse_since(value: str) -> str:
    """
    Valida il formato 'YYYY-MM-DD' e restituisce la stessa stringa.

    Usata sia dal CLI (validazione argparse) sia dalla PipelineConfig
    per evitare di accettare stringhe mal-formattate che verrebbero
    silenziosamente ignorate a runtime.
    """
    try:
        date.fromisoformat(value)
    except (TypeError, ValueError) as e:
        raise ValueError(f"--since: formato atteso 'YYYY-MM-DD', ricevuto {value!r}") from e
    return value


def filter_by_date(records: list[Record], since: str | None) -> tuple[list[Record], int]:
    """
    Scarta i record con `date < since`.

    Args:
        records: lista di Record puliti.
        since:   stringa 'YYYY-MM-DD'. Se None, i record passano inalterati.

    Returns:
        Tupla (record_mantenuti, numero_scartati).
    """
    if not since:
        return records, 0

    since_date = date.fromisoformat(since)  # già validato da parse_since

    kept: list[Record] = []
    dropped = 0
    for r in records:
        # record senza data: manteniamo (si veda nota nel modulo)
        if not r.date:
            kept.append(r)
            continue
        try:
            record_date = date.fromisoformat(r.date)
        except (ValueError, TypeError):
            # data malformata nel record → manteniamo per principio di conservazione
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
