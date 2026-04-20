"""pipeline/language_filter.py — Filtro per lingua dopo l'enrichment.

Record con language=None vengono sempre mantenuti: la lingua non è rilevabile
per alcune fonti (Wikipedia, wikitalk, fonte senza testo sufficiente) e scartarli
penalizzerebbe ingiustamente contenuto potenzialmente rilevante.

I codici lingua attesi sono ISO 639-1 lowercase (es. 'en', 'it', 'fr').
"""

from __future__ import annotations

import logging

from models import Record

log = logging.getLogger(__name__)


def filter_by_language(
    records: list[Record],
    languages: list[str] | None,
) -> tuple[list[Record], int]:
    """
    Mantiene solo i record la cui lingua è in `languages`.
    Se `languages` è None o vuota non applica alcun filtro.
    Record con language=None vengono sempre mantenuti.

    Returns:
        (record_mantenuti, n_scartati)
    """
    if not languages:
        return records, 0

    allowed = {lang.strip().lower() for lang in languages}

    kept: list[Record] = []
    dropped = 0

    for r in records:
        if r.language is None:
            kept.append(r)
            continue
        if r.language.lower() in allowed:
            kept.append(r)
        else:
            dropped += 1

    if dropped:
        log.info(
            "[language_filter] Scartati %d record fuori dalle lingue ammesse %s.",
            dropped, sorted(allowed),
        )

    return kept, dropped
