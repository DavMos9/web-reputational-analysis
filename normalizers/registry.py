"""
normalizers/registry.py

Registro centrale dei normalizer source-specific.

Design:
- Ogni normalizer si auto-registra chiamando register() al momento dell'import.
- Il dispatcher normalize() è completamente ignaro delle sorgenti conosciute:
  non contiene alcun if/elif né riferimento diretto ai moduli sorgente.
- Aggiungere una sorgente: creare normalizers/<source>.py + aggiungere l'import
  in normalizers/__init__.py. Il registry non va mai modificato.
- Rimuovere una sorgente: rimuovere l'import in normalizers/__init__.py.
"""

from __future__ import annotations

import logging
from typing import Callable

from models import RawRecord, Record

log = logging.getLogger(__name__)

# Tipo canonico per una funzione normalizer.
# Prende un RawRecord (grezzo), restituisce Record normalizzato o None.
NormalizerFn = Callable[[RawRecord], Record | None]

# Registro interno: source_id → funzione normalizer
_REGISTRY: dict[str, NormalizerFn] = {}


def register(source_name: str, fn: NormalizerFn) -> None:
    """
    Registra una funzione normalizer per una sorgente.

    Chiamato tipicamente a livello di modulo in normalizers/<source>.py,
    così la registrazione avviene automaticamente all'import del modulo.

    Args:
        source_name: identificatore della sorgente (deve corrispondere
                     a RawRecord.source, es. "news", "reddit").
        fn:          funzione (RawRecord) → Record | None.
    """
    if source_name in _REGISTRY:
        log.warning(
            "Normalizer per '%s' già registrato — sovrascrittura.",
            source_name,
        )
    _REGISTRY[source_name] = fn
    log.debug("Normalizer registrato per sorgente: '%s'", source_name)


def registered_sources() -> list[str]:
    """Restituisce la lista delle sorgenti con un normalizer registrato."""
    return list(_REGISTRY.keys())


def normalize(raw: RawRecord) -> Record | None:
    """
    Normalizza un RawRecord nel formato Record canonico.

    Dispatcha al normalizer registrato per raw.source.
    Gestisce internamente tutti gli errori: non propaga mai eccezioni.

    Args:
        raw: RawRecord prodotto da un collector.

    Returns:
        Record normalizzato, oppure None se:
        - la sorgente non ha un normalizer registrato
        - il normalizer solleva ValueError (campo obbligatorio mancante)
        - l'URL risultante è vuoto (record non linkabile → inutilizzabile)
        - qualsiasi errore imprevisto durante la normalizzazione
    """
    fn = _REGISTRY.get(raw.source)
    if fn is None:
        log.warning("Sorgente sconosciuta '%s': record scartato.", raw.source)
        return None

    try:
        record = fn(raw)
    except ValueError as e:
        # ValueError da Record.__post_init__: campo obbligatorio non valido
        log.warning(
            "[%s] Record scartato: %s (query='%s')",
            raw.source, e, raw.query,
        )
        return None
    except Exception as e:
        log.error(
            "Errore normalizzando record da '%s' (query='%s'): %s",
            raw.source, raw.query, e,
        )
        return None

    if record is None:
        return None

    # Un record senza URL è inutilizzabile in analisi reputazionale
    if not record.url:
        log.warning(
            "[%s] Record scartato: URL mancante (query='%s', title='%s')",
            raw.source,
            raw.query,
            str(raw.payload.get("title", ""))[:60],
        )
        return None

    return record


def normalize_all(raws: list[RawRecord]) -> list[Record]:
    """
    Normalizza una lista di RawRecord, scartando silenziosamente i None.

    Args:
        raws: lista di RawRecord prodotti dai collector.

    Returns:
        Lista di Record validi nello stesso ordine dell'input (esclusi i None).
    """
    results: list[Record] = []
    for raw in raws:
        record = normalize(raw)
        if record is not None:
            results.append(record)

    log.info("Normalizzati %d/%d record.", len(results), len(raws))
    return results
