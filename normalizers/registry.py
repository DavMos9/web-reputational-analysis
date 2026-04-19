"""
normalizers/registry.py

Registro centrale dei normalizer source-specific.

Design:
- Ogni normalizer si auto-registra chiamando register() al momento dell'import.
- Il dispatcher normalize() è completamente ignaro delle sorgenti conosciute:
  non contiene alcun if/elif né riferimento diretto ai moduli sorgente.
- Aggiungere una sorgente: creare normalizers/<source>.py e chiamare register()
  in fondo al file. Nessun altro file va modificato: l'auto-discovery in
  normalizers/__init__.py importa automaticamente ogni nuovo modulo.
  Questo modulo (registry.py) non va mai toccato.
- Rimuovere una sorgente: eliminare il file normalizers/<source>.py.
"""

from __future__ import annotations

import logging
from types import MappingProxyType
from typing import Callable, Mapping

from models import RawRecord, Record
from normalizers.utils import first_non_empty, to_date, to_domain, to_url

log = logging.getLogger(__name__)

NormalizerFn = Callable[[RawRecord], Record | None]

# Registro interno mutabile: solo register() può scriverci.
# Accesso esterno in sola lettura tramite REGISTRY (MappingProxyType).
_REGISTRY: dict[str, NormalizerFn] = {}

# View read-only esposta pubblicamente.
# MappingProxyType riflette dinamicamente il dict sottostante:
# le registrazioni successive a register() sono visibili, ma nessuno
# può mutare REGISTRY direttamente (TypeError su tentativi di assegnazione).
REGISTRY: Mapping[str, NormalizerFn] = MappingProxyType(_REGISTRY)

# Chiavi comuni per titolo/testo/url/data — usate dal fallback normalizer.
# L'ordine riflette la priorità: il primo valore non vuoto viene usato.
_TITLE_KEYS  = ("title", "headline", "name", "subject", "webTitle")
_TEXT_KEYS   = ("text", "body", "content", "description", "trailText", "selftext")
_URL_KEYS    = ("url", "link", "webUrl", "ap_id", "uri", "shortUrl")
_DATE_KEYS   = ("date", "published", "published_at", "webPublicationDate",
                "pubDate", "created_at", "createdUtc")


def _fallback_normalize(raw: RawRecord) -> Record | None:
    """
    Normalizer generico di fallback per sorgenti senza normalizer registrato.

    Tenta di estrarre titolo, testo, URL e data cercando chiavi comuni nel
    payload. Se nessun URL è recuperabile, restituisce None (record inutilizzabile).
    Non è preciso come un normalizer specifico, ma evita di scartare silenziosamente
    record che contengono informazioni utilizzabili.
    """
    p = raw.payload
    url = to_url(first_non_empty(*(str(p.get(k) or "") for k in _URL_KEYS)))
    if not url:
        log.warning(
            "[fallback] Sorgente '%s': URL non trovato nel payload — record scartato.",
            raw.source,
        )
        return None

    return Record(
        source=raw.source,
        title=first_non_empty(*(str(p.get(k) or "") for k in _TITLE_KEYS)),
        text=first_non_empty(*(str(p.get(k) or "") for k in _TEXT_KEYS)),
        date=to_date(
            first_non_empty(*(str(p.get(k) or "") for k in _DATE_KEYS)) or None
        ),
        url=url,
        query=raw.query,
        target=raw.target,
        author=first_non_empty(
            str(p.get("author") or ""),
            str(p.get("byline") or ""),
            str(p.get("creator") or ""),
        ),
        language=None,
        domain=to_domain(url),
        retrieved_at=raw.retrieved_at,
        raw_payload=p,
    )


def register(source_name: str, fn: NormalizerFn) -> None:
    """
    Registra una funzione normalizer per una sorgente.

    Chiamato tipicamente a livello di modulo in normalizers/<source>.py,
    così la registrazione avviene automaticamente all'import del modulo.

    Args:
        source_name: identificatore della sorgente (deve corrispondere
                     a RawRecord.source, es. "news", "gdelt").
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
        log.warning(
            "Sorgente sconosciuta '%s': nessun normalizer registrato, "
            "uso fallback generico.",
            raw.source,
        )
        fn = _fallback_normalize

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
