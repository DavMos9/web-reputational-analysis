"""normalizers/registry.py — Dispatcher centrale. NON modificare."""

from __future__ import annotations

import logging
from types import MappingProxyType
from typing import Callable, Mapping

from models import RawRecord, Record
from normalizers.utils import first_non_empty, to_date, to_domain, to_url

log = logging.getLogger(__name__)

NormalizerFn = Callable[[RawRecord], Record | None]

_REGISTRY: dict[str, NormalizerFn] = {}
# MappingProxyType: view read-only che riflette dinamicamente _REGISTRY.
REGISTRY: Mapping[str, NormalizerFn] = MappingProxyType(_REGISTRY)

# Chiavi comuni per il fallback normalizer (ordine = priorità).
_TITLE_KEYS  = ("title", "headline", "name", "subject", "webTitle")
_TEXT_KEYS   = ("text", "body", "content", "description", "trailText", "selftext")
_URL_KEYS    = ("url", "link", "webUrl", "ap_id", "uri", "shortUrl")
_DATE_KEYS   = ("date", "published", "published_at", "webPublicationDate",
                "pubDate", "created_at", "createdUtc")


def _fallback_normalize(raw: RawRecord) -> Record | None:
    """Fallback per sorgenti senza normalizer: tenta di estrarre campi da chiavi comuni."""
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
    """Registra un normalizer per una sorgente. Chiamato a livello di modulo in normalizers/<source>.py."""
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


def _extract_topic(target: str, query: str) -> str:
    """Estrae il topic dalla query composta — inverso di build_query() in main.py.

    Se la query inizia con '{target} ' (case-insensitive), restituisce la parte
    rimanente (es. "Elon Musk Tesla" → "Tesla").
    Altrimenti restituisce la query intera — caso passthrough di build_query
    (es. target="Emmanuel Macron", query="Macron" → "Macron").
    """
    prefix = target + " "
    if query.lower().startswith(prefix.lower()):
        return query[len(prefix):]
    return query


def normalize(raw: RawRecord) -> Record | None:
    """Dispatcha al normalizer registrato. Non propaga eccezioni. None se record non normalizzabile."""
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

    if not record.url:
        log.warning(
            "[%s] Record scartato: URL mancante (query='%s', title='%s')",
            raw.source,
            raw.query,
            str(raw.payload.get("title", ""))[:60],
        )
        return None

    # Calcola il topic in un unico posto: tutti i record passano da qui.
    record.topic = _extract_topic(raw.target, raw.query)

    return record


def normalize_all(raws: list[RawRecord]) -> list[Record]:
    """Normalizza una lista di RawRecord, scartando i None."""
    results: list[Record] = []
    for raw in raws:
        record = normalize(raw)
        if record is not None:
            results.append(record)

    log.info("Normalizzati %d/%d record.", len(results), len(raws))
    return results
