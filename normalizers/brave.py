"""
normalizers/brave.py

Normalizer per Brave Search API (web search).

Payload raw atteso (subset rilevante):
    title          (str):  titolo del risultato
    url            (str):  URL canonico
    description    (str):  snippet principale
    extra_snippets (list): snippet aggiuntivi, opzionali — concatenati al text
                           quando la description è troppo breve
    page_age       (str):  data ISO 8601 di pubblicazione (opzionale)
    age            (str):  data relativa "3 days ago" — ignorata, non riproducibile
    language       (str):  codice lingua ISO 639-1, quando rilevato da Brave
    meta_url       (dict): contiene `hostname` per il dominio

Note:
    - `date` può essere None: Brave non sempre restituisce `page_age`.
      In quel caso il record resta valido ma contribuisce meno al recency_score
      a valle. La data relativa `age` viene ignorata deliberatamente: parsarla
      richiederebbe un'ora di riferimento e introdurrebbe approssimazioni
      inaccettabili in una pipeline di analisi riproducibile.
    - `text` viene arricchito con `extra_snippets` quando presenti e la
      description è corta: aumenta la probabilità di superare MIN_TEXT_LENGTH
      nel cleaner senza alterare la semantica del risultato.
"""

from __future__ import annotations

from typing import Iterable

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, to_domain, first_non_empty, strip_html

# Soglia sotto la quale proviamo ad arricchire il text con gli extra_snippets.
# Coerente con MIN_TEXT_LENGTH di config.py (30 caratteri) con margine.
_MIN_TEXT_FOR_SNIPPET_MERGE = 80


def _compose_text(description: str, extra_snippets: Iterable[str] | None) -> str:
    """
    Costruisce il campo text del record.

    Brave restituisce gli snippet con markup di evidenziazione (<strong>...</strong>):
    viene rimosso qui per evitare che finisca nel modello di sentiment o negli export.

    Se la description (pulita) è sufficientemente lunga, la restituisce così com'è.
    Altrimenti concatena description + extra_snippets (puliti), separati da spazi,
    per aumentare la copertura informativa senza cambiare semantica.
    """
    desc = strip_html(description)
    if len(desc) >= _MIN_TEXT_FOR_SNIPPET_MERGE or not extra_snippets:
        return desc

    parts: list[str] = [desc] if desc else []
    for snippet in extra_snippets:
        if isinstance(snippet, str):
            s = strip_html(snippet)
            if s:
                parts.append(s)
    return " ".join(parts)


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload
    url = to_url(p.get("url"))

    meta_url = p.get("meta_url") or {}
    hostname = meta_url.get("hostname") if isinstance(meta_url, dict) else None

    language = p.get("language")
    language = str(language).lower() if isinstance(language, str) and language.strip() else None

    return Record(
        source=raw.source,
        title=strip_html(first_non_empty(p.get("title"))),
        text=_compose_text(p.get("description", ""), p.get("extra_snippets")),
        date=to_date(p.get("page_age")),
        url=url,
        query=raw.query,
        target=raw.target,
        author=None,                # Brave non espone l'autore in modo strutturato
        language=language,          # None se non rilevato; enricher completa dopo
        domain=hostname or to_domain(url),
        retrieved_at=raw.retrieved_at,
        raw_payload=p,
    )


register("brave", _normalize)
