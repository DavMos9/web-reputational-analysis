"""
normalizers/ansa.py

Normalizer per ANSA RSS (source_id: "ansa").

Payload raw atteso (prodotto da AnsaCollector._parse_rss):
    title       (str | None): titolo dell'articolo
    link        (str | None): URL dell'articolo (permalink ANSA)
    pubDate     (str | None): data pubblicazione RFC 2822
    description (str | None): abstract/snippet dell'articolo

Note:
    - `language` è impostata direttamente a "it": ANSA è un'agenzia italiana
      e tutti i feed sono in italiano.
    - `domain` è estratto dall'URL (tipicamente "www.ansa.it").
    - `author` non è disponibile nei feed RSS ANSA: rimane None.
    - `description` è solitamente breve (1-2 frasi). Il titolo è il campo
      portante per il sentiment analysis.
"""

from __future__ import annotations

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, to_domain, first_non_empty, strip_html


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload

    url = to_url(p.get("link"))
    text = strip_html(p.get("description") or "")

    return Record(
        source=raw.source,
        title=first_non_empty(p.get("title")),
        text=text,
        date=to_date(p.get("pubDate")),
        url=url,
        query=raw.query,
        target=raw.target,
        author=None,             # non disponibile nei feed RSS ANSA
        language="it",           # agenzia italiana: lingua certa
        domain=to_domain(url),
        retrieved_at=raw.retrieved_at,
        raw_payload=p,
    )


register("ansa", _normalize)
