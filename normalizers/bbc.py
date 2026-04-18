"""
normalizers/bbc.py

Normalizer per BBC News RSS (source_id: "bbc").

Payload raw atteso (prodotto da BbcCollector._parse_rss):
    title       (str | None): titolo dell'articolo
    link        (str | None): URL dell'articolo (permalink BBC)
    pubDate     (str | None): data pubblicazione RFC 2822
    description (str | None): abstract/snippet dell'articolo

Note:
    - `language` non è dichiarata nel feed RSS: viene rilevata dall'enricher
      tramite langdetect. BBC pubblica principalmente in inglese.
    - `domain` è estratto dall'URL (tipicamente "www.bbc.com" o "www.bbc.co.uk").
    - `author` non è disponibile nei feed RSS BBC: rimane None.
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
        author=None,             # non disponibile nei feed RSS BBC
        language=None,           # rilevata dall'enricher (BBC pubblica in più lingue)
        domain=to_domain(url),
        retrieved_at=raw.retrieved_at,
        raw_payload=p,
    )


register("bbc", _normalize)
