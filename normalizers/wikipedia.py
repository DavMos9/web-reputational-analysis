"""
normalizers/wikipedia.py

Normalizer per Wikipedia (WikipediaCollector).

Payload raw atteso:
    title, summary, text, url, language
Nota: Wikipedia non ha data di pubblicazione.
"""

from __future__ import annotations

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_url, first_non_empty


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload
    url = to_url(p.get("url"))
    return Record(
        source=raw.source,
        title=first_non_empty(p.get("title")),
        text=first_non_empty(p.get("summary"), p.get("text")),
        date=None,  # Wikipedia non ha data di pubblicazione
        url=url,
        query=raw.query,
        target=raw.target,
        language=first_non_empty(p.get("language")),
        domain="wikipedia.org",
        retrieved_at=raw.retrieved_at,
        raw_payload=p,
    )


register("wikipedia", _normalize)
