"""
normalizers/news.py

Normalizer per NewsAPI (/v2/everything).

Payload raw atteso (singolo articolo):
    title, description, content, url, author, publishedAt, source.name
"""

from __future__ import annotations

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, to_domain, first_non_empty


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload
    url = to_url(p.get("url"))
    return Record(
        source=raw.source,
        title=first_non_empty(p.get("title")),
        text=first_non_empty(p.get("description"), p.get("content")),
        date=to_date(p.get("publishedAt")),
        url=url,
        query=raw.query,
        target=raw.target,
        author=first_non_empty(p.get("author")),
        language=first_non_empty(p.get("language")),
        domain=to_domain(url) or first_non_empty(
            p.get("source", {}).get("name") if isinstance(p.get("source"), dict) else None
        ),
        retrieved_at=raw.retrieved_at,
        raw_payload=p,
    )


register("news", _normalize)
