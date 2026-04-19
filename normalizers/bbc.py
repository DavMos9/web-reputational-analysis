"""normalizers/bbc.py — Normalizer per BBC News RSS (source_id: "bbc")."""

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
        author=None,
        language=None,
        domain=to_domain(url),
        retrieved_at=raw.retrieved_at,
        raw_payload=p,
    )


register("bbc", _normalize)
