"""normalizers/guardian.py — Normalizer per The Guardian Open Platform (source_id: "guardian")."""

from __future__ import annotations

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, to_domain, first_non_empty


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload
    fields = p.get("fields", {})
    url = to_url(fields.get("shortUrl") or p.get("webUrl"))
    return Record(
        source=raw.source,
        title=first_non_empty(fields.get("headline"), p.get("webTitle")),
        text=first_non_empty(fields.get("bodyText"), fields.get("trailText")),
        date=to_date(p.get("webPublicationDate")),
        url=url,
        query=raw.query,
        target=raw.target,
        author=first_non_empty(fields.get("byline")),
        language=None,
        domain=to_domain(url) or "theguardian.com",
        retrieved_at=raw.retrieved_at,
        raw_payload=p,
    )


register("guardian", _normalize)
