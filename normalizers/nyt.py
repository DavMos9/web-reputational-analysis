"""normalizers/nyt.py — Normalizer per NYT Article Search API (source_id: "nyt")."""

from __future__ import annotations

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, to_domain, first_non_empty


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload
    url = to_url(p.get("web_url"))

    byline = p.get("byline", {})
    author_raw = byline.get("original", "") if isinstance(byline, dict) else ""
    # Rimuove il prefisso "By " aggiunto dalla NYT nel campo byline.original
    author = author_raw.removeprefix("By ").strip() if author_raw else ""

    headline = p.get("headline", {})
    title = headline.get("main", "") if isinstance(headline, dict) else ""

    return Record(
        source=raw.source,
        title=first_non_empty(title),
        text=first_non_empty(p.get("abstract"), p.get("lead_paragraph")),
        date=to_date(p.get("pub_date")),
        url=url,
        query=raw.query,
        target=raw.target,
        author=first_non_empty(author),
        language=None,
        domain=to_domain(url) or "nytimes.com",
        retrieved_at=raw.retrieved_at,
        raw_payload=p,
    )


register("nyt", _normalize)
