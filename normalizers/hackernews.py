"""normalizers/hackernews.py — Normalizer per Hacker News Algolia API (source_id: "hackernews").

Per Ask HN / Show HN (senza url esterno), URL = permalink HN sull'objectID.
"""

from __future__ import annotations

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, to_domain, first_non_empty, strip_html, to_int


_HN_BASE = "https://news.ycombinator.com/item?id="


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload

    object_id = str(p.get("objectID", ""))

    raw_url = p.get("url")
    url = to_url(raw_url) if raw_url else f"{_HN_BASE}{object_id}"
    text = strip_html(p.get("story_text") or "")

    return Record(
        source=raw.source,
        title=first_non_empty(p.get("title")),
        text=text,
        date=to_date(p.get("created_at")),
        url=url,
        query=raw.query,
        target=raw.target,
        author=p.get("author"),
        language=None,
        domain=to_domain(url),
        retrieved_at=raw.retrieved_at,
        views_count=None,
        likes_count=to_int(p.get("points")),
        comments_count=to_int(p.get("num_comments")),
        raw_payload=p,
    )


register("hackernews", _normalize)
