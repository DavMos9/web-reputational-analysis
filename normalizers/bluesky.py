"""normalizers/bluesky.py — Normalizer per AT Protocol /app.bsky.feed.searchPosts (source_id: "bluesky")."""

from __future__ import annotations

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, first_non_empty, to_int


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload
    record_data = p.get("record", {})
    author = p.get("author", {})

    # rkey = ultimo segmento dell'AT URI (at://did:.../post/<rkey>)
    uri = p.get("uri", "")
    handle = author.get("handle", "")
    rkey = uri.rsplit("/", 1)[-1] if uri else ""

    url = to_url(
        f"https://bsky.app/profile/{handle}/post/{rkey}"
        if handle and rkey else ""
    )

    return Record(
        source=raw.source,
        title="",
        text=first_non_empty(record_data.get("text")),
        date=to_date(record_data.get("createdAt") or p.get("indexedAt")),
        url=url,
        query=raw.query,
        target=raw.target,
        author=first_non_empty(author.get("displayName"), author.get("handle")),
        language=None,
        domain="bsky.app",
        retrieved_at=raw.retrieved_at,
        likes_count=to_int(p.get("likeCount")),
        comments_count=to_int(p.get("replyCount")),
        raw_payload=p,
    )


register("bluesky", _normalize)
