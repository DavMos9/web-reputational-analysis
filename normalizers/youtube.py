"""normalizers/youtube.py — Normalizer per YouTube Data API v3 (source_id: "youtube")."""

from __future__ import annotations

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, first_non_empty, to_int


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload
    snippet = p.get("snippet", {})
    stats = p.get("statistics", {})
    video_id = p.get("id", {}).get("videoId") if isinstance(p.get("id"), dict) else None
    url = to_url(f"https://www.youtube.com/watch?v={video_id}") if video_id else ""

    return Record(
        source=raw.source,
        title=first_non_empty(snippet.get("title")),
        text=first_non_empty(snippet.get("description")),
        date=to_date(snippet.get("publishedAt")),
        url=url,
        query=raw.query,
        target=raw.target,
        author=first_non_empty(snippet.get("channelTitle")),
        domain="youtube.com",
        retrieved_at=raw.retrieved_at,
        views_count=to_int(stats.get("viewCount")),
        likes_count=to_int(stats.get("likeCount")),
        comments_count=to_int(stats.get("commentCount")),
        raw_payload=p,
    )


register("youtube", _normalize)
