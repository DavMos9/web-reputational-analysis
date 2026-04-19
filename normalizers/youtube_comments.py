"""normalizers/youtube_comments.py — Normalizer per commenti YouTube (source_id: "youtube_comments")."""

from __future__ import annotations

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, first_non_empty, to_int


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload
    comment = p.get("comment", {})
    snippet = comment.get("snippet", {})
    video_id = p.get("video_id", "")
    comment_id = comment.get("id", "")

    url = to_url(
        f"https://www.youtube.com/watch?v={video_id}&lc={comment_id}"
        if video_id and comment_id else ""
    )

    return Record(
        source=raw.source,
        title=first_non_empty(p.get("video_title")),
        text=first_non_empty(snippet.get("textDisplay")),
        date=to_date(snippet.get("publishedAt")),
        url=url,
        query=raw.query,
        target=raw.target,
        author=first_non_empty(snippet.get("authorDisplayName")),
        language=None,
        domain="youtube.com",
        retrieved_at=raw.retrieved_at,
        likes_count=to_int(snippet.get("likeCount")),
        comments_count=to_int(p.get("reply_count")),
        raw_payload=p,
    )


register("youtube_comments", _normalize)
