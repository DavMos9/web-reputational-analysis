"""normalizers/stackexchange.py — Normalizer per Stack Exchange API v2.3 (source_id: "stackexchange")."""

from __future__ import annotations

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, first_non_empty, to_int, strip_html


def _unix_to_date(timestamp: int | None) -> str | None:
    """Converte un unix timestamp in 'YYYY-MM-DD'."""
    if timestamp is None:
        return None
    try:
        from datetime import datetime, timezone
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except (ValueError, OSError, OverflowError):
        return None


def _build_url(p: dict) -> str:
    """Costruisce l'URL permalink basandosi su tipo e sito."""
    site = p.get("_site", "stackoverflow")
    item_type = p.get("item_type", "question")

    if item_type == "answer":
        answer_id = p.get("answer_id")
        if answer_id:
            return f"https://{site}.com/a/{answer_id}"
    # Per le domande (o fallback)
    question_id = p.get("question_id")
    if question_id:
        return f"https://{site}.com/questions/{question_id}"
    return ""


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload
    owner = p.get("owner", {})

    item_type = p.get("item_type", "question")
    title_raw = p.get("title", "")
    excerpt_raw = p.get("excerpt", "")

    title = strip_html(title_raw)
    excerpt = strip_html(excerpt_raw)

    if item_type == "answer" and title:
        title = f"[Answer] {title}"

    url = to_url(_build_url(p))
    site = p.get("_site", "stackoverflow")

    return Record(
        source=raw.source,
        title=title,
        text=excerpt,
        date=_unix_to_date(p.get("creation_date")),
        url=url,
        query=raw.query,
        target=raw.target,
        author=first_non_empty(owner.get("display_name")),
        language=None,
        domain=f"{site}.com",
        retrieved_at=raw.retrieved_at,
        likes_count=to_int(p.get("score")),
        raw_payload=p,
    )


register("stackexchange", _normalize)
