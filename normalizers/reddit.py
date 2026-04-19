"""normalizers/reddit.py — Normalizer per Reddit JSON API (source_id: "reddit").

URL canonico = permalink Reddit (non link esterno): garantisce unicità nella deduplicazione.
"""

from __future__ import annotations

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, to_domain, first_non_empty, strip_html, to_int

_REDDIT_BASE = "https://www.reddit.com"
_REMOVED_TEXTS = {"[removed]", "[deleted]", ""}


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload

    permalink = p.get("permalink", "")
    url = to_url(f"{_REDDIT_BASE}{permalink}") if permalink else ""

    selftext = str(p.get("selftext") or "").strip()
    text = strip_html(selftext) if selftext not in _REMOVED_TEXTS else ""

    author_raw = p.get("author")
    subreddit   = p.get("subreddit")
    if author_raw and subreddit:
        author = f"{author_raw} [r/{subreddit}]"
    elif author_raw:
        author = str(author_raw)
    else:
        author = None

    # created_utc è float (epoch seconds); dateutil non gestisce float, convertiamo prima.
    created_utc = p.get("created_utc")
    date_str: str | None = None
    if created_utc is not None:
        try:
            from datetime import datetime, timezone
            date_str = datetime.fromtimestamp(float(created_utc), tz=timezone.utc).strftime("%Y-%m-%d")
        except (ValueError, OSError, OverflowError):
            date_str = None

    return Record(
        source=raw.source,
        title=first_non_empty(p.get("title")),
        text=text,
        date=date_str,
        url=url,
        query=raw.query,
        target=raw.target,
        author=author,
        language=None,
        domain=to_domain(url),
        retrieved_at=raw.retrieved_at,
        views_count=None,
        likes_count=to_int(p.get("score")),
        comments_count=to_int(p.get("num_comments")),
        raw_payload=p,
    )


register("reddit", _normalize)
