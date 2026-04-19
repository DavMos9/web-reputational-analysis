"""normalizers/mastodon.py — Normalizer per Mastodon API (source_id: "mastodon").

I post non hanno titolo. spoiler_text (Content Warning) viene usato come title se presente.
"""

from __future__ import annotations

import html
import re

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, first_non_empty, to_int, HTML_TAG_RE


def _html_to_text(content: str) -> str:
    """HTML Mastodon → testo pulito (br/paragrafi → newline, strip tag e entità)."""
    if not content:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", content)
    text = re.sub(r"</p>\s*<p>", "\n\n", text)
    text = HTML_TAG_RE.sub("", text)
    text = html.unescape(text)
    lines = [line.strip() for line in text.splitlines()]
    text = "\n".join(line for line in lines if line)
    return text.strip()


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload
    account = p.get("account", {})
    instance = p.get("_instance", "mastodon.social")

    content_html = p.get("content", "")
    spoiler_text = p.get("spoiler_text", "")
    text = _html_to_text(content_html)

    title = spoiler_text.strip() if spoiler_text else ""
    post_url = to_url(p.get("url") or p.get("uri", ""))
    author = first_non_empty(
        account.get("display_name"),
        account.get("acct"),
    )

    return Record(
        source=raw.source,
        title=title,
        text=text,
        date=to_date(p.get("created_at")),
        url=post_url,
        query=raw.query,
        target=raw.target,
        author=author,
        language=p.get("language"),
        domain=instance,
        retrieved_at=raw.retrieved_at,
        likes_count=to_int(p.get("favourites_count")),
        comments_count=to_int(p.get("replies_count")),
        views_count=to_int(p.get("reblogs_count")),
        raw_payload=p,
    )


register("mastodon", _normalize)
