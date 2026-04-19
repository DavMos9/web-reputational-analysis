"""
normalizers/mastodon.py

Normalizer per Mastodon API (/api/v2/search statuses, /api/v1/timelines/tag).

Payload raw atteso (entity Status):
    id:                str
    created_at:        str (ISO 8601)
    content:           str (HTML con tag <p>, <a>, <span>, ecc.)
    spoiler_text:      str (Content Warning, se presente)
    url:               str (permalink web)
    account:           dict con display_name, acct, url
    favourites_count:  int
    reblogs_count:     int
    replies_count:     int
    language:          str | None (ISO 639-1)
    _instance:         str (iniettato dal collector)

I post Mastodon non hanno titolo. Se c'è un Content Warning (spoiler_text),
lo usiamo come titolo — è la prassi del fediverse.
"""

from __future__ import annotations

import html
import re

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, first_non_empty, to_int, HTML_TAG_RE


def _html_to_text(content: str) -> str:
    """
    Converte contenuto HTML Mastodon in testo pulito.

    - Sostituisce <br> e </p><p> con newline
    - Rimuove tutti i tag HTML
    - Decodifica entità HTML
    - Strip whitespace ridondante
    """
    if not content:
        return ""
    # <br> e varianti → newline
    text = re.sub(r"<br\s*/?>", "\n", content)
    # </p><p> → doppio newline (cambio paragrafo)
    text = re.sub(r"</p>\s*<p>", "\n\n", text)
    text = HTML_TAG_RE.sub("", text)
    text = html.unescape(text)
    # Normalizza whitespace (preserva singoli newline)
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

    # Spoiler text (Content Warning) come titolo, se presente
    title = spoiler_text.strip() if spoiler_text else ""

    # URL: usa il campo url del post, oppure costruisci dal campo uri
    post_url = to_url(p.get("url") or p.get("uri", ""))

    # Autore: display_name preferito, fallback su acct (user@instance)
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
        language=p.get("language"),   # ISO 639-1 se dichiarato dal client, None altrimenti
        domain=instance,
        retrieved_at=raw.retrieved_at,
        likes_count=to_int(p.get("favourites_count")),
        comments_count=to_int(p.get("replies_count")),
        views_count=to_int(p.get("reblogs_count")),  # reblogs come proxy di visibilità
        raw_payload=p,
    )


register("mastodon", _normalize)
