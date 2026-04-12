"""
normalizers/stackexchange.py

Normalizer per Stack Exchange API v2.3 (/search/excerpts).

Payload raw atteso (per item):
    item_type:     "question" | "answer"
    question_id:   int
    answer_id:     int (solo per answer)
    title:         str (titolo della domanda — presente anche per risposte)
    excerpt:       str (snippet HTML con <span class="highlight">)
    body:          str (opzionale, se il filter include body)
    score:         int (voti netti: upvotes - downvotes)
    tags:          list[str] (solo per questions)
    creation_date: int (unix timestamp)
    owner:         dict con display_name, link, user_id
    _site:         str (iniettato dal collector: "stackoverflow", ecc.)

URL costruito:
    Question → https://{site}.com/questions/{question_id}
    Answer   → https://{site}.com/a/{answer_id}
"""

from __future__ import annotations

import html
import re

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, first_non_empty, to_int


# Rimuove tag HTML dall'excerpt (es. <span class="highlight">...</span>)
_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    """Rimuove tag HTML e decodifica entità HTML."""
    if not text:
        return ""
    # Prima rimuovi i tag, poi decodifica le entità (&amp; → &, ecc.)
    cleaned = _HTML_TAG_RE.sub("", text)
    return html.unescape(cleaned).strip()


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

    # Pulisci HTML da titolo ed excerpt
    title = _strip_html(title_raw)
    excerpt = _strip_html(excerpt_raw)

    # Per le risposte, prefissa il titolo per chiarire il contesto
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
        domain=f"{site}.com",
        retrieved_at=raw.retrieved_at,
        likes_count=to_int(p.get("score")),  # score = upvotes - downvotes
        raw_payload=p,
    )


register("stackexchange", _normalize)
