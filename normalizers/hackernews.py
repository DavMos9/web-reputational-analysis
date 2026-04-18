"""
normalizers/hackernews.py

Normalizer per Hacker News Algolia API (source_id: "hackernews").

Payload raw atteso (subset rilevante):
    title         (str):       titolo della storia
    url           (str | None): URL esterno linkato (assente per Ask HN / Show HN)
    author        (str):       username HN dell'autore
    created_at    (str):       data ISO 8601 di pubblicazione
    story_text    (str | None): corpo del post (solo per Ask HN / Show HN self-post)
    points        (int | None): punteggio (upvotes)
    num_comments  (int | None): numero di commenti
    objectID      (str):       ID numerico della storia su HN

Note:
    - Per i post senza `url` (Ask HN, Show HN), l'URL canonico viene costruito
      come https://news.ycombinator.com/item?id={objectID}. Il dominio risultante
      è "news.ycombinator.com" — corretto: il contenuto è sul sito HN.
    - `text` usa `story_text` se disponibile (self-post), altrimenti stringa vuota.
      Il cleaner potrebbe scartare record con testo breve; il titolo è il campo
      informativo principale per i link post.
    - `views_count` non esiste su HN; `likes_count` mappa su `points`.
"""

from __future__ import annotations

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, to_domain, first_non_empty, strip_html, to_int


_HN_BASE = "https://news.ycombinator.com/item?id="


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload

    object_id = str(p.get("objectID", ""))

    # URL: link esterno se presente, altrimenti permalink HN del post.
    raw_url = p.get("url")
    url = to_url(raw_url) if raw_url else f"{_HN_BASE}{object_id}"

    # Testo: disponibile solo per self-post (Ask HN, Show HN).
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
        language=None,           # non fornito da Algolia; enricher rileva dopo
        domain=to_domain(url),
        retrieved_at=raw.retrieved_at,
        views_count=None,
        likes_count=to_int(p.get("points")),
        comments_count=to_int(p.get("num_comments")),
        raw_payload=p,
    )


register("hackernews", _normalize)
