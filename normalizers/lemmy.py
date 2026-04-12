"""
normalizers/lemmy.py

Normalizer per Lemmy API v3 (/api/v3/search).

Due tipi di payload:

POST (type_ = "Posts"):
    post:    { name, body, ap_id, published, community_id, ... }
    creator: { name, actor_id, ... }
    counts:  { score, comments, upvotes, downvotes, ... }

COMMENT (type_ = "Comments"):
    comment: { content, ap_id, published, ... }
    creator: { name, actor_id, ... }
    post:    { name, ap_id, ... }  (il post genitore)
    counts:  { score, upvotes, downvotes, child_count, ... }

Campi iniettati dal collector:
    _instance:     str (es. "lemmy.world")
    _content_type: str ("Posts" o "Comments")
"""

from __future__ import annotations

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, first_non_empty, to_int


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload
    content_type = p.get("_content_type", "Posts")
    instance = p.get("_instance", "lemmy.world")
    creator = p.get("creator", {})
    counts = p.get("counts", {})

    if content_type == "Comments":
        return _normalize_comment(raw, p, instance, creator, counts)
    return _normalize_post(raw, p, instance, creator, counts)


def _normalize_post(
    raw: RawRecord,
    p: dict,
    instance: str,
    creator: dict,
    counts: dict,
) -> Record:
    """Normalizza un post Lemmy."""
    post = p.get("post", {})

    title = post.get("name", "")
    body = post.get("body", "")
    # Se il post è un link senza body, usa il titolo come testo
    text = body if body else title

    url = to_url(post.get("ap_id", ""))

    return Record(
        source=raw.source,
        title=title,
        text=text,
        date=to_date(post.get("published")),
        url=url,
        query=raw.query,
        target=raw.target,
        author=first_non_empty(creator.get("name")),
        domain=instance,
        retrieved_at=raw.retrieved_at,
        likes_count=to_int(counts.get("score")),
        comments_count=to_int(counts.get("comments")),
        raw_payload=p,
    )


def _normalize_comment(
    raw: RawRecord,
    p: dict,
    instance: str,
    creator: dict,
    counts: dict,
) -> Record:
    """Normalizza un commento Lemmy."""
    comment = p.get("comment", {})
    parent_post = p.get("post", {})

    # Titolo: dal post genitore, prefissato
    parent_title = parent_post.get("name", "")
    title = f"[Comment] {parent_title}" if parent_title else ""

    text = comment.get("content", "")
    url = to_url(comment.get("ap_id", ""))

    return Record(
        source=raw.source,
        title=title,
        text=text,
        date=to_date(comment.get("published")),
        url=url,
        query=raw.query,
        target=raw.target,
        author=first_non_empty(creator.get("name")),
        domain=instance,
        retrieved_at=raw.retrieved_at,
        likes_count=to_int(counts.get("score")),
        comments_count=to_int(counts.get("child_count")),
        raw_payload=p,
    )


register("lemmy", _normalize)
