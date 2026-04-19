"""normalizers/wikitalk.py — Normalizer per Wikipedia Talk Pages (source_id: "wikitalk")."""

from __future__ import annotations

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_url, first_non_empty


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload

    page_title = p.get("page_title", "")
    section_title = p.get("section_title", "")
    wikitext = p.get("wikitext", "")
    language = p.get("language", "en")

    if section_title:
        title = f"[Talk] {page_title}: {section_title}"
    else:
        title = f"[Talk] {page_title}"

    url = to_url(p.get("url", ""))

    return Record(
        source=raw.source,
        title=title,
        text=wikitext,
        date=None,
        url=url,
        query=raw.query,
        target=raw.target,
        author=None,
        language=language,
        domain=f"{language}.wikipedia.org",
        retrieved_at=raw.retrieved_at,
        raw_payload=p,
    )


register("wikitalk", _normalize)
