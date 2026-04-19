"""normalizers/brave.py — Normalizer per Brave Search API (source_id: "brave").

age (data relativa "3 days ago") ignorata: non riproducibile senza ora di riferimento.
extra_snippets concatenati alla description quando quest'ultima è corta.
"""

from __future__ import annotations

from typing import Iterable

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, to_domain, first_non_empty, strip_html

_MIN_TEXT_FOR_SNIPPET_MERGE = 80  # margine sopra MIN_TEXT_LENGTH del cleaner


def _compose_text(description: str, extra_snippets: Iterable[str] | None) -> str:
    """Rimuove markup Brave (<strong>) e concatena extra_snippets se description è corta."""
    desc = strip_html(description)
    if len(desc) >= _MIN_TEXT_FOR_SNIPPET_MERGE or not extra_snippets:
        return desc

    parts: list[str] = [desc] if desc else []
    for snippet in extra_snippets:
        if isinstance(snippet, str):
            s = strip_html(snippet)
            if s:
                parts.append(s)
    return " ".join(parts)


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload
    url = to_url(p.get("url"))

    meta_url = p.get("meta_url") or {}
    hostname = meta_url.get("hostname") if isinstance(meta_url, dict) else None

    language = p.get("language")
    language = str(language).lower() if isinstance(language, str) and language.strip() else None

    return Record(
        source=raw.source,
        title=strip_html(first_non_empty(p.get("title"))),
        text=_compose_text(p.get("description", ""), p.get("extra_snippets")),
        date=to_date(p.get("page_age")),
        url=url,
        query=raw.query,
        target=raw.target,
        author=None,
        language=language,
        domain=hostname or to_domain(url),
        retrieved_at=raw.retrieved_at,
        raw_payload=p,
    )


register("brave", _normalize)
