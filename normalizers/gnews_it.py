"""normalizers/gnews_it.py — Normalizer per Google News RSS Italia (source_id: "gnews_it").

domain estratto da source_url (testata reale), non da link (redirect Google),
così rimane corretto anche se il collector ha mantenuto l'URL redirect come fallback.
"""

from __future__ import annotations

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, to_domain, first_non_empty, strip_html


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload

    url = to_url(p.get("link"))

    source_url = p.get("source_url")
    domain = to_domain(source_url) if source_url else to_domain(url)
    text = strip_html(p.get("description") or "")

    return Record(
        source=raw.source,
        title=first_non_empty(p.get("title")),
        text=text,
        date=to_date(p.get("pubDate")),
        url=url,
        query=raw.query,
        target=raw.target,
        author=p.get("source_name"),
        language="it",
        domain=domain,
        retrieved_at=raw.retrieved_at,
        raw_payload=p,
    )


register("gnews_it", _normalize)
