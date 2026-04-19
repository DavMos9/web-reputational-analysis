"""
normalizers/gdelt.py

Normalizer per GDELT DOC 2.0 (/api/v2/doc/doc).

Payload raw atteso:
    title, url, seendate, language, sourcecountry, domain
Nota: GDELT non fornisce snippet/testo — text rimane vuoto.
"""

from __future__ import annotations

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, to_domain, first_non_empty, normalize_language_code


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload
    url = to_url(p.get("url"))
    return Record(
        source=raw.source,
        title=first_non_empty(p.get("title")),
        text="",  # GDELT non fornisce body
        date=to_date(p.get("seendate")),  # formato: "20260408T120000Z"
        url=url,
        query=raw.query,
        target=raw.target,
        language=normalize_language_code(p.get("language")),
        domain=to_domain(url) or first_non_empty(p.get("domain")),
        retrieved_at=raw.retrieved_at,
        raw_payload=p,
    )


register("gdelt", _normalize)
