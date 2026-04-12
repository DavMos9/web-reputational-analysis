"""
normalizers/wikitalk.py

Normalizer per Wikipedia Talk Pages (pagine di discussione).

Payload raw atteso (per sezione):
    page_title:     str (titolo dell'articolo, es. "Elon Musk")
    section_title:  str (titolo della sezione di discussione)
    section_index:  str (indice della sezione)
    section_level:  int (livello header: 2, 3, ecc.)
    wikitext:       str (testo della sezione, già pulito dal collector)
    url:            str (permalink alla sezione, con anchor #)
    language:       str (codice lingua, es. "en")

Il titolo del Record è composto da: "[Talk] {page_title}: {section_title}"
per chiarire che si tratta di una discussione, non di contenuto enciclopedico.
"""

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
        date=None,  # le talk page non hanno una data di pubblicazione univoca
        url=url,
        query=raw.query,
        target=raw.target,
        author=None,  # discussioni multi-autore, nessun autore singolo
        language=language,
        domain=f"{language}.wikipedia.org",
        retrieved_at=raw.retrieved_at,
        raw_payload=p,
    )


register("wikitalk", _normalize)
