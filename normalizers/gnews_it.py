"""
normalizers/gnews_it.py

Normalizer per Google News RSS Italia (source_id: "gnews_it").

Payload raw atteso (prodotto da GNewsItCollector._parse_rss):
    title        (str | None): titolo dell'articolo
    link         (str | None): URL redirect Google (google.com/rss/articles/...)
    pubDate      (str | None): data pubblicazione RFC 2822 (es. "Mon, 18 Apr 2026 10:00:00 GMT")
    description  (str | None): snippet HTML con articoli correlati (solitamente breve)
    source_name  (str | None): nome della testata (es. "Corriere della Sera")
    source_url   (str | None): URL della testata (es. "https://www.corriere.it")

Note:
    - `url` è il permalink canonico dell'articolo dopo la risoluzione del
      redirect eseguita dal collector (HEAD request parallela). Per articoli
      internazionali su un feed IT, Google può interporre una consent page
      (consent.google.com): in quel caso il collector mantiene l'URL redirect
      originale come fallback. Il `domain` viene comunque estratto da
      `source_url` (testata reale), non da `link`, quindi rimane corretto
      indipendentemente dall'esito della risoluzione.
    - `description` dal RSS di Google News contiene HTML di navigazione
      (lista di articoli correlati). Dopo strip_html può essere vuota o breve:
      il cleaner gestirà il filtro qualità. Il titolo è il campo portante.
    - `author` è il nome della testata (non l'autore individuale, non disponibile
      nel feed RSS).
"""

from __future__ import annotations

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, to_domain, first_non_empty, strip_html


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload

    url = to_url(p.get("link"))

    # Dominio: preferisce source_url (testata reale) al redirect Google.
    source_url = p.get("source_url")
    domain = to_domain(source_url) if source_url else to_domain(url)

    # Testo: la description RSS di Google News è HTML di navigazione.
    # Dopo la pulizia può essere vuota; il titolo è il campo informativo.
    text = strip_html(p.get("description") or "")

    return Record(
        source=raw.source,
        title=first_non_empty(p.get("title")),
        text=text,
        date=to_date(p.get("pubDate")),
        url=url,
        query=raw.query,
        target=raw.target,
        author=p.get("source_name"),   # nome testata, non autore individuale
        language="it",                 # feed filtrato per italiano: lingua certa
        domain=domain,
        retrieved_at=raw.retrieved_at,
        raw_payload=p,
    )


register("gnews_it", _normalize)
