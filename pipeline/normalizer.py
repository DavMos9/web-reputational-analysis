"""
pipeline/normalizer.py

Converte RawRecord → Record applicando logica source-specific.

Ogni sorgente ha un estrattore dedicato che conosce la struttura
del suo payload raw. Il dispatcher `normalize()` smista in base
a `raw.source`.

Regole:
- Nessun campo viene inventato: se manca → None o stringa vuota.
- `date` è sempre "YYYY-MM-DD" o None (mai full ISO datetime).
- `text` usa il campo più ricco disponibile per la sorgente.
- I log di warning segnalano sorgenti sconosciute ma non bloccano.
"""

from __future__ import annotations

import logging
from datetime import timezone
from urllib.parse import urlparse

from dateutil import parser as dateutil_parser

from models import RawRecord, Record

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Utility di normalizzazione — usate da tutti gli estrattori
# ---------------------------------------------------------------------------

def _to_date(value: str | None) -> str | None:
    """
    Converte qualsiasi stringa data in formato "YYYY-MM-DD".
    Gestisce: ISO 8601, GDELT "20260408T120000Z", NYT "2026-04-08T...", ecc.
    Restituisce None se il parsing fallisce.
    """
    if not value:
        return None
    try:
        dt = dateutil_parser.parse(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except (ValueError, OverflowError):
        return None


def _to_url(url: str | None) -> str:
    """
    Normalizza un URL: aggiunge schema se mancante, strip whitespace.
    Restituisce stringa vuota se l'URL non è valido.
    """
    if not url:
        return ""
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        parsed = urlparse(url)
        return url if parsed.netloc else ""
    except Exception:
        return ""


def _to_domain(url: str) -> str:
    """Estrae il dominio da un URL normalizzato."""
    try:
        return urlparse(url).netloc or ""
    except Exception:
        return ""


def _first_non_empty(*values: str | None) -> str:
    """Restituisce il primo valore non vuoto tra quelli forniti."""
    for v in values:
        if v and str(v).strip():
            return str(v).strip()
    return ""


# ---------------------------------------------------------------------------
# Estrattori source-specific
# ---------------------------------------------------------------------------

def _from_news(raw: RawRecord) -> Record:
    """
    Payload raw: risposta singolo articolo da NewsAPI /v2/everything.
    Campi rilevanti: title, description, content, url, author,
                     publishedAt, source.name
    """
    p = raw.payload
    url = _to_url(p.get("url"))
    return Record(
        source=raw.source,
        title=_first_non_empty(p.get("title")),
        text=_first_non_empty(p.get("description"), p.get("content")),
        date=_to_date(p.get("publishedAt")),
        url=url,
        query=raw.query,
        target=raw.target,
        author=_first_non_empty(p.get("author")),
        language=_first_non_empty(p.get("language")),
        domain=_to_domain(url) or _first_non_empty(
            p.get("source", {}).get("name") if isinstance(p.get("source"), dict) else None
        ),
        retrieved_at=raw.retrieved_at,
        raw_payload=p,
    )


def _from_gdelt(raw: RawRecord) -> Record:
    """
    Payload raw: articolo da GDELT DOC 2.0 /api/v2/doc/doc.
    Campi rilevanti: title, url, seendate, language, sourcecountry, domain.
    Nota: GDELT non fornisce snippet/testo — text rimane vuoto.
    """
    p = raw.payload
    url = _to_url(p.get("url"))
    return Record(
        source=raw.source,
        title=_first_non_empty(p.get("title")),
        text="",                              # GDELT non fornisce body
        date=_to_date(p.get("seendate")),    # formato: "20260408T120000Z"
        url=url,
        query=raw.query,
        target=raw.target,
        language=_first_non_empty(p.get("language")),
        domain=_to_domain(url) or _first_non_empty(p.get("domain")),
        retrieved_at=raw.retrieved_at,
        raw_payload=p,
    )


def _from_youtube(raw: RawRecord) -> Record:
    """
    Payload raw: item da YouTube Data API v3 /search + /videos (statistics).
    Campi rilevanti: snippet.title, snippet.description, snippet.channelTitle,
                     snippet.publishedAt, id.videoId, statistics.*, rank.
    """
    p = raw.payload
    snippet = p.get("snippet", {})
    stats = p.get("statistics", {})
    video_id = p.get("id", {}).get("videoId") if isinstance(p.get("id"), dict) else None
    url = _to_url(f"https://www.youtube.com/watch?v={video_id}") if video_id else ""

    def _to_int(val: str | None) -> int | None:
        try:
            return int(val) if val is not None else None
        except (ValueError, TypeError):
            return None

    return Record(
        source=raw.source,
        title=_first_non_empty(snippet.get("title")),
        text=_first_non_empty(snippet.get("description")),
        date=_to_date(snippet.get("publishedAt")),
        url=url,
        query=raw.query,
        target=raw.target,
        author=_first_non_empty(snippet.get("channelTitle")),
        domain="youtube.com",
        retrieved_at=raw.retrieved_at,
        views_count=_to_int(stats.get("viewCount")),
        likes_count=_to_int(stats.get("likeCount")),
        comments_count=_to_int(stats.get("commentCount")),
        raw_payload=p,
    )


def _from_wikipedia(raw: RawRecord) -> Record:
    """
    Payload raw: dizionario costruito dal WikipediaCollector con:
    title, summary, text, url, language.
    """
    p = raw.payload
    url = _to_url(p.get("url"))
    return Record(
        source=raw.source,
        title=_first_non_empty(p.get("title")),
        text=_first_non_empty(p.get("summary"), p.get("text")),
        date=None,                           # Wikipedia non ha data di pubblicazione
        url=url,
        query=raw.query,
        target=raw.target,
        language=_first_non_empty(p.get("language")),
        domain="wikipedia.org",
        retrieved_at=raw.retrieved_at,
        raw_payload=p,
    )


def _from_guardian(raw: RawRecord) -> Record:
    """
    Payload raw: articolo da The Guardian Open Platform.
    Campi rilevanti: fields.headline, fields.trailText, fields.bodyText,
                     fields.byline, fields.shortUrl, webUrl, webPublicationDate,
                     _rank (aggiunto dal collector).
    """
    p = raw.payload
    fields = p.get("fields", {})
    url = _to_url(fields.get("shortUrl") or p.get("webUrl"))
    return Record(
        source=raw.source,
        title=_first_non_empty(fields.get("headline"), p.get("webTitle")),
        text=_first_non_empty(fields.get("trailText"), fields.get("bodyText")),
        date=_to_date(p.get("webPublicationDate")),
        url=url,
        query=raw.query,
        target=raw.target,
        author=_first_non_empty(fields.get("byline")),
        language="en",
        domain=_to_domain(url) or "theguardian.com",
        retrieved_at=raw.retrieved_at,
        raw_payload=p,
    )


def _from_nyt(raw: RawRecord) -> Record:
    """
    Payload raw: documento da NYT Article Search API /articlesearch.json.
    Campi rilevanti: web_url, headline.main, abstract, lead_paragraph,
                     byline.original, pub_date, source, _rank.
    """
    p = raw.payload
    url = _to_url(p.get("web_url"))

    byline = p.get("byline", {})
    author_raw = byline.get("original", "") if isinstance(byline, dict) else ""
    author = author_raw.replace("By ", "").strip() if author_raw else ""

    headline = p.get("headline", {})
    title = headline.get("main", "") if isinstance(headline, dict) else ""

    return Record(
        source=raw.source,
        title=_first_non_empty(title),
        text=_first_non_empty(p.get("abstract"), p.get("lead_paragraph")),
        date=_to_date(p.get("pub_date")),
        url=url,
        query=raw.query,
        target=raw.target,
        author=_first_non_empty(author),
        language="en",
        domain=_to_domain(url) or "nytimes.com",
        retrieved_at=raw.retrieved_at,
        raw_payload=p,
    )


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

_EXTRACTORS = {
    "news":      _from_news,
    "gdelt":     _from_gdelt,
    "youtube":   _from_youtube,
    "wikipedia": _from_wikipedia,
    "guardian":  _from_guardian,
    "nyt":       _from_nyt,
}


def normalize(raw: RawRecord) -> Record | None:
    """
    Converte un RawRecord in Record applicando l'estrattore corretto.

    Args:
        raw: RawRecord prodotto da un collector.

    Returns:
        Record normalizzato, oppure None se la sorgente è sconosciuta
        o il record non può essere costruito (es. URL mancante).
    """
    extractor = _EXTRACTORS.get(raw.source)
    if extractor is None:
        log.warning("Sorgente sconosciuta '%s': record scartato.", raw.source)
        return None

    try:
        record = extractor(raw)
    except ValueError as e:
        # ValueError da Record.__post_init__: campo obbligatorio mancante (es. URL vuoto)
        log.warning("[%s] Record scartato: %s (query='%s')", raw.source, e, raw.query)
        return None
    except Exception as e:
        log.error("Errore normalizzando record da '%s' (query='%s'): %s", raw.source, raw.query, e)
        return None

    if not record.url:
        log.warning(
            "[%s] Record scartato: URL mancante (query='%s', title='%s')",
            raw.source, raw.query, raw.payload.get("title", "")[:60],
        )
        return None

    return record


def normalize_all(raws: list[RawRecord]) -> list[Record]:
    """
    Normalizza una lista di RawRecord, scartando silenziosamente i None.

    Args:
        raws: lista di RawRecord prodotti dai collector.

    Returns:
        Lista di Record validi.
    """
    results: list[Record] = []
    for raw in raws:
        record = normalize(raw)
        if record is not None:
            results.append(record)
    log.info("Normalizzati %d/%d record.", len(results), len(raws))
    return results
