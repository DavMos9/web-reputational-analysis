"""
normalizers/reddit.py

Normalizer per Reddit JSON API non autenticata (source_id: "reddit").

Payload raw atteso (subset rilevante del campo `data` di ogni post):
    title       (str):       titolo del post
    selftext    (str):       corpo del post (self-post); vuoto per link post
    url         (str):       URL linkato (link post) o permalink completo
    permalink   (str):       percorso relativo Reddit (es. "/r/italy/comments/...")
    author      (str):       username dell'autore
    created_utc (float):     timestamp Unix UTC di pubblicazione
    score       (int):       punteggio netto (upvotes - downvotes)
    num_comments(int):       numero di commenti
    subreddit   (str):       nome del subreddit (senza r/)
    over_18     (bool):      flag contenuto NSFW

Note:
    - `url` canonico del record è sempre il permalink Reddit (non il link esterno).
      Garantisce unicità nella deduplicazione: ogni post ha un permalink univoco.
    - `text` usa `selftext` se non vuoto/rimosso. Per link post è stringa vuota:
      il titolo è l'unico campo testuale disponibile.
    - Filtra post con selftext "[removed]" o "[deleted]": non hanno contenuto utile.
    - `created_utc` è un timestamp Unix: viene convertito in ISO 8601 da to_date
      tramite dateutil (riconosce valori numerici come epoch).
    - `subreddit` viene aggiunto come prefisso all'autore (es. "username [r/italy]")
      per contestualizzare l'origine del post.
"""

from __future__ import annotations

from models import RawRecord, Record
from normalizers.registry import register
from normalizers.utils import to_date, to_url, to_domain, first_non_empty, strip_html, to_int

_REDDIT_BASE = "https://www.reddit.com"
_REMOVED_TEXTS = {"[removed]", "[deleted]", ""}


def _normalize(raw: RawRecord) -> Record:
    p = raw.payload

    # URL canonico: sempre il permalink Reddit per garantire dedup univoco.
    permalink = p.get("permalink", "")
    url = to_url(f"{_REDDIT_BASE}{permalink}") if permalink else ""

    # Testo: selftext per self-post, vuoto per link post o post rimossi.
    selftext = str(p.get("selftext") or "").strip()
    text = strip_html(selftext) if selftext not in _REMOVED_TEXTS else ""

    # Autore: username + subreddit di provenienza per contestualizzare.
    author_raw = p.get("author")
    subreddit   = p.get("subreddit")
    if author_raw and subreddit:
        author = f"{author_raw} [r/{subreddit}]"
    elif author_raw:
        author = str(author_raw)
    else:
        author = None

    # Data: created_utc è float (epoch seconds) — dateutil lo gestisce
    # solo come stringa, quindi convertiamo prima.
    created_utc = p.get("created_utc")
    date_str: str | None = None
    if created_utc is not None:
        try:
            from datetime import datetime, timezone
            date_str = datetime.fromtimestamp(float(created_utc), tz=timezone.utc).strftime("%Y-%m-%d")
        except (ValueError, OSError, OverflowError):
            date_str = None

    return Record(
        source=raw.source,
        title=first_non_empty(p.get("title")),
        text=text,
        date=date_str,
        url=url,
        query=raw.query,
        target=raw.target,
        author=author,
        language=None,           # non fornito da Reddit; enricher rileva dopo
        domain=to_domain(url),
        retrieved_at=raw.retrieved_at,
        views_count=None,        # Reddit non espone il conteggio visualizzazioni
        likes_count=to_int(p.get("score")),
        comments_count=to_int(p.get("num_comments")),
        raw_payload=p,
    )


register("reddit", _normalize)
