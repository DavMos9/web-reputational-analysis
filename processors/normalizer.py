"""
Normalizer
Porta ogni record allo schema del data contract:
- date in formato ISO 8601
- URL completi e validi
- dominio estratto automaticamente se mancante
- source_type forzato a minuscolo
"""

from datetime import datetime, timezone
from urllib.parse import urlparse
from dateutil import parser as dateutil_parser


VALID_SOURCE_TYPES = {"news", "youtube", "gdelt", "wikipedia", "reddit"}


def _normalize_date(value: str | None) -> str | None:
    """Converte qualsiasi stringa data in formato ISO 8601. Restituisce None se fallisce."""
    if not value:
        return None
    try:
        dt = dateutil_parser.parse(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except (ValueError, OverflowError):
        return None


def _normalize_url(url: str | None) -> str | None:
    """Verifica che l'URL abbia protocollo. Restituisce None se non valido."""
    if not url:
        return None
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        parsed = urlparse(url)
        if parsed.netloc:
            return url
    except Exception:
        pass
    return None


def _extract_domain(url: str | None) -> str:
    """Estrae il dominio dall'URL."""
    if not url:
        return ""
    try:
        return urlparse(url).netloc or ""
    except Exception:
        return ""


def normalize(record: dict) -> dict:
    """
    Normalizza un singolo record secondo il data contract.

    Args:
        record: dizionario grezzo proveniente da un collector

    Returns:
        Record normalizzato.
    """
    r = record.copy()

    # source_type in minuscolo e validato
    source_type = str(r.get("source_type") or "").lower().strip()
    r["source_type"] = source_type if source_type in VALID_SOURCE_TYPES else source_type

    # Date in ISO 8601
    r["published_at"] = _normalize_date(r.get("published_at"))
    r["retrieved_at"] = _normalize_date(r.get("retrieved_at")) or datetime.now(timezone.utc).isoformat()

    # URL e dominio
    url = _normalize_url(r.get("url"))
    r["url"] = url or ""
    r["domain"] = r.get("domain") or _extract_domain(url)

    # Campi interi: nessuna stringa dove serve un numero
    for int_field in ("rank", "views_count", "likes_count", "comments_count"):
        val = r.get(int_field)
        if val is not None:
            try:
                r[int_field] = int(val)
            except (ValueError, TypeError):
                r[int_field] = None

    # engagement_score come float
    val = r.get("engagement_score")
    if val is not None:
        try:
            r["engagement_score"] = float(val)
        except (ValueError, TypeError):
            r["engagement_score"] = None

    # keywords_found sempre lista
    kw = r.get("keywords_found")
    if not isinstance(kw, list):
        r["keywords_found"] = []

    # raw_payload sempre dict
    if not isinstance(r.get("raw_payload"), dict):
        r["raw_payload"] = {}

    return r


def normalize_all(records: list[dict]) -> list[dict]:
    """Normalizza una lista di record."""
    return [normalize(r) for r in records]
