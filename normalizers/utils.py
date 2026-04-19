"""normalizers/utils.py — Helper condivisi tra i normalizer source-specific."""

from __future__ import annotations

import html
import re
from datetime import timezone
from urllib.parse import urlparse

from dateutil import parser as dateutil_parser


# Pubblica perché importata da normalizer che implementano propria logica HTML→testo.
HTML_TAG_RE = re.compile(r"<[^>]+>")


_ISO3_TO_ISO1: dict[str, str] = {
    "ara": "ar", "zho": "zh", "nld": "nl", "eng": "en", "fra": "fr",
    "deu": "de", "ell": "el", "hin": "hi", "hun": "hu", "ind": "id",
    "ita": "it", "jpn": "ja", "kor": "ko", "nor": "no", "pol": "pl",
    "por": "pt", "rum": "ro", "ron": "ro", "rus": "ru", "spa": "es",
    "swe": "sv", "tur": "tr", "ukr": "uk", "vie": "vi",
}

_LANG_NAME_TO_ISO1: dict[str, str] = {
    "arabic": "ar", "chinese": "zh", "dutch": "nl", "english": "en",
    "french": "fr", "german": "de", "greek": "el", "hindi": "hi",
    "hungarian": "hu", "indonesian": "id", "italian": "it",
    "japanese": "ja", "korean": "ko", "norwegian": "no", "polish": "pl",
    "portuguese": "pt", "romanian": "ro", "russian": "ru",
    "spanish": "es", "swedish": "sv", "turkish": "tr",
    "ukrainian": "uk", "vietnamese": "vi",
}


def normalize_language_code(raw_lang: str | None) -> str | None:
    """ISO 639-1, ISO 639-3, varianti regionali (en-US) e nomi estesi → codice 2 lettere. None se irriconoscibile."""
    if not raw_lang:
        return None

    normalized = raw_lang.strip().lower()

    primary = normalized.split("-")[0].split("_")[0]

    if len(primary) == 2:
        return primary

    if len(primary) == 3:
        mapped = _ISO3_TO_ISO1.get(primary)
        if mapped:
            return mapped

    mapped = _LANG_NAME_TO_ISO1.get(normalized)
    if mapped:
        return mapped

    return None


def to_date(value: str | None) -> str | None:
    """Converte qualsiasi stringa data in 'YYYY-MM-DD'. None se parsing fallisce."""
    if not value:
        return None
    try:
        dt = dateutil_parser.parse(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except (ValueError, OverflowError):
        return None


def to_url(url: str | None) -> str:
    """Aggiunge https:// se mancante. Stringa vuota se URL non valido."""
    if not url:
        return ""
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        parsed = urlparse(url)
        return url if parsed.netloc else ""
    except ValueError:
        return ""


def to_domain(url: str) -> str:
    """Estrae il dominio (netloc) da un URL già normalizzato."""
    try:
        return urlparse(url).netloc or ""
    except ValueError:
        return ""


def first_non_empty(*values: str | None) -> str:
    """Restituisce il primo valore non vuoto/None tra quelli forniti."""
    for v in values:
        if v and str(v).strip():
            return str(v).strip()
    return ""


def to_int(val: object) -> int | None:
    """Converte un valore in int, restituisce None se non convertibile."""
    try:
        return int(val) if val is not None else None  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return None


def strip_html(text: str | None) -> str:
    """Rimuove tag HTML e decodifica entità. Sufficiente per snippet API-generati."""
    if not text:
        return ""
    cleaned = HTML_TAG_RE.sub("", text)
    return html.unescape(cleaned).strip()
