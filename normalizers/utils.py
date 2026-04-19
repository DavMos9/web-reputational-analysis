"""
normalizers/utils.py

Funzioni di utilità condivise tra tutti i normalizer source-specific.
Operano solo su stringhe primitive — nessuna dipendenza da sorgenti esterne.

Queste funzioni sono pubbliche (niente prefisso _) perché:
- sono usate da tutti i normalizer del package
- sono testate direttamente in tests/test_normalizer.py
"""

from __future__ import annotations

import html
import re
from datetime import timezone
from urllib.parse import urlparse

from dateutil import parser as dateutil_parser


# Tag HTML semplice: apertura/chiusura di qualsiasi elemento.
# Non tenta di gestire HTML malformato (non serve — l'input è API-generato).
# Pubblica (senza prefisso _) perché viene importata anche da normalizers
# che implementano la propria logica di conversione HTML → testo (es. mastodon.py).
HTML_TAG_RE = re.compile(r"<[^>]+>")


# ---------------------------------------------------------------------------
# Normalizzazione codice lingua
# ---------------------------------------------------------------------------

# Mapping ISO 639-3 → ISO 639-1 per i codici più comuni restituiti da GDELT
# e da sorgenti che usano codici a 3 lettere.
_ISO3_TO_ISO1: dict[str, str] = {
    "ara": "ar", "zho": "zh", "nld": "nl", "eng": "en", "fra": "fr",
    "deu": "de", "ell": "el", "hin": "hi", "hun": "hu", "ind": "id",
    "ita": "it", "jpn": "ja", "kor": "ko", "nor": "no", "pol": "pl",
    "por": "pt", "rum": "ro", "ron": "ro", "rus": "ru", "spa": "es",
    "swe": "sv", "tur": "tr", "ukr": "uk", "vie": "vi",
}

# Mapping nome lingua esteso (inglese) → ISO 639-1 (usato da GDELT tra gli altri).
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
    """
    Normalizza un codice lingua in formato ISO 639-1 (2 lettere lowercase).

    Gestisce i seguenti formati in input:
    - ISO 639-1 standard: "en", "it", "fr"
    - ISO 639-1 con variante regionale: "en-US", "zh-CN", "pt-BR" → "en", "zh", "pt"
    - ISO 639-3 (3 lettere): "eng", "ita", "fra" → "en", "it", "fr"
    - Nome esteso (inglese): "English", "Italian" → "en", "it"

    Returns:
        Codice ISO 639-1 normalizzato, oppure None se il formato è irriconoscibile.
    """
    if not raw_lang:
        return None

    normalized = raw_lang.strip().lower()

    # Formato "en-US", "zh-CN", "pt-BR": prendi il codice primario
    primary = normalized.split("-")[0].split("_")[0]

    if len(primary) == 2:
        # Già ISO 639-1 valido
        return primary

    if len(primary) == 3:
        # Potrebbe essere ISO 639-3
        mapped = _ISO3_TO_ISO1.get(primary)
        if mapped:
            return mapped

    # Potrebbe essere un nome esteso ("english", "italian", ecc.)
    mapped = _LANG_NAME_TO_ISO1.get(normalized)
    if mapped:
        return mapped

    # Formato non riconosciuto
    return None


def to_date(value: str | None) -> str | None:
    """
    Converte qualsiasi stringa data in formato "YYYY-MM-DD".

    Gestisce: ISO 8601, GDELT "20260408T120000Z", NYT "2026-04-08T...", ecc.
    Restituisce None se il parsing fallisce o il valore è assente.
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


def to_url(url: str | None) -> str:
    """
    Normalizza un URL: aggiunge schema https:// se mancante, strip whitespace.
    Restituisce stringa vuota se l'URL non è valido (netloc assente).
    """
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
    """
    Rimuove tag HTML e decodifica le entità (es. &amp; → &).

    Utility condivisa per le sorgenti che restituiscono snippet con
    markup inline (Brave, Stack Exchange, Mastodon, ecc.).
    Non è un parser HTML completo: si limita a rimuovere i tag e lasciare
    il testo pulito, sufficiente per pipeline di sentiment/dedup.

    Restituisce stringa vuota se l'input è None o vuoto.
    """
    if not text:
        return ""
    cleaned = HTML_TAG_RE.sub("", text)
    return html.unescape(cleaned).strip()
