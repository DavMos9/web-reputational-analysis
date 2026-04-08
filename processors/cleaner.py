"""
Cleaner
Pulisce i valori testuali dei record:
- rimuove spazi iniziali/finali
- normalizza encoding UTF-8
- imposta None per stringhe vuote nei campi opzionali
- garantisce che i campi obbligatori abbiano sempre un valore stringa
"""

import unicodedata

# Campi stringa obbligatori: non possono essere None
REQUIRED_STR_FIELDS = ("source_type", "source_name", "target_entity", "query", "title", "url", "domain")

# Campi stringa opzionali: "" diventa None
OPTIONAL_STR_FIELDS = ("snippet", "content", "author", "language", "country", "sentiment_stub")


def _clean_string(value) -> str:
    """Rimuove spazi, normalizza encoding UTF-8."""
    if value is None:
        return ""
    text = str(value).strip()
    # Normalizzazione Unicode NFC (forme composte)
    text = unicodedata.normalize("NFC", text)
    return text


def clean(record: dict) -> dict:
    """
    Pulisce un singolo record.

    Args:
        record: dizionario normalizzato

    Returns:
        Record con testo pulito.
    """
    r = record.copy()

    # Campi obbligatori: pulizia, mai None
    for field in REQUIRED_STR_FIELDS:
        r[field] = _clean_string(r.get(field))

    # Campi opzionali: pulizia, "" → None
    for field in OPTIONAL_STR_FIELDS:
        val = _clean_string(r.get(field))
        r[field] = val if val else None

    # keywords_found: pulisci ogni elemento, rimuovi vuoti
    kw = r.get("keywords_found", [])
    if isinstance(kw, list):
        r["keywords_found"] = [_clean_string(k) for k in kw if _clean_string(k)]
    else:
        r["keywords_found"] = []

    return r


def clean_all(records: list[dict]) -> list[dict]:
    """Pulisce una lista di record."""
    return [clean(r) for r in records]
