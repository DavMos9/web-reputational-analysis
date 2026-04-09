"""
utils/slugify.py

Utility per generare slug e timestamp usati nei nomi dei file
da RawStore e Exporter.
"""

import re
from datetime import datetime, timezone


def target_slug(target: str, max_len: int = 30) -> str:
    """
    Converte un nome entità in uno slug sicuro per filesystem.

    Es: "Elon Musk" → "elon_musk"
        "Apple Inc." → "apple_inc"

    Args:
        target:  stringa da slugificare.
        max_len: lunghezza massima del risultato.
    """
    slug = target.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)        # rimuovi caratteri speciali
    slug = re.sub(r"[\s-]+", "_", slug)          # spazi/trattini → underscore
    slug = re.sub(r"_+", "_", slug).strip("_")   # underscore multipli/bordo
    return slug[:max_len]


def now_timestamp() -> str:
    """
    Restituisce il timestamp corrente in formato "YYYYMMDDTHHMMSSz".
    Es: "20260409T120000Z"
    """
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
