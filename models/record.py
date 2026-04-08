"""
models/record.py

Definisce i due tipi fondamentali della pipeline:

- RawRecord: prodotto dai collector. Contiene il payload grezzo dell'API
  senza alcuna trasformazione. È immutabile dopo la creazione.

- Record: modello normalizzato e unico in tutta la pipeline.
  Prodotto dal normalizer a partire da un RawRecord.
  È il tipo che attraversa cleaner, deduplicator ed exporter.

Nessun modulo esterno al package models/ dovrebbe definire schemi dati alternativi.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, asdict
from typing import Any

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# ---------------------------------------------------------------------------
# RawRecord — prodotto dai collector
# ---------------------------------------------------------------------------

@dataclass
class RawRecord:
    """
    Payload grezzo restituito da un collector.

    Il campo `payload` contiene la risposta API esattamente come ricevuta,
    senza trasformazioni. Il normalizer legge da qui.

    Attributi:
        source      Identificatore della sorgente ("news", "gdelt", "youtube",
                    "wikipedia", "guardian", "nyt").
        query       Query di ricerca usata per raccogliere questo record.
        target      Entità analizzata (es. "Elon Musk").
        payload     Dizionario con la risposta raw dell'API. Immutabile.
        retrieved_at Timestamp ISO 8601 del momento della raccolta.
    """

    source: str
    query: str
    target: str
    payload: dict[str, Any]
    retrieved_at: str  # ISO 8601, impostato dal collector

    def __post_init__(self) -> None:
        if not self.source:
            raise ValueError("RawRecord.source non può essere vuoto")
        if not self.target:
            raise ValueError("RawRecord.target non può essere vuoto")
        if not isinstance(self.payload, dict):
            raise TypeError(f"RawRecord.payload deve essere un dict, ricevuto: {type(self.payload)}")


# ---------------------------------------------------------------------------
# Record — modello normalizzato
# ---------------------------------------------------------------------------

@dataclass
class Record:
    """
    Modello dati unico e normalizzato della pipeline.

    Prodotto dal normalizer a partire da un RawRecord.
    Attraversa cleaner → deduplicator → exporter senza cambiare schema.

    Campi canonici (allineati al data contract di progetto):
        source      Sorgente normalizzata (es. "news", "youtube").
        title       Titolo dell'articolo / video / pagina.
        text        Corpo del testo (snippet o contenuto, scelto dal normalizer).
        date        Data di pubblicazione in formato "YYYY-MM-DD". None se assente.
        url         URL canonico del contenuto.
        query       Query usata per raccogliere questo record.
        target      Entità analizzata.

    Campi estesi (opzionali, preservano valore analitico):
        author      Autore / canale.
        language    Codice lingua ISO 639-1 (es. "it", "en").
        domain      Dominio estratto dall'URL (es. "bbc.co.uk").
        retrieved_at Timestamp ISO 8601 della raccolta.

    Metriche di engagement (solo sorgenti social/video):
        views_count, likes_count, comments_count

    Metadati pipeline:
        raw_payload Copia del payload grezzo per tracciabilità.
                    NON viene incluso nell'export finale.
    """

    # --- Campi canonici (obbligatori) ---
    source: str
    title: str
    text: str
    date: str | None          # "YYYY-MM-DD"
    url: str
    query: str
    target: str

    # --- Campi estesi (opzionali) ---
    author: str | None        = None
    language: str | None      = None
    domain: str               = ""
    retrieved_at: str         = ""

    # --- Metriche engagement ---
    views_count: int | None   = None
    likes_count: int | None   = None
    comments_count: int | None = None

    # --- Metadati pipeline (esclusi dall'export) ---
    raw_payload: dict[str, Any] = field(default_factory=dict, repr=False)

    # Campi che NON devono comparire nell'export finale
    _EXPORT_EXCLUDE: frozenset[str] = field(
        default=frozenset({"raw_payload"}),
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if not self.source:
            raise ValueError("Record.source non può essere vuoto")
        if not self.url:
            raise ValueError("Record.url non può essere vuoto")
        if self.date is not None and not _DATE_RE.match(self.date):
            raise ValueError(f"Record.date deve essere 'YYYY-MM-DD', ricevuto: '{self.date}'")

    # ------------------------------------------------------------------
    # Serializzazione
    # ------------------------------------------------------------------

    def to_dict(self, include_raw: bool = False) -> dict[str, Any]:
        """
        Converte il record in dizionario per serializzazione.

        Args:
            include_raw: se True include raw_payload (utile per debug/storage).
                         Default False — gli exporter non devono includere il raw.

        Returns:
            Dizionario con i campi del record.
        """
        d = asdict(self)
        # Rimuovi il campo interno _EXPORT_EXCLUDE (non è un campo dati)
        d.pop("_EXPORT_EXCLUDE", None)
        if not include_raw:
            d.pop("raw_payload", None)
        return d

    @staticmethod
    def export_fields() -> list[str]:
        """
        Restituisce l'elenco ordinato dei campi da includere nell'export CSV.
        Usato da CsvExporter per definire i fieldnames in modo canonico.
        """
        return [
            "source", "title", "text", "date", "url", "query", "target",
            "author", "language", "domain", "retrieved_at",
            "views_count", "likes_count", "comments_count",
        ]
