"""
models/record.py

Definisce i due tipi fondamentali della pipeline:

- RawRecord: prodotto dai collector. Contiene il payload grezzo dell'API
  senza alcuna trasformazione. È immutabile dopo la creazione.

- Record: modello normalizzato e unico in tutta la pipeline.
  Prodotto dal normalizer a partire da un RawRecord.
  È il tipo che attraversa cleaner, deduplicator, enricher ed exporter.

Nessun modulo esterno a models/ dovrebbe definire schemi dati alternativi.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, asdict
from typing import Any, ClassVar

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
        source       Identificatore della sorgente ("news", "gdelt", "youtube", ecc.).
        query        Query di ricerca usata per raccogliere questo record.
        target       Entità analizzata (es. "Elon Musk").
        payload      Dizionario con la risposta raw dell'API.
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
            raise TypeError(
                f"RawRecord.payload deve essere un dict, ricevuto: {type(self.payload)}"
            )


# ---------------------------------------------------------------------------
# Record — modello normalizzato
# ---------------------------------------------------------------------------

@dataclass
class Record:
    """
    Modello dati unico e normalizzato della pipeline.

    Prodotto dal normalizer a partire da un RawRecord.
    Attraversa cleaner → deduplicator → enricher → exporter senza cambiare schema.

    Campi canonici (obbligatori):
        source   Sorgente normalizzata (es. "news", "youtube").
        query    Query usata per raccogliere questo record.
        target   Entità analizzata (es. "Elon Musk").
        title    Titolo dell'articolo / video / pagina.
        text     Corpo del testo (snippet o contenuto completo).
        date     Data di pubblicazione "YYYY-MM-DD", None se assente.
        url      URL canonico del contenuto.

    Campi estesi (opzionali — aggiungere qui i futuri arricchimenti):
        author       Autore / canale.
        language     Codice lingua ISO 639-1 (es. "it", "en").
        domain       Dominio estratto dall'URL (es. "bbc.co.uk").
        retrieved_at Timestamp ISO 8601 della raccolta.

    Metriche di engagement (rilevanti solo per social/video):
        views_count, likes_count, comments_count

    Analisi (opzionali, prodotti da step successivi):
        sentiment    Score sentiment nel range [-1.0, 1.0]. None se non calcolato.

    Metadati pipeline (non inclusi nell'export finale):
        raw_payload  Copia del payload grezzo per tracciabilità e debug.
    """

    # --- Campi canonici (obbligatori) ---
    source: str
    query: str
    target: str
    title: str
    text: str
    date: str | None        # "YYYY-MM-DD" oppure None
    url: str

    # --- Campi estesi (opzionali) ---
    author: str | None      = None
    language: str | None    = None
    domain: str             = ""
    retrieved_at: str       = ""

    # --- Metriche engagement ---
    views_count: int | None    = None
    likes_count: int | None    = None
    comments_count: int | None = None

    # --- Analisi opzionali (prodotte da step successivi alla normalizzazione) ---
    sentiment: float | None = None  # [-1.0, 1.0]; None = non ancora calcolato

    # --- Metadati pipeline (esclusi dall'export) ---
    raw_payload: dict[str, Any] = field(default_factory=dict, repr=False)

    # Campi esclusi dall'export: dichiarati a livello di classe, non come field().
    # In questo modo asdict() non li tocca e non serve nessun pop() sull'output.
    _EXPORT_EXCLUDE: ClassVar[frozenset[str]] = frozenset({"raw_payload"})

    # Ordine canonico dei campi nell'export CSV (definito una volta sola qui).
    _EXPORT_FIELDS: ClassVar[tuple[str, ...]] = (
        "source", "query", "target",
        "title", "text", "date", "url",
        "author", "language", "domain", "retrieved_at",
        "views_count", "likes_count", "comments_count",
        "sentiment",
    )

    def __post_init__(self) -> None:
        if not self.source:
            raise ValueError("Record.source non può essere vuoto")
        if not self.target:
            raise ValueError("Record.target non può essere vuoto")
        if not self.url:
            raise ValueError("Record.url non può essere vuoto")
        if self.date is not None and not _DATE_RE.match(self.date):
            raise ValueError(
                f"Record.date deve essere 'YYYY-MM-DD', ricevuto: '{self.date}'"
            )
        if self.sentiment is not None and not (-1.0 <= self.sentiment <= 1.0):
            raise ValueError(
                f"Record.sentiment deve essere in [-1.0, 1.0], ricevuto: {self.sentiment}"
            )

    # ------------------------------------------------------------------
    # Serializzazione
    # ------------------------------------------------------------------

    def to_dict(self, include_raw: bool = False) -> dict[str, Any]:
        """
        Converte il record in dizionario serializzabile (JSON-safe).

        Args:
            include_raw: se True include raw_payload (utile per debug/storage raw).
                         Default False — gli exporter NON devono includere il raw.

        Returns:
            Dizionario con i campi del record, pronto per json.dumps().
        """
        d = asdict(self)
        if not include_raw:
            for key in self._EXPORT_EXCLUDE:
                d.pop(key, None)
        return d

    def to_json(self, include_raw: bool = False, **kwargs: Any) -> str:
        """Serializza il record in stringa JSON."""
        return json.dumps(self.to_dict(include_raw=include_raw), ensure_ascii=False, **kwargs)

    @classmethod
    def export_fields(cls) -> tuple[str, ...]:
        """
        Restituisce l'ordine canonico dei campi per l'export CSV.
        Usato da CsvExporter per definire i fieldnames in modo deterministico.
        """
        return cls._EXPORT_FIELDS
