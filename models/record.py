"""models/record.py — RawRecord (payload grezzo dai collector) e Record (modello normalizzato)."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, asdict
from typing import Any, ClassVar

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass
class RawRecord:
    """Payload grezzo di un collector. Il normalizer legge da payload senza trasformazioni."""

    source: str
    query: str
    target: str
    payload: dict[str, Any]
    retrieved_at: str

    def __post_init__(self) -> None:
        if not self.source:
            raise ValueError("RawRecord.source non può essere vuoto")
        if not self.target:
            raise ValueError("RawRecord.target non può essere vuoto")
        if not isinstance(self.payload, dict):
            raise TypeError(
                f"RawRecord.payload deve essere un dict, ricevuto: {type(self.payload)}"
            )


@dataclass
class Record:
    """Modello normalizzato della pipeline. Attraversa cleaner → enricher → exporter."""

    source: str
    query: str
    target: str
    title: str
    text: str
    date: str | None
    url: str

    topic: str           = ""   # topic originale (es. "Euphoria"), calcolato dal normalizer registry
    author: str | None      = None
    language: str | None    = None
    domain: str             = ""
    retrieved_at: str       = ""

    views_count: int | None    = None
    likes_count: int | None    = None
    comments_count: int | None = None

    sentiment: float | None = None  # [-1.0, 1.0]

    raw_payload: dict[str, Any] = field(default_factory=dict, repr=False)

    _EXPORT_EXCLUDE: ClassVar[frozenset[str]] = frozenset({"raw_payload"})

    _EXPORT_FIELDS: ClassVar[tuple[str, ...]] = (
        "source", "query", "topic", "target",
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

    def to_dict(self, include_raw: bool = False) -> dict[str, Any]:
        """Dizionario JSON-safe. include_raw=True aggiunge raw_payload (debug/storage)."""
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
        """Ordine canonico dei campi per l'export CSV."""
        return cls._EXPORT_FIELDS
