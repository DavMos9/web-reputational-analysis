"""collectors/base.py — Interfaccia comune per tutti i collector."""

from __future__ import annotations

import inspect
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone

from models import RawRecord

log = logging.getLogger(__name__)


class BaseCollector(ABC):
    """Classe base per tutti i collector. Chiama la sorgente e restituisce RawRecord."""

    source_id: str = ""  # override obbligatorio nelle sottoclassi

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        # Controlla solo le sottoclassi concrete: le astratte intermedie
        # non hanno ancora un source_id definito.
        if not inspect.isabstract(cls) and not getattr(cls, "source_id", ""):
            raise TypeError(
                f"{cls.__name__} deve definire l'attributo di classe 'source_id'"
            )

    @abstractmethod
    def collect(self, target: str, query: str, max_results: int = 20, **kwargs: object) -> list[RawRecord]:
        """Raccoglie dati dalla sorgente. Restituisce lista vuota su errore."""

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _make_raw(
        self,
        target: str,
        query: str,
        payload: dict,
    ) -> RawRecord:
        """Factory per costruire un RawRecord con retrieved_at impostato ora."""
        return RawRecord(
            source=self.source_id,
            query=query,
            target=target,
            payload=payload,
            retrieved_at=self._now_iso(),
        )

    def _log_collected(self, query: str, count: int) -> None:
        log.info("[%s] Raccolti %d record per query: '%s'", self.source_id, count, query)

    def _log_skip(self, reason: str) -> None:
        log.warning("[%s] Raccolta saltata: %s", self.source_id, reason)

    def _log_error(self, query: str, error: Exception) -> None:
        log.error("[%s] Errore per query '%s': %s", self.source_id, query, error)
