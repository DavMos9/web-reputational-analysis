"""
collectors/base.py

Interfaccia comune per tutti i collector della pipeline.

Ogni collector concreto deve:
- ereditare da BaseCollector
- implementare il metodo `collect()`
- restituire list[RawRecord] con il payload grezzo dell'API
- NON fare trasformazioni sui dati (nessuna normalizzazione di date, URL, ecc.)
- gestire errori API con logging, senza propagare eccezioni non gestite
"""

from __future__ import annotations

import inspect
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone

from models import RawRecord

log = logging.getLogger(__name__)


class BaseCollector(ABC):
    """
    Classe base per tutti i collector.

    Un collector ha una sola responsabilità: chiamare la sorgente dati
    e restituire i payload grezzi come List[RawRecord].
    Non normalizza, non pulisce, non salva.

    Attributi di classe da sovrascrivere:
        source_id   Identificatore unico della sorgente (es. "news", "gdelt").
                    Deve corrispondere alle chiavi usate dal normalizer.
    """

    source_id: str = ""  # override obbligatorio nelle sottoclassi

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        # Le classi astratte intermedie (es. una futura RSSBaseCollector che
        # aggiunge utility comuni ma non implementa collect()) non hanno ancora
        # un source_id concreto e non devono essere penalizzate dal check.
        # Il controllo viene applicato solo alle sottoclassi concrete (non astratte).
        if not inspect.isabstract(cls) and not getattr(cls, "source_id", ""):
            raise TypeError(
                f"{cls.__name__} deve definire l'attributo di classe 'source_id'"
            )

    @abstractmethod
    def collect(self, target: str, query: str, max_results: int = 20, **kwargs: object) -> list[RawRecord]:
        """
        Raccoglie dati dalla sorgente per il target e la query indicati.

        Args:
            target:      entità oggetto dell'analisi (es. "Elon Musk").
            query:       stringa di ricerca (es. "Elon Musk Tesla").
            max_results: numero massimo di risultati da raccogliere.
                         Ogni collector applica internamente il proprio limite di API.
            **kwargs:    parametri aggiuntivi specifici della sorgente (es. lang).

        Returns:
            Lista di RawRecord con il payload grezzo dell'API.
            Lista vuota in caso di errore o nessun risultato.
        """

    # ------------------------------------------------------------------
    # Utility condivise
    # ------------------------------------------------------------------

    def _now_iso(self) -> str:
        """Restituisce il timestamp corrente in formato ISO 8601 UTC."""
        return datetime.now(timezone.utc).isoformat()

    def _make_raw(
        self,
        target: str,
        query: str,
        payload: dict,
    ) -> RawRecord:
        """
        Factory per costruire un RawRecord in modo uniforme.

        Args:
            target:  entità analizzata.
            query:   query usata per la raccolta.
            payload: dizionario con la risposta grezza dell'API.

        Returns:
            RawRecord istanziato con retrieved_at impostato ora.
        """
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
