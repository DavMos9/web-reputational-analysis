"""
storage/raw_store.py

Salva i RawRecord grezzi su disco in formato JSON.

Responsabilità:
- serializzare RawRecord come dizionari
- scrivere il file in data/raw/
- NON trasformare né interpretare il contenuto

Il file raw è immutabile dopo la scrittura: rappresenta
lo stato esatto della risposta API al momento della raccolta.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from models import RawRecord
from utils.slugify import target_slug

log = logging.getLogger(__name__)


class RawStore:
    """
    Persiste i RawRecord grezzi in data/raw/.

    Rispetta RawStoreProtocol definito in pipeline/runner.py.

    Args:
        base_dir: directory radice del progetto.
                  I file vengono scritti in base_dir/data/raw/.
    """

    def __init__(self, base_dir: Path) -> None:
        self._raw_dir = base_dir / "data" / "raw"

    def save(self, records: list[RawRecord], target: str, timestamp: str) -> None:
        """
        Serializza e scrive i RawRecord in un file JSON.

        Nome file: {target_slug}_{timestamp}_raw.json
        Es: elon_musk_20260409T120000Z_raw.json

        Args:
            records:   lista di RawRecord da salvare.
            target:    entità analizzata (usata per il nome file).
            timestamp: stringa timestamp (es. "20260409T120000Z").
        """
        if not records:
            log.warning("[RawStore] Nessun record da salvare.")
            return

        self._raw_dir.mkdir(parents=True, exist_ok=True)

        slug = target_slug(target)
        path = self._raw_dir / f"{slug}_{timestamp}_raw.json"

        payload = [self._serialize(r) for r in records]

        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
            log.info("[RawStore] Salvati %d record in: %s", len(records), path)
        except OSError as e:
            log.error("[RawStore] Errore scrittura file '%s': %s", path, e)
            raise

    # ------------------------------------------------------------------

    @staticmethod
    def _serialize(raw: RawRecord) -> dict:
        """Converte un RawRecord in dizionario JSON-serializzabile."""
        return {
            "source":       raw.source,
            "query":        raw.query,
            "target":       raw.target,
            "retrieved_at": raw.retrieved_at,
            "payload":      raw.payload,
        }
