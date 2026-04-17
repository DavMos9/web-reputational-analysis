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
import time
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

    def purge_old_files(self, keep_days: int) -> None:
        """
        Elimina i file raw più vecchi di `keep_days` giorni.

        La scelta si basa sul mtime del file (data di modifica),
        che coincide con la data di scrittura per file immutabili come i raw.
        File non-JSON o non presenti vengono ignorati senza errori.

        Args:
            keep_days: numero di giorni da mantenere. Deve essere >= 1.

        Raises:
            ValueError: se keep_days < 1.
        """
        if keep_days < 1:
            raise ValueError(f"keep_days deve essere >= 1, ricevuto: {keep_days}")

        if not self._raw_dir.exists():
            log.debug("[RawStore] data/raw/ non esiste, nessuna pulizia necessaria.")
            return

        cutoff = time.time() - keep_days * 86_400  # secondi
        deleted = 0

        for path in self._raw_dir.glob("*_raw.json"):
            try:
                if path.stat().st_mtime < cutoff:
                    path.unlink()
                    log.info("[RawStore] Eliminato file vecchio: %s", path.name)
                    deleted += 1
            except OSError as e:
                log.warning("[RawStore] Impossibile eliminare '%s': %s", path.name, e)

        if deleted:
            log.info("[RawStore] Pulizia completata: %d file eliminati (keep_days=%d).", deleted, keep_days)
        else:
            log.info("[RawStore] Pulizia: nessun file da eliminare (keep_days=%d).", keep_days)

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
