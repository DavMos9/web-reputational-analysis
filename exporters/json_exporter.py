"""
exporters/json_exporter.py

Esporta i Record finali in formato JSON in data/final/.

Usa Record.to_dict() che esclude raw_payload per default,
mantenendo il file di output pulito e privo di dati grezzi.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from models import Record
from utils.slugify import target_slug

log = logging.getLogger(__name__)


class JsonExporter:
    """
    Scrive i Record finali in un file .json.

    Rispetta ExporterProtocol definito in pipeline/runner.py.

    Args:
        base_dir: directory radice del progetto.
                  I file vengono scritti in base_dir/data/final/.
    """

    def __init__(self, base_dir: Path) -> None:
        self._final_dir = base_dir / "data" / "final"

    def export(self, records: list[Record], target: str, timestamp: str) -> None:
        """
        Serializza e scrive i Record in un file JSON.

        Nome file: {target_slug}_{timestamp}_final.json
        Es: elon_musk_20260409T120000Z_final.json

        Args:
            records:   lista di Record da esportare.
            target:    entità analizzata (usata per il nome file).
            timestamp: stringa timestamp (es. "20260409T120000Z").
        """
        if not records:
            log.warning("[JsonExporter] Nessun record da esportare.")
            return

        self._final_dir.mkdir(parents=True, exist_ok=True)

        slug = target_slug(target)
        path = self._final_dir / f"{slug}_{timestamp}_final.json"

        data = [r.to_dict() for r in records]

        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            log.info("[JsonExporter] Esportati %d record in: %s", len(records), path)
        except OSError as e:
            log.error("[JsonExporter] Errore scrittura '%s': %s", path, e)
            raise
