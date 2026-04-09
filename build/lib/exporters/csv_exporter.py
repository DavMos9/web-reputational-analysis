"""
exporters/csv_exporter.py

Esporta i Record finali in formato CSV in data/final/.

I fieldnames sono definiti da Record.export_fields() per garantire
che l'ordine delle colonne sia sempre canonico e non dipenda
dall'implementazione del dizionario.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from models import Record
from utils.slugify import target_slug

log = logging.getLogger(__name__)


class CsvExporter:
    """
    Scrive i Record finali in un file .csv.

    Rispetta ExporterProtocol definito in pipeline/runner.py.

    Args:
        base_dir: directory radice del progetto.
                  I file vengono scritti in base_dir/data/final/.
    """

    def __init__(self, base_dir: Path) -> None:
        self._final_dir = base_dir / "data" / "final"

    def export(self, records: list[Record], target: str, timestamp: str) -> None:
        """
        Scrive i Record in un file CSV con intestazione.

        Nome file: {target_slug}_{timestamp}_final.csv
        Es: elon_musk_20260409T120000Z_final.csv

        Args:
            records:   lista di Record da esportare.
            target:    entità analizzata (usata per il nome file).
            timestamp: stringa timestamp (es. "20260409T120000Z").
        """
        if not records:
            log.warning("[CsvExporter] Nessun record da esportare.")
            return

        self._final_dir.mkdir(parents=True, exist_ok=True)

        slug = target_slug(target)
        path = self._final_dir / f"{slug}_{timestamp}_final.csv"
        fieldnames = Record.export_fields()

        try:
            with open(path, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=fieldnames,
                    extrasaction="ignore",  # ignora campi non in fieldnames
                )
                writer.writeheader()
                for record in records:
                    writer.writerow(record.to_dict())
            log.info("[CsvExporter] Esportati %d record in: %s", len(records), path)
        except OSError as e:
            log.error("[CsvExporter] Errore scrittura '%s': %s", path, e)
            raise
