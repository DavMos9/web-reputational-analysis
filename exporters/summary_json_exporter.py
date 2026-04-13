"""
exporters/summary_json_exporter.py

Esporta un EntitySummary in formato JSON in data/final/.

Produce un file separato dal record-level export, con suffisso _summary.json.
Rispetta SummaryExporterProtocol definito in pipeline/runner.py.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from pipeline.aggregator import EntitySummary
from utils.slugify import target_slug

log = logging.getLogger(__name__)


class SummaryJsonExporter:
    """
    Scrive un EntitySummary in un file JSON.

    Args:
        base_dir: directory radice del progetto.
                  I file vengono scritti in base_dir/data/final/.
    """

    def __init__(self, base_dir: Path) -> None:
        self._final_dir = base_dir / "data" / "final"

    def export_summary(self, summary: EntitySummary, timestamp: str) -> None:
        """
        Serializza e scrive l'EntitySummary in un file JSON.

        Nome file: {entity_slug}_{timestamp}_summary.json
        Es: elon_musk_20260409T120000Z_summary.json

        Args:
            summary:   EntitySummary da esportare.
            timestamp: stringa timestamp (es. "20260409T120000Z").
        """
        self._final_dir.mkdir(parents=True, exist_ok=True)

        slug = target_slug(summary.entity)
        path = self._final_dir / f"{slug}_{timestamp}_summary.json"

        data = summary.to_dict()

        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            log.info("[SummaryJsonExporter] Summary esportato in: %s", path)
        except OSError as e:
            log.error("[SummaryJsonExporter] Errore scrittura '%s': %s", path, e)
            raise
