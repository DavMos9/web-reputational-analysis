"""
tests/test_summary_json_exporter.py

Test per exporters/summary_json_exporter.py.

Copertura:
- export_summary(): crea il file con nome corretto
- export_summary(): contenuto JSON valido e coerente con EntitySummary
- export_summary(): crea la directory data/final/ se non esiste
- export_summary(): date_range serializzato come {from, to}
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from models import Record
from pipeline.aggregator import aggregate, EntitySummary
from exporters.summary_json_exporter import SummaryJsonExporter


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _record(
    source: str = "news",
    sentiment: float | None = 0.5,
    date_str: str | None = "2026-04-10",
    url: str = "https://example.com/1",
) -> Record:
    return Record(
        source=source,
        query="test query",
        target="Test Entity",
        title="Test Title",
        text="Test text content for analysis.",
        date=date_str,
        url=url,
        sentiment=sentiment,
    )


def _summary() -> EntitySummary:
    return aggregate([
        _record(source="news", sentiment=0.6, url="https://a.com/1", date_str="2026-04-01"),
        _record(source="guardian", sentiment=0.3, url="https://b.com/2", date_str="2026-04-10"),
    ])


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

class TestSummaryJsonExporter:

    def test_creates_file_with_correct_name(self, tmp_path: Path):
        exporter = SummaryJsonExporter(tmp_path)
        summary = _summary()
        exporter.export_summary(summary, "20260410T120000Z")

        expected = tmp_path / "data" / "final" / "test_entity_20260410T120000Z_summary.json"
        assert expected.exists()

    def test_creates_directory_if_missing(self, tmp_path: Path):
        exporter = SummaryJsonExporter(tmp_path)
        final_dir = tmp_path / "data" / "final"
        assert not final_dir.exists()

        exporter.export_summary(_summary(), "20260410T120000Z")
        assert final_dir.exists()

    def test_valid_json_content(self, tmp_path: Path):
        exporter = SummaryJsonExporter(tmp_path)
        summary = _summary()
        exporter.export_summary(summary, "20260410T120000Z")

        path = tmp_path / "data" / "final" / "test_entity_20260410T120000Z_summary.json"
        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        assert data["entity"] == "Test Entity"
        assert data["record_count"] == 2
        assert isinstance(data["reputation_score"], float)
        assert data["trend"] in ("up", "down", "stable", "unknown")
        assert isinstance(data["source_distribution"], dict)

    def test_date_range_serialized_as_object(self, tmp_path: Path):
        exporter = SummaryJsonExporter(tmp_path)
        summary = _summary()
        exporter.export_summary(summary, "20260410T120000Z")

        path = tmp_path / "data" / "final" / "test_entity_20260410T120000Z_summary.json"
        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        assert data["date_range"] == {"from": "2026-04-01", "to": "2026-04-10"}

    def test_none_date_range(self, tmp_path: Path):
        summary = aggregate([_record(date_str=None)])
        exporter = SummaryJsonExporter(tmp_path)
        exporter.export_summary(summary, "20260410T120000Z")

        path = tmp_path / "data" / "final" / "test_entity_20260410T120000Z_summary.json"
        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        assert data["date_range"] is None

    def test_sentiment_fields_present(self, tmp_path: Path):
        exporter = SummaryJsonExporter(tmp_path)
        exporter.export_summary(_summary(), "20260410T120000Z")

        path = tmp_path / "data" / "final" / "test_entity_20260410T120000Z_summary.json"
        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        assert "sentiment_avg" in data
        assert "sentiment_std" in data
        assert "records_with_sentiment" in data
