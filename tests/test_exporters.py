"""
tests/test_exporters.py

Test per exporters/json_exporter.py (JsonExporter) e
exporters/csv_exporter.py (CsvExporter).

Copertura JSON:
- export(): crea il file in data/final/
- export(): nome file rispetta il pattern {slug}_{timestamp}_final.json
- export(): JSON valido, lista di oggetti
- export(): raw_payload escluso dall'output
- export(): tutti i campi canonici presenti
- export(): lista vuota → nessun file creato
- export(): crea data/final/ se mancante
- export(): OSError propagata

Copertura CSV:
- export(): crea il file in data/final/
- export(): nome file rispetta il pattern {slug}_{timestamp}_final.csv
- export(): intestazione CSV corrisponde a Record.export_fields()
- export(): righe CSV con valori corretti
- export(): lista vuota → nessun file creato
- export(): crea data/final/ se mancante
- export(): più record → più righe
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path

import pytest

from models import Record
from exporters.json_exporter import JsonExporter
from exporters.csv_exporter import CsvExporter


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _record(
    source: str = "news",
    url: str = "https://example.com/article",
    title: str = "Test Article",
    raw: bool = True,
) -> Record:
    return Record(
        source=source,
        query="test query",
        target="Test Target",
        title=title,
        text="Test text content.",
        date="2026-04-08",
        url=url,
        author="Test Author",
        language="en",
        domain="example.com",
        retrieved_at="2026-04-08T10:00:00+00:00",
        raw_payload={"original": "data"} if raw else {},
    )


TIMESTAMP = "20260409T120000Z"
TARGET    = "Elon Musk"


# ---------------------------------------------------------------------------
# Test: JsonExporter
# ---------------------------------------------------------------------------

class TestJsonExporterFile:

    def test_creates_file_in_final_directory(self, tmp_path: Path):
        exp = JsonExporter(base_dir=tmp_path)
        exp.export([_record()], target=TARGET, timestamp=TIMESTAMP)

        final_dir = tmp_path / "data" / "final"
        files = list(final_dir.glob("*.json"))
        assert len(files) == 1

    def test_filename_pattern(self, tmp_path: Path):
        exp = JsonExporter(base_dir=tmp_path)
        exp.export([_record()], target=TARGET, timestamp=TIMESTAMP)

        final_dir = tmp_path / "data" / "final"
        filename = list(final_dir.glob("*.json"))[0].name
        # atteso: elon_musk_20260409T120000Z_final.json
        assert re.match(r"elon_musk_20260409T120000Z_final\.json", filename)

    def test_creates_final_directory_if_missing(self, tmp_path: Path):
        exp = JsonExporter(base_dir=tmp_path)
        assert not (tmp_path / "data" / "final").exists()

        exp.export([_record()], target=TARGET, timestamp=TIMESTAMP)

        assert (tmp_path / "data" / "final").is_dir()

    def test_empty_list_creates_no_file(self, tmp_path: Path):
        exp = JsonExporter(base_dir=tmp_path)
        exp.export([], target=TARGET, timestamp=TIMESTAMP)

        final_dir = tmp_path / "data" / "final"
        files = list(final_dir.glob("*.json")) if final_dir.exists() else []
        assert files == []


class TestJsonExporterContent:

    def _load(self, tmp_path: Path) -> list[dict]:
        path = next((tmp_path / "data" / "final").glob("*.json"))
        return json.loads(path.read_text(encoding="utf-8"))

    def test_output_is_json_list(self, tmp_path: Path):
        exp = JsonExporter(base_dir=tmp_path)
        exp.export([_record()], target=TARGET, timestamp=TIMESTAMP)

        data = self._load(tmp_path)
        assert isinstance(data, list)
        assert len(data) == 1

    def test_multiple_records_all_written(self, tmp_path: Path):
        exp = JsonExporter(base_dir=tmp_path)
        records = [_record(url=f"https://example.com/{i}") for i in range(4)]
        exp.export(records, target=TARGET, timestamp=TIMESTAMP)

        data = self._load(tmp_path)
        assert len(data) == 4

    def test_raw_payload_excluded(self, tmp_path: Path):
        """raw_payload non deve apparire nel file JSON esportato."""
        exp = JsonExporter(base_dir=tmp_path)
        exp.export([_record(raw=True)], target=TARGET, timestamp=TIMESTAMP)

        record_dict = self._load(tmp_path)[0]
        assert "raw_payload" not in record_dict

    def test_canonical_fields_present(self, tmp_path: Path):
        """Tutti i campi di Record._EXPORT_FIELDS devono essere nel JSON."""
        exp = JsonExporter(base_dir=tmp_path)
        exp.export([_record()], target=TARGET, timestamp=TIMESTAMP)

        record_dict = self._load(tmp_path)[0]
        for field_name in Record.export_fields():
            assert field_name in record_dict, f"Campo mancante: {field_name}"

    def test_field_values_correct(self, tmp_path: Path):
        exp = JsonExporter(base_dir=tmp_path)
        exp.export([_record(source="gdelt", title="Special Title")], target=TARGET, timestamp=TIMESTAMP)

        record_dict = self._load(tmp_path)[0]
        assert record_dict["source"] == "gdelt"
        assert record_dict["title"] == "Special Title"
        assert record_dict["url"]   == "https://example.com/article"
        assert record_dict["date"]  == "2026-04-08"

    def test_oserror_propagated(self, tmp_path: Path, monkeypatch):
        """Se open() solleva OSError, deve essere rilanciata."""
        exp = JsonExporter(base_dir=tmp_path)
        (tmp_path / "data" / "final").mkdir(parents=True)

        import builtins
        real_open = builtins.open

        def _fail_open(path, *args, **kwargs):
            if str(path).endswith(".json"):
                raise OSError("Disk full")
            return real_open(path, *args, **kwargs)

        monkeypatch.setattr(builtins, "open", _fail_open)

        with pytest.raises(OSError, match="Disk full"):
            exp.export([_record()], target=TARGET, timestamp=TIMESTAMP)


# ---------------------------------------------------------------------------
# Test: CsvExporter
# ---------------------------------------------------------------------------

class TestCsvExporterFile:

    def test_creates_file_in_final_directory(self, tmp_path: Path):
        exp = CsvExporter(base_dir=tmp_path)
        exp.export([_record()], target=TARGET, timestamp=TIMESTAMP)

        final_dir = tmp_path / "data" / "final"
        files = list(final_dir.glob("*.csv"))
        assert len(files) == 1

    def test_filename_pattern(self, tmp_path: Path):
        exp = CsvExporter(base_dir=tmp_path)
        exp.export([_record()], target=TARGET, timestamp=TIMESTAMP)

        final_dir = tmp_path / "data" / "final"
        filename = list(final_dir.glob("*.csv"))[0].name
        # atteso: elon_musk_20260409T120000Z_final.csv
        assert re.match(r"elon_musk_20260409T120000Z_final\.csv", filename)

    def test_creates_final_directory_if_missing(self, tmp_path: Path):
        exp = CsvExporter(base_dir=tmp_path)
        assert not (tmp_path / "data" / "final").exists()

        exp.export([_record()], target=TARGET, timestamp=TIMESTAMP)

        assert (tmp_path / "data" / "final").is_dir()

    def test_empty_list_creates_no_file(self, tmp_path: Path):
        exp = CsvExporter(base_dir=tmp_path)
        exp.export([], target=TARGET, timestamp=TIMESTAMP)

        final_dir = tmp_path / "data" / "final"
        files = list(final_dir.glob("*.csv")) if final_dir.exists() else []
        assert files == []


class TestCsvExporterContent:

    def _load_csv(self, tmp_path: Path) -> tuple[list[str], list[dict]]:
        """Restituisce (fieldnames, rows) dal file CSV prodotto."""
        path = next((tmp_path / "data" / "final").glob("*.csv"))
        with open(path, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            rows = list(reader)
        return list(fieldnames), rows

    def test_header_matches_export_fields(self, tmp_path: Path):
        """L'intestazione CSV deve corrispondere esattamente a Record.export_fields()."""
        exp = CsvExporter(base_dir=tmp_path)
        exp.export([_record()], target=TARGET, timestamp=TIMESTAMP)

        fieldnames, _ = self._load_csv(tmp_path)
        assert fieldnames == list(Record.export_fields())

    def test_single_record_written(self, tmp_path: Path):
        exp = CsvExporter(base_dir=tmp_path)
        exp.export([_record()], target=TARGET, timestamp=TIMESTAMP)

        _, rows = self._load_csv(tmp_path)
        assert len(rows) == 1

    def test_multiple_records_all_written(self, tmp_path: Path):
        exp = CsvExporter(base_dir=tmp_path)
        records = [_record(url=f"https://example.com/{i}", title=f"Title {i}") for i in range(3)]
        exp.export(records, target=TARGET, timestamp=TIMESTAMP)

        _, rows = self._load_csv(tmp_path)
        assert len(rows) == 3

    def test_field_values_correct(self, tmp_path: Path):
        exp = CsvExporter(base_dir=tmp_path)
        exp.export([_record(source="wikipedia", title="Wiki Article")], target=TARGET, timestamp=TIMESTAMP)

        _, rows = self._load_csv(tmp_path)
        row = rows[0]
        assert row["source"] == "wikipedia"
        assert row["title"]  == "Wiki Article"
        assert row["url"]    == "https://example.com/article"
        assert row["date"]   == "2026-04-08"

    def test_raw_payload_not_in_csv(self, tmp_path: Path):
        """raw_payload non deve comparire come colonna nel CSV."""
        exp = CsvExporter(base_dir=tmp_path)
        exp.export([_record(raw=True)], target=TARGET, timestamp=TIMESTAMP)

        fieldnames, _ = self._load_csv(tmp_path)
        assert "raw_payload" not in fieldnames

    def test_oserror_propagated(self, tmp_path: Path, monkeypatch):
        """Se open() solleva OSError, deve essere rilanciata."""
        exp = CsvExporter(base_dir=tmp_path)
        (tmp_path / "data" / "final").mkdir(parents=True)

        import builtins
        real_open = builtins.open

        def _fail_open(path, *args, **kwargs):
            if str(path).endswith(".csv"):
                raise OSError("No space left")
            return real_open(path, *args, **kwargs)

        monkeypatch.setattr(builtins, "open", _fail_open)

        with pytest.raises(OSError, match="No space left"):
            exp.export([_record()], target=TARGET, timestamp=TIMESTAMP)
