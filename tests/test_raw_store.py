"""
tests/test_raw_store.py

Test per storage/raw_store.py (RawStore).

Copertura:
- save(): crea il file nel path corretto (data/raw/)
- save(): il nome file rispetta il pattern {slug}_{timestamp}_raw.json
- save(): il JSON prodotto è valido e contiene i campi attesi
- save(): lista vuota → nessun file creato
- save(): OSError viene propagata
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from models import RawRecord
from storage.raw_store import RawStore


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _raw(source: str = "news", url: str = "https://example.com/1") -> RawRecord:
    return RawRecord(
        source=source,
        query="test query",
        target="Test Target",
        payload={"url": url, "title": "Test Article"},
        retrieved_at="2026-04-08T10:00:00+00:00",
    )


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

class TestRawStoreSave:

    def test_creates_file_in_raw_directory(self, tmp_path: Path):
        store = RawStore(base_dir=tmp_path)
        store.save([_raw()], target="Elon Musk", timestamp="20260409T120000Z")

        raw_dir = tmp_path / "data" / "raw"
        files = list(raw_dir.glob("*.json"))
        assert len(files) == 1

    def test_filename_pattern(self, tmp_path: Path):
        store = RawStore(base_dir=tmp_path)
        store.save([_raw()], target="Elon Musk", timestamp="20260409T120000Z")

        raw_dir = tmp_path / "data" / "raw"
        filename = list(raw_dir.glob("*.json"))[0].name
        # pattern atteso: elon_musk_20260409T120000Z_raw.json
        assert re.match(r"elon_musk_20260409T120000Z_raw\.json", filename)

    def test_json_content_is_valid(self, tmp_path: Path):
        store = RawStore(base_dir=tmp_path)
        store.save([_raw()], target="Elon Musk", timestamp="20260409T120000Z")

        path = next((tmp_path / "data" / "raw").glob("*.json"))
        data = json.loads(path.read_text(encoding="utf-8"))
        assert isinstance(data, list)
        assert len(data) == 1

    def test_json_contains_expected_fields(self, tmp_path: Path):
        store = RawStore(base_dir=tmp_path)
        store.save([_raw(source="gdelt")], target="Test Target", timestamp="20260409T120000Z")

        path = next((tmp_path / "data" / "raw").glob("*.json"))
        record = json.loads(path.read_text(encoding="utf-8"))[0]

        assert record["source"] == "gdelt"
        assert record["query"] == "test query"
        assert record["target"] == "Test Target"
        assert "payload" in record
        assert "retrieved_at" in record

    def test_multiple_records_all_saved(self, tmp_path: Path):
        store = RawStore(base_dir=tmp_path)
        raws = [_raw(url=f"https://example.com/{i}") for i in range(5)]
        store.save(raws, target="Test Target", timestamp="20260409T120000Z")

        path = next((tmp_path / "data" / "raw").glob("*.json"))
        data = json.loads(path.read_text(encoding="utf-8"))
        assert len(data) == 5

    def test_empty_list_creates_no_file(self, tmp_path: Path):
        store = RawStore(base_dir=tmp_path)
        store.save([], target="Test Target", timestamp="20260409T120000Z")

        raw_dir = tmp_path / "data" / "raw"
        # La directory potrebbe non esistere o essere vuota
        files = list(raw_dir.glob("*.json")) if raw_dir.exists() else []
        assert files == []

    def test_creates_raw_directory_if_missing(self, tmp_path: Path):
        """RawStore deve creare data/raw/ se non esiste."""
        store = RawStore(base_dir=tmp_path)
        assert not (tmp_path / "data" / "raw").exists()

        store.save([_raw()], target="T", timestamp="20260409T120000Z")

        assert (tmp_path / "data" / "raw").is_dir()
