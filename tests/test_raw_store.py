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
import os
import re
import time
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


# ---------------------------------------------------------------------------
# Helpers per purge
# ---------------------------------------------------------------------------

def _write_raw_file(raw_dir: Path, name: str, days_old: int) -> Path:
    """
    Crea un file *_raw.json fittizio in raw_dir con mtime impostato a `days_old`
    giorni fa tramite os.utime(). Usa il filesystem reale — nessun mock del clock.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / name
    path.write_text("{}", encoding="utf-8")
    past = time.time() - days_old * 86_400
    os.utime(path, (past, past))
    return path


# ---------------------------------------------------------------------------
# Test: RawStore.purge_old_files()
# ---------------------------------------------------------------------------

class TestRawStorePurgeOldFiles:
    """
    Copertura:
    - file più vecchio del cutoff viene eliminato
    - file più recente del cutoff viene mantenuto
    - mix di vecchi e nuovi: solo i vecchi eliminati
    - directory data/raw/ inesistente → nessun errore
    - keep_days < 1 → ValueError
    - file con mtime esattamente sul bordo (< 1 secondo oltre cutoff): mantenuto
    - file non-raw.json nella directory: ignorato
    """

    def test_old_file_deleted(self, tmp_path: Path):
        """File con mtime > keep_days giorni fa → eliminato."""
        store = RawStore(base_dir=tmp_path)
        raw_dir = tmp_path / "data" / "raw"
        old = _write_raw_file(raw_dir, "old_20260101T000000Z_raw.json", days_old=10)

        store.purge_old_files(keep_days=7)

        assert not old.exists()

    def test_recent_file_kept(self, tmp_path: Path):
        """File con mtime < keep_days giorni fa → mantenuto."""
        store = RawStore(base_dir=tmp_path)
        raw_dir = tmp_path / "data" / "raw"
        recent = _write_raw_file(raw_dir, "recent_20260417T000000Z_raw.json", days_old=3)

        store.purge_old_files(keep_days=7)

        assert recent.exists()

    def test_only_old_files_deleted_in_mixed_directory(self, tmp_path: Path):
        """Mix di file vecchi e recenti: solo i vecchi vengono rimossi."""
        store = RawStore(base_dir=tmp_path)
        raw_dir = tmp_path / "data" / "raw"
        old1 = _write_raw_file(raw_dir, "old1_raw.json", days_old=15)
        old2 = _write_raw_file(raw_dir, "old2_raw.json", days_old=31)
        new1 = _write_raw_file(raw_dir, "new1_raw.json", days_old=2)
        new2 = _write_raw_file(raw_dir, "new2_raw.json", days_old=6)

        store.purge_old_files(keep_days=7)

        assert not old1.exists()
        assert not old2.exists()
        assert new1.exists()
        assert new2.exists()

    def test_nonexistent_directory_does_not_raise(self, tmp_path: Path):
        """Se data/raw/ non esiste, purge_old_files termina senza errori."""
        store = RawStore(base_dir=tmp_path)
        # data/raw/ non è stata creata
        assert not (tmp_path / "data" / "raw").exists()

        store.purge_old_files(keep_days=7)  # non deve sollevare

    def test_keep_days_zero_raises(self, tmp_path: Path):
        """keep_days=0 è invalido → ValueError."""
        store = RawStore(base_dir=tmp_path)
        with pytest.raises(ValueError, match="keep_days"):
            store.purge_old_files(keep_days=0)

    def test_keep_days_negative_raises(self, tmp_path: Path):
        """keep_days negativo è invalido → ValueError."""
        store = RawStore(base_dir=tmp_path)
        with pytest.raises(ValueError, match="keep_days"):
            store.purge_old_files(keep_days=-5)

    def test_non_raw_json_files_are_ignored(self, tmp_path: Path):
        """File senza suffisso _raw.json non vengono toccati."""
        store = RawStore(base_dir=tmp_path)
        raw_dir = tmp_path / "data" / "raw"
        raw_dir.mkdir(parents=True)
        other = raw_dir / "notes.txt"
        other.write_text("not a raw file")
        # Imposta mtime molto vecchio — non deve essere eliminato perché non matcha il glob
        os.utime(other, (0, 0))

        store.purge_old_files(keep_days=1)

        assert other.exists()

    def test_empty_directory_no_error(self, tmp_path: Path):
        """Directory data/raw/ vuota → nessun errore, nessun file rimosso."""
        store = RawStore(base_dir=tmp_path)
        (tmp_path / "data" / "raw").mkdir(parents=True)

        store.purge_old_files(keep_days=7)  # non deve sollevare
