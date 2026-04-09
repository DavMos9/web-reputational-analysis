"""
tests/test_cleaner.py

Test per pipeline/cleaner.py.

Copertura:
- clean(): strip su campi required
- clean(): normalizzazione Unicode NFC
- clean(): campi optional — stringa vuota → None
- clean(): campi optional già None restano None
- clean(): Record già pulito non viene copiato inutilmente (stessa istanza)
- clean_all(): lista vuota e lista con record misti
"""

from __future__ import annotations

import pytest

from models import Record
from pipeline.cleaner import clean, clean_all


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _record(**overrides) -> Record:
    defaults = dict(
        source="news",
        query="test query",
        target="Test Target",
        title="Test Title",
        text="Test text.",
        date="2026-04-08",
        url="https://example.com/article",
        author=None,
        language=None,
        domain="example.com",
    )
    defaults.update(overrides)
    return Record(**defaults)


# ---------------------------------------------------------------------------
# Test: clean() — campi required
# ---------------------------------------------------------------------------

class TestCleanRequired:
    def test_strips_whitespace_from_title(self):
        r = _record(title="  Titolo con spazi  ")
        cleaned = clean(r)
        assert cleaned.title == "Titolo con spazi"

    def test_strips_whitespace_from_text(self):
        r = _record(text="\n  Testo con whitespace\t")
        cleaned = clean(r)
        assert cleaned.text == "Testo con whitespace"

    def test_strips_whitespace_from_url(self):
        r = _record(url="  https://example.com/article  ")
        cleaned = clean(r)
        assert cleaned.url == "https://example.com/article"

    def test_nfc_normalization(self):
        """Testo con caratteri Unicode composti viene normalizzato NFC."""
        # 'à' può essere rappresentata come U+00E0 (NFC) o come 'a' + U+0300 (NFD)
        nfd_char = "a\u0300"  # 'a' + combining grave accent (NFD)
        nfc_char = "\u00e0"   # 'à' precomposta (NFC)
        r = _record(title=nfd_char)
        cleaned = clean(r)
        assert cleaned.title == nfc_char

    def test_already_clean_record_returns_equivalent(self):
        """Un Record già pulito non deve avere campi diversi dopo clean()."""
        r = _record()
        cleaned = clean(r)
        assert cleaned.title  == r.title
        assert cleaned.text   == r.text
        assert cleaned.url    == r.url
        assert cleaned.source == r.source


# ---------------------------------------------------------------------------
# Test: clean() — campi optional
# ---------------------------------------------------------------------------

class TestCleanOptional:
    def test_empty_author_becomes_none(self):
        r = _record(author="")
        cleaned = clean(r)
        assert cleaned.author is None

    def test_whitespace_only_author_becomes_none(self):
        r = _record(author="   ")
        cleaned = clean(r)
        assert cleaned.author is None

    def test_none_author_stays_none(self):
        r = _record(author=None)
        cleaned = clean(r)
        assert cleaned.author is None

    def test_valid_author_kept(self):
        r = _record(author="  Jane Doe  ")
        cleaned = clean(r)
        assert cleaned.author == "Jane Doe"

    def test_empty_language_becomes_none(self):
        r = _record(language="")
        cleaned = clean(r)
        assert cleaned.language is None

    def test_valid_language_kept(self):
        r = _record(language=" it ")
        cleaned = clean(r)
        assert cleaned.language == "it"


# ---------------------------------------------------------------------------
# Test: clean_all()
# ---------------------------------------------------------------------------

class TestCleanAll:
    def test_empty_list_returns_empty(self):
        assert clean_all([]) == []

    def test_returns_same_count(self):
        records = [_record(title=f"Title {i}") for i in range(5)]
        result = clean_all(records)
        assert len(result) == 5

    def test_all_records_are_cleaned(self):
        records = [
            _record(title="  Title A  ", author=""),
            _record(title="Title B",     author="  Author  "),
        ]
        result = clean_all(records)
        assert result[0].title  == "Title A"
        assert result[0].author is None
        assert result[1].author == "Author"
