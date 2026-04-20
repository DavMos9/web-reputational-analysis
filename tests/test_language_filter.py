"""
tests/test_language_filter.py

Test per pipeline/language_filter.py.

Copertura:
- languages=None → lista inalterata
- languages=[] → lista inalterata
- tutti i record nella lingua ammessa → tutti mantenuti
- tutti i record in lingue diverse → tutti scartati
- misto → solo le lingue ammesse sopravvivono
- record con language=None → sempre mantenuti (policy esplicita)
- case-insensitivity: 'EN', 'En' → matchano 'en'
- più lingue ammesse: ['en', 'it'] → entrambe mantenute
"""

from __future__ import annotations

import pytest

from models import Record
from pipeline.language_filter import filter_by_language


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _rec(language: str | None, title: str = "T") -> Record:
    return Record(
        source="news",
        query="q",
        target="Target",
        title=title,
        text="Some text long enough to pass cleaner thresholds in normal runs.",
        date=None,
        url="https://example.com/" + (title or "x"),
        domain="example.com",
        language=language,
    )


# ---------------------------------------------------------------------------
# Test: nessun filtro attivo
# ---------------------------------------------------------------------------

class TestNoFilter:
    def test_none_languages_returns_all(self):
        records = [_rec("en"), _rec("it"), _rec("ar"), _rec(None)]
        kept, dropped = filter_by_language(records, None)
        assert len(kept) == 4
        assert dropped == 0

    def test_empty_list_languages_returns_all(self):
        records = [_rec("en"), _rec("fr")]
        kept, dropped = filter_by_language(records, [])
        assert len(kept) == 2
        assert dropped == 0

    def test_empty_records(self):
        kept, dropped = filter_by_language([], ["en"])
        assert kept == []
        assert dropped == 0


# ---------------------------------------------------------------------------
# Test: filtro attivo
# ---------------------------------------------------------------------------

class TestFilterActive:
    def test_all_matching_kept(self):
        records = [_rec("en", "a"), _rec("en", "b"), _rec("en", "c")]
        kept, dropped = filter_by_language(records, ["en"])
        assert len(kept) == 3
        assert dropped == 0

    def test_all_non_matching_dropped(self):
        records = [_rec("ar", "a"), _rec("fr", "b"), _rec("zh", "c")]
        kept, dropped = filter_by_language(records, ["en"])
        assert kept == []
        assert dropped == 3

    def test_mixed_single_language(self):
        records = [
            _rec("en", "english"),
            _rec("fr", "french"),
            _rec("ar", "arabic"),
            _rec("en", "english2"),
        ]
        kept, dropped = filter_by_language(records, ["en"])
        titles = {r.title for r in kept}
        assert titles == {"english", "english2"}
        assert dropped == 2

    def test_multiple_languages_allowed(self):
        records = [
            _rec("en", "english"),
            _rec("it", "italian"),
            _rec("fr", "french"),
            _rec("ar", "arabic"),
        ]
        kept, dropped = filter_by_language(records, ["en", "it"])
        titles = {r.title for r in kept}
        assert titles == {"english", "italian"}
        assert dropped == 2


# ---------------------------------------------------------------------------
# Test: policy language=None
# ---------------------------------------------------------------------------

class TestNoneLanguagePolicy:
    def test_none_language_always_kept(self):
        """Record senza lingua rilevata (Wikipedia, wikitalk, ecc.) non vengono scartati."""
        records = [
            _rec(None, "wikipedia"),
            _rec("fr", "french_article"),
            _rec("en", "english_article"),
        ]
        kept, dropped = filter_by_language(records, ["en"])
        titles = {r.title for r in kept}
        assert titles == {"wikipedia", "english_article"}
        assert dropped == 1

    def test_all_none_language_kept(self):
        records = [_rec(None, "a"), _rec(None, "b")]
        kept, dropped = filter_by_language(records, ["en"])
        assert len(kept) == 2
        assert dropped == 0


# ---------------------------------------------------------------------------
# Test: case-insensitivity
# ---------------------------------------------------------------------------

class TestCaseInsensitivity:
    def test_uppercase_record_language_matches(self):
        """language='EN' nel Record deve matchare 'en' nel filtro."""
        records = [_rec("EN", "a"), _rec("FR", "b")]
        kept, dropped = filter_by_language(records, ["en"])
        assert len(kept) == 1
        assert dropped == 1

    def test_uppercase_filter_language_matches(self):
        """Codice 'EN' nel filtro deve matchare language='en' nel Record."""
        records = [_rec("en", "a"), _rec("fr", "b")]
        kept, dropped = filter_by_language(records, ["EN"])
        assert len(kept) == 1
        assert dropped == 1

    def test_mixed_case_both_sides(self):
        records = [_rec("En", "a"), _rec("fR", "b")]
        kept, dropped = filter_by_language(records, ["eN", "FR"])
        assert len(kept) == 2
        assert dropped == 0
