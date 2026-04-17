"""
tests/test_date_filter.py

Test per pipeline/date_filter.py.

Copertura:
- parse_since: formati validi, invalidi, None
- filter_by_date:
  - since=None → lista inalterata
  - tutti i record posteriori → tutti mantenuti
  - tutti i record anteriori → tutti scartati
  - misto → solo quelli >= since sopravvivono
  - record con date=None → mantenuti (policy esplicita)
  - boundary exact match (record.date == since) → mantenuto (>=)
"""

from __future__ import annotations

import pytest

from models import Record
from pipeline.date_filter import filter_by_date, parse_since


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _rec(date: str | None, title: str = "T") -> Record:
    return Record(
        source="news",
        query="q",
        target="Target",
        title=title,
        text="Some text long enough to pass cleaner thresholds in normal runs.",
        date=date,
        url="https://example.com/" + (title or "x"),
        domain="example.com",
    )


# ---------------------------------------------------------------------------
# parse_since
# ---------------------------------------------------------------------------

class TestParseSince:
    def test_valid_iso_date(self):
        assert parse_since("2026-01-01") == "2026-01-01"

    def test_rejects_bad_format(self):
        with pytest.raises(ValueError):
            parse_since("01/01/2026")

    def test_rejects_non_date_string(self):
        with pytest.raises(ValueError):
            parse_since("not-a-date")

    def test_rejects_empty_string(self):
        with pytest.raises(ValueError):
            parse_since("")

    def test_rejects_datetime_with_time(self):
        """parse_since accetta solo date pure 'YYYY-MM-DD'."""
        with pytest.raises(ValueError):
            parse_since("2026-01-01T12:00:00")


# ---------------------------------------------------------------------------
# filter_by_date
# ---------------------------------------------------------------------------

class TestFilterByDate:
    def test_none_since_returns_all(self):
        records = [_rec("2020-01-01"), _rec("2026-01-01"), _rec(None)]
        kept, dropped = filter_by_date(records, None)
        assert len(kept) == 3
        assert dropped == 0

    def test_empty_list(self):
        kept, dropped = filter_by_date([], "2026-01-01")
        assert kept == []
        assert dropped == 0

    def test_all_after_since_kept(self):
        records = [_rec("2026-02-01"), _rec("2026-03-15"), _rec("2026-04-01")]
        kept, dropped = filter_by_date(records, "2026-01-01")
        assert len(kept) == 3
        assert dropped == 0

    def test_all_before_since_dropped(self):
        records = [_rec("2020-01-01"), _rec("2021-06-15")]
        kept, dropped = filter_by_date(records, "2026-01-01")
        assert kept == []
        assert dropped == 2

    def test_mixed(self):
        records = [
            _rec("2020-01-01", "old1"),
            _rec("2026-03-01", "recent1"),
            _rec("2015-12-31", "old2"),
            _rec("2026-04-17", "recent2"),
        ]
        kept, dropped = filter_by_date(records, "2026-01-01")
        titles = {r.title for r in kept}
        assert titles == {"recent1", "recent2"}
        assert dropped == 2

    def test_exact_boundary_is_kept(self):
        """Il confronto è >=, il giorno stesso passa."""
        records = [_rec("2026-01-01")]
        kept, dropped = filter_by_date(records, "2026-01-01")
        assert len(kept) == 1
        assert dropped == 0

    def test_records_without_date_are_kept(self):
        """Policy esplicita: date=None viene mantenuto (non si può giudicare)."""
        records = [
            _rec(None, "wikipedia"),
            _rec("2020-01-01", "old_article"),
            _rec("2026-04-17", "recent"),
        ]
        kept, dropped = filter_by_date(records, "2026-01-01")
        titles = {r.title for r in kept}
        assert titles == {"wikipedia", "recent"}
        assert dropped == 1
