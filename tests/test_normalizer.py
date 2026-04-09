"""
tests/test_normalizer.py

Test per pipeline/normalizer.py.

Copertura:
- normalize() per ogni sorgente con payload minimo valido
- normalize() con payload vuoto / URL mancante → None
- normalize() con sorgente sconosciuta → None
- normalize_all() con lista vuota e lista mista
- _to_date(): formati multipli e valori non parsabili
- _to_url(): schema mancante, URL malformato, stringa vuota
"""

from __future__ import annotations

import pytest

from models import RawRecord, Record
from pipeline.normalizer import (
    normalize,
    normalize_all,
    _to_date,
    _to_url,
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _raw(source: str, payload: dict, query: str = "test query", target: str = "Test Target") -> RawRecord:
    return RawRecord(
        source=source,
        query=query,
        target=target,
        payload=payload,
        retrieved_at="2026-04-08T10:00:00+00:00",
    )


# ---------------------------------------------------------------------------
# Test: _to_date
# ---------------------------------------------------------------------------

class TestToDate:
    def test_iso8601_full(self):
        assert _to_date("2026-04-08T10:00:00Z") == "2026-04-08"

    def test_date_only(self):
        assert _to_date("2026-04-08") == "2026-04-08"

    def test_gdelt_format(self):
        """GDELT usa il formato "20260408T120000Z"."""
        assert _to_date("20260408T120000Z") == "2026-04-08"

    def test_none_input(self):
        assert _to_date(None) is None

    def test_empty_string(self):
        assert _to_date("") is None

    def test_invalid_string(self):
        assert _to_date("not-a-date") is None

    def test_overflow_value(self):
        """Anno fuori range non deve sollevare eccezione."""
        assert _to_date("99999-01-01") is None


# ---------------------------------------------------------------------------
# Test: _to_url
# ---------------------------------------------------------------------------

class TestToUrl:
    def test_valid_https_url(self):
        assert _to_url("https://example.com/path") == "https://example.com/path"

    def test_adds_https_schema(self):
        """URL senza schema → aggiunge https://"""
        result = _to_url("example.com/path")
        assert result == "https://example.com/path"

    def test_strips_whitespace(self):
        assert _to_url("  https://example.com  ") == "https://example.com"

    def test_none_returns_empty(self):
        assert _to_url(None) == ""

    def test_empty_string_returns_empty(self):
        assert _to_url("") == ""

    def test_invalid_url_no_netloc(self):
        """Percorso senza host (es. '/path/only') non produce un URL valido."""
        # "/path/only" → "https:///path/only" → netloc vuoto → ""
        assert _to_url("/path/only") == ""


# ---------------------------------------------------------------------------
# Test: normalize() — sorgenti valide
# ---------------------------------------------------------------------------

class TestNormalizeNews:
    def test_full_payload(self):
        raw = _raw("news", {
            "title": "Test Article",
            "description": "A test description.",
            "url": "https://example.com/article-1",
            "publishedAt": "2026-04-08T10:00:00Z",
            "author": "John Doe",
            "source": {"name": "Example News"},
        })
        record = normalize(raw)

        assert record is not None
        assert isinstance(record, Record)
        assert record.source == "news"
        assert record.title == "Test Article"
        assert record.date == "2026-04-08"
        assert record.url == "https://example.com/article-1"
        assert record.author == "John Doe"

    def test_missing_url_returns_none(self):
        raw = _raw("news", {
            "title": "No URL article",
            "description": "Something.",
            "url": None,
        })
        assert normalize(raw) is None

    def test_empty_payload_returns_none(self):
        """Payload vuoto → URL mancante → None."""
        raw = _raw("news", {})
        assert normalize(raw) is None

    def test_description_fallback_to_content(self):
        """Se description è vuota, text usa content."""
        raw = _raw("news", {
            "title": "T",
            "description": "",
            "content": "Full content text.",
            "url": "https://example.com/x",
        })
        record = normalize(raw)
        assert record is not None
        assert record.text == "Full content text."


class TestNormalizeGdelt:
    def test_full_payload(self):
        raw = _raw("gdelt", {
            "title": "GDELT Article",
            "url": "https://gdelt-source.com/article",
            "seendate": "20260408T120000Z",
            "language": "English",
            "domain": "gdelt-source.com",
        })
        record = normalize(raw)

        assert record is not None
        assert record.source == "gdelt"
        assert record.date == "2026-04-08"
        assert record.text == ""  # GDELT non fornisce body

    def test_missing_url_returns_none(self):
        raw = _raw("gdelt", {"title": "No URL", "seendate": "20260408T120000Z"})
        assert normalize(raw) is None


class TestNormalizeYoutube:
    def test_full_payload(self):
        raw = _raw("youtube", {
            "id": {"videoId": "abc123"},
            "snippet": {
                "title": "Test Video",
                "description": "Video description",
                "publishedAt": "2026-04-01T00:00:00Z",
                "channelTitle": "Test Channel",
            },
            "statistics": {
                "viewCount": "1000",
                "likeCount": "50",
                "commentCount": "10",
            },
        })
        record = normalize(raw)

        assert record is not None
        assert record.url == "https://www.youtube.com/watch?v=abc123"
        assert record.domain == "youtube.com"
        assert record.views_count == 1000
        assert record.likes_count == 50
        assert record.comments_count == 10

    def test_missing_video_id_returns_none(self):
        """Senza videoId, l'URL non può essere costruito → scartato."""
        raw = _raw("youtube", {
            "id": {},
            "snippet": {"title": "No ID Video"},
        })
        assert normalize(raw) is None


class TestNormalizeWikipedia:
    def test_full_payload(self):
        raw = _raw("wikipedia", {
            "title": "Elon Musk",
            "summary": "South African-born entrepreneur.",
            "url": "https://en.wikipedia.org/wiki/Elon_Musk",
            "language": "en",
        })
        record = normalize(raw)

        assert record is not None
        assert record.domain == "wikipedia.org"
        assert record.date is None  # Wikipedia non ha data
        assert "entrepreneur" in record.text

    def test_missing_url_returns_none(self):
        raw = _raw("wikipedia", {"title": "No URL page", "summary": "Something."})
        assert normalize(raw) is None


class TestNormalizeGuardian:
    def test_full_payload(self):
        raw = _raw("guardian", {
            "webTitle": "Guardian Article",
            "webPublicationDate": "2026-04-07T09:00:00Z",
            "webUrl": "https://www.theguardian.com/article",
            "fields": {
                "headline": "Guardian Headline",
                "trailText": "Trail text here.",
                "byline": "Jane Smith",
            },
        })
        record = normalize(raw)

        assert record is not None
        assert record.title == "Guardian Headline"
        assert record.author == "Jane Smith"
        # Guardian API non espone un campo lingua nel payload standard.
        # La language detection è responsabilità dell'enricher (step successivo).
        assert record.language is None

    def test_missing_url_returns_none(self):
        raw = _raw("guardian", {"fields": {"headline": "No URL"}})
        assert normalize(raw) is None


class TestNormalizeNyt:
    def test_full_payload(self):
        raw = _raw("nyt", {
            "web_url": "https://www.nytimes.com/2026/04/08/article.html",
            "headline": {"main": "NYT Headline"},
            "abstract": "NYT abstract text.",
            "pub_date": "2026-04-08T05:00:00+0000",
            "byline": {"original": "By Jane Doe"},
        })
        record = normalize(raw)

        assert record is not None
        assert record.title == "NYT Headline"
        assert record.author == "Jane Doe"
        assert record.date == "2026-04-08"

    def test_missing_url_returns_none(self):
        raw = _raw("nyt", {"headline": {"main": "No URL"}})
        assert normalize(raw) is None


# ---------------------------------------------------------------------------
# Test: normalize() — sorgente sconosciuta
# ---------------------------------------------------------------------------

class TestNormalizeUnknownSource:
    def test_unknown_source_returns_none(self):
        raw = _raw("unknown_source", {"url": "https://example.com", "title": "X"})
        assert normalize(raw) is None


# ---------------------------------------------------------------------------
# Test: normalize_all()
# ---------------------------------------------------------------------------

class TestNormalizeAll:
    def test_empty_list_returns_empty(self):
        assert normalize_all([]) == []

    def test_all_valid_records(self):
        raws = [
            _raw("news", {
                "title": f"Article {i}",
                "url": f"https://example.com/article-{i}",
                "publishedAt": "2026-04-08T10:00:00Z",
            })
            for i in range(3)
        ]
        results = normalize_all(raws)
        assert len(results) == 3

    def test_mixed_valid_and_invalid(self):
        """Record senza URL vengono scartati silenziosamente."""
        raws = [
            _raw("news", {"title": "Valid", "url": "https://example.com/valid"}),
            _raw("news", {"title": "Invalid"}),          # URL mancante → scartato
            _raw("unknown", {"url": "https://x.com"}),  # sorgente sconosciuta → scartato
        ]
        results = normalize_all(raws)
        assert len(results) == 1
        assert results[0].title == "Valid"
