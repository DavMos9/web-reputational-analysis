"""
tests/test_normalizer.py

Test per pipeline/normalizer.py.

Copertura:
- normalize() per ogni sorgente con payload minimo valido
- normalize() con payload vuoto / URL mancante → None
- normalize() con sorgente sconosciuta → None
- normalize_all() con lista vuota e lista mista
- to_date(): formati multipli e valori non parsabili
- to_url(): schema mancante, URL malformato, stringa vuota
"""

from __future__ import annotations

import pytest

from models import RawRecord, Record
from pipeline.normalizer import normalize, normalize_all
from normalizers.utils import to_date, to_url, strip_html


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
# Test: to_date
# ---------------------------------------------------------------------------

class TestToDate:
    def test_iso8601_full(self):
        assert to_date("2026-04-08T10:00:00Z") == "2026-04-08"

    def test_date_only(self):
        assert to_date("2026-04-08") == "2026-04-08"

    def test_gdelt_format(self):
        """GDELT usa il formato "20260408T120000Z"."""
        assert to_date("20260408T120000Z") == "2026-04-08"

    def test_none_input(self):
        assert to_date(None) is None

    def test_empty_string(self):
        assert to_date("") is None

    def test_invalid_string(self):
        assert to_date("not-a-date") is None

    def test_overflow_value(self):
        """Anno fuori range non deve sollevare eccezione."""
        assert to_date("99999-01-01") is None


# ---------------------------------------------------------------------------
# Test: to_url
# ---------------------------------------------------------------------------

class TestToUrl:
    def test_valid_https_url(self):
        assert to_url("https://example.com/path") == "https://example.com/path"

    def test_adds_https_schema(self):
        """URL senza schema → aggiunge https://"""
        result = to_url("example.com/path")
        assert result == "https://example.com/path"

    def test_strips_whitespace(self):
        assert to_url("  https://example.com  ") == "https://example.com"

    def test_none_returns_empty(self):
        assert to_url(None) == ""

    def test_empty_string_returns_empty(self):
        assert to_url("") == ""

    def test_invalid_url_no_netloc(self):
        """Percorso senza host (es. '/path/only') non produce un URL valido."""
        # "/path/only" → "https:///path/only" → netloc vuoto → ""
        assert to_url("/path/only") == ""


# ---------------------------------------------------------------------------
# Test: strip_html
# ---------------------------------------------------------------------------

class TestStripHtml:
    def test_removes_strong_tag(self):
        assert strip_html("Foo <strong>bar</strong> baz") == "Foo bar baz"

    def test_removes_multiple_tags(self):
        text = "<p>Hello <em>world</em>, <b>again</b>.</p>"
        assert strip_html(text) == "Hello world, again."

    def test_decodes_entities(self):
        assert strip_html("Tom &amp; Jerry") == "Tom & Jerry"
        assert strip_html("&lt;not a tag&gt;") == "<not a tag>"

    def test_strips_surrounding_whitespace(self):
        assert strip_html("  <span>x</span>  ") == "x"

    def test_none_returns_empty(self):
        assert strip_html(None) == ""

    def test_empty_returns_empty(self):
        assert strip_html("") == ""

    def test_plain_text_untouched(self):
        assert strip_html("No markup here.") == "No markup here."


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


class TestNormalizeBrave:
    def test_full_payload(self):
        raw = _raw("brave", {
            "title":       "Brave Result",
            "url":         "https://example.com/page",
            "description": "This is a sufficiently long description returned by Brave.",
            "page_age":    "2026-04-10T08:00:00",
            "language":    "en",
            "meta_url":    {"hostname": "example.com"},
        })
        record = normalize(raw)

        assert record is not None
        assert record.source == "brave"
        assert record.title == "Brave Result"
        assert record.url == "https://example.com/page"
        assert record.date == "2026-04-10"
        assert record.language == "en"
        assert record.domain == "example.com"
        assert record.author is None

    def test_missing_url_returns_none(self):
        raw = _raw("brave", {"title": "No URL", "description": "x"})
        assert normalize(raw) is None

    def test_missing_page_age_returns_none_date(self):
        """Brave non sempre fornisce page_age: date=None, record comunque valido."""
        raw = _raw("brave", {
            "title":       "No date result",
            "url":         "https://example.com/x",
            "description": "A fairly long description that exceeds the minimum threshold.",
            "meta_url":    {"hostname": "example.com"},
        })
        record = normalize(raw)
        assert record is not None
        assert record.date is None

    def test_short_description_merged_with_extra_snippets(self):
        """Description corta → testo arricchito con extra_snippets."""
        raw = _raw("brave", {
            "title":       "Short",
            "url":         "https://example.com/x",
            "description": "Too short.",
            "extra_snippets": [
                "First additional snippet giving more context.",
                "Second snippet.",
            ],
            "meta_url": {"hostname": "example.com"},
        })
        record = normalize(raw)
        assert record is not None
        assert "Too short." in record.text
        assert "First additional snippet" in record.text
        assert "Second snippet" in record.text

    def test_long_description_not_altered(self):
        """Description lunga → extra_snippets ignorati per non diluire il segnale."""
        long_desc = "A description long enough to not require merging any extra snippets because it already exceeds the threshold."
        raw = _raw("brave", {
            "title":       "Long",
            "url":         "https://example.com/x",
            "description": long_desc,
            "extra_snippets": ["Should not appear."],
            "meta_url": {"hostname": "example.com"},
        })
        record = normalize(raw)
        assert record is not None
        assert record.text == long_desc
        assert "Should not appear" not in record.text

    def test_domain_fallback_from_url_when_meta_missing(self):
        """Se meta_url manca, dominio estratto dall'URL."""
        raw = _raw("brave", {
            "title":       "No meta",
            "url":         "https://foo.bar.com/path",
            "description": "Some descriptive text long enough to pass cleaner checks easily.",
        })
        record = normalize(raw)
        assert record is not None
        assert record.domain == "foo.bar.com"

    def test_html_tags_stripped_from_description(self):
        """Brave restituisce <strong>...</strong> per evidenziare keyword: deve essere rimosso."""
        raw = _raw("brave", {
            "title":       "Result",
            "url":         "https://example.com/x",
            "description": "Donald John Trump is the <strong>47th president of the United States</strong>. Born in 1946.",
            "meta_url":    {"hostname": "example.com"},
        })
        record = normalize(raw)
        assert record is not None
        assert "<strong>" not in record.text
        assert "</strong>" not in record.text
        assert "47th president of the United States" in record.text

    def test_html_tags_stripped_from_extra_snippets(self):
        """Anche gli extra_snippets, quando usati come fallback, devono essere puliti."""
        raw = _raw("brave", {
            "title":       "T",
            "url":         "https://example.com/x",
            "description": "Too short.",
            "extra_snippets": [
                "First <em>highlighted</em> snippet.",
                "Second <b>bold</b> snippet.",
            ],
            "meta_url": {"hostname": "example.com"},
        })
        record = normalize(raw)
        assert record is not None
        assert "<em>" not in record.text
        assert "<b>" not in record.text
        assert "highlighted" in record.text
        assert "bold" in record.text

    def test_html_entities_decoded(self):
        """Le entità HTML devono essere decodificate (&amp; → &)."""
        raw = _raw("brave", {
            "title":       "Tom &amp; Jerry",
            "url":         "https://example.com/x",
            "description": "An article about <strong>Tom &amp; Jerry</strong> and their adventures together.",
            "meta_url":    {"hostname": "example.com"},
        })
        record = normalize(raw)
        assert record is not None
        assert record.title == "Tom & Jerry"
        assert "Tom & Jerry" in record.text
        assert "&amp;" not in record.text


# ---------------------------------------------------------------------------
# Test: normalize() — sorgente sconosciuta
# ---------------------------------------------------------------------------

class TestNormalizeUnknownSource:
    def test_unknown_source_uses_fallback_normalizer(self):
        """Sorgente sconosciuta → fallback generico, non scarto."""
        raw = _raw("unknown_source", {"url": "https://example.com", "title": "X"})
        record = normalize(raw)
        assert record is not None
        assert record.source == "unknown_source"
        assert record.url == "https://example.com"
        assert record.title == "X"

    def test_unknown_source_no_url_returns_none(self):
        """Sorgente sconosciuta senza URL recuperabile → scartato anche dal fallback."""
        raw = _raw("unknown_source", {"title": "No URL here"})
        assert normalize(raw) is None

    def test_fallback_reads_headline_as_title(self):
        """Chiave alternativa 'headline' viene usata come title quando 'title' assente."""
        raw = _raw("unknown_source", {
            "headline": "Breaking News",
            "url": "https://example.com/article",
        })
        record = normalize(raw)
        assert record is not None
        assert record.title == "Breaking News"

    def test_fallback_reads_link_as_url(self):
        """Chiave alternativa 'link' viene usata come url quando 'url' assente."""
        raw = _raw("unknown_source", {
            "title": "Some Title",
            "link": "https://example.com/via-link",
        })
        record = normalize(raw)
        assert record is not None
        assert record.url == "https://example.com/via-link"

    def test_fallback_reads_body_as_text(self):
        """Chiave alternativa 'body' viene usata come text quando 'text' assente."""
        raw = _raw("unknown_source", {
            "title": "T",
            "url": "https://example.com",
            "body": "Full body content here",
        })
        record = normalize(raw)
        assert record is not None
        assert record.text == "Full body content here"

    def test_fallback_reads_weburl_as_url(self):
        """Chiave 'webUrl' (Guardian-style) estratta correttamente dal fallback."""
        raw = _raw("unknown_source", {
            "webTitle": "Guardian-style Title",
            "webUrl": "https://theguardian.com/article",
        })
        record = normalize(raw)
        assert record is not None
        assert record.url == "https://theguardian.com/article"
        assert record.title == "Guardian-style Title"

    def test_fallback_title_priority_over_headline(self):
        """'title' ha priorità su 'headline' se entrambi presenti."""
        raw = _raw("unknown_source", {
            "title": "Primary Title",
            "headline": "Secondary Headline",
            "url": "https://example.com",
        })
        record = normalize(raw)
        assert record is not None
        assert record.title == "Primary Title"

    def test_fallback_url_priority_title_url(self):
        """'url' ha priorità su 'link' se entrambi presenti."""
        raw = _raw("unknown_source", {
            "title": "T",
            "url": "https://primary.com",
            "link": "https://secondary.com",
        })
        record = normalize(raw)
        assert record is not None
        assert record.url == "https://primary.com"


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
        """Record senza URL vengono scartati; sorgente sconosciuta con URL usa il fallback."""
        raws = [
            _raw("news", {"title": "Valid", "url": "https://example.com/valid"}),
            _raw("news", {"title": "Invalid"}),              # URL mancante → scartato
            _raw("unknown", {"url": "https://x.com"}),       # fallback: ha URL → tenuto
        ]
        results = normalize_all(raws)
        assert len(results) == 2
        assert results[0].title == "Valid"
        assert results[1].url == "https://x.com"

    def test_unknown_source_no_url_discarded(self):
        """Sorgente sconosciuta senza URL recuperabile → scartata anche dal fallback."""
        raws = [
            _raw("news", {"title": "Valid", "url": "https://example.com/valid"}),
            _raw("unknown", {"title": "No URL"}),  # fallback: nessun URL → scartato
        ]
        results = normalize_all(raws)
        assert len(results) == 1
        assert results[0].title == "Valid"
