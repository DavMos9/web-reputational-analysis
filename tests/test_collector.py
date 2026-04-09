"""
tests/test_collector.py

Test per BaseCollector e NewsCollector.

Copertura:
- raccolta con risposta API valida
- API failure (RequestException) → lista vuota
- API key mancante → lista vuota
- risposta API con lista articoli vuota
- validazione source_id obbligatorio
- _make_raw produce un RawRecord valido
"""

from __future__ import annotations

import pytest
import requests

from unittest.mock import MagicMock, patch

from collectors.base import BaseCollector
from collectors.news_collector import NewsCollector
from models import RawRecord


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def collector() -> NewsCollector:
    return NewsCollector()


def _mock_response(articles: list[dict], status_code: int = 200) -> MagicMock:
    """Crea un mock di requests.Response con una lista di articoli."""
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = {"status": "ok", "articles": articles}
    mock.raise_for_status.return_value = None
    return mock


SAMPLE_ARTICLE = {
    "title": "Test Article",
    "description": "A test description.",
    "content": "Full content here.",
    "url": "https://example.com/article-1",
    "author": "John Doe",
    "publishedAt": "2026-04-08T10:00:00Z",
    "source": {"id": "example", "name": "Example News"},
}


# ---------------------------------------------------------------------------
# Test: BaseCollector — source_id obbligatorio
# ---------------------------------------------------------------------------

class TestBaseCollectorValidation:
    def test_missing_source_id_raises_typeerror(self):
        """Sottoclasse senza source_id deve sollevare TypeError a definizione."""
        with pytest.raises(TypeError, match="source_id"):
            class BrokenCollector(BaseCollector):
                source_id = ""  # vuoto — deve fallire

                def collect(self, target, query, max_results=20, **kwargs):
                    return []

    def test_valid_source_id_does_not_raise(self):
        """Sottoclasse con source_id valido viene istanziata senza errori."""
        class ValidCollector(BaseCollector):
            source_id = "valid_source"

            def collect(self, target, query, max_results=20, **kwargs):
                return []

        collector = ValidCollector()
        assert collector.source_id == "valid_source"

    def test_make_raw_returns_raw_record(self):
        """_make_raw produce un RawRecord con i campi corretti."""
        collector = NewsCollector()
        raw = collector._make_raw("Elon Musk", "Elon Musk Tesla", {"title": "Test"})

        assert isinstance(raw, RawRecord)
        assert raw.source == "news"
        assert raw.target == "Elon Musk"
        assert raw.query == "Elon Musk Tesla"
        assert raw.payload == {"title": "Test"}
        assert raw.retrieved_at  # non vuoto


# ---------------------------------------------------------------------------
# Test: NewsCollector — collect()
# ---------------------------------------------------------------------------

class TestNewsCollectorCollect:

    @patch("collectors.news_collector.NEWS_API_KEY", "fake-key")
    @patch("collectors.news_collector.requests.get")
    def test_collect_returns_raw_records(self, mock_get, collector):
        """Con risposta valida, restituisce una lista di RawRecord."""
        mock_get.return_value = _mock_response([SAMPLE_ARTICLE])

        results = collector.collect("Elon Musk", "Elon Musk Tesla")

        assert len(results) == 1
        assert isinstance(results[0], RawRecord)
        assert results[0].source == "news"
        assert results[0].target == "Elon Musk"
        assert results[0].payload["url"] == "https://example.com/article-1"

    @patch("collectors.news_collector.NEWS_API_KEY", "fake-key")
    @patch("collectors.news_collector.requests.get")
    def test_collect_empty_articles(self, mock_get, collector):
        """API risponde con lista articoli vuota → lista vuota."""
        mock_get.return_value = _mock_response([])

        results = collector.collect("Elon Musk", "Elon Musk Tesla")

        assert results == []

    @patch("collectors.news_collector.NEWS_API_KEY", "")
    def test_collect_missing_api_key_returns_empty(self, collector):
        """Se NEWS_API_KEY non è configurata, il collector torna lista vuota senza chiamare HTTP."""
        results = collector.collect("Elon Musk", "Elon Musk Tesla")

        assert results == []

    @patch("collectors.news_collector.NEWS_API_KEY", "fake-key")
    @patch("collectors.news_collector.requests.get")
    def test_collect_api_failure_returns_empty(self, mock_get, collector):
        """RequestException viene gestita internamente: restituisce [] senza propagare."""
        mock_get.side_effect = requests.RequestException("Connection refused")

        results = collector.collect("Elon Musk", "Elon Musk Tesla")

        assert results == []

    @patch("collectors.news_collector.NEWS_API_KEY", "fake-key")
    @patch("collectors.news_collector.requests.get")
    def test_collect_timeout_returns_empty(self, mock_get, collector):
        """Timeout viene gestito come RequestException."""
        mock_get.side_effect = requests.Timeout("timed out")

        results = collector.collect("Elon Musk", "Elon Musk Tesla")

        assert results == []

    @patch("collectors.news_collector.NEWS_API_KEY", "fake-key")
    @patch("collectors.news_collector.requests.get")
    def test_collect_http_error_returns_empty(self, mock_get, collector):
        """HTTP 429 (rate limit) viene gestito: restituisce []."""
        mock = MagicMock()
        mock.raise_for_status.side_effect = requests.HTTPError("429 Too Many Requests")
        mock_get.return_value = mock

        results = collector.collect("Elon Musk", "Elon Musk Tesla")

        assert results == []

    @patch("collectors.news_collector.NEWS_API_KEY", "fake-key")
    @patch("collectors.news_collector.requests.get")
    def test_collect_skips_articles_without_url(self, mock_get, collector):
        """Articoli senza campo 'url' vengono scartati dal collector."""
        article_no_url = {**SAMPLE_ARTICLE, "url": None}
        mock_get.return_value = _mock_response([article_no_url, SAMPLE_ARTICLE])

        results = collector.collect("Elon Musk", "Elon Musk Tesla")

        assert len(results) == 1
        assert results[0].payload["url"] == "https://example.com/article-1"

    @patch("collectors.news_collector.NEWS_API_KEY", "fake-key")
    @patch("collectors.news_collector.requests.get")
    def test_collect_respects_max_results(self, mock_get, collector):
        """max_results viene passato come pageSize nei params."""
        mock_get.return_value = _mock_response([SAMPLE_ARTICLE])

        collector.collect("Elon Musk", "Elon Musk Tesla", max_results=5)

        call_params = mock_get.call_args.kwargs.get("params") or mock_get.call_args[1].get("params", {})
        assert call_params["pageSize"] == 5
