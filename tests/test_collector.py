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


# ---------------------------------------------------------------------------
# Test: BraveCollector — collect()
# ---------------------------------------------------------------------------

from collectors.brave_collector import BraveCollector  # noqa: E402


SAMPLE_BRAVE_RESULT = {
    "title":       "Brave Search Result",
    "url":         "https://example.com/page",
    "description": "A short description returned by Brave.",
    "page_age":    "2026-04-10T08:00:00",
    "language":    "en",
    "meta_url":    {"hostname": "example.com"},
    "extra_snippets": ["Additional snippet 1.", "Additional snippet 2."],
}


def _mock_brave_response(results: list[dict], status_code: int = 200) -> MagicMock:
    """Mock della risposta Brave (/res/v1/web/search)."""
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = {"web": {"results": results}}
    mock.raise_for_status.return_value = None
    return mock


class TestBraveCollectorCollect:
    @pytest.fixture
    def brave(self) -> BraveCollector:
        return BraveCollector()

    @patch("collectors.brave_collector.BRAVE_API_KEY", "fake-key")
    @patch("collectors.brave_collector.requests.get")
    def test_collect_returns_raw_records(self, mock_get, brave):
        """Con risposta valida, restituisce RawRecord con source='brave'."""
        mock_get.return_value = _mock_brave_response([SAMPLE_BRAVE_RESULT])

        results = brave.collect("Elon Musk", "Elon Musk Tesla")

        assert len(results) == 1
        assert isinstance(results[0], RawRecord)
        assert results[0].source == "brave"
        assert results[0].payload["url"] == "https://example.com/page"
        # Il collector aggiunge _rank per tracciare l'ordine originale.
        assert results[0].payload["_rank"] == 1

    @patch("collectors.brave_collector.BRAVE_API_KEY", "")
    def test_collect_missing_api_key_returns_empty(self, brave):
        """Senza API key il collector non chiama HTTP e restituisce []."""
        assert brave.collect("Elon Musk", "Elon Musk Tesla") == []

    @patch("collectors.brave_collector.BRAVE_API_KEY", "fake-key")
    @patch("collectors.brave_collector.requests.get")
    def test_collect_api_failure_returns_empty(self, mock_get, brave):
        """RequestException gestita: nessuna eccezione propagata."""
        mock_get.side_effect = requests.RequestException("network")
        assert brave.collect("Elon Musk", "Elon Musk Tesla") == []

    @patch("collectors.brave_collector.BRAVE_API_KEY", "fake-key")
    @patch("collectors.brave_collector.requests.get")
    def test_collect_http_error_returns_empty(self, mock_get, brave):
        """HTTP 429 (rate limit) viene gestito come errore recuperabile."""
        mock = MagicMock()
        mock.raise_for_status.side_effect = requests.HTTPError("429 Too Many Requests")
        mock_get.return_value = mock
        assert brave.collect("Elon Musk", "Elon Musk Tesla") == []

    @patch("collectors.brave_collector.BRAVE_API_KEY", "fake-key")
    @patch("collectors.brave_collector.requests.get")
    def test_collect_skips_results_without_url(self, mock_get, brave):
        """Risultati senza URL vengono scartati a livello collector."""
        without_url = {**SAMPLE_BRAVE_RESULT, "url": None}
        mock_get.return_value = _mock_brave_response([without_url, SAMPLE_BRAVE_RESULT])
        results = brave.collect("Elon Musk", "Elon Musk Tesla")
        assert len(results) == 1

    @patch("collectors.brave_collector.BRAVE_API_KEY", "fake-key")
    @patch("collectors.brave_collector.requests.get")
    def test_collect_respects_max_results_cap_20(self, mock_get, brave):
        """count è capato a 20 (limite piano gratuito) anche se max_results è maggiore."""
        mock_get.return_value = _mock_brave_response([SAMPLE_BRAVE_RESULT])
        brave.collect("Elon Musk", "Elon Musk Tesla", max_results=100)
        call_params = mock_get.call_args.kwargs.get("params", {})
        assert call_params["count"] == 20

    @patch("collectors.brave_collector.BRAVE_API_KEY", "fake-key")
    @patch("collectors.brave_collector.requests.get")
    def test_collect_sends_auth_header(self, mock_get, brave):
        """Il token API viene inviato come header X-Subscription-Token."""
        mock_get.return_value = _mock_brave_response([SAMPLE_BRAVE_RESULT])
        brave.collect("Elon Musk", "Elon Musk Tesla")
        headers = mock_get.call_args.kwargs.get("headers", {})
        assert headers.get("X-Subscription-Token") == "fake-key"

    @patch("collectors.brave_collector.BRAVE_API_KEY", "fake-key")
    @patch("collectors.brave_collector.requests.get")
    def test_collect_passes_optional_kwargs(self, mock_get, brave):
        """I kwargs opzionali (country, search_lang, freshness) arrivano nei params."""
        mock_get.return_value = _mock_brave_response([SAMPLE_BRAVE_RESULT])
        brave.collect(
            "Elon Musk", "Elon Musk Tesla",
            country="IT", search_lang="it", freshness="pw",
        )
        call_params = mock_get.call_args.kwargs.get("params", {})
        assert call_params["country"] == "IT"
        assert call_params["search_lang"] == "it"
        assert call_params["freshness"] == "pw"

    @patch("collectors.brave_collector.BRAVE_API_KEY", "fake-key")
    @patch("collectors.brave_collector.requests.get")
    def test_collect_ignores_empty_optional_kwargs(self, mock_get, brave):
        """Kwargs opzionali vuoti/None non vengono inviati."""
        mock_get.return_value = _mock_brave_response([SAMPLE_BRAVE_RESULT])
        brave.collect(
            "Elon Musk", "Elon Musk Tesla",
            country=None, search_lang="", freshness=None,
        )
        call_params = mock_get.call_args.kwargs.get("params", {})
        assert "country" not in call_params
        assert "search_lang" not in call_params
        assert "freshness" not in call_params
