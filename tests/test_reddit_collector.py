"""
tests/test_reddit_collector.py

Copertura:
- Raccolta normale → lista di RawRecord
- HTTP 429 → retry dopo delay + jitter; se persiste → lista vuota
- HTTP 403 → lista vuota
- RequestException → lista vuota
- Risposta senza figli → lista vuota
- Post senza permalink → scartato
- Parametri sort/time passati correttamente
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch, call

from collectors.reddit_collector import RedditCollector
from models import RawRecord


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_response(status_code: int, children: list[dict] | None = None) -> MagicMock:
    mock = MagicMock()
    mock.status_code = status_code
    mock.raise_for_status.return_value = None
    if children is not None:
        mock.json.return_value = {
            "data": {"children": [{"data": c} for c in children]}
        }
    return mock


SAMPLE_POST = {
    "permalink": "/r/test/comments/abc/test_post/",
    "title": "Test post",
    "selftext": "Some text here",
    "author": "user1",
    "subreddit": "test",
    "created_utc": 1712345678,
    "score": 42,
    "url": "https://www.reddit.com/r/test/comments/abc/test_post/",
    "num_comments": 5,
}


@pytest.fixture
def collector() -> RedditCollector:
    return RedditCollector()


# ---------------------------------------------------------------------------
# Test: raccolta normale
# ---------------------------------------------------------------------------

class TestRedditCollectorSuccess:

    def test_returns_raw_records(self, collector):
        response = _make_response(200, [SAMPLE_POST])
        with patch("collectors.reddit_collector.requests.get", return_value=response):
            records = collector.collect(target="Test", query="test query")
        assert len(records) == 1
        assert isinstance(records[0], RawRecord)
        assert records[0].source == "reddit"
        assert records[0].query == "test query"
        assert records[0].target == "Test"

    def test_passes_sort_and_time_params(self, collector):
        response = _make_response(200, [SAMPLE_POST])
        with patch("collectors.reddit_collector.requests.get", return_value=response) as mock_get:
            collector.collect(target="T", query="q", sort="new", time="week")
        _, kwargs = mock_get.call_args
        params = kwargs["params"]
        assert params["sort"] == "new"
        assert params["t"] == "week"

    def test_empty_children_returns_empty_list(self, collector):
        response = _make_response(200, [])
        with patch("collectors.reddit_collector.requests.get", return_value=response):
            records = collector.collect(target="T", query="q")
        assert records == []

    def test_post_without_permalink_skipped(self, collector):
        post_no_permalink = {k: v for k, v in SAMPLE_POST.items() if k != "permalink"}
        response = _make_response(200, [post_no_permalink, SAMPLE_POST])
        with patch("collectors.reddit_collector.requests.get", return_value=response):
            records = collector.collect(target="T", query="q")
        # Solo il post con permalink deve essere incluso
        assert len(records) == 1

    def test_max_results_capped_at_100(self, collector):
        response = _make_response(200, [])
        with patch("collectors.reddit_collector.requests.get", return_value=response) as mock_get:
            collector.collect(target="T", query="q", max_results=200)
        _, kwargs = mock_get.call_args
        assert kwargs["params"]["limit"] == 100


# ---------------------------------------------------------------------------
# Test: HTTP 403
# ---------------------------------------------------------------------------

class TestRedditCollector403:

    def test_403_returns_empty_list(self, collector):
        response = _make_response(403)
        with patch("collectors.reddit_collector.requests.get", return_value=response):
            records = collector.collect(target="T", query="q")
        assert records == []


# ---------------------------------------------------------------------------
# Test: HTTP 429 — retry con jitter
# ---------------------------------------------------------------------------

class TestRedditCollector429:

    def test_429_then_success_returns_records(self, collector):
        """Primo tentativo 429, retry ha successo."""
        r429 = _make_response(429)
        r200 = _make_response(200, [SAMPLE_POST])

        with patch("collectors.reddit_collector.requests.get", side_effect=[r429, r200]), \
             patch("collectors.reddit_collector.time.sleep") as mock_sleep, \
             patch("collectors.reddit_collector.random.uniform", return_value=5.0):
            records = collector.collect(target="T", query="q")

        assert len(records) == 1
        # Verifica che sleep sia stato chiamato con base + jitter
        mock_sleep.assert_called_once_with(35.0)  # 30 + 5.0

    def test_429_persistent_returns_empty_list(self, collector):
        """Sia il tentativo iniziale sia il retry restituiscono 429."""
        r429 = _make_response(429)

        with patch("collectors.reddit_collector.requests.get", side_effect=[r429, r429]), \
             patch("collectors.reddit_collector.time.sleep"), \
             patch("collectors.reddit_collector.random.uniform", return_value=0.0):
            records = collector.collect(target="T", query="q")

        assert records == []

    def test_429_jitter_is_applied(self, collector):
        """Il delay totale include il jitter casuale."""
        r429 = _make_response(429)
        r200 = _make_response(200, [])

        with patch("collectors.reddit_collector.requests.get", side_effect=[r429, r200]), \
             patch("collectors.reddit_collector.time.sleep") as mock_sleep, \
             patch("collectors.reddit_collector.random.uniform", return_value=7.3):
            collector.collect(target="T", query="q")

        actual_delay = mock_sleep.call_args[0][0]
        assert actual_delay == pytest.approx(37.3)  # 30 + 7.3


# ---------------------------------------------------------------------------
# Test: errori di rete
# ---------------------------------------------------------------------------

class TestRedditCollectorNetworkError:

    def test_request_exception_returns_empty_list(self, collector):
        import requests as req
        with patch(
            "collectors.reddit_collector.requests.get",
            side_effect=req.RequestException("timeout"),
        ):
            records = collector.collect(target="T", query="q")
        assert records == []
