"""
tests/test_bluesky_collector.py

Copertura:
- Credenziali non configurate → lista vuota (senza chiamate HTTP)
- Login fallito per RequestException → lista vuota
- Login fallito per KeyError (accessJwt mancante) → lista vuota
- Login riuscito + ricerca OK → lista di RawRecord
- Token in cache riutilizzato → login non ri-chiamato
- Token scaduto (401) + refresh riuscito → lista di RawRecord
- Token scaduto (401) + refresh fallito → lista vuota
- Ricerca senza post → lista vuota
- Errore di rete sulla ricerca → lista vuota
"""

from __future__ import annotations

import pytest
import requests
from unittest.mock import MagicMock, patch, call

from collectors.bluesky_collector import BlueskyCollector
from models import RawRecord


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _login_response(access_jwt: str = "fake-jwt") -> MagicMock:
    """Risposta di login riuscita."""
    mock = MagicMock()
    mock.raise_for_status.return_value = None
    mock.json.return_value = {"accessJwt": access_jwt}
    return mock


def _search_response(status_code: int, posts: list[dict] | None = None) -> MagicMock:
    """Risposta di ricerca con status code e lista di post opzionale."""
    mock = MagicMock()
    mock.status_code = status_code
    mock.raise_for_status.return_value = None
    mock.json.return_value = {"posts": posts or []}
    return mock


SAMPLE_POST = {
    "uri": "at://did:plc:abc123/app.bsky.feed.post/xyz",
    "author": {"handle": "user.bsky.social", "displayName": "User"},
    "record": {
        "text": "This is a test post about the target",
        "createdAt": "2024-01-15T10:00:00.000Z",
    },
    "likeCount": 10,
    "replyCount": 2,
}


@pytest.fixture
def collector() -> BlueskyCollector:
    return BlueskyCollector()


# ---------------------------------------------------------------------------
# Test: credenziali mancanti
# ---------------------------------------------------------------------------

class TestBlueskyCredentialsMissing:

    def test_missing_handle_returns_empty(self, collector):
        with patch("collectors.bluesky_collector.BLUESKY_HANDLE", ""), \
             patch("collectors.bluesky_collector.BLUESKY_APP_PASSWORD", "secret"), \
             patch("collectors.bluesky_collector.requests.post") as mock_post:
            records = collector.collect(target="T", query="q")
        assert records == []
        mock_post.assert_not_called()

    def test_missing_password_returns_empty(self, collector):
        with patch("collectors.bluesky_collector.BLUESKY_HANDLE", "user.bsky.social"), \
             patch("collectors.bluesky_collector.BLUESKY_APP_PASSWORD", ""), \
             patch("collectors.bluesky_collector.requests.post") as mock_post:
            records = collector.collect(target="T", query="q")
        assert records == []
        mock_post.assert_not_called()

    def test_both_missing_returns_empty(self, collector):
        with patch("collectors.bluesky_collector.BLUESKY_HANDLE", ""), \
             patch("collectors.bluesky_collector.BLUESKY_APP_PASSWORD", ""), \
             patch("collectors.bluesky_collector.requests.post") as mock_post:
            records = collector.collect(target="T", query="q")
        assert records == []
        mock_post.assert_not_called()


# ---------------------------------------------------------------------------
# Test: login fallito
# ---------------------------------------------------------------------------

class TestBlueskyLoginFailure:

    def test_login_request_exception_returns_empty(self, collector):
        """Se il login solleva RequestException, collect() restituisce []."""
        with patch("collectors.bluesky_collector.BLUESKY_HANDLE", "user.bsky.social"), \
             patch("collectors.bluesky_collector.BLUESKY_APP_PASSWORD", "secret"), \
             patch("collectors.bluesky_collector.requests.post",
                   side_effect=requests.RequestException("connection error")), \
             patch("collectors.bluesky_collector.requests.get") as mock_get:
            records = collector.collect(target="T", query="q")
        assert records == []
        mock_get.assert_not_called()  # La ricerca non viene avviata

    def test_login_missing_access_jwt_returns_empty(self, collector):
        """Se la risposta di login non contiene accessJwt, collect() restituisce []."""
        bad_login = MagicMock()
        bad_login.raise_for_status.return_value = None
        bad_login.json.return_value = {"did": "did:plc:abc"}  # niente accessJwt

        with patch("collectors.bluesky_collector.BLUESKY_HANDLE", "user.bsky.social"), \
             patch("collectors.bluesky_collector.BLUESKY_APP_PASSWORD", "secret"), \
             patch("collectors.bluesky_collector.requests.post", return_value=bad_login), \
             patch("collectors.bluesky_collector.requests.get") as mock_get:
            records = collector.collect(target="T", query="q")
        assert records == []
        mock_get.assert_not_called()


# ---------------------------------------------------------------------------
# Test: raccolta normale
# ---------------------------------------------------------------------------

class TestBlueskyCollectSuccess:

    def test_returns_raw_records(self, collector):
        search = _search_response(200, [SAMPLE_POST])

        with patch("collectors.bluesky_collector.BLUESKY_HANDLE", "user.bsky.social"), \
             patch("collectors.bluesky_collector.BLUESKY_APP_PASSWORD", "secret"), \
             patch("collectors.bluesky_collector.requests.post", return_value=_login_response()), \
             patch("collectors.bluesky_collector.requests.get", return_value=search):
            records = collector.collect(target="Target", query="test query")

        assert len(records) == 1
        assert isinstance(records[0], RawRecord)
        assert records[0].source == "bluesky"
        assert records[0].query == "test query"
        assert records[0].target == "Target"
        assert records[0].payload == SAMPLE_POST

    def test_max_results_capped_at_100(self, collector):
        search = _search_response(200, [])

        with patch("collectors.bluesky_collector.BLUESKY_HANDLE", "user.bsky.social"), \
             patch("collectors.bluesky_collector.BLUESKY_APP_PASSWORD", "secret"), \
             patch("collectors.bluesky_collector.requests.post", return_value=_login_response()), \
             patch("collectors.bluesky_collector.requests.get", return_value=search) as mock_get:
            collector.collect(target="T", query="q", max_results=999)

        _, kwargs = mock_get.call_args
        assert kwargs["params"]["limit"] == 100

    def test_sort_param_passed_correctly(self, collector):
        search = _search_response(200, [])

        with patch("collectors.bluesky_collector.BLUESKY_HANDLE", "user.bsky.social"), \
             patch("collectors.bluesky_collector.BLUESKY_APP_PASSWORD", "secret"), \
             patch("collectors.bluesky_collector.requests.post", return_value=_login_response()), \
             patch("collectors.bluesky_collector.requests.get", return_value=search) as mock_get:
            collector.collect(target="T", query="q", sort="top")

        _, kwargs = mock_get.call_args
        assert kwargs["params"]["sort"] == "top"

    def test_empty_posts_returns_empty_list(self, collector):
        search = _search_response(200, [])

        with patch("collectors.bluesky_collector.BLUESKY_HANDLE", "user.bsky.social"), \
             patch("collectors.bluesky_collector.BLUESKY_APP_PASSWORD", "secret"), \
             patch("collectors.bluesky_collector.requests.post", return_value=_login_response()), \
             patch("collectors.bluesky_collector.requests.get", return_value=search):
            records = collector.collect(target="T", query="q")

        assert records == []


# ---------------------------------------------------------------------------
# Test: token in cache
# ---------------------------------------------------------------------------

class TestBlueskyTokenCache:

    def test_token_reused_across_calls(self, collector):
        """Il login viene eseguito una sola volta; il JWT è riutilizzato."""
        search = _search_response(200, [SAMPLE_POST])

        with patch("collectors.bluesky_collector.BLUESKY_HANDLE", "user.bsky.social"), \
             patch("collectors.bluesky_collector.BLUESKY_APP_PASSWORD", "secret"), \
             patch("collectors.bluesky_collector.requests.post",
                   return_value=_login_response()) as mock_post, \
             patch("collectors.bluesky_collector.requests.get", return_value=search):
            collector.collect(target="T", query="q1")
            collector.collect(target="T", query="q2")

        # Login chiamato una sola volta — il secondo collect riusa il JWT in cache
        assert mock_post.call_count == 1


# ---------------------------------------------------------------------------
# Test: token scaduto (401)
# ---------------------------------------------------------------------------

class TestBlueskyTokenRefresh:

    def test_401_triggers_token_refresh_and_retries(self, collector):
        """Primo tentativo 401 → refresh JWT → nuovo tentativo → 200 con post."""
        r401 = _search_response(401)
        r200 = _search_response(200, [SAMPLE_POST])

        with patch("collectors.bluesky_collector.BLUESKY_HANDLE", "user.bsky.social"), \
             patch("collectors.bluesky_collector.BLUESKY_APP_PASSWORD", "secret"), \
             patch("collectors.bluesky_collector.requests.post",
                   return_value=_login_response()) as mock_post, \
             patch("collectors.bluesky_collector.requests.get",
                   side_effect=[r401, r200]):
            records = collector.collect(target="T", query="q")

        # Login chiamato due volte: una iniziale, una per il refresh
        assert mock_post.call_count == 2
        assert len(records) == 1

    def test_401_with_failed_refresh_returns_empty(self, collector):
        """401 → refresh fallisce → collect() restituisce []."""
        r401 = _search_response(401)
        login_ok = _login_response()

        with patch("collectors.bluesky_collector.BLUESKY_HANDLE", "user.bsky.social"), \
             patch("collectors.bluesky_collector.BLUESKY_APP_PASSWORD", "secret"), \
             patch("collectors.bluesky_collector.requests.post",
                   side_effect=[login_ok, requests.RequestException("login error")]), \
             patch("collectors.bluesky_collector.requests.get", return_value=r401):
            records = collector.collect(target="T", query="q")

        assert records == []


# ---------------------------------------------------------------------------
# Test: errori di rete sulla ricerca
# ---------------------------------------------------------------------------

class TestBlueskyNetworkError:

    def test_search_request_exception_returns_empty(self, collector):
        """RequestException durante la ricerca → collect() restituisce []."""
        with patch("collectors.bluesky_collector.BLUESKY_HANDLE", "user.bsky.social"), \
             patch("collectors.bluesky_collector.BLUESKY_APP_PASSWORD", "secret"), \
             patch("collectors.bluesky_collector.requests.post", return_value=_login_response()), \
             patch("collectors.bluesky_collector.requests.get",
                   side_effect=requests.RequestException("timeout")):
            records = collector.collect(target="T", query="q")
        assert records == []
