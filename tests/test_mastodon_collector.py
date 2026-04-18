"""
tests/test_mastodon_collector.py

Copertura:
- Ricerca full-text restituisce statuses → lista di RawRecord
- Ricerca senza statuses → fallback su hashtag timeline
- Hashtag timeline restituisce statuses → lista di RawRecord
- Hashtag timeline vuota → lista vuota
- Errore di rete su ricerca → lista vuota, nessun crash
- Token incluso solo per l'istanza corretta
- Multi-istanza: record aggregati da più istanze
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from collectors.mastodon_collector import MastodonCollector
from models import RawRecord


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_response(status_code: int, body: dict | list | None = None) -> MagicMock:
    mock = MagicMock()
    mock.status_code = status_code
    mock.raise_for_status.return_value = None
    mock.json.return_value = body if body is not None else {}
    return mock


SAMPLE_STATUS = {
    "id": "123",
    "content": "<p>Test post content</p>",
    "spoiler_text": "",
    "created_at": "2026-04-10T10:00:00.000Z",
    "url": "https://mastodon.social/@user/123",
    "uri": "https://mastodon.social/users/user/statuses/123",
    "account": {"display_name": "Test User", "acct": "user@mastodon.social"},
    "favourites_count": 10,
    "reblogs_count": 2,
    "replies_count": 1,
    "language": "en",
}


@pytest.fixture
def collector() -> MastodonCollector:
    return MastodonCollector()


# ---------------------------------------------------------------------------
# Test: ricerca full-text con successo
# ---------------------------------------------------------------------------

class TestMastodonSearchSuccess:

    def test_search_returns_records(self, collector):
        search_resp = _make_response(200, {"statuses": [SAMPLE_STATUS]})
        with patch("collectors.mastodon_collector.requests.get", return_value=search_resp), \
             patch("collectors.mastodon_collector.time.sleep"):
            records = collector.collect(
                target="Test", query="test query",
                instances=("mastodon.social",),
            )
        assert len(records) == 1
        assert isinstance(records[0], RawRecord)
        assert records[0].source == "mastodon"

    def test_instance_injected_in_payload(self, collector):
        search_resp = _make_response(200, {"statuses": [SAMPLE_STATUS]})
        with patch("collectors.mastodon_collector.requests.get", return_value=search_resp), \
             patch("collectors.mastodon_collector.time.sleep"):
            records = collector.collect(
                target="T", query="q",
                instances=("mastodon.social",),
            )
        assert records[0].payload["_instance"] == "mastodon.social"


# ---------------------------------------------------------------------------
# Test: fallback su hashtag timeline
# ---------------------------------------------------------------------------

class TestMastodonHashtagFallback:

    def test_empty_search_triggers_hashtag_fallback(self, collector):
        """Ricerca senza statuses → fallback hashtag."""
        search_resp = _make_response(200, {"statuses": []})
        hashtag_resp = _make_response(200, [SAMPLE_STATUS])

        responses = [search_resp, hashtag_resp]
        with patch("collectors.mastodon_collector.requests.get", side_effect=responses), \
             patch("collectors.mastodon_collector.time.sleep"):
            records = collector.collect(
                target="T", query="OpenAI",
                instances=("mastodon.social",),
            )
        assert len(records) == 1

    def test_hashtag_fallback_empty_returns_empty(self, collector):
        """Ricerca vuota + hashtag vuota → lista vuota."""
        search_resp = _make_response(200, {"statuses": []})
        hashtag_resp = _make_response(200, [])

        responses = [search_resp, hashtag_resp]
        with patch("collectors.mastodon_collector.requests.get", side_effect=responses), \
             patch("collectors.mastodon_collector.time.sleep"):
            records = collector.collect(
                target="T", query="OpenAI",
                instances=("mastodon.social",),
            )
        assert records == []


# ---------------------------------------------------------------------------
# Test: errori di rete
# ---------------------------------------------------------------------------

class TestMastodonNetworkErrors:

    def test_request_exception_on_search_returns_empty(self, collector):
        import requests as req
        with patch(
            "collectors.mastodon_collector.requests.get",
            side_effect=req.RequestException("timeout"),
        ), patch("collectors.mastodon_collector.time.sleep"):
            records = collector.collect(
                target="T", query="q",
                instances=("mastodon.social",),
            )
        assert records == []


# ---------------------------------------------------------------------------
# Test: autenticazione token
# ---------------------------------------------------------------------------

class TestMastodonAuth:

    def test_token_included_only_for_token_instance(self, collector):
        """Il token deve comparire negli header solo per MASTODON_TOKEN_INSTANCE."""
        search_resp = _make_response(200, {"statuses": []})
        hashtag_resp = _make_response(200, [])

        with patch("collectors.mastodon_collector.requests.get",
                   side_effect=[search_resp, hashtag_resp]) as mock_get, \
             patch("collectors.mastodon_collector.MASTODON_ACCESS_TOKEN", "mytoken"), \
             patch("collectors.mastodon_collector.MASTODON_TOKEN_INSTANCE", "mastodon.social"), \
             patch("collectors.mastodon_collector.time.sleep"):
            collector.collect(target="T", query="q", instances=("mastodon.social",))

        headers = mock_get.call_args_list[0][1]["headers"]
        assert "Authorization" in headers
        assert headers["Authorization"] == "Bearer mytoken"

    def test_token_excluded_for_other_instance(self, collector):
        """Su un'istanza diversa da MASTODON_TOKEN_INSTANCE il token non deve comparire."""
        search_resp = _make_response(200, {"statuses": []})
        hashtag_resp = _make_response(200, [])

        with patch("collectors.mastodon_collector.requests.get",
                   side_effect=[search_resp, hashtag_resp]) as mock_get, \
             patch("collectors.mastodon_collector.MASTODON_ACCESS_TOKEN", "mytoken"), \
             patch("collectors.mastodon_collector.MASTODON_TOKEN_INSTANCE", "mastodon.social"), \
             patch("collectors.mastodon_collector.time.sleep"):
            collector.collect(target="T", query="q", instances=("other.instance",))

        headers = mock_get.call_args_list[0][1]["headers"]
        assert "Authorization" not in headers


# ---------------------------------------------------------------------------
# Test: multi-istanza
# ---------------------------------------------------------------------------

class TestMastodonMultiInstance:

    def test_records_aggregated_from_multiple_instances(self, collector):
        import copy
        r1 = _make_response(200, {"statuses": [copy.deepcopy(SAMPLE_STATUS)]})
        r2 = _make_response(200, {"statuses": [copy.deepcopy(SAMPLE_STATUS)]})

        with patch("collectors.mastodon_collector.requests.get",
                   side_effect=[r1, r2]), \
             patch("collectors.mastodon_collector.time.sleep"):
            records = collector.collect(
                target="T", query="q",
                instances=("mastodon.social", "mastodon.online"),
            )
        assert len(records) == 2
        instances_in_payload = {r.payload["_instance"] for r in records}
        assert instances_in_payload == {"mastodon.social", "mastodon.online"}
