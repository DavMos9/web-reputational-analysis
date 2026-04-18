"""
tests/test_lemmy_collector.py

Copertura:
- Raccolta post → lista di RawRecord con _content_type="Posts"
- Raccolta commenti → lista di RawRecord con _content_type="Comments"
- Risposta vuota → lista vuota
- Errore di rete → lista vuota, nessun crash
- Metadati _instance e _content_type iniettati nel payload
- Multi-istanza: record aggregati da più istanze
- Parametro sort passato correttamente
- content_types limita i tipi raccotti
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from collectors.lemmy_collector import LemmyCollector
from models import RawRecord


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_response(status_code: int, body: dict | None = None) -> MagicMock:
    mock = MagicMock()
    mock.status_code = status_code
    mock.raise_for_status.return_value = None
    mock.json.return_value = body if body is not None else {}
    return mock


SAMPLE_POST_ITEM = {
    "post": {
        "name": "Test post title",
        "body": "Test post body",
        "ap_id": "https://lemmy.world/post/123",
        "published": "2026-04-10T10:00:00.000000",
    },
    "creator": {"name": "testuser", "actor_id": "https://lemmy.world/u/testuser"},
    "counts": {"score": 42, "comments": 5, "upvotes": 50, "downvotes": 8},
}

SAMPLE_COMMENT_ITEM = {
    "comment": {
        "content": "This is a comment",
        "ap_id": "https://lemmy.world/comment/456",
        "published": "2026-04-10T11:00:00.000000",
    },
    "post": {"name": "Parent post title", "ap_id": "https://lemmy.world/post/123"},
    "creator": {"name": "commenter", "actor_id": "https://lemmy.world/u/commenter"},
    "counts": {"score": 10, "upvotes": 12, "downvotes": 2, "child_count": 1},
}


@pytest.fixture
def collector() -> LemmyCollector:
    return LemmyCollector()


# ---------------------------------------------------------------------------
# Test: raccolta post
# ---------------------------------------------------------------------------

class TestLemmyCollectorPosts:

    def test_collects_posts(self, collector):
        response = _make_response(200, {"posts": [SAMPLE_POST_ITEM], "comments": []})
        with patch("collectors.lemmy_collector.requests.get", return_value=response), \
             patch("collectors.lemmy_collector.time.sleep"):
            records = collector.collect(
                target="T", query="q",
                instances=("lemmy.world",),
                content_types=("Posts",),
            )
        assert len(records) == 1
        assert isinstance(records[0], RawRecord)
        assert records[0].source == "lemmy"

    def test_post_metadata_injected(self, collector):
        response = _make_response(200, {"posts": [SAMPLE_POST_ITEM]})
        with patch("collectors.lemmy_collector.requests.get", return_value=response), \
             patch("collectors.lemmy_collector.time.sleep"):
            records = collector.collect(
                target="T", query="q",
                instances=("lemmy.world",),
                content_types=("Posts",),
            )
        payload = records[0].payload
        assert payload["_instance"] == "lemmy.world"
        assert payload["_content_type"] == "Posts"


# ---------------------------------------------------------------------------
# Test: raccolta commenti
# ---------------------------------------------------------------------------

class TestLemmyCollectorComments:

    def test_collects_comments(self, collector):
        response = _make_response(200, {"posts": [], "comments": [SAMPLE_COMMENT_ITEM]})
        with patch("collectors.lemmy_collector.requests.get", return_value=response), \
             patch("collectors.lemmy_collector.time.sleep"):
            records = collector.collect(
                target="T", query="q",
                instances=("lemmy.world",),
                content_types=("Comments",),
            )
        assert len(records) == 1
        assert records[0].payload["_content_type"] == "Comments"

    def test_comment_metadata_injected(self, collector):
        response = _make_response(200, {"comments": [SAMPLE_COMMENT_ITEM]})
        with patch("collectors.lemmy_collector.requests.get", return_value=response), \
             patch("collectors.lemmy_collector.time.sleep"):
            records = collector.collect(
                target="T", query="q",
                instances=("lemmy.world",),
                content_types=("Comments",),
            )
        assert records[0].payload["_instance"] == "lemmy.world"


# ---------------------------------------------------------------------------
# Test: risposta vuota
# ---------------------------------------------------------------------------

class TestLemmyCollectorEmpty:

    def test_empty_posts_returns_empty(self, collector):
        response = _make_response(200, {"posts": [], "comments": []})
        with patch("collectors.lemmy_collector.requests.get", return_value=response), \
             patch("collectors.lemmy_collector.time.sleep"):
            records = collector.collect(
                target="T", query="q",
                instances=("lemmy.world",),
                content_types=("Posts",),
            )
        assert records == []


# ---------------------------------------------------------------------------
# Test: errori di rete
# ---------------------------------------------------------------------------

class TestLemmyCollectorNetworkError:

    def test_request_exception_returns_empty(self, collector):
        import requests as req
        with patch(
            "collectors.lemmy_collector.requests.get",
            side_effect=req.RequestException("timeout"),
        ), patch("collectors.lemmy_collector.time.sleep"):
            records = collector.collect(
                target="T", query="q",
                instances=("lemmy.world",),
            )
        assert records == []


# ---------------------------------------------------------------------------
# Test: parametri
# ---------------------------------------------------------------------------

class TestLemmyCollectorParams:

    def test_sort_passed_in_params(self, collector):
        response = _make_response(200, {"posts": []})
        with patch("collectors.lemmy_collector.requests.get",
                   return_value=response) as mock_get, \
             patch("collectors.lemmy_collector.time.sleep"):
            collector.collect(
                target="T", query="q",
                instances=("lemmy.world",),
                sort="New",
                content_types=("Posts",),
            )
        params = mock_get.call_args[1]["params"]
        assert params["sort"] == "New"

    def test_content_types_limits_requests(self, collector):
        """Con content_types=("Posts",) deve fare una sola richiesta per istanza."""
        response = _make_response(200, {"posts": []})
        with patch("collectors.lemmy_collector.requests.get",
                   return_value=response) as mock_get, \
             patch("collectors.lemmy_collector.time.sleep"):
            collector.collect(
                target="T", query="q",
                instances=("lemmy.world",),
                content_types=("Posts",),
            )
        assert mock_get.call_count == 1


# ---------------------------------------------------------------------------
# Test: multi-istanza
# ---------------------------------------------------------------------------

class TestLemmyCollectorMultiInstance:

    def test_records_from_multiple_instances(self, collector):
        import copy
        r1 = _make_response(200, {"posts": [copy.deepcopy(SAMPLE_POST_ITEM)], "comments": []})
        r2 = _make_response(200, {"posts": [copy.deepcopy(SAMPLE_POST_ITEM)], "comments": []})

        with patch("collectors.lemmy_collector.requests.get",
                   side_effect=[r1, r2]) as mock_get, \
             patch("collectors.lemmy_collector.time.sleep"):
            records = collector.collect(
                target="T", query="q",
                instances=("lemmy.world", "lemmy.ml"),
                content_types=("Posts",),
            )
        assert len(records) == 2
        instances = {r.payload["_instance"] for r in records}
        assert instances == {"lemmy.world", "lemmy.ml"}
