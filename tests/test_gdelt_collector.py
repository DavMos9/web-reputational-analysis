"""
tests/test_gdelt_collector.py

Test focalizzati sul comportamento di retry di GdeltCollector._request_with_retry.

Copertura:
- _compute_backoff: cap, crescita monotona nel regime pre-cap, jitter range
- 429 con recupero: dorme e ritenta, poi ritorna il JSON
- 429 persistente: esaurisce i tentativi e ritorna None (senza sleep finale)
- body vuoto con recupero: dorme e ritenta
- body vuoto persistente: ritorna None (senza sleep finale)
- bug "sleep-then-exit": verificato tramite conteggio sleep() <= retries - 1
  nelle condizioni di fallimento persistente
- timeout transitorio: ritenta con backoff
- Content-Type inatteso: no retry, ritorna None immediatamente
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from collectors import gdelt_collector as gc
from collectors.gdelt_collector import GdeltCollector, _compute_backoff


# ---------------------------------------------------------------------------
# Helper: fake response
# ---------------------------------------------------------------------------

def _make_response(
    *,
    status_code: int = 200,
    content: bytes = b'{"articles": []}',
    content_type: str = "application/json",
    json_data: dict | None = None,
) -> MagicMock:
    """Costruisce un mock di requests.Response con il minimo necessario."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.content = content
    resp.text = content.decode("utf-8", errors="replace") if content else ""
    resp.headers = {"Content-Type": content_type}
    resp.json = MagicMock(return_value=json_data if json_data is not None else {"articles": []})
    if status_code >= 400 and status_code != 429:
        resp.raise_for_status = MagicMock(side_effect=requests.HTTPError(f"HTTP {status_code}"))
    else:
        resp.raise_for_status = MagicMock(return_value=None)
    return resp


# ---------------------------------------------------------------------------
# Test: _compute_backoff
# ---------------------------------------------------------------------------

class TestComputeBackoff:
    def test_grows_monotonically_in_precap_regime(self):
        """Nel regime pre-cap, il backoff medio cresce con attempt."""
        # Per neutralizzare il jitter usiamo un seed fisso
        with patch.object(gc.random, "uniform", return_value=1.0):
            b1 = _compute_backoff(1)
            b2 = _compute_backoff(2)
            b3 = _compute_backoff(3)
        assert b1 < b2 < b3

    def test_cap_is_respected(self):
        """Oltre _MAX_BACKOFF il valore (pre-jitter) è clampato."""
        # attempt=10 → base = 3.0 * 2**10 = 3072s, clampato a _MAX_BACKOFF
        with patch.object(gc.random, "uniform", return_value=1.0):
            b = _compute_backoff(10)
        assert b == pytest.approx(gc._MAX_BACKOFF)

    def test_jitter_applies_both_sides(self):
        """Il jitter produce valori sia sotto che sopra la base."""
        with patch.object(gc.random, "uniform", return_value=0.75):
            b_low = _compute_backoff(1)
        with patch.object(gc.random, "uniform", return_value=1.25):
            b_high = _compute_backoff(1)
        assert b_low < b_high
        # Il rapporto deve riflettere i moltiplicatori di jitter
        assert b_high / b_low == pytest.approx(1.25 / 0.75)

    def test_jitter_within_declared_range(self):
        """Il jitter non eccede il range dichiarato (±25%)."""
        lo, hi = gc._JITTER_RANGE
        base = gc._REQUEST_DELAY * 2  # attempt=1
        for _ in range(50):
            b = _compute_backoff(1)
            assert base * lo <= b <= base * hi


# ---------------------------------------------------------------------------
# Test: retry 429
# ---------------------------------------------------------------------------

class TestRateLimit:
    @patch.object(gc.time, "sleep")
    @patch.object(gc.requests, "get")
    def test_429_then_success(self, mock_get, mock_sleep):
        """429 al primo tentativo, poi 200: il risultato è il JSON del 200."""
        mock_get.side_effect = [
            _make_response(status_code=429, content=b"rate"),
            _make_response(status_code=200, json_data={"articles": [{"url": "x"}]}),
        ]
        collector = GdeltCollector()
        result = collector._request_with_retry({}, "test")
        assert result == {"articles": [{"url": "x"}]}
        assert mock_get.call_count == 2

    @patch.object(gc.time, "sleep")
    @patch.object(gc.requests, "get")
    def test_429_persistent_returns_none(self, mock_get, mock_sleep):
        """429 a ogni tentativo: ritorna None dopo _MAX_RETRIES tentativi HTTP."""
        mock_get.return_value = _make_response(status_code=429, content=b"rate")
        collector = GdeltCollector()
        result = collector._request_with_retry({}, "test")
        assert result is None
        assert mock_get.call_count == gc._MAX_RETRIES

    @patch.object(gc.time, "sleep")
    @patch.object(gc.requests, "get")
    def test_no_sleep_after_last_attempt(self, mock_get, mock_sleep):
        """
        Regressione del bug "sleep-then-exit": all'ultimo tentativo non si
        deve dormire, perché il loop finirebbe subito dopo.

        Sleep totali attesi:
        - 1 iniziale (_REQUEST_DELAY prima del for)
        - (_MAX_RETRIES - 1) backoff tra i tentativi
        """
        mock_get.return_value = _make_response(status_code=429, content=b"rate")
        collector = GdeltCollector()
        collector._request_with_retry({}, "test")
        expected_sleeps = 1 + (gc._MAX_RETRIES - 1)
        assert mock_sleep.call_count == expected_sleeps


# ---------------------------------------------------------------------------
# Test: retry body vuoto
# ---------------------------------------------------------------------------

class TestEmptyBody:
    @patch.object(gc.time, "sleep")
    @patch.object(gc.requests, "get")
    def test_empty_then_success(self, mock_get, mock_sleep):
        mock_get.side_effect = [
            _make_response(status_code=200, content=b""),
            _make_response(status_code=200, json_data={"articles": []}),
        ]
        collector = GdeltCollector()
        result = collector._request_with_retry({}, "test")
        assert result == {"articles": []}
        assert mock_get.call_count == 2

    @patch.object(gc.time, "sleep")
    @patch.object(gc.requests, "get")
    def test_empty_persistent_returns_none(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(status_code=200, content=b"")
        collector = GdeltCollector()
        result = collector._request_with_retry({}, "test")
        assert result is None
        assert mock_get.call_count == gc._MAX_RETRIES

    @patch.object(gc.time, "sleep")
    @patch.object(gc.requests, "get")
    def test_no_sleep_after_last_empty_attempt(self, mock_get, mock_sleep):
        """Stesso bug "sleep-then-exit" ma sul ramo body vuoto."""
        mock_get.return_value = _make_response(status_code=200, content=b"")
        collector = GdeltCollector()
        collector._request_with_retry({}, "test")
        expected_sleeps = 1 + (gc._MAX_RETRIES - 1)
        assert mock_sleep.call_count == expected_sleeps


# ---------------------------------------------------------------------------
# Test: errori di rete
# ---------------------------------------------------------------------------

class TestNetworkErrors:
    @patch.object(gc.time, "sleep")
    @patch.object(gc.requests, "get")
    def test_timeout_then_success(self, mock_get, mock_sleep):
        mock_get.side_effect = [
            requests.Timeout("timeout"),
            _make_response(status_code=200, json_data={"articles": []}),
        ]
        collector = GdeltCollector()
        result = collector._request_with_retry({}, "test")
        assert result == {"articles": []}
        assert mock_get.call_count == 2

    @patch.object(gc.time, "sleep")
    @patch.object(gc.requests, "get")
    def test_5xx_then_success(self, mock_get, mock_sleep):
        mock_get.side_effect = [
            _make_response(status_code=503, content=b"unavailable"),
            _make_response(status_code=200, json_data={"articles": []}),
        ]
        collector = GdeltCollector()
        result = collector._request_with_retry({}, "test")
        assert result == {"articles": []}
        assert mock_get.call_count == 2

    @patch.object(gc.time, "sleep")
    @patch.object(gc.requests, "get")
    def test_5xx_persistent_returns_none(self, mock_get, mock_sleep):
        mock_get.return_value = _make_response(status_code=503, content=b"unavailable")
        collector = GdeltCollector()
        result = collector._request_with_retry({}, "test")
        assert result is None
        assert mock_get.call_count == gc._MAX_RETRIES


# ---------------------------------------------------------------------------
# Test: fallimenti non transitori (no retry)
# ---------------------------------------------------------------------------

class TestNoRetryFailures:
    @patch.object(gc.time, "sleep")
    @patch.object(gc.requests, "get")
    def test_unexpected_content_type_no_retry(self, mock_get, mock_sleep):
        """Content-Type inatteso → fallimento immediato, nessun retry HTTP."""
        mock_get.return_value = _make_response(
            status_code=200,
            content=b"<html>Error page</html>",
            content_type="text/html",
        )
        collector = GdeltCollector()
        result = collector._request_with_retry({}, "test")
        assert result is None
        assert mock_get.call_count == 1
