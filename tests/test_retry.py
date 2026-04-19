"""
tests/test_retry.py

Copertura:
- 200 OK → restituisce la response senza retry
- 429 primo → retry, secondo OK → restituisce response OK
- 429 persistente (max_retries=1) → restituisce l'ultima response 429
- 500 primo → retry con backoff, secondo OK
- 500 persistente → restituisce l'ultima response 500
- 404 → restituisce subito senza retry
- Jitter applicato su 429 (delay = base + U(0, jitter_max))
- Backoff esponenziale su 5xx (delay = base * 2^i)
- requests.Timeout → ritentato una volta, poi ri-sollevato
- requests.ConnectionError → ritentato una volta, poi ri-sollevato
- requests.RequestException generica → propaga subito (non coperta dal retry)
- max_retries=0 → nessun retry, Timeout propagato immediatamente
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, call, patch

import requests as req

from collectors.retry import http_get_with_retry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resp(status: int) -> MagicMock:
    m = MagicMock()
    m.status_code = status
    return m


# ---------------------------------------------------------------------------
# Risposta immediata (nessun retry)
# ---------------------------------------------------------------------------

class TestNoRetryNeeded:

    def test_200_returns_immediately(self):
        r = _resp(200)
        with patch("collectors.retry.requests.get", return_value=r) as mock_get, \
             patch("collectors.retry.time.sleep") as mock_sleep:
            result = http_get_with_retry("https://example.com")
        assert result is r
        mock_get.assert_called_once()
        mock_sleep.assert_not_called()

    def test_404_returns_immediately(self):
        r = _resp(404)
        with patch("collectors.retry.requests.get", return_value=r) as mock_get, \
             patch("collectors.retry.time.sleep"):
            result = http_get_with_retry("https://example.com")
        assert result.status_code == 404
        mock_get.assert_called_once()


# ---------------------------------------------------------------------------
# Retry su 429
# ---------------------------------------------------------------------------

class TestRetryOn429:

    def test_429_then_200_returns_ok(self):
        r429 = _resp(429)
        r200 = _resp(200)
        with patch("collectors.retry.requests.get", side_effect=[r429, r200]), \
             patch("collectors.retry.time.sleep") as mock_sleep, \
             patch("collectors.retry.random.uniform", return_value=5.0):
            result = http_get_with_retry(
                "https://example.com",
                base_delay=30.0,
                jitter_max=10.0,
                max_retries=1,
            )
        assert result.status_code == 200
        mock_sleep.assert_called_once_with(35.0)  # 30 + 5 jitter

    def test_429_persistent_returns_last_429(self):
        with patch("collectors.retry.requests.get", return_value=_resp(429)), \
             patch("collectors.retry.time.sleep"), \
             patch("collectors.retry.random.uniform", return_value=0.0):
            result = http_get_with_retry(
                "https://example.com",
                max_retries=1,
            )
        assert result.status_code == 429

    def test_jitter_applied_on_429(self):
        r429 = _resp(429)
        r200 = _resp(200)
        with patch("collectors.retry.requests.get", side_effect=[r429, r200]), \
             patch("collectors.retry.time.sleep") as mock_sleep, \
             patch("collectors.retry.random.uniform", return_value=7.3):
            http_get_with_retry(
                "https://example.com",
                base_delay=20.0,
                jitter_max=10.0,
                max_retries=1,
            )
        mock_sleep.assert_called_once_with(27.3)  # 20 + 7.3

    def test_multiple_retries_on_429(self):
        """Con max_retries=2: due 429 poi un 200."""
        responses = [_resp(429), _resp(429), _resp(200)]
        with patch("collectors.retry.requests.get", side_effect=responses) as mock_get, \
             patch("collectors.retry.time.sleep"), \
             patch("collectors.retry.random.uniform", return_value=0.0):
            result = http_get_with_retry(
                "https://example.com",
                max_retries=2,
            )
        assert result.status_code == 200
        assert mock_get.call_count == 3


# ---------------------------------------------------------------------------
# Retry su 5xx
# ---------------------------------------------------------------------------

class TestRetryOn5xx:

    def test_500_then_200_returns_ok(self):
        r500 = _resp(500)
        r200 = _resp(200)
        with patch("collectors.retry.requests.get", side_effect=[r500, r200]), \
             patch("collectors.retry.time.sleep") as mock_sleep:
            result = http_get_with_retry(
                "https://example.com",
                base_delay=10.0,
                max_retries=1,
            )
        assert result.status_code == 200
        mock_sleep.assert_called_once_with(10.0)  # 10 * 2^0

    def test_5xx_exponential_backoff(self):
        """Secondo retry: delay = base * 2^1."""
        responses = [_resp(503), _resp(503), _resp(200)]
        with patch("collectors.retry.requests.get", side_effect=responses), \
             patch("collectors.retry.time.sleep") as mock_sleep:
            http_get_with_retry(
                "https://example.com",
                base_delay=5.0,
                max_retries=2,
            )
        assert mock_sleep.call_args_list == [call(5.0), call(10.0)]

    def test_5xx_persistent_returns_last(self):
        with patch("collectors.retry.requests.get", return_value=_resp(502)), \
             patch("collectors.retry.time.sleep"):
            result = http_get_with_retry("https://example.com", max_retries=1)
        assert result.status_code == 502


# ---------------------------------------------------------------------------
# Errori di rete
# ---------------------------------------------------------------------------

class TestNetworkErrors:

    def test_generic_request_exception_propagates_immediately(self):
        """
        requests.RequestException generica (non Timeout, non ConnectionError)
        non è catturata dal retry e viene propagata subito.
        """
        with patch(
            "collectors.retry.requests.get",
            side_effect=req.RequestException("ssl error"),
        ) as mock_get, patch("collectors.retry.time.sleep"):
            with pytest.raises(req.RequestException):
                http_get_with_retry("https://example.com")
        # Non deve fare retry: chiamata singola
        mock_get.assert_called_once()

    def test_timeout_is_retried_then_raised(self):
        """
        requests.Timeout viene ritentato max_retries volte prima di
        essere ri-sollevato. Con max_retries=1: 2 chiamate totali.
        """
        with patch(
            "collectors.retry.requests.get",
            side_effect=req.Timeout("timed out"),
        ) as mock_get, patch("collectors.retry.time.sleep") as mock_sleep, \
             patch("collectors.retry.random.uniform", return_value=0.0):
            with pytest.raises(req.Timeout):
                http_get_with_retry(
                    "https://example.com",
                    max_retries=1,
                    base_delay=5.0,
                )
        # 1 tentativo iniziale + 1 retry = 2 chiamate
        assert mock_get.call_count == 2
        mock_sleep.assert_called_once()

    def test_timeout_then_ok_returns_response(self):
        """
        Timeout al primo tentativo, risposta 200 al secondo → restituisce 200.
        """
        r200 = _resp(200)
        with patch(
            "collectors.retry.requests.get",
            side_effect=[req.Timeout("timed out"), r200],
        ) as mock_get, patch("collectors.retry.time.sleep"), \
             patch("collectors.retry.random.uniform", return_value=0.0):
            result = http_get_with_retry(
                "https://example.com",
                max_retries=1,
            )
        assert result.status_code == 200
        assert mock_get.call_count == 2

    def test_connection_error_is_retried_then_raised(self):
        """
        requests.ConnectionError viene ritentato max_retries volte prima
        di essere ri-sollevato. Con max_retries=1: 2 chiamate totali.
        """
        with patch(
            "collectors.retry.requests.get",
            side_effect=req.ConnectionError("connection reset"),
        ) as mock_get, patch("collectors.retry.time.sleep"), \
             patch("collectors.retry.random.uniform", return_value=0.0):
            with pytest.raises(req.ConnectionError):
                http_get_with_retry(
                    "https://example.com",
                    max_retries=1,
                )
        assert mock_get.call_count == 2

    def test_timeout_with_max_retries_zero_propagates_immediately(self):
        """
        Con max_retries=0 nessun retry: Timeout propagato alla prima chiamata.
        """
        with patch(
            "collectors.retry.requests.get",
            side_effect=req.Timeout("timed out"),
        ) as mock_get, patch("collectors.retry.time.sleep"):
            with pytest.raises(req.Timeout):
                http_get_with_retry(
                    "https://example.com",
                    max_retries=0,
                )
        mock_get.assert_called_once()

    def test_network_error_retry_uses_jitter_delay(self):
        """
        Il delay per il retry di rete usa base_delay + jitter (stesso schema di 429).
        """
        with patch(
            "collectors.retry.requests.get",
            side_effect=req.Timeout("timed out"),
        ), patch("collectors.retry.time.sleep") as mock_sleep, \
             patch("collectors.retry.random.uniform", return_value=3.0):
            with pytest.raises(req.Timeout):
                http_get_with_retry(
                    "https://example.com",
                    max_retries=1,
                    base_delay=10.0,
                    jitter_max=5.0,
                )
        mock_sleep.assert_called_once_with(13.0)  # 10 + 3 jitter
