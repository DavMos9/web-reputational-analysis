"""
collectors/retry.py — GET HTTP con retry e backoff esponenziale + jitter.

Politica:
    - 429: attende base_delay + U(0, jitter_max), poi riprova.
    - 5xx: backoff esponenziale base_delay * 2^i.
    - Timeout / ConnectionError: stesso delay base (errori transitori).
    - Altri errori: ri-sollevati immediatamente.
"""

from __future__ import annotations

import logging
import random
import time

import requests

log = logging.getLogger(__name__)

_DEFAULT_BASE_DELAY  = 30.0
_DEFAULT_JITTER_MAX  = 10.0
_DEFAULT_MAX_RETRIES = 1


def http_get_with_retry(
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    timeout: float = 10.0,
    max_retries: int = _DEFAULT_MAX_RETRIES,
    base_delay: float = _DEFAULT_BASE_DELAY,
    jitter_max: float = _DEFAULT_JITTER_MAX,
    source_id: str = "",
) -> requests.Response:
    """GET con retry su 429/5xx/Timeout. Restituisce la Response dell'ultimo tentativo."""
    label = f"[{source_id}]" if source_id else ""
    attempt = 0

    while True:
        try:
            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=timeout,
            )
        except (requests.Timeout, requests.ConnectionError) as exc:
            if attempt >= max_retries:
                log.warning(
                    "%s Errore di rete (%s) — tentativi esauriti (%d/%d). "
                    "Ri-sollevo l'eccezione.",
                    label, type(exc).__name__, attempt, max_retries,
                )
                raise

            delay = base_delay + random.uniform(0.0, jitter_max)
            log.warning(
                "%s Errore di rete (%s) — attendo %.1fs e riprovo "
                "(tentativo %d/%d): %s",
                label, type(exc).__name__, delay, attempt + 1, max_retries, exc,
            )
            time.sleep(delay)
            attempt += 1
            continue

        if response.status_code == 429:
            if attempt >= max_retries:
                log.warning(
                    "%s Rate limit (HTTP 429) — tentativi esauriti (%d/%d). "
                    "Restituisco risposta al caller.",
                    label, attempt, max_retries,
                )
                return response

            delay = base_delay + random.uniform(0.0, jitter_max)
            log.warning(
                "%s Rate limit (HTTP 429) — attendo %.1fs e riprovo "
                "(tentativo %d/%d).",
                label, delay, attempt + 1, max_retries,
            )
            time.sleep(delay)
            attempt += 1
            continue

        if response.status_code >= 500:
            if attempt >= max_retries:
                log.warning(
                    "%s Errore server (HTTP %d) — tentativi esauriti (%d/%d). "
                    "Restituisco risposta al caller.",
                    label, response.status_code, attempt, max_retries,
                )
                return response

            delay = base_delay * (2 ** attempt)
            log.warning(
                "%s Errore server (HTTP %d) — attendo %.1fs e riprovo "
                "(tentativo %d/%d).",
                label, response.status_code, delay, attempt + 1, max_retries,
            )
            time.sleep(delay)
            attempt += 1
            continue

        return response
