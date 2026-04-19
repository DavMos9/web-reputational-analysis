"""
collectors/retry.py

Utility HTTP con retry e backoff esponenziale con jitter.

Fornisce http_get_with_retry() come sostituto drop-in di requests.get()
per i collector che vogliono retry automatico senza implementare la logica
di backoff in ogni file.

Politica di retry:
    - HTTP 429 (Rate Limit):           attende base_delay + U(0, jitter_max) secondi
    - HTTP 5xx (Server Error):          attende base_delay * (2 ** tentativo) secondi
    - requests.Timeout / ConnectTimeout: attende base_delay secondi (1 retry)
    - requests.ConnectionError:         attende base_delay secondi (1 retry)
    - Altri errori di rete:             ri-sollevati immediatamente (no retry)

Il retry su errori di rete (Timeout, ConnectionError) usa lo stesso numero
massimo di tentativi degli errori HTTP (max_retries). Per pipeline batch
che girano una sola volta, un timeout passeggero vale la pena di essere
ritentato anziché scartare l'intera sorgente.

Uso:
    from collectors.retry import http_get_with_retry

    response = http_get_with_retry(
        url,
        params=params,
        headers=headers,
        timeout=10,
        source_id=self.source_id,  # per il logging
    )
    response.raise_for_status()

La funzione mantiene la stessa firma di requests.get() per i parametri
principali (url, params, headers, timeout) ed è un drop-in replacement.
Solleva requests.RequestException sugli errori di rete non recuperabili,
e restituisce la Response dell'ultimo tentativo anche se è un 4xx/5xx
(il caller può ancora chiamare raise_for_status()).
"""

from __future__ import annotations

import logging
import random
import time

import requests

log = logging.getLogger(__name__)

# Valori default allineati alla logica esistente in reddit_collector.py.
_DEFAULT_BASE_DELAY  = 30.0   # secondi prima del primo retry
_DEFAULT_JITTER_MAX  = 10.0   # finestra jitter uniforme
_DEFAULT_MAX_RETRIES = 1      # un solo retry per default (come reddit)


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
    """
    Esegue una GET HTTP con retry su 429 e 5xx.

    Args:
        url:         URL da richiedere.
        params:      Query parameters (come requests.get).
        headers:     Headers HTTP (come requests.get).
        timeout:     Timeout in secondi (come requests.get).
        max_retries: Numero massimo di tentativi aggiuntivi (default 1).
        base_delay:  Tempo base di attesa in secondi prima del retry.
                     Per 429 viene aggiunto jitter random; per 5xx viene
                     applicato backoff esponenziale (base_delay * 2^i).
        jitter_max:  Ampiezza massima del jitter uniforme aggiunto su 429.
        source_id:   Identificatore della sorgente per il logging.

    Returns:
        requests.Response dell'ultimo tentativo eseguito.

    Raises:
        requests.RequestException: su errori di rete non recuperabili dopo
            aver esaurito i tentativi (es. Timeout persistente, SSL error).
    """
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
            # Errori di rete transitori: Timeout (server non risponde entro
            # `timeout` secondi) e ConnectionError (reset TCP, DNS failure, ecc.).
            # Vale la pena ritentare con lo stesso delay base degli errori HTTP.
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

        # Risposta valida (2xx, 3xx, 4xx non-429)
        return response
