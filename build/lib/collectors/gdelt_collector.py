"""
collectors/gdelt_collector.py

Collector per GDELT DOC 2.0 (https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/).
Gratuito, senza API key. Aggiornato ogni 15 minuti, 65+ lingue.

Nota: GDELT applica rate limiting su chiamate ravvicinate e restituisce
occasionalmente risposte vuote o HTML invece di JSON (transitorie).
Il metodo _request_with_retry gestisce entrambi i casi con retry differenziato:
- body vuoto o 429 → retry con backoff esponenziale
- JSON invalido o Content-Type inatteso → fallimento immediato (no retry)
"""

import logging
import time

import requests

from collectors.base import BaseCollector
from models import RawRecord

log = logging.getLogger(__name__)

BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

_REQUEST_DELAY = 2.0   # secondi di pausa minima prima di ogni chiamata
_MAX_RETRIES   = 3
_BODY_PREVIEW  = 300   # caratteri di anteprima del body nei log di errore


class GdeltCollector(BaseCollector):
    source_id = "gdelt"

    def collect(self, target: str, query: str, max_results: int = 75) -> list[RawRecord]:
        """
        Args:
            target:      entità analizzata.
            query:       stringa di ricerca.
            max_results: numero massimo di risultati (max 250).
        """
        params = {
            "query":      query,
            "mode":       "artlist",
            "maxrecords": min(max_results, 250),
            "format":     "json",
            "sort":       "datedesc",
        }

        data = self._request_with_retry(params, query)
        if data is None:
            return []

        records = [
            self._make_raw(target, query, article)
            for article in data.get("articles", [])
            if article.get("url")
        ]

        self._log_collected(query, len(records))
        return records

    # ------------------------------------------------------------------

    def _request_with_retry(self, params: dict, query: str) -> dict | None:
        """
        Esegue la richiesta HTTP a GDELT con retry differenziato per tipo di errore.

        Logica di retry:
        - HTTP 429 (rate limit):      retry con backoff esponenziale
        - Body vuoto (transitorio):   retry con attesa lineare
        - HTTP 5xx:                   retry fino a _MAX_RETRIES
        - Content-Type inatteso:      fallimento immediato (no retry)
        - JSONDecodeError:            fallimento immediato (no retry)
        - Errori di rete (timeout):   retry fino a _MAX_RETRIES

        Il body viene ispezionato PRIMA di chiamare .json() per produrre
        messaggi di errore diagnostici anziché un generico ValueError.
        """
        time.sleep(_REQUEST_DELAY)

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                response = requests.get(BASE_URL, params=params, timeout=15)

                # --- Rate limit: retry con backoff esponenziale ---
                if response.status_code == 429:
                    wait = _REQUEST_DELAY * (2 ** attempt)
                    self._log_skip(
                        f"rate limit (429), attendo {wait:.0f}s "
                        f"(tentativo {attempt}/{_MAX_RETRIES})"
                    )
                    time.sleep(wait)
                    continue

                response.raise_for_status()

                # --- Body vuoto: possibile condizione transitoria ---
                if not response.content:
                    if attempt < _MAX_RETRIES:
                        wait = _REQUEST_DELAY * attempt
                        log.warning(
                            "[%s] Risposta vuota da GDELT (tentativo %d/%d), "
                            "attendo %.0fs e riprovo. Query: '%s'",
                            self.source_id, attempt, _MAX_RETRIES, wait, query,
                        )
                        time.sleep(wait)
                        continue
                    log.warning(
                        "[%s] Risposta vuota persistente dopo %d tentativi. "
                        "Query saltata: '%s'",
                        self.source_id, _MAX_RETRIES, query,
                    )
                    return None

                # --- Content-Type inatteso: HTML o pagina di errore ---
                # GDELT può restituire "application/json" o "text/javascript".
                content_type = response.headers.get("Content-Type", "")
                if "json" not in content_type and "javascript" not in content_type:
                    log.warning(
                        "[%s] Content-Type inatteso '%s' (atteso JSON). "
                        "GDELT potrebbe restituire una pagina di errore. "
                        "Anteprima body: %r. Query: '%s'",
                        self.source_id, content_type,
                        response.text[:_BODY_PREVIEW], query,
                    )
                    return None  # No retry: non è un problema transitorio

                # --- Parsing JSON ---
                return response.json()

            except requests.exceptions.HTTPError as e:
                self._log_error(query, e)
                if attempt == _MAX_RETRIES:
                    return None

            except requests.RequestException as e:
                # Timeout, connection error, ecc. — potenzialmente transitori
                self._log_error(query, e)
                if attempt == _MAX_RETRIES:
                    return None

            except ValueError as e:
                # JSONDecodeError: body non è JSON valido nonostante Content-Type corretto.
                # Non ha senso riprovare: il contenuto non cambierà.
                log.error(
                    "[%s] JSON non valido nella risposta GDELT. "
                    "Content-Type: '%s'. Anteprima body: %r. Errore: %s. Query: '%s'",
                    self.source_id,
                    response.headers.get("Content-Type", "n/a"),
                    response.text[:_BODY_PREVIEW],
                    e,
                    query,
                )
                return None

        return None
