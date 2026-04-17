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
import random
import re
import time

import requests

from collectors.base import BaseCollector
from models import RawRecord

log = logging.getLogger(__name__)

BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

_REQUEST_DELAY  = 3.0   # secondi di pausa minima tra chiamate consecutive
_MAX_RETRIES    = 5     # tentativi HTTP totali prima di rinunciare
_MAX_BACKOFF    = 60.0  # cap sul tempo di attesa tra retry (secondi)
_JITTER_RANGE   = (0.75, 1.25)  # moltiplicatore ±25% anti-thundering-herd
_BODY_PREVIEW   = 300   # caratteri di anteprima del body nei log di errore
_MIN_TOKEN_LEN  = 3     # token GDELT devono avere almeno questo numero di caratteri


def _compute_backoff(attempt: int) -> float:
    """
    Backoff esponenziale con cap e jitter.

    Formula: min(_MAX_BACKOFF, _REQUEST_DELAY * 2**attempt) * uniform(0.75, 1.25)

    - Il cap evita attese eccessive con _MAX_RETRIES elevati (es. 96s al 5°
      tentativo senza cap).
    - Il jitter ±25% desincronizza i retry di query parallele che vengono
      rate-limited contemporaneamente, evitando il thundering-herd sul server.
    """
    base = _REQUEST_DELAY * (2 ** attempt)
    capped = min(base, _MAX_BACKOFF)
    return capped * random.uniform(*_JITTER_RANGE)


def _sanitize_gdelt_query(query: str) -> str:
    """
    Rimuove token con meno di 3 caratteri alfanumerici dalla query GDELT.
    Se il risultato è vuoto, restituisce la query originale come frase esatta tra virgolette.
    """
    tokens = query.strip().split()
    valid_tokens = [t for t in tokens if len(re.sub(r'[^\w]', '', t)) >= _MIN_TOKEN_LEN]

    if valid_tokens:
        sanitized = " ".join(valid_tokens)
    else:
        # Fallback: frase esatta tra virgolette (GDELT supporta quoted phrases)
        sanitized = f'"{query.strip()}"'

    if sanitized != query:
        log.debug("[gdelt] Query sanitizzata: '%s' → '%s'", query, sanitized)

    return sanitized


class GdeltCollector(BaseCollector):
    source_id = "gdelt"

    def collect(self, target: str, query: str, max_results: int = 75) -> list[RawRecord]:
        """
        Args:
            target:      entità analizzata.
            query:       stringa di ricerca.
            max_results: numero massimo di risultati (max 250).
        """
        sanitized_query = _sanitize_gdelt_query(query)

        params = {
            "query":      sanitized_query,
            "mode":       "artlist",
            "maxrecords": min(max_results, 250),
            "format":     "json",
            "sort":       "datedesc",
        }

        data = self._request_with_retry(params, query)
        if data is None:
            return []

        articles = data.get("articles", [])
        if not isinstance(articles, list):
            log.warning(
                "[%s] Campo 'articles' inatteso (tipo: %s). Query: '%s'",
                self.source_id, type(articles).__name__, query,
            )
            return []

        records = [
            self._make_raw(target, query, article)
            for article in articles
            if article.get("url")
        ]

        self._log_collected(query, len(records))
        return records

    # ------------------------------------------------------------------

    def _request_with_retry(self, params: dict, query: str) -> dict | None:
        """
        Esegue la richiesta HTTP a GDELT con retry differenziato per tipo di errore.

        Backoff: esponenziale con cap (_MAX_BACKOFF) e jitter ±25% (vedi
        `_compute_backoff`). Applicato a tutte le condizioni retry-able.
        All'ultimo tentativo il sleep viene SALTATO (non avrebbe più effetto:
        il loop termina subito dopo) e si restituisce None immediatamente.

        Logica di retry:
        - HTTP 429 (rate limit):      retry con backoff
        - Body vuoto (transitorio):   retry con backoff
        - HTTP 5xx:                   retry con backoff fino a _MAX_RETRIES
        - Errori di rete (timeout):   retry con backoff fino a _MAX_RETRIES
        - Content-Type inatteso:      fallimento immediato (no retry)
        - JSONDecodeError:            fallimento immediato (no retry)

        Il body viene ispezionato PRIMA di chiamare .json() per produrre
        messaggi di errore diagnostici anziché un generico ValueError.
        """
        time.sleep(_REQUEST_DELAY)

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                response = requests.get(BASE_URL, params=params, timeout=15)

                # --- Rate limit: retry con backoff esponenziale + jitter ---
                # All'ultimo tentativo NON dormiamo: non ci sarebbe un'altra
                # iterazione a cui ridare il controllo, quindi rinunciamo subito.
                if response.status_code == 429:
                    if attempt == _MAX_RETRIES:
                        log.warning(
                            "[%s] Rate limit (429) persistente dopo %d tentativi. "
                            "Query saltata: '%s'",
                            self.source_id, _MAX_RETRIES, query,
                        )
                        return None
                    wait = _compute_backoff(attempt)
                    self._log_skip(
                        f"rate limit (429), attendo {wait:.1f}s "
                        f"(tentativo {attempt}/{_MAX_RETRIES})"
                    )
                    time.sleep(wait)
                    continue

                response.raise_for_status()

                # --- Body vuoto: possibile condizione transitoria ---
                # Stessa logica del 429: all'ultimo tentativo non dormiamo.
                if not response.content:
                    if attempt == _MAX_RETRIES:
                        log.warning(
                            "[%s] Risposta vuota persistente dopo %d tentativi. "
                            "Query saltata: '%s'",
                            self.source_id, _MAX_RETRIES, query,
                        )
                        return None
                    wait = _compute_backoff(attempt)
                    log.warning(
                        "[%s] Risposta vuota da GDELT (tentativo %d/%d), "
                        "attendo %.1fs e riprovo. Query: '%s'",
                        self.source_id, attempt, _MAX_RETRIES, wait, query,
                    )
                    time.sleep(wait)
                    continue

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

                return response.json()

            except requests.exceptions.HTTPError as e:
                self._log_error(query, e)
                if attempt == _MAX_RETRIES:
                    return None
                time.sleep(_compute_backoff(attempt))

            except requests.RequestException as e:
                # Timeout, connection error, ecc. — potenzialmente transitori
                self._log_error(query, e)
                if attempt == _MAX_RETRIES:
                    return None
                time.sleep(_compute_backoff(attempt))

            except ValueError as e:
                # JSONDecodeError: body non è JSON valido — no retry.
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
