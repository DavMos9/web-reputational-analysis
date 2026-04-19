"""
collectors/reddit_collector.py

Collector per Reddit tramite endpoint JSON non autenticato.

Endpoint:
    https://www.reddit.com/search.json

Autenticazione:
    Nessuna. L'endpoint pubblico non richiede OAuth.
    Richiede però uno User-Agent descrittivo: Reddit blocca richieste con
    UA generico o assente (risponde con 429 o 403 senza spiegazione).

Limiti:
    - ~30 richieste/minuto senza autenticazione.
    - Massimo 100 risultati per richiesta (parametro `limit`).
    - Accesso solo ai post pubblici (niente subreddit privati o quarantinati).
    - I post più vecchi di ~1 anno hanno visibilità ridotta nei risultati di
      ricerca (Reddit penalizza il contenuto datato nella rilevanza).

Uso accademico:
    L'endpoint JSON non autenticato è ampiamente usato nella ricerca accademica
    per analisi di testo e sentiment su dati social. Le policy di Reddit
    del 2023 limitano l'uso commerciale ad alto volume; per ricerca accademica
    a basso volume è prassi consolidata.

Note sui campi:
    - `url` nel payload Reddit punta al contenuto linkato (per link post)
      o è uguale al permalink (per self post). Usiamo sempre il permalink
      come URL canonico del record per garantire unicità nella deduplicazione.
    - `selftext` contiene il corpo del post per i self-post (testo libero).
      Per i link post è vuoto o "[removed]" / "[deleted]".
    - `score` = upvotes netti (upvotes - downvotes). Mappa su likes_count.
"""

from __future__ import annotations

import logging

import requests
from collectors.base import BaseCollector
from collectors.retry import http_get_with_retry
from config import APP_USER_AGENT
from models import RawRecord

log = logging.getLogger(__name__)

_BASE_URL        = "https://www.reddit.com/search.json"
_MAX_RESULTS_CAP = 100

# User-Agent obbligatorio. Reddit blocca richieste con UA generico.
# Letto da config.APP_USER_AGENT (versione sincronizzata con pyproject.toml).
_USER_AGENT = APP_USER_AGENT


class RedditCollector(BaseCollector):
    source_id = "reddit"

    def collect(
        self,
        target: str,
        query: str,
        max_results: int = 20,
        **kwargs: object,
    ) -> list[RawRecord]:
        """
        Args:
            target:      entità analizzata.
            query:       stringa di ricerca.
            max_results: numero massimo di risultati (cap a 100).
            kwargs:
                sort (str): ordinamento risultati — "relevance" (default),
                            "new", "hot", "top". "relevance" è ottimale
                            per reputation analysis.
                time (str): filtro temporale passato come `time_filter` interno —
                            "all" (default), "year", "month", "week", "day".
        """
        sort: str        = str(kwargs.get("sort", "relevance"))
        time_filter: str = str(kwargs.get("time", "all"))

        params = {
            "q":      query,
            "sort":   sort,
            "t":      time_filter,
            "limit":  min(max_results, _MAX_RESULTS_CAP),
            "type":   "link",     # solo post, non commenti
        }

        headers = {"User-Agent": _USER_AGENT}

        try:
            response = http_get_with_retry(
                _BASE_URL,
                params=params,
                headers=headers,
                timeout=15,
                source_id=self.source_id,
            )

            # 403: subreddit privato o blocco User-Agent — non recuperabile con retry.
            if response.status_code == 403:
                log.warning(
                    "[RedditCollector] Accesso negato (HTTP 403). "
                    "Possibile blocco User-Agent o subreddit privato."
                )
                return []

            # 429 persistente dopo i retry di http_get_with_retry
            if response.status_code == 429:
                log.warning(
                    "[RedditCollector] Rate limit (HTTP 429) ancora attivo dopo retry. "
                    "Sorgente skippata per questa esecuzione."
                )
                return []

            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            self._log_error(query, e)
            return []

        children = data.get("data", {}).get("children", [])

        records = [
            self._make_raw(target, query, child.get("data", {}))
            for child in children
            if child.get("data", {}).get("permalink")
        ]

        self._log_collected(query, len(records))
        return records
