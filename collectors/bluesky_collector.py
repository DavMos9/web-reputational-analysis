"""
collectors/bluesky_collector.py

Collector per Bluesky Social tramite AT Protocol API.

Autenticazione richiesta: l'endpoint searchPosts non è più accessibile
in forma anonima. Richiede App Password (BLUESKY_HANDLE + BLUESKY_APP_PASSWORD).

Come ottenere le credenziali:
  1. Accedi a https://bsky.app/settings/app-passwords
  2. Crea una App Password dedicata (non usare la password principale)
  3. Imposta BLUESKY_HANDLE=tuo.handle.bsky.social e BLUESKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx

Documentazione API:
  - Login:  https://docs.bsky.app/docs/api/com-atproto-server-create-session
  - Search: https://docs.bsky.app/docs/api/app-bsky-feed-search-posts

Rate limit: 1500 richieste/5 minuti per IP + account.
"""

from __future__ import annotations

import logging
import threading
import requests

from collectors.base import BaseCollector
from collectors.retry import http_get_with_retry
from config import BLUESKY_HANDLE, BLUESKY_APP_PASSWORD
from models import RawRecord

log = logging.getLogger(__name__)

_BASE_URL = "https://bsky.social/xrpc"
_SESSION_URL = f"{_BASE_URL}/com.atproto.server.createSession"
_SEARCH_URL  = f"{_BASE_URL}/app.bsky.feed.searchPosts"

# Limite max per singola richiesta (imposto dall'API)
_MAX_LIMIT = 100

# Timeout HTTP unico per tutte le chiamate Bluesky (login + search).
# Valore conservativo (20s) perché l'endpoint searchPosts può essere lento sotto
# carico: 10s causavano timeout sporadici su target con alto volume di post.
_HTTP_TIMEOUT = 20.0


class BlueskyCollector(BaseCollector):
    source_id = "bluesky"

    def __init__(self) -> None:
        self._access_jwt: str | None = None
        # Lock per proteggere _access_jwt in contesto parallelo (ThreadPoolExecutor):
        # più thread possono chiamare collect() sulla stessa istanza contemporaneamente.
        self._jwt_lock: threading.Lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def collect(
        self,
        target: str,
        query: str,
        max_results: int = 50,
        sort: str = "latest",
        **kwargs: object,
    ) -> list[RawRecord]:
        """
        Args:
            target:      entità analizzata.
            query:       stringa di ricerca.
            max_results: numero massimo di post (max 100).
            sort:        "latest" (cronologico, default) o "top" (per engagement).
        """
        if not BLUESKY_HANDLE or not BLUESKY_APP_PASSWORD:
            self._log_skip("BLUESKY_HANDLE e BLUESKY_APP_PASSWORD non configurati")
            return []

        token = self._get_token()
        if not token:
            return []

        params = {
            "q":     query,
            "limit": min(max_results, _MAX_LIMIT),
            "sort":  sort,
        }
        headers = {"Authorization": f"Bearer {token}"}

        try:
            response = http_get_with_retry(
                _SEARCH_URL, params=params, headers=headers, timeout=_HTTP_TIMEOUT,
                source_id=self.source_id,
            )
            # Token scaduto → invalida il cache e rigenera una volta.
            # Il reset sotto lock garantisce che altri thread non usino un token scaduto.
            if response.status_code == 401:
                log.info("[bluesky] Token scaduto, rinnovo sessione.")
                with self._jwt_lock:
                    self._access_jwt = None
                token = self._get_token()
                if not token:
                    return []
                headers = {"Authorization": f"Bearer {token}"}
                response = http_get_with_retry(
                    _SEARCH_URL, params=params, headers=headers, timeout=_HTTP_TIMEOUT,
                    source_id=self.source_id,
                )
            response.raise_for_status()
            posts = response.json().get("posts", [])
        except requests.RequestException as e:
            self._log_error(query, e)
            return []

        if not posts:
            self._log_collected(query, 0)
            return []

        records = [self._make_raw(target, query, post) for post in posts]
        self._log_collected(query, len(records))
        return records

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_token(self) -> str | None:
        """
        Restituisce il JWT in cache o ne crea uno nuovo.

        Thread-safe: usa un Lock per prevenire login concorrenti se più thread
        chiamano collect() sulla stessa istanza (ThreadPoolExecutor con più query).
        Double-checked locking: fast path senza lock se il token è già disponibile.
        """
        # Fast path: token già disponibile, nessun lock richiesto.
        if self._access_jwt:
            return self._access_jwt

        with self._jwt_lock:
            # Secondo check dentro il lock: un altro thread potrebbe aver già
            # completato il login mentre aspettavamo.
            if self._access_jwt:
                return self._access_jwt

            try:
                response = requests.post(
                    _SESSION_URL,
                    json={"identifier": BLUESKY_HANDLE, "password": BLUESKY_APP_PASSWORD},
                    timeout=_HTTP_TIMEOUT,
                )
                response.raise_for_status()
                self._access_jwt = response.json()["accessJwt"]
                log.info("[bluesky] Sessione autenticata creata per %s.", BLUESKY_HANDLE)
                return self._access_jwt
            except requests.RequestException as e:
                log.error("[bluesky] Impossibile creare sessione: %s", e)
                return None
            except KeyError:
                log.error("[bluesky] Risposta login non contiene accessJwt.")
                return None
