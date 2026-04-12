"""
collectors/mastodon_collector.py

Collector per Mastodon tramite API v2 (ricerca) e v1 (timeline hashtag).

Strategia di raccolta:
1. Ricerca full-text via /api/v2/search (type=statuses).
   Funziona senza autenticazione dalla versione 4.0.0, ma la ricerca sugli
   statuses dipende dalla configurazione ElasticSearch del server.
   mastodon.social (istanza più grande) ha ElasticSearch attivo.
2. Fallback: se la ricerca non restituisce statuses, usa la timeline
   pubblica per hashtag (/api/v1/timelines/tag/:hashtag).

Autenticazione: opzionale, instance-specific.
- Un access token Mastodon è valido SOLO sull'istanza dove è stato creato.
  Es: un token creato su mastodon.social funziona solo su mastodon.social.
- MASTODON_ACCESS_TOKEN + MASTODON_TOKEN_INSTANCE nel .env specificano
  quale token usare e a quale istanza appartiene.
- Su istanze senza token: fallback automatico su hashtag timeline (sempre pubblico).
- Per ottenere un token app-level senza login utente:
    POST /api/v1/apps → client_id, client_secret
    POST /oauth/token (grant_type=client_credentials)

Documentazione: https://docs.joinmastodon.org/methods/search/
Rate limit: 300 richieste / 5 minuti per IP (unauthenticated).
"""

from __future__ import annotations

import logging
import re
import time

import requests

from collectors.base import BaseCollector
from config import MASTODON_ACCESS_TOKEN, MASTODON_TOKEN_INSTANCE, MASTODON_INSTANCES
from models import RawRecord

log = logging.getLogger(__name__)

# Limite massimo dell'API per la ricerca (max 40 per tipo)
_SEARCH_LIMIT = 40

# Limite per la timeline hashtag
_TIMELINE_LIMIT = 40

# Pausa tra richieste a istanze diverse
_INTER_INSTANCE_DELAY = 0.5


class MastodonCollector(BaseCollector):
    source_id = "mastodon"

    def collect(
        self,
        target: str,
        query: str,
        max_results: int = 30,
        instances: tuple[str, ...] | None = None,
        **kwargs,
    ) -> list[RawRecord]:
        """
        Args:
            target:      entità analizzata.
            query:       stringa di ricerca.
            max_results: numero massimo di risultati per istanza (max 40).
            instances:   tuple di istanze Mastodon da interrogare.
                         Default da config.MASTODON_INSTANCES.
        """
        instances = instances or MASTODON_INSTANCES
        limit = min(max_results, _SEARCH_LIMIT)

        all_records: list[RawRecord] = []

        for instance in instances:
            records = self._collect_from_instance(target, query, instance, limit)
            all_records.extend(records)

            if len(instances) > 1:
                time.sleep(_INTER_INSTANCE_DELAY)

        self._log_collected(query, len(all_records))
        return all_records

    def _collect_from_instance(
        self,
        target: str,
        query: str,
        instance: str,
        limit: int,
    ) -> list[RawRecord]:
        """
        Raccoglie post da una singola istanza Mastodon.
        Prova prima la ricerca full-text, poi fallback su hashtag timeline.
        """
        base_url = f"https://{instance}"

        # Strategia 1: ricerca full-text sugli statuses
        records = self._search_statuses(target, query, instance, base_url, limit)
        if records:
            return records

        # Strategia 2: fallback su hashtag timeline
        log.info(
            "[%s] Ricerca su '%s' non ha restituito statuses, provo timeline hashtag",
            self.source_id, instance,
        )
        return self._hashtag_timeline(target, query, instance, base_url, limit)

    def _build_headers(self, instance: str) -> dict[str, str]:
        """
        Costruisce gli header HTTP.

        Il token viene incluso SOLO se l'istanza corrisponde a quella
        per cui il token è stato generato (MASTODON_TOKEN_INSTANCE).
        Un token Mastodon non è valido su istanze diverse dalla propria.
        """
        headers: dict[str, str] = {
            "Accept": "application/json",
        }
        if MASTODON_ACCESS_TOKEN and instance == MASTODON_TOKEN_INSTANCE:
            headers["Authorization"] = f"Bearer {MASTODON_ACCESS_TOKEN}"
        return headers

    def _search_statuses(
        self,
        target: str,
        query: str,
        instance: str,
        base_url: str,
        limit: int,
    ) -> list[RawRecord]:
        """Ricerca full-text via /api/v2/search."""
        params: dict[str, object] = {
            "q": query,
            "type": "statuses",
            "limit": limit,
        }

        try:
            response = requests.get(
                f"{base_url}/api/v2/search",
                params=params,
                headers=self._build_headers(instance),
                timeout=15,
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            self._log_error(query, e)
            return []

        statuses = data.get("statuses", [])
        if not statuses:
            return []

        records = []
        for status in statuses:
            status["_instance"] = instance
            records.append(self._make_raw(target, query, status))

        log.info(
            "[%s] Ricerca su '%s': %d statuses per query '%s'",
            self.source_id, instance, len(records), query,
        )
        return records

    def _hashtag_timeline(
        self,
        target: str,
        query: str,
        instance: str,
        base_url: str,
        limit: int,
    ) -> list[RawRecord]:
        """
        Fallback: recupera post dalla timeline pubblica di un hashtag.
        Converte la query in hashtag rimuovendo spazi e caratteri speciali.
        """
        hashtag = self._query_to_hashtag(query)
        if not hashtag:
            log.warning(
                "[%s] Impossibile derivare un hashtag dalla query '%s'",
                self.source_id, query,
            )
            return []

        params: dict[str, object] = {
            "limit": min(limit, _TIMELINE_LIMIT),
        }

        try:
            response = requests.get(
                f"{base_url}/api/v1/timelines/tag/{hashtag}",
                params=params,
                headers=self._build_headers(instance),
                timeout=15,
            )
            response.raise_for_status()
            statuses = response.json()
        except requests.RequestException as e:
            self._log_error(query, e)
            return []

        if not statuses or not isinstance(statuses, list):
            log.info(
                "[%s] Nessun post per hashtag '#%s' su '%s'",
                self.source_id, hashtag, instance,
            )
            return []

        records = []
        for status in statuses:
            status["_instance"] = instance
            status["_hashtag_fallback"] = hashtag
            records.append(self._make_raw(target, query, status))

        log.info(
            "[%s] Timeline hashtag '#%s' su '%s': %d statuses",
            self.source_id, hashtag, instance, len(records),
        )
        return records

    @staticmethod
    def _query_to_hashtag(query: str) -> str:
        """
        Converte una query di ricerca in un hashtag Mastodon.

        Logica:
        - Rimuove caratteri non alfanumerici (tranne underscore)
        - Unisce le parole in CamelCase
        - Es: "Elon Musk" → "ElonMusk", "open ai" → "OpenAi"
        """
        cleaned = re.sub(r"[^\w\s]", "", query)
        words = cleaned.split()
        if not words:
            return ""
        return "".join(w.capitalize() for w in words)
