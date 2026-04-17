"""
collectors/brave_collector.py

Collector per Brave Search API (Web Search endpoint).

Documentazione:
    https://api-dashboard.search.brave.com/app/documentation/web-search/get-started

Piano gratuito ("Data for AI" Free):
    2.000 query/mese, rate limit 1 query/sec.
Auth:
    Header `X-Subscription-Token: <BRAVE_API_KEY>`.

Il collector interroga l'endpoint /res/v1/web/search e restituisce i risultati
della chiave `web.results`. Non trasforma il payload: il parsing è lavoro del
normalizer (normalizers/brave.py).

Parametri opzionali (tramite **kwargs di collect()):
    country      (str): codice paese ISO 3166-1 alpha-2 (es. "US", "IT").
                        Influenza il ranking dei risultati. Default: non passato
                        (Brave usa il default "US").
    search_lang  (str): lingua ISO 639-1 dei risultati (es. "en", "it").
                        Default: non passato.
    freshness    (str): filtro temporale ("pd"=past day, "pw"=past week,
                        "pm"=past month, "py"=past year). Default: non passato.
    safesearch   (str): "off" | "moderate" | "strict". Default: "moderate".
"""

from __future__ import annotations

import requests
from config import BRAVE_API_KEY
from models import RawRecord
from collectors.base import BaseCollector

BASE_URL = "https://api.search.brave.com/res/v1/web/search"

# Limite massimo del piano gratuito per singola richiesta.
MAX_RESULTS_PER_REQUEST = 20


class BraveCollector(BaseCollector):
    source_id = "brave"

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
            max_results: numero massimo di risultati (cap a 20 nel piano gratuito).
            kwargs:
                country (str):     codice paese ISO 3166-1 alpha-2 (es. "US").
                search_lang (str): lingua ISO 639-1 (es. "en").
                freshness (str):   "pd" | "pw" | "pm" | "py".
                safesearch (str):  "off" | "moderate" | "strict".
        """
        if not BRAVE_API_KEY:
            self._log_skip("BRAVE_API_KEY non configurata")
            return []

        params: dict[str, object] = {
            "q":     query,
            "count": min(max_results, MAX_RESULTS_PER_REQUEST),
        }

        # Parametri opzionali: inclusi solo se presenti in kwargs.
        # Evitiamo di passare valori vuoti/None che Brave potrebbe interpretare
        # come filtri restrittivi.
        for optional in ("country", "search_lang", "freshness", "safesearch"):
            value = kwargs.get(optional)
            if value:
                params[optional] = str(value)

        headers = {
            "Accept":                "application/json",
            "Accept-Encoding":       "gzip",
            "X-Subscription-Token":  BRAVE_API_KEY,
        }

        try:
            response = requests.get(BASE_URL, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            self._log_error(query, e)
            return []

        results = data.get("web", {}).get("results", [])

        records = [
            self._make_raw(target, query, {**item, "_rank": rank})
            for rank, item in enumerate(results, start=1)
            if item.get("url")
        ]

        self._log_collected(query, len(records))
        return records
