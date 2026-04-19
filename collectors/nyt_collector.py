"""
collectors/nyt_collector.py

Collector per NYT Article Search API.
Piano gratuito: 10 req/min, 4.000 req/giorno. Archivio dal 1851.
Registrazione: https://developer.nytimes.com/accounts/create
"""

import requests
from config import NYT_API_KEY
from models import RawRecord
from collectors.base import BaseCollector
from collectors.retry import http_get_with_retry

BASE_URL = "https://api.nytimes.com/svc/search/v2/articlesearch.json"


class NytCollector(BaseCollector):
    source_id = "nyt"

    def collect(self, target: str, query: str, max_results: int = 10, **kwargs: object) -> list[RawRecord]:
        if not NYT_API_KEY:
            self._log_skip("NYT_API_KEY non configurata")
            return []

        params = {
            "q":       query,
            "api-key": NYT_API_KEY,
            "sort":    "relevance",
        }

        try:
            response = http_get_with_retry(
                BASE_URL, params=params, timeout=10, source_id=self.source_id
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            self._log_error(query, e)
            return []

        docs = data.get("response", {}).get("docs", [])

        records = [
            self._make_raw(target, query, {**doc, "_rank": rank})
            for rank, doc in enumerate(docs[:max_results], start=1)
            if doc.get("web_url")
        ]

        self._log_collected(query, len(records))
        return records
