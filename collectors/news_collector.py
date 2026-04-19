"""
collectors/news_collector.py

Collector per NewsAPI (https://newsapi.org).
Piano gratuito: 100 richieste/giorno, notizie degli ultimi 30 giorni.
"""

import logging

import requests
from config import NEWS_API_KEY
from models import RawRecord
from collectors.base import BaseCollector
from collectors.retry import http_get_with_retry

log = logging.getLogger(__name__)

BASE_URL = "https://newsapi.org/v2/everything"


class NewsCollector(BaseCollector):
    source_id = "news"

    def collect(self, target: str, query: str, max_results: int = 20, **kwargs: object) -> list[RawRecord]:
        """kwargs: language (str ISO 639-1, default "en")."""
        if not NEWS_API_KEY:
            self._log_skip("NEWS_API_KEY non configurata")
            return []

        language: str = str(kwargs.get("language", "en"))

        params = {
            "q": query,
            "apiKey": NEWS_API_KEY,
            "pageSize": min(max_results, 100),
            "sortBy": "relevancy",
            "language": language,
        }

        data = self._fetch(params, query)
        if data is None:
            return []

        articles = data.get("articles", [])

        # Fallback senza lingua: copre target non anglofoni interrogati con language="en".
        if not articles and language:
            log.info(
                "[NewsCollector] 0 risultati con language='%s'. "
                "Riprovo senza filtro lingua per query: '%s'.",
                language, query,
            )
            params_nolang = {k: v for k, v in params.items() if k != "language"}
            data = self._fetch(params_nolang, query)
            if data is None:
                return []
            articles = data.get("articles", [])

        records = [
            self._make_raw(target, query, article)
            for article in articles
            if article.get("url")
        ]

        self._log_collected(query, len(records))
        return records

    def _fetch(self, params: dict, query: str) -> dict | None:
        """Esegue la chiamata HTTP a NewsAPI e gestisce gli errori."""
        try:
            response = http_get_with_retry(
                BASE_URL, params=params, timeout=10, source_id=self.source_id
            )

            if response.status_code == 429:
                log.warning("[NewsCollector] Limite giornaliero raggiunto (HTTP 429).")
                return None

            response.raise_for_status()
            data = response.json()

            if data.get("status") == "error":
                log.warning(
                    "[NewsCollector] Errore API: code='%s', message='%s'",
                    data.get("code", "unknown"),
                    data.get("message", ""),
                )
                return None

            return data

        except requests.RequestException as e:
            self._log_error(query, e)
            return None
