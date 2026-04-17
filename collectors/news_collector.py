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

log = logging.getLogger(__name__)

BASE_URL = "https://newsapi.org/v2/everything"


class NewsCollector(BaseCollector):
    source_id = "news"

    def collect(self, target: str, query: str, max_results: int = 20, **kwargs: object) -> list[RawRecord]:
        """
        Args:
            target:      entità analizzata.
            query:       stringa di ricerca.
            max_results: numero massimo di risultati (max 100 nel piano gratuito).
            kwargs:
                language (str): codice lingua ISO 639-1, default "en".
                                Passare "it" per restringere alle fonti italiane.
        """
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

        try:
            response = requests.get(BASE_URL, params=params, timeout=10)

            if response.status_code == 429:
                log.warning(
                    "[NewsCollector] Limite giornaliero raggiunto (HTTP 429). "
                    "Piano gratuito: 100 req/giorno. Riprova domani o passa a un piano superiore."
                )
                return []

            response.raise_for_status()
            data = response.json()

            # Segnala se si è vicini al limite (campo 'code' nel body di NewsAPI)
            if data.get("status") == "error":
                log.warning(
                    "[NewsCollector] Errore API: code='%s', message='%s'",
                    data.get("code", "unknown"),
                    data.get("message", ""),
                )
                return []

        except requests.RequestException as e:
            self._log_error(query, e)
            return []

        records = [
            self._make_raw(target, query, article)
            for article in data.get("articles", [])
            if article.get("url")
        ]

        self._log_collected(query, len(records))
        return records
