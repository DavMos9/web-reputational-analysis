"""
collectors/news_collector.py

Collector per NewsAPI (https://newsapi.org).
Piano gratuito: 100 richieste/giorno, notizie degli ultimi 30 giorni.
"""

import requests
from config import NEWS_API_KEY
from models import RawRecord
from collectors.base import BaseCollector

BASE_URL = "https://newsapi.org/v2/everything"


class NewsCollector(BaseCollector):
    source_id = "news"

    def collect(self, target: str, query: str, page_size: int = 20) -> list[RawRecord]:
        """
        Args:
            target:    entità analizzata.
            query:     stringa di ricerca.
            page_size: numero massimo di risultati (max 100 nel piano gratuito).
        """
        if not NEWS_API_KEY:
            self._log_skip("NEWS_API_KEY non configurata")
            return []

        params = {
            "q": query,
            "apiKey": NEWS_API_KEY,
            "pageSize": min(page_size, 100),
            "sortBy": "relevancy",
            "language": "it",
        }

        try:
            response = requests.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
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
