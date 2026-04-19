"""
collectors/youtube_collector.py

Collector per YouTube Data API v3.
Piano gratuito: 10.000 unità/giorno (search = 100 unità, videos.list = 1 unità/video).
"""

import logging

import requests

from collectors.base import BaseCollector
from collectors.retry import http_get_with_retry
from config import YOUTUBE_API_KEY
from models import RawRecord

log = logging.getLogger(__name__)

BASE_URL = "https://www.googleapis.com/youtube/v3"


class YouTubeCollector(BaseCollector):
    source_id = "youtube"

    def collect(self, target: str, query: str, max_results: int = 20, **kwargs: object) -> list[RawRecord]:
        if not YOUTUBE_API_KEY:
            self._log_skip("YOUTUBE_API_KEY non configurata")
            return []

        params = {
            "part":       "snippet",
            "q":          query,
            "type":       "video",
            "maxResults": min(max_results, 50),
            "key":        YOUTUBE_API_KEY,
            "order":      "relevance",
        }

        try:
            response = http_get_with_retry(f"{BASE_URL}/search", params=params, timeout=10, source_id=self.source_id)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            self._log_error(query, e)
            return []

        items = data.get("items", [])
        if not items:
            self._log_collected(query, 0)
            return []

        video_ids = [
            item["id"]["videoId"]
            for item in items
            if item.get("id", {}).get("videoId")
        ]
        stats_map = self._fetch_stats(video_ids)

        records = []
        for rank, item in enumerate(items, start=1):
            video_id = item.get("id", {}).get("videoId")
            if not video_id:
                continue

            payload = {
                **item,
                "statistics": stats_map.get(video_id, {}),
                "rank":       rank,
            }
            records.append(self._make_raw(target, query, payload))

        self._log_collected(query, len(records))
        return records

    def _fetch_stats(self, video_ids: list[str]) -> dict[str, dict]:
        """Recupera statistiche per una lista di video in una sola chiamata."""
        if not video_ids:
            return {}

        params = {
            "part": "statistics",
            "id":   ",".join(video_ids),
            "key":  YOUTUBE_API_KEY,
        }
        try:
            response = http_get_with_retry(f"{BASE_URL}/videos", params=params, timeout=10, source_id=self.source_id)
            response.raise_for_status()
            data = response.json()
            return {
                item["id"]: item.get("statistics", {})
                for item in data.get("items", [])
            }
        except requests.RequestException as e:
            self._log_error("stats", e)
            return {}
