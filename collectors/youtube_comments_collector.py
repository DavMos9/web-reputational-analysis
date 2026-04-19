"""
collectors/youtube_comments_collector.py — Commenti YouTube Data API v3.

Quota: ~100 + max_videos unità per chiamata. Piano gratuito: 10.000 unità/giorno.
403 su video con commenti disabilitati: il video viene saltato.
"""

import logging

import requests

from collectors.base import BaseCollector
from collectors.retry import http_get_with_retry
from config import YOUTUBE_API_KEY
from models import RawRecord

log = logging.getLogger(__name__)

BASE_URL = "https://www.googleapis.com/youtube/v3"


class YouTubeCommentsCollector(BaseCollector):
    source_id = "youtube_comments"

    def collect(
        self,
        target: str,
        query: str,
        max_results: int = 50,
        max_videos: int = 3,
        order: str = "relevance",
        **kwargs: object,
    ) -> list[RawRecord]:
        """order: "relevance" (default) o "time"."""
        if not YOUTUBE_API_KEY:
            self._log_skip("YOUTUBE_API_KEY non configurata")
            return []

        video_items = self._search_videos(query, max_videos)
        if not video_items:
            self._log_collected(query, 0)
            return []

        comments_per_video = max(1, max_results // len(video_items))
        records: list[RawRecord] = []

        for video_item in video_items:
            video_id = video_item.get("id", {}).get("videoId")
            if not video_id:
                continue
            video_title = video_item.get("snippet", {}).get("title", "")
            payloads = self._fetch_comments(video_id, video_title, comments_per_video, order)
            records.extend(self._make_raw(target, query, p) for p in payloads)

        records = records[:max_results]
        self._log_collected(query, len(records))
        return records

    def _search_videos(self, query: str, max_videos: int) -> list[dict]:
        """Cerca i video più rilevanti per la query."""
        params = {
            "part":       "snippet",
            "q":          query,
            "type":       "video",
            "maxResults": min(max_videos, 10),
            "key":        YOUTUBE_API_KEY,
            "order":      "relevance",
        }
        try:
            response = http_get_with_retry(f"{BASE_URL}/search", params=params, timeout=10, source_id=self.source_id)
            response.raise_for_status()
            return response.json().get("items", [])
        except requests.RequestException as e:
            self._log_error(query, e)
            return []

    def _fetch_comments(
        self,
        video_id: str,
        video_title: str,
        max_per_video: int,
        order: str,
    ) -> list[dict]:
        """Recupera i top-level comment di un video. 403 = commenti disabilitati."""
        params = {
            "part":       "snippet",
            "videoId":    video_id,
            "maxResults": min(max_per_video, 100),
            "order":      order,
            "key":        YOUTUBE_API_KEY,
            "textFormat": "plainText",
        }
        try:
            response = http_get_with_retry(f"{BASE_URL}/commentThreads", params=params, timeout=10, source_id=self.source_id)

            if response.status_code == 403:
                log.warning(
                    "[%s] Commenti disabilitati per video '%s' (%s)",
                    self.source_id, video_title, video_id,
                )
                return []

            response.raise_for_status()
            items = response.json().get("items", [])

        except requests.RequestException as e:
            self._log_error(video_id, e)
            return []

        payloads = []
        for item in items:
            top_comment = item.get("snippet", {}).get("topLevelComment", {})
            payloads.append({
                "comment":     top_comment,
                "reply_count": item.get("snippet", {}).get("totalReplyCount", 0),
                "video_id":    video_id,
                "video_title": video_title,
            })
        return payloads
