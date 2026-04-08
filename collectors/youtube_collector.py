"""
YouTube Data API v3 Collector
Recupera video e canali tramite YouTube Data API v3.
Piano gratuito: 10.000 unità/giorno (una ricerca = 100 unità).
"""

import requests
from datetime import datetime, timezone
from urllib.parse import urlparse
from config import YOUTUBE_API_KEY

BASE_URL = "https://www.googleapis.com/youtube/v3"
YOUTUBE_DOMAIN = "youtube.com"


def _empty_record() -> dict:
    return {
        "source_type": "youtube",
        "source_name": "YouTube",
        "target_entity": "",
        "query": "",
        "title": "",
        "snippet": "",
        "content": "",
        "url": "",
        "domain": YOUTUBE_DOMAIN,
        "author": "",
        "published_at": None,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "language": "",
        "country": "",
        "rank": None,
        "views_count": None,
        "likes_count": None,
        "comments_count": None,
        "engagement_score": None,
        "keywords_found": [],
        "sentiment_stub": None,
        "raw_payload": {},
    }


def _get_video_stats(video_ids: list[str]) -> dict:
    """Recupera statistiche (views, likes, comments) per una lista di video ID."""
    if not video_ids:
        return {}

    params = {
        "part": "statistics",
        "id": ",".join(video_ids),
        "key": YOUTUBE_API_KEY,
    }
    try:
        response = requests.get(f"{BASE_URL}/videos", params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return {
            item["id"]: item.get("statistics", {})
            for item in data.get("items", [])
        }
    except requests.RequestException as e:
        print(f"[YouTube] Errore nel recupero statistiche: {e}")
        return {}


def collect(target_entity: str, query: str, max_results: int = 20) -> list[dict]:
    """
    Recupera video da YouTube Data API per la query indicata.

    Args:
        target_entity: entità oggetto dell'analisi (es. "Mario Rossi")
        query: stringa di ricerca (es. "Mario Rossi intervista")
        max_results: numero massimo di risultati (max 50 per richiesta)

    Returns:
        Lista di record normalizzati secondo il data contract.
    """
    if not YOUTUBE_API_KEY:
        print("[YouTube] YOUTUBE_API_KEY non configurata. Salto la raccolta.")
        return []

    params = {
        "part": "snippet",
        "q": query,
        "type": "video",
        "maxResults": min(max_results, 50),
        "key": YOUTUBE_API_KEY,
        "order": "relevance",
    }

    try:
        response = requests.get(f"{BASE_URL}/search", params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        print(f"[YouTube] Errore nella richiesta: {e}")
        return []

    items = data.get("items", [])
    if not items:
        print(f"[YouTube] Nessun risultato per query: '{query}'")
        return []

    # Recupera statistiche per tutti i video in un'unica chiamata
    video_ids = [
        item["id"]["videoId"]
        for item in items
        if item.get("id", {}).get("videoId")
    ]
    stats_map = _get_video_stats(video_ids)

    records = []
    for rank, item in enumerate(items, start=1):
        video_id = item.get("id", {}).get("videoId")
        if not video_id:
            continue

        snippet = item.get("snippet", {})
        stats = stats_map.get(video_id, {})
        url = f"https://www.youtube.com/watch?v={video_id}"

        views = stats.get("viewCount")
        likes = stats.get("likeCount")
        comments = stats.get("commentCount")

        record = _empty_record()
        record["target_entity"] = target_entity
        record["query"] = query
        record["title"] = snippet.get("title") or ""
        record["snippet"] = snippet.get("description") or ""
        record["url"] = url
        record["author"] = snippet.get("channelTitle") or ""
        record["published_at"] = snippet.get("publishedAt") or None
        record["rank"] = rank
        record["views_count"] = int(views) if views else None
        record["likes_count"] = int(likes) if likes else None
        record["comments_count"] = int(comments) if comments else None
        record["raw_payload"] = {**item, "statistics": stats}

        records.append(record)

    print(f"[YouTube] Raccolti {len(records)} video per query: '{query}'")
    return records
