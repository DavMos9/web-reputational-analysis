"""collectors/__init__.py — build_registry() come unico entry point."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collectors.base import BaseCollector


def build_registry() -> dict[str, "BaseCollector"]:
    """Costruisce il registro source_id → istanza collector con import lazy."""
    from collectors.news_collector import NewsCollector
    from collectors.gdelt_collector import GdeltCollector
    from collectors.wikipedia_collector import WikipediaCollector
    from collectors.youtube_collector import YouTubeCollector
    from collectors.youtube_comments_collector import YouTubeCommentsCollector
    from collectors.guardian_collector import GuardianCollector
    from collectors.nyt_collector import NytCollector
    from collectors.bluesky_collector import BlueskyCollector
    from collectors.stackexchange_collector import StackExchangeCollector
    from collectors.mastodon_collector import MastodonCollector
    from collectors.lemmy_collector import LemmyCollector
    from collectors.wikitalk_collector import WikiTalkCollector
    from collectors.brave_collector import BraveCollector
    from collectors.gnews_it_collector import GNewsItCollector
    from collectors.hackernews_collector import HackerNewsCollector
    from collectors.reddit_collector import RedditCollector
    from collectors.bbc_collector import BbcCollector
    from collectors.ansa_collector import AnsaCollector

    return {
        "news":             NewsCollector(),
        "gdelt":            GdeltCollector(),
        "wikipedia":        WikipediaCollector(),
        "youtube":          YouTubeCollector(),
        "youtube_comments": YouTubeCommentsCollector(),
        "guardian":         GuardianCollector(),
        "nyt":              NytCollector(),
        "bluesky":          BlueskyCollector(),
        "stackexchange":    StackExchangeCollector(),
        "mastodon":         MastodonCollector(),
        "lemmy":            LemmyCollector(),
        "wikitalk":         WikiTalkCollector(),
        "brave":            BraveCollector(),
        "gnews_it":         GNewsItCollector(),
        "hackernews":       HackerNewsCollector(),
        "reddit":           RedditCollector(),
        "bbc":              BbcCollector(),
        "ansa":             AnsaCollector(),
    }


__all__ = ["build_registry"]
