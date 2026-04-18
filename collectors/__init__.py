"""
collectors/__init__.py

Espone build_registry() come unico entry point per costruire il registro
dei collector. I collector NON vengono importati a livello di modulo:
questo evita dipendenze transitive durante i test.

Esempio:
    from collectors import build_registry
    registry = build_registry()          # importa i collector solo qui
    runner = PipelineRunner(registry=registry, ...)

In test che coinvolgono un singolo collector, importa direttamente
il modulo concreto senza passare da qui:
    from collectors.news_collector import NewsCollector
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collectors.base import BaseCollector


def build_registry() -> dict[str, "BaseCollector"]:
    """
    Costruisce e restituisce il registro source_id → istanza collector.

    L'import dei collector avviene qui, in modo lazy: il semplice
    ``import collectors`` o ``from collectors.base import BaseCollector``
    non causerà l'import di tutti i collector e delle loro dipendenze.
    """
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
