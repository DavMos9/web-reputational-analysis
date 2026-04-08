from collectors.base import BaseCollector
from collectors.news_collector import NewsCollector
from collectors.gdelt_collector import GdeltCollector
from collectors.wikipedia_collector import WikipediaCollector
from collectors.youtube_collector import YouTubeCollector
from collectors.guardian_collector import GuardianCollector
from collectors.nyt_collector import NytCollector

# Registro centralizzato: source_id → istanza collector
# Usato dal PipelineRunner per selezionare i collector attivi.
REGISTRY: dict[str, BaseCollector] = {
    "news":      NewsCollector(),
    "gdelt":     GdeltCollector(),
    "wikipedia": WikipediaCollector(),
    "youtube":   YouTubeCollector(),
    "guardian":  GuardianCollector(),
    "nyt":       NytCollector(),
}

__all__ = [
    "BaseCollector",
    "NewsCollector",
    "GdeltCollector",
    "WikipediaCollector",
    "YouTubeCollector",
    "GuardianCollector",
    "NytCollector",
    "REGISTRY",
]
