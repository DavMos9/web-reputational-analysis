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
    from collectors.guardian_collector import GuardianCollector
    from collectors.nyt_collector import NytCollector

    return {
        "news":      NewsCollector(),
        "gdelt":     GdeltCollector(),
        "wikipedia": WikipediaCollector(),
        "youtube":   YouTubeCollector(),
        "guardian":  GuardianCollector(),
        "nyt":       NytCollector(),
    }


__all__ = ["build_registry"]
