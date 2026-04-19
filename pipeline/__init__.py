"""
pipeline/__init__.py

API pubblica del package pipeline.

Componenti principali:
    PipelineRunner / PipelineConfig  — orchestrazione completa della pipeline
    aggregate / EntitySummary        — aggregazione reputazionale entity-level

Funzioni di step (utili in test, notebook e pipeline custom):
    normalize / normalize_all            — RawRecord → Record
    clean / clean_all                    — pulizia testuale
    filter_quality / filter_quality_all  — filtro qualità minima
    deduplicate                          — deduplicazione URL + titolo/dominio

Nota: Enricher non è esportato da qui perché è un dettaglio implementativo
del runner. Chi vuole usarlo direttamente lo importa da pipeline.enricher.
"""

from pipeline.normalizer import normalize, normalize_all
from pipeline.cleaner import clean, clean_all, filter_quality, filter_quality_all
from pipeline.deduplicator import deduplicate
from pipeline.aggregator import aggregate, EntitySummary
from pipeline.runner import PipelineRunner, PipelineConfig

__all__ = [
    # Orchestrazione
    "PipelineRunner",
    "PipelineConfig",
    # Step della pipeline (uso diretto o testing)
    "normalize", "normalize_all",
    "clean", "clean_all",
    "filter_quality", "filter_quality_all",
    "deduplicate",
    # Aggregazione
    "aggregate", "EntitySummary",
]
