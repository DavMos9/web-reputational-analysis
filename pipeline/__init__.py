"""pipeline — API pubblica del package."""

from pipeline.normalizer import normalize, normalize_all
from pipeline.cleaner import clean, clean_all, filter_quality, filter_quality_all
from pipeline.deduplicator import deduplicate
from pipeline.aggregator import aggregate, EntitySummary
from pipeline.runner import PipelineRunner, PipelineConfig

__all__ = [
    "PipelineRunner",
    "PipelineConfig",
    "normalize", "normalize_all",
    "clean", "clean_all",
    "filter_quality", "filter_quality_all",
    "deduplicate",
    "aggregate", "EntitySummary",
]
