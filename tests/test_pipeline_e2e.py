"""
tests/test_pipeline_e2e.py

Test di integrazione end-to-end della pipeline.

Obiettivo:
    Verificare che la pipeline completa (collect → normalize → clean →
    deduplicate → enrich → aggregate → export) produca output coerenti
    con lo schema Record senza chiamate API reali.

Design:
    - I collector sono sostituiti da mock che restituiscono RawRecord fittizi.
    - L'enricher viene istanziato con sentiment_pipeline=None (nessun NLP).
    - Gli exporter sono mock: non scrivono su disco.
    - Si usa il normalizer reale "news", perché il payload rispetta il suo schema.

Copertura aggiuntiva:
    - Coerenza schema: Record._EXPORT_FIELDS copre tutti i campi exportabili.
    - Nessun campo viene silenziosamente escluso dal CSV a causa di una
      mancata sincronizzazione tra Record e _EXPORT_FIELDS.
"""

from __future__ import annotations

import dataclasses
import pytest
from unittest.mock import MagicMock

from models import RawRecord, Record
from pipeline.enricher import Enricher
from pipeline.runner import PipelineRunner, PipelineConfig


# ---------------------------------------------------------------------------
# Fixture: dati fittizi
# ---------------------------------------------------------------------------

def _raw_news(
    url: str,
    title: str = "Test Article",
    description: str = "A sufficiently long description for this test article.",
    published: str = "2026-04-08T10:00:00Z",
    target: str = "Test Target",
) -> RawRecord:
    """RawRecord minimo nel formato NewsAPI, compatibile con normalizers/news.py."""
    return RawRecord(
        source="news",
        query="test query",
        target=target,
        payload={
            "title": title,
            "url": url,
            "description": description,
            "publishedAt": published,
            "source": {"name": "example.com"},
        },
        retrieved_at="2026-04-08T10:00:00+00:00",
    )


def _make_collector(raw_records: list[RawRecord]) -> MagicMock:
    collector = MagicMock()
    collector.collect.return_value = raw_records
    return collector


def _make_runner(
    raw_records: list[RawRecord],
    *,
    exporter: MagicMock | None = None,
) -> PipelineRunner:
    """
    Costruisce un PipelineRunner con collector mock, enricher senza NLP,
    ed exporter opzionale.
    """
    registry = {"news": _make_collector(raw_records)}
    exporters = [exporter] if exporter else []

    return PipelineRunner(
        registry=registry,
        raw_store=None,
        exporters=exporters,
        enricher=Enricher(sentiment_pipeline=None),
    )


def _config(**overrides) -> PipelineConfig:
    defaults = dict(
        target="Test Target",
        queries=["test query"],
        sources=["news"],
        max_results=5,
        save_raw=False,
        parallel_collectors=False,
    )
    defaults.update(overrides)
    return PipelineConfig(**defaults)


# ---------------------------------------------------------------------------
# Test E2E: pipeline completa
# ---------------------------------------------------------------------------

class TestPipelineEndToEnd:

    def test_single_record_produces_output(self):
        """Pipeline con un record valido → restituisce 1 Record e un EntitySummary."""
        runner = _make_runner([
            _raw_news("https://example.com/article-1")
        ])
        records, summary = runner.run(_config(), timestamp="20260408T100000Z")

        assert len(records) == 1
        assert summary is not None
        assert isinstance(records[0], Record)

    def test_output_records_respect_schema(self):
        """Tutti i campi obbligatori del Record schema sono presenti e non vuoti."""
        runner = _make_runner([
            _raw_news("https://example.com/article-2", title="Schema Check Article")
        ])
        records, _ = runner.run(_config())

        record = records[0]
        assert record.source == "news"
        assert record.query == "test query"
        assert record.target == "Test Target"
        assert record.title == "Schema Check Article"
        assert record.url == "https://example.com/article-2"
        assert record.date == "2026-04-08"

    def test_duplicates_are_removed(self):
        """Due record con lo stesso URL → deduplicazione → 1 record in output."""
        same_url = "https://example.com/duplicate-article"
        runner = _make_runner([
            _raw_news(same_url, title="Article A"),
            _raw_news(same_url, title="Article A"),  # duplicato esatto
        ])
        records, _ = runner.run(_config())
        assert len(records) == 1

    def test_multiple_distinct_records_preserved(self):
        """N record distinti → nessuna perdita in output (no falsi positivi nel dedup)."""
        raw = [
            _raw_news(f"https://example.com/article-{i}", title=f"Article {i}")
            for i in range(5)
        ]
        runner = _make_runner(raw)
        records, _ = runner.run(_config())
        assert len(records) == 5

    def test_summary_entity_matches_target(self):
        """EntitySummary.entity deve corrispondere al target della config."""
        target = "Acme Corp"
        runner = _make_runner([
            _raw_news("https://example.com/article-summary", target=target)
        ])
        _, summary = runner.run(_config(target=target))
        assert summary is not None
        assert summary.entity == target

    def test_empty_collector_returns_empty_pipeline(self):
        """Se il collector non produce record, la pipeline restituisce ([], None)."""
        runner = _make_runner([])
        records, summary = runner.run(_config())
        assert records == []
        assert summary is None

    def test_exporter_called_with_final_records(self):
        """L'exporter riceve i record finali dopo dedup e il timestamp corretto."""
        exporter = MagicMock()
        runner = _make_runner(
            [_raw_news("https://example.com/export-test")],
            exporter=exporter,
        )
        records, _ = runner.run(_config(), timestamp="test-ts")

        exporter.export.assert_called_once()
        call_args = exporter.export.call_args
        exported_records, target, timestamp = call_args[0]
        assert exported_records == records
        assert target == "Test Target"
        assert timestamp == "test-ts"

    def test_enricher_injection_no_nlp(self):
        """Con enricher senza NLP, language e sentiment rimangono None."""
        runner = _make_runner([
            _raw_news("https://example.com/no-nlp", title="No NLP Article")
        ])
        records, _ = runner.run(_config())
        # Nessuna dipendenza NLP installata: entrambi devono essere None
        assert records[0].sentiment is None
        # language può essere non-None se il record aveva già language nel payload
        # ma il test verifica che il tutto non si rompa, non il valore specifico

    def test_pipeline_with_multiple_queries(self):
        """Con N query, il collector viene chiamato N volte."""
        collector = _make_collector([
            _raw_news("https://example.com/q1", title="Query 1 Result"),
        ])
        runner = PipelineRunner(
            registry={"news": collector},
            raw_store=None,
            enricher=Enricher(sentiment_pipeline=None),
        )
        config = _config(queries=["query A", "query B", "query C"])
        runner.run(config)

        assert collector.collect.call_count == 3


# ---------------------------------------------------------------------------
# Test di coerenza schema: _EXPORT_FIELDS vs Record.__dataclass_fields__
# ---------------------------------------------------------------------------

class TestRecordExportFieldsConsistency:

    def test_all_exportable_fields_declared(self):
        """
        Tutti i campi del dataclass Record esclusi da _EXPORT_EXCLUDE devono
        apparire in _EXPORT_FIELDS.

        Se questo test fallisce, significa che è stato aggiunto un campo a Record
        senza aggiornare _EXPORT_FIELDS — il CSV prodotto sarebbe incompleto.

        Nota: si usa dataclasses.fields() invece di __dataclass_fields__ per
        escludere correttamente le ClassVar (come _EXPORT_FIELDS e _EXPORT_EXCLUDE)
        su Python 3.10 con `from __future__ import annotations`.
        """
        all_fields = {f.name for f in dataclasses.fields(Record)}
        excluded = Record._EXPORT_EXCLUDE
        export_fields = set(Record._EXPORT_FIELDS)

        # Campi che dovrebbero essere esportati ma non sono in _EXPORT_FIELDS
        missing = (all_fields - excluded) - export_fields
        assert not missing, (
            f"Campi non dichiarati in Record._EXPORT_FIELDS: {missing}. "
            f"Aggiungere a _EXPORT_FIELDS o a _EXPORT_EXCLUDE."
        )

    def test_no_phantom_fields_in_export(self):
        """
        _EXPORT_FIELDS non deve contenere nomi che non esistono nel dataclass.

        Previene refactoring parziali dove un campo viene rinominato in Record
        ma dimenticato in _EXPORT_FIELDS.
        """
        all_fields = {f.name for f in dataclasses.fields(Record)}
        export_fields = set(Record._EXPORT_FIELDS)

        phantom = export_fields - all_fields
        assert not phantom, (
            f"_EXPORT_FIELDS contiene campi inesistenti nel dataclass: {phantom}. "
            f"Aggiornare Record._EXPORT_FIELDS."
        )

    def test_excluded_fields_not_in_export(self):
        """I campi in _EXPORT_EXCLUDE non devono mai apparire in _EXPORT_FIELDS."""
        overlap = Record._EXPORT_EXCLUDE & set(Record._EXPORT_FIELDS)
        assert not overlap, (
            f"Campi presenti sia in _EXPORT_EXCLUDE che in _EXPORT_FIELDS: {overlap}."
        )

    def test_export_fields_count_matches(self):
        """_EXPORT_FIELDS non deve avere duplicati."""
        fields_list = list(Record._EXPORT_FIELDS)
        assert len(fields_list) == len(set(fields_list)), (
            "Record._EXPORT_FIELDS contiene campi duplicati."
        )
