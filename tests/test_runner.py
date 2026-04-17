"""
tests/test_runner.py

Test per pipeline/runner.py (PipelineRunner e PipelineConfig).

Copertura:
- PipelineConfig: validazione dei campi obbligatori
- PipelineRunner.run(): lista vuota se nessun collector produce record
- PipelineRunner.run(): scarta sorgenti non nel registry con warning
- PipelineRunner.run(): collector che solleva eccezione non blocca la pipeline
- PipelineRunner.run(): flusso normale — record normalizzati, puliti, deduplicati
- PipelineRunner.run(): save_raw=True invoca raw_store.save()
- PipelineRunner.run(): save_raw=False non invoca raw_store.save()
- PipelineRunner.run(): exporter viene chiamato con i record finali
- PipelineRunner.run(): errore dell'exporter non blocca la pipeline
- PipelineRunner.run(): deduplicazione applicata correttamente
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch, call

from models import RawRecord, Record
from pipeline.runner import PipelineRunner, PipelineConfig


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _raw(source: str = "news", url: str = "https://example.com/1") -> RawRecord:
    return RawRecord(
        source=source,
        query="test query",
        target="Test Target",
        payload={
            "title": "Test Article",
            "url": url,
            "description": "Test description.",
            "publishedAt": "2026-04-08T10:00:00Z",
        },
        retrieved_at="2026-04-08T10:00:00+00:00",
    )


def _make_collector(source_id: str, returns: list[RawRecord]) -> MagicMock:
    """Crea un mock collector che restituisce `returns` quando collect() è chiamato."""
    collector = MagicMock()
    collector.source_id = source_id
    collector.collect.return_value = returns
    return collector


def _config(**overrides) -> PipelineConfig:
    defaults = dict(
        target="Test Target",
        queries=["test query"],
        sources=[],
        max_results=20,
        save_raw=False,
    )
    defaults.update(overrides)
    return PipelineConfig(**defaults)


# ---------------------------------------------------------------------------
# Test: PipelineConfig — validazione
# ---------------------------------------------------------------------------

class TestPipelineConfig:
    def test_empty_target_raises(self):
        with pytest.raises(ValueError, match="target"):
            PipelineConfig(target="", queries=["q"])

    def test_empty_queries_raises(self):
        with pytest.raises(ValueError, match="queries"):
            PipelineConfig(target="T", queries=[])

    def test_max_results_zero_raises(self):
        with pytest.raises(ValueError, match="max_results"):
            PipelineConfig(target="T", queries=["q"], max_results=0)

    def test_valid_config(self):
        cfg = _config(target="Elon Musk", queries=["Elon Musk Tesla"])
        assert cfg.target == "Elon Musk"


# ---------------------------------------------------------------------------
# Test: PipelineRunner.run() — raccolta e orchestrazione
# ---------------------------------------------------------------------------

class TestPipelineRunnerRun:

    def test_no_records_collected_returns_empty(self):
        """Nessun record prodotto → pipeline restituisce lista vuota e summary None."""
        collector = _make_collector("news", returns=[])
        registry = {"news": collector}
        runner = PipelineRunner(registry=registry)
        records, summary = runner.run(_config(sources=["news"]))
        assert records == []
        assert summary is None

    def test_unknown_source_skipped(self):
        """Sorgente non nel registry viene ignorata senza eccezione."""
        registry = {"news": _make_collector("news", returns=[])}
        runner = PipelineRunner(registry=registry)
        # "gdelt" non esiste nel registry
        records, summary = runner.run(_config(sources=["gdelt"]))
        assert records == []
        assert summary is None

    def test_collector_exception_does_not_crash_pipeline(self):
        """Collector che solleva eccezione non interrompe l'esecuzione."""
        bad_collector = MagicMock()
        bad_collector.source_id = "news"
        bad_collector.collect.side_effect = RuntimeError("API down")

        good_collector = _make_collector("gdelt", returns=[_raw("gdelt")])
        registry = {"news": bad_collector, "gdelt": good_collector}

        runner = PipelineRunner(registry=registry)
        records, summary = runner.run(_config(sources=["news", "gdelt"]))
        # I record del collector funzionante devono passare
        assert len(records) >= 1

    def test_normal_flow_returns_records_and_summary(self):
        """Flusso normale: collector produce record → pipeline li restituisce con summary."""
        collector = _make_collector("news", returns=[_raw("news")])
        registry = {"news": collector}
        runner = PipelineRunner(registry=registry)
        records, summary = runner.run(_config(sources=["news"]))
        assert len(records) == 1
        assert isinstance(records[0], Record)
        assert records[0].source == "news"
        assert summary is not None
        assert summary.entity == "Test Target"
        assert summary.record_count == 1

    def test_multiple_queries_called_for_each_query(self):
        """collect() viene chiamato una volta per ogni query."""
        collector = _make_collector("news", returns=[_raw("news")])
        registry = {"news": collector}
        runner = PipelineRunner(registry=registry)

        runner.run(_config(sources=["news"], queries=["q1", "q2"]))

        assert collector.collect.call_count == 2

    def test_deduplication_applied(self):
        """Record con lo stesso URL vengono deduplicati."""
        raw1 = _raw("news", url="https://example.com/same")
        raw2 = _raw("news", url="https://example.com/same")  # duplicato
        collector = _make_collector("news", returns=[raw1, raw2])
        registry = {"news": collector}

        runner = PipelineRunner(registry=registry)
        records, summary = runner.run(_config(sources=["news"]))
        assert len(records) == 1


# ---------------------------------------------------------------------------
# Test: PipelineRunner — raw_store
# ---------------------------------------------------------------------------

class TestPipelineRunnerRawStore:

    def test_save_raw_true_calls_raw_store(self):
        """save_raw=True → raw_store.save() chiamato con i RawRecord."""
        raw_store = MagicMock()
        collector = _make_collector("news", returns=[_raw("news")])
        registry = {"news": collector}

        runner = PipelineRunner(registry=registry, raw_store=raw_store)
        runner.run(_config(sources=["news"], save_raw=True), timestamp="20260409T000000Z")

        raw_store.save.assert_called_once()

    def test_save_raw_false_does_not_call_raw_store(self):
        """save_raw=False → raw_store.save() non viene chiamato."""
        raw_store = MagicMock()
        collector = _make_collector("news", returns=[_raw("news")])
        registry = {"news": collector}

        runner = PipelineRunner(registry=registry, raw_store=raw_store)
        runner.run(_config(sources=["news"], save_raw=False))

        raw_store.save.assert_not_called()

    def test_raw_store_error_does_not_crash_pipeline(self):
        """Errore nel salvataggio raw non interrompe la pipeline."""
        raw_store = MagicMock()
        raw_store.save.side_effect = OSError("Disk full")
        collector = _make_collector("news", returns=[_raw("news")])
        registry = {"news": collector}

        runner = PipelineRunner(registry=registry, raw_store=raw_store)
        records, summary = runner.run(_config(sources=["news"], save_raw=True))

        # La pipeline deve continuare e restituire i record
        assert len(records) >= 1


# ---------------------------------------------------------------------------
# Test: PipelineRunner — exporters
# ---------------------------------------------------------------------------

class TestPipelineRunnerExporters:

    def test_exporter_called_with_final_records(self):
        """exporter.export() viene chiamato con i Record finali."""
        exporter = MagicMock()
        collector = _make_collector("news", returns=[_raw("news")])
        registry = {"news": collector}

        runner = PipelineRunner(registry=registry, exporters=[exporter])
        runner.run(_config(sources=["news"]), timestamp="20260409T000000Z")

        exporter.export.assert_called_once()
        args = exporter.export.call_args
        records_arg = args[0][0] if args[0] else args[1].get("records", [])
        assert len(records_arg) >= 1
        assert isinstance(records_arg[0], Record)

    def test_exporter_error_does_not_crash_pipeline(self):
        """Errore nell'exporter non blocca la pipeline."""
        exporter = MagicMock()
        exporter.export.side_effect = OSError("Disk full")
        collector = _make_collector("news", returns=[_raw("news")])
        registry = {"news": collector}

        runner = PipelineRunner(registry=registry, exporters=[exporter])
        records, summary = runner.run(_config(sources=["news"]))

        # runner deve restituire i record anche se l'exporter ha fallito
        assert len(records) >= 1


# ---------------------------------------------------------------------------
# Test: PipelineRunner — parallelizzazione _collect
# ---------------------------------------------------------------------------

class TestPipelineRunnerParallel:
    """
    Verifica che la parallelizzazione dei collector (ThreadPoolExecutor) produca
    lo stesso output della modalità seriale, in ordine deterministico.
    """

    def test_serial_mode_preserves_output(self):
        """parallel_collectors=False: flusso equivalente al comportamento seriale."""
        c_news = _make_collector("news", returns=[_raw("news", url="https://a/1")])
        c_gdelt = _make_collector("gdelt", returns=[_raw("gdelt", url="https://b/2")])
        registry = {"news": c_news, "gdelt": c_gdelt}

        runner = PipelineRunner(registry=registry)
        records, _ = runner.run(_config(
            sources=["news", "gdelt"],
            parallel_collectors=False,
        ))
        assert len(records) == 2
        # Con 2 sorgenti e 1 query l'ordine deterministico è [news, gdelt].
        assert [r.source for r in records] == ["news", "gdelt"]

    def test_parallel_and_serial_produce_same_output(self):
        """Output parallelo e seriale identici (stesso ordine, stessi record)."""
        c_news = _make_collector("news", returns=[
            _raw("news", url="https://a/1"),
            _raw("news", url="https://a/2"),
        ])
        c_gdelt = _make_collector("gdelt", returns=[_raw("gdelt", url="https://b/1")])
        registry = {"news": c_news, "gdelt": c_gdelt}

        runner = PipelineRunner(registry=registry)
        records_par, _ = runner.run(_config(
            sources=["news", "gdelt"],
            queries=["q1", "q2"],
            parallel_collectors=True,
            max_workers=4,
        ))

        # Reset dei mock prima della seconda esecuzione: le collect() vengono
        # richiamate e i counter vanno azzerati per evitare side-effect.
        c_news.reset_mock()
        c_gdelt.reset_mock()
        c_news.collect.return_value = [
            _raw("news", url="https://a/1"),
            _raw("news", url="https://a/2"),
        ]
        c_gdelt.collect.return_value = [_raw("gdelt", url="https://b/1")]

        records_ser, _ = runner.run(_config(
            sources=["news", "gdelt"],
            queries=["q1", "q2"],
            parallel_collectors=False,
        ))

        # URL in ordine deterministico: stessa sequenza indipendentemente
        # dalla modalità di esecuzione.
        urls_par = [r.url for r in records_par]
        urls_ser = [r.url for r in records_ser]
        assert urls_par == urls_ser

    def test_max_workers_one_forces_serial_behaviour(self):
        """max_workers=1 deve comportarsi come seriale (nessun pool reale)."""
        c = _make_collector("news", returns=[_raw("news")])
        registry = {"news": c}
        runner = PipelineRunner(registry=registry)

        records, _ = runner.run(_config(
            sources=["news"],
            parallel_collectors=True,
            max_workers=1,
        ))
        assert len(records) == 1

    def test_max_workers_zero_raises(self):
        """max_workers < 1 viene rifiutato in PipelineConfig.__post_init__."""
        with pytest.raises(ValueError, match="max_workers"):
            PipelineConfig(target="T", queries=["q"], max_workers=0)


# ---------------------------------------------------------------------------
# Test: PipelineRunner — dry_run
# ---------------------------------------------------------------------------

class TestPipelineRunnerDryRun:
    """
    Copertura:
    - dry_run=True → collector.collect() chiamato con max_results=1
      indipendentemente da config.max_results
    - dry_run=False (default) → collector.collect() chiamato con config.max_results
    - dry_run=True non impedisce l'esecuzione completa della pipeline
    - PipelineConfig.dry_run default è False
    """

    def test_dry_run_forces_max_results_one(self):
        """Con dry_run=True, max_results passato al collector è sempre 1."""
        collector = _make_collector("news", returns=[_raw("news")])
        registry = {"news": collector}
        runner = PipelineRunner(registry=registry)

        runner.run(_config(sources=["news"], max_results=50, dry_run=True))

        call_kwargs = collector.collect.call_args
        actual_max = (
            call_kwargs.kwargs.get("max_results")
            or call_kwargs[1].get("max_results")
            or call_kwargs[0][2]  # posizionale: target, query, max_results
        )
        assert actual_max == 1

    def test_normal_run_uses_config_max_results(self):
        """Con dry_run=False, max_results rispetta config.max_results."""
        collector = _make_collector("news", returns=[_raw("news")])
        registry = {"news": collector}
        runner = PipelineRunner(registry=registry)

        runner.run(_config(sources=["news"], max_results=42, dry_run=False))

        call_kwargs = collector.collect.call_args
        actual_max = (
            call_kwargs.kwargs.get("max_results")
            or call_kwargs[1].get("max_results")
            or call_kwargs[0][2]
        )
        assert actual_max == 42

    def test_dry_run_pipeline_still_produces_output(self):
        """dry_run=True non impedisce alla pipeline di restituire record e summary."""
        collector = _make_collector("news", returns=[_raw("news")])
        registry = {"news": collector}
        runner = PipelineRunner(registry=registry)

        records, summary = runner.run(_config(sources=["news"], dry_run=True))

        assert len(records) == 1
        assert summary is not None

    def test_dry_run_default_is_false(self):
        """PipelineConfig.dry_run deve essere False di default."""
        cfg = PipelineConfig(target="T", queries=["q"])
        assert cfg.dry_run is False

    def test_dry_run_applies_to_all_queries(self):
        """Con dry_run=True e più query, ogni chiamata usa max_results=1."""
        collector = _make_collector("news", returns=[_raw("news")])
        registry = {"news": collector}
        runner = PipelineRunner(registry=registry)

        runner.run(_config(
            sources=["news"],
            queries=["q1", "q2", "q3"],
            max_results=30,
            dry_run=True,
        ))

        assert collector.collect.call_count == 3
        for call in collector.collect.call_args_list:
            actual_max = (
                call.kwargs.get("max_results")
                or call[1].get("max_results")
                or call[0][2]
            )
            assert actual_max == 1
