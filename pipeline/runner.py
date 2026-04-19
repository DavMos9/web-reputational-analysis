"""pipeline/runner.py — Orchestratore della pipeline dati (collect → normalize → clean → deduplicate → enrich → export)."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from models import RawRecord, Record
from pipeline.normalizer import normalize_all
from pipeline.cleaner import clean_all, filter_quality
from pipeline.date_filter import filter_by_date, parse_since
from pipeline.deduplicator import deduplicate
from pipeline.enricher import Enricher
from pipeline.aggregator import aggregate, EntitySummary

log = logging.getLogger(__name__)


@runtime_checkable
class RawStoreProtocol(Protocol):
    def save(self, records: list[RawRecord], target: str, timestamp: str) -> None: ...


@runtime_checkable
class ExporterProtocol(Protocol):
    def export(self, records: list[Record], target: str, timestamp: str) -> None: ...


@runtime_checkable
class SummaryExporterProtocol(Protocol):
    def export_summary(self, summary: EntitySummary, timestamp: str) -> None: ...


@dataclass
class PipelineConfig:
    """
    Parametri di esecuzione della pipeline.

    sources vuota ([]) = usa tutte le sorgenti del registry (comportamento default).
    dry_run forza max_results=1 per ogni collector/query.
    Record con date=None passano sempre il filtro since.
    """
    target: str
    queries: list[str]
    sources: list[str]              = field(default_factory=list)
    max_results: int                = 20
    save_raw: bool                  = True
    collector_kwargs: dict[str, dict] = field(default_factory=dict)
    parallel_collectors: bool       = True
    max_workers: int                = 8
    since: str | None               = None
    dry_run: bool                   = False

    def __post_init__(self) -> None:
        if not self.target:
            raise ValueError("PipelineConfig.target non può essere vuoto")
        if not self.queries:
            raise ValueError("PipelineConfig.queries non può essere vuota")
        if self.max_results < 1:
            raise ValueError(f"PipelineConfig.max_results deve essere >= 1, ricevuto: {self.max_results}")
        if self.max_workers < 1:
            raise ValueError(f"PipelineConfig.max_workers deve essere >= 1, ricevuto: {self.max_workers}")
        if self.since is not None:
            self.since = parse_since(self.since)


class PipelineRunner:

    def __init__(
        self,
        registry: dict,
        raw_store: RawStoreProtocol | None = None,
        exporters: list[ExporterProtocol] | None = None,
        summary_exporters: list[SummaryExporterProtocol] | None = None,
        enricher: Enricher | None = None,
    ) -> None:
        self._registry  = registry
        self._raw_store = raw_store
        self._exporters = exporters or []
        self._summary_exporters = summary_exporters or []
        self._enricher  = enricher or Enricher()

    def run(self, config: PipelineConfig, timestamp: str = "") -> tuple[list[Record], EntitySummary | None]:
        """Esegue la pipeline completa. Ritorna (records, summary) o ([], None) se nessun record."""
        # Fail-fast: valida sorgenti prima di qualsiasi I/O di rete.
        if config.sources:
            unknown = [s for s in config.sources if s not in self._registry]
            if unknown:
                raise ValueError(
                    f"Sorgenti sconosciute nella configurazione: {unknown}. "
                    f"Sorgenti disponibili: {sorted(self._registry)}"
                )

        log.info("=== Pipeline avviata: target='%s', fonti=%s ===",
                 config.target, config.sources or list(self._registry))

        raw_records = self._collect(config)
        log.info("Raccolti %d RawRecord totali.", len(raw_records))
        if not raw_records:
            log.warning("Nessun record raccolto. Pipeline terminata.")
            return [], None

        self._save_raw(raw_records, config, timestamp)

        records = self._normalize_clean_filter(raw_records, config)

        records, n_removed = self._deduplicate(records)
        log.info("Deduplicati: %d rimossi, %d record unici.", n_removed, len(records))
        if not records:
            log.warning("Nessun record rimasto dopo deduplicazione.")
            return [], None

        records = self._enrich(records)
        summary = aggregate(records)
        self._export_all(records, summary, config, timestamp)

        log.info("=== Pipeline completata: %d record finali, reputation=%.4f ===",
                 len(records), summary.reputation_score)
        return records, summary

    def _save_raw(
        self,
        raw_records: list[RawRecord],
        config: PipelineConfig,
        timestamp: str,
    ) -> None:
        """Salva i RawRecord grezzi tramite raw_store (effetto collaterale opzionale)."""
        if not (config.save_raw and self._raw_store):
            return
        try:
            self._raw_store.save(raw_records, config.target, timestamp)
        except Exception as e:
            log.error("Errore durante il salvataggio raw: %s", e)

    def _normalize_clean_filter(
        self,
        raw_records: list[RawRecord],
        config: PipelineConfig,
    ) -> list[Record]:
        # Filtro temporale prima di dedup/enrich per non sprecare lavoro su record fuori finestra.
        records = normalize_all(raw_records)
        records = clean_all(records)

        records, skipped = filter_quality(records)
        log.info("Puliti: %d record validi, %d scartati per qualità.", len(records), skipped)

        if config.since:
            records, dropped = filter_by_date(records, config.since)
            log.info("Filtro temporale (>= %s): %d mantenuti, %d scartati.",
                     config.since, len(records), dropped)

        return records

    def _deduplicate(self, records: list[Record]) -> tuple[list[Record], int]:
        """Applica la deduplicazione e restituisce (record_unici, n_rimossi)."""
        deduped, removed = deduplicate(records)
        return deduped, removed

    def _enrich(self, records: list[Record]) -> list[Record]:
        # Posizionato dopo dedup per non sprecare NLP su duplicati.
        return self._enricher.enrich_all(records)

    def _export_all(
        self,
        records: list[Record],
        summary: EntitySummary,
        config: PipelineConfig,
        timestamp: str,
    ) -> None:
        """Invoca tutti gli exporter (record-level e summary). Isola gli errori."""
        for exporter in self._exporters:
            try:
                exporter.export(records, config.target, timestamp)
            except Exception as e:
                log.error("Errore exporter %s: %s", type(exporter).__name__, e)

        for exporter in self._summary_exporters:
            try:
                exporter.export_summary(summary, timestamp)
            except Exception as e:
                log.error("Errore summary exporter %s: %s", type(exporter).__name__, e)

    def _collect(self, config: PipelineConfig) -> list[RawRecord]:
        """Invoca i collector per ogni (sorgente, query). Output riordinato per riproducibilità."""
        active_sources = config.sources if config.sources else list(self._registry)

        tasks: list[tuple[int, str, str, dict]] = []
        for s_idx, source_id in enumerate(active_sources):
            if source_id not in self._registry:
                log.warning("Sorgente '%s' non trovata nel registry, ignorata.", source_id)
                continue
            extra_kwargs = config.collector_kwargs.get(source_id, {})
            for q_idx, query in enumerate(config.queries):
                task_index = s_idx * len(config.queries) + q_idx
                tasks.append((task_index, source_id, query, extra_kwargs))

        if not tasks:
            return []

        use_parallel = config.parallel_collectors and config.max_workers > 1
        if use_parallel:
            results = self._collect_parallel(config, tasks)
        else:
            results = self._collect_serial(config, tasks)

        results.sort(key=lambda r: r[0])
        all_raws: list[RawRecord] = []
        for _, raws in results:
            all_raws.extend(raws)
        return all_raws

    def _run_single_task(
        self,
        config: PipelineConfig,
        source_id: str,
        query: str,
        extra_kwargs: dict,
    ) -> list[RawRecord]:
        """Esegue un singolo task collector.collect(). Isola gli errori."""
        collector = self._registry[source_id]
        effective_max = 1 if config.dry_run else config.max_results
        log.info("Raccolta da '%s' per query: '%s'", source_id, query)
        try:
            return collector.collect(
                target=config.target,
                query=query,
                max_results=effective_max,
                **extra_kwargs,
            )
        except Exception as e:
            log.error("Errore collector '%s' / query '%s': %s", source_id, query, e)
            return []

    def _collect_serial(
        self,
        config: PipelineConfig,
        tasks: list[tuple[int, str, str, dict]],
    ) -> list[tuple[int, list[RawRecord]]]:
        out: list[tuple[int, list[RawRecord]]] = []
        for task_index, source_id, query, extra_kwargs in tasks:
            raws = self._run_single_task(config, source_id, query, extra_kwargs)
            out.append((task_index, raws))
        return out

    def _collect_parallel(
        self,
        config: PipelineConfig,
        tasks: list[tuple[int, str, str, dict]],
    ) -> list[tuple[int, list[RawRecord]]]:
        workers = min(config.max_workers, len(tasks))
        out: list[tuple[int, list[RawRecord]]] = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_to_index = {
                pool.submit(
                    self._run_single_task,
                    config,
                    source_id,
                    query,
                    extra_kwargs,
                ): task_index
                for task_index, source_id, query, extra_kwargs in tasks
            }
            for fut in as_completed(future_to_index):
                task_index = future_to_index[fut]
                try:
                    raws = fut.result()
                except Exception as e:
                    log.error("Future fallito (task_index=%d): %s", task_index, e)
                    raws = []
                out.append((task_index, raws))
        return out
