"""
pipeline/runner.py

Orchestratore della pipeline dati.

Il PipelineRunner coordina i passi:
    collect → normalize → clean → deduplicate → enrich → export

È disaccoppiato da CLI e file I/O:
- riceve i collector tramite il registro (REGISTRY)
- riceve raw_store ed exporters via dependency injection
- restituisce i Record finali, indipendentemente dagli effetti collaterali

Questo design permette di usare il runner anche in test o notebook
senza che scriva nulla su disco.
"""

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


# ---------------------------------------------------------------------------
# Protocolli — interfacce attese da runner per raw_store ed exporters
# ---------------------------------------------------------------------------

@runtime_checkable
class RawStoreProtocol(Protocol):
    def save(self, records: list[RawRecord], target: str, timestamp: str) -> None: ...


@runtime_checkable
class ExporterProtocol(Protocol):
    def export(self, records: list[Record], target: str, timestamp: str) -> None: ...


@runtime_checkable
class SummaryExporterProtocol(Protocol):
    def export_summary(self, summary: EntitySummary, timestamp: str) -> None: ...


# ---------------------------------------------------------------------------
# Configurazione della pipeline
# ---------------------------------------------------------------------------

@dataclass
class PipelineConfig:
    """
    Parametri di esecuzione della pipeline.

    Attributi:
        target:              entità da analizzare (es. "Elon Musk").
        queries:             lista di query di ricerca.
        sources:             lista di source_id da interrogare
                             (es. ["news", "gdelt"]).
                             ATTENZIONE: una lista VUOTA ([]) significa
                             "usa TUTTE le sorgenti nel registry", NON
                             "non usare nessuna sorgente". Questo è il
                             comportamento di default quando il campo non
                             viene specificato. Per restringere a un
                             sottoinsieme, passare la lista esplicita.
        max_results:         numero massimo di risultati per collector per query.
        save_raw:            se True, invoca raw_store.save() dopo la raccolta.
        collector_kwargs:    kwargs aggiuntivi per collector specifici, indicizzati
                             per source_id. Es: {"news": {"language": "it"}}.
                             Vengono passati a collector.collect() come **kwargs.
        parallel_collectors: se True, esegue i collector in parallelo tramite
                             ThreadPoolExecutor (una fetch HTTP per (source, query)).
                             Tutti i collector sono indipendenti e I/O-bound, quindi
                             il GIL non è un problema. Disattivare per debug.
        max_workers:         numero massimo di thread concorrenti quando
                             parallel_collectors=True. Deve essere >= 1.
                             Con max_workers=1 il comportamento è equivalente
                             a seriale.
        since:               data minima 'YYYY-MM-DD'. Se impostata, i record
                             con date anteriore vengono scartati dopo il cleaner.
                             I record con date=None vengono mantenuti (cfr.
                             pipeline/date_filter.py). None = nessun filtro.
        dry_run:             se True, forza max_results=1 per ogni collector/query.
                             Utile per verificare che le API rispondano e la pipeline
                             funzioni end-to-end senza consumare quota.
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
            # valida formato e normalizza (stringa rimane uguale se valida)
            self.since = parse_since(self.since)


# ---------------------------------------------------------------------------
# PipelineRunner
# ---------------------------------------------------------------------------

class PipelineRunner:
    """
    Orchestratore della pipeline dati.

    Args:
        registry:   dizionario source_id → istanza BaseCollector.
                    Tipicamente `collectors.REGISTRY`.
        raw_store:  istanza opzionale per salvare i RawRecord grezzi.
        exporters:  lista opzionale di exporter per l'output finale.
    """

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

    # ------------------------------------------------------------------
    # Entry point principale
    # ------------------------------------------------------------------

    def run(self, config: PipelineConfig, timestamp: str = "") -> tuple[list[Record], EntitySummary | None]:
        """
        Esegue la pipeline completa per la configurazione fornita.

        Flusso: collect → save_raw → normalize/clean/filter →
                deduplicate → enrich → aggregate → export

        Args:
            config:    parametri di esecuzione.
            timestamp: stringa timestamp usata per i nomi file (es. "20260409T120000Z").
                       Se vuota viene ignorata dagli exporter.

        Returns:
            Tupla (records, summary):
            - records: lista di Record finali validi, deduplicati e pronti per l'analisi.
            - summary: EntitySummary con metriche reputazionali aggregate,
                       None se nessun record è rimasto dopo la pipeline.
        """
        # Validazione sorgenti: fail-fast se un source_id non è nel registry.
        # Errore esplicito prima di iniziare qualsiasi I/O di rete.
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

    # ------------------------------------------------------------------
    # Step privati della pipeline
    # ------------------------------------------------------------------

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
        """
        Esegue normalizzazione, pulizia testuale, filtro qualità e filtro temporale.

        Il filtro temporale viene applicato prima di dedup ed enrich
        per non sprecare lavoro su record fuori finestra.
        """
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
        """
        Applica language detection e sentiment analysis tramite self._enricher.

        Posizionato dopo la deduplicazione per non sprecare NLP su duplicati.
        Se le dipendenze NLP non sono installate, enrich_all restituisce
        i record invariati (cfr. pipeline/enricher.py).
        In test, self._enricher può essere un'istanza mockata via costruttore.
        """
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

    # ------------------------------------------------------------------
    # Raccolta interna
    # ------------------------------------------------------------------

    def _collect(self, config: PipelineConfig) -> list[RawRecord]:
        """
        Invoca i collector selezionati per ogni query.

        Sorgenti attive: `config.sources` se specificato,
        altrimenti tutte le sorgenti nel registry.

        Se `config.parallel_collectors` è True esegue i task in parallelo
        via ThreadPoolExecutor; altrimenti esegue in modo seriale.
        In entrambi i casi l'ordine dell'output è deterministico,
        ricostruito dall'indice del task (source_index, query_index),
        così i test e le analisi downstream restano riproducibili.
        """
        active_sources = config.sources if config.sources else list(self._registry)

        # Costruisce la lista dei task in ordine canonico.
        # Ogni task è una tupla (task_index, source_id, query, extra_kwargs).
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

        # Esecuzione: parallela (default) o seriale (fallback debug).
        use_parallel = config.parallel_collectors and config.max_workers > 1
        if use_parallel:
            results = self._collect_parallel(config, tasks)
        else:
            results = self._collect_serial(config, tasks)

        # Riordina per task_index → output deterministico.
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
        """Esecuzione seriale: utile per debug e test deterministici."""
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
        """
        Esecuzione parallela via ThreadPoolExecutor.

        I collector sono stateless per istanza e I/O-bound (HTTP): il threading
        evita di sprecare tempo in attesa sulla rete. Eventuali eccezioni non
        gestite da un collector vengono assorbite da _run_single_task.
        """
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
                    # Safety net: _run_single_task già cattura, ma teniamolo
                    # robusto a eventuali errori di serializzazione del future.
                    log.error("Future fallito (task_index=%d): %s", task_index, e)
                    raws = []
                out.append((task_index, raws))
        return out
