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
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from models import RawRecord, Record
from pipeline.normalizer import normalize_all
from pipeline.cleaner import clean_all, filter_quality
from pipeline.deduplicator import deduplicate
from pipeline.enricher import enrich_all

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


# ---------------------------------------------------------------------------
# Configurazione della pipeline
# ---------------------------------------------------------------------------

@dataclass
class PipelineConfig:
    """
    Parametri di esecuzione della pipeline.

    Attributi:
        target:           entità da analizzare (es. "Elon Musk").
        queries:          lista di query di ricerca.
        sources:          lista di source_id da interrogare
                          (es. ["news", "gdelt"]). Se vuota usa tutte le sorgenti.
        max_results:      numero massimo di risultati per collector per query.
        save_raw:         se True, invoca raw_store.save() dopo la raccolta.
        collector_kwargs: kwargs aggiuntivi per collector specifici, indicizzati
                          per source_id. Es: {"news": {"language": "it"}}.
                          Vengono passati a collector.collect() come **kwargs.
    """
    target: str
    queries: list[str]
    sources: list[str]              = field(default_factory=list)
    max_results: int                = 20
    save_raw: bool                  = True
    collector_kwargs: dict[str, dict] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.target:
            raise ValueError("PipelineConfig.target non può essere vuoto")
        if not self.queries:
            raise ValueError("PipelineConfig.queries non può essere vuota")
        if self.max_results < 1:
            raise ValueError(f"PipelineConfig.max_results deve essere >= 1, ricevuto: {self.max_results}")


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
    ) -> None:
        self._registry  = registry
        self._raw_store = raw_store
        self._exporters = exporters or []

    # ------------------------------------------------------------------
    # Entry point principale
    # ------------------------------------------------------------------

    def run(self, config: PipelineConfig, timestamp: str = "") -> list[Record]:
        """
        Esegue la pipeline completa per la configurazione fornita.

        Args:
            config:    parametri di esecuzione.
            timestamp: stringa timestamp usata per i nomi file (es. "20260409T120000Z").
                       Se vuota viene ignorata dagli exporter.

        Returns:
            Lista di Record finali validi, deduplicati e pronti per l'analisi.
        """
        log.info("=== Pipeline avviata: target='%s', fonti=%s ===",
                 config.target, config.sources or list(self._registry))

        # 1. Raccolta
        raw_records = self._collect(config)
        log.info("Raccolti %d RawRecord totali.", len(raw_records))

        if not raw_records:
            log.warning("Nessun record raccolto. Pipeline terminata.")
            return []

        # 2. Salvataggio raw (effetto collaterale opzionale)
        if config.save_raw and self._raw_store:
            try:
                self._raw_store.save(raw_records, config.target, timestamp)
            except Exception as e:
                log.error("Errore durante il salvataggio raw: %s", e)

        # 3. Normalizzazione
        records = normalize_all(raw_records)

        # 4. Pulizia testuale
        records = clean_all(records)

        # 4b. Filtro qualità (min lunghezza title/text — soglie in config.py)
        records, skipped = filter_quality(records)
        log.info("Puliti: %d record validi, %d scartati per qualità.", len(records), skipped)

        # 5. Deduplicazione
        records, removed = deduplicate(records)
        log.info("Deduplicati: %d rimossi, %d record unici.", removed, len(records))

        if not records:
            log.warning("Nessun record rimasto dopo deduplicazione.")
            return []

        # 6. Enrichment: language detection + sentiment analysis
        # Posizionato dopo la deduplicazione per non sprecare NLP su duplicati.
        records = enrich_all(records)

        # 7. Export (effetti collaterali opzionali)
        for exporter in self._exporters:
            try:
                exporter.export(records, config.target, timestamp)
            except Exception as e:
                log.error("Errore exporter %s: %s", type(exporter).__name__, e)

        log.info("=== Pipeline completata: %d record finali. ===", len(records))

        return records

    # ------------------------------------------------------------------
    # Raccolta interna
    # ------------------------------------------------------------------

    def _collect(self, config: PipelineConfig) -> list[RawRecord]:
        """
        Invoca i collector selezionati per ogni query.

        Sorgenti attive: `config.sources` se specificato,
        altrimenti tutte le sorgenti nel registry.
        """
        active_sources = config.sources if config.sources else list(self._registry)
        all_raws: list[RawRecord] = []

        for source_id in active_sources:
            collector = self._registry.get(source_id)
            if collector is None:
                log.warning("Sorgente '%s' non trovata nel registry, ignorata.", source_id)
                continue

            extra_kwargs = config.collector_kwargs.get(source_id, {})

            for query in config.queries:
                log.info("Raccolta da '%s' per query: '%s'", source_id, query)
                try:
                    raws = collector.collect(
                        target=config.target,
                        query=query,
                        max_results=config.max_results,
                        **extra_kwargs,
                    )
                    all_raws.extend(raws)
                except Exception as e:
                    log.error(
                        "Errore collector '%s' / query '%s': %s",
                        source_id, query, e,
                    )

        return all_raws
