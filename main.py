"""
Web Reputational Analysis — Entry point CLI

Uso:
    python main.py --target "Nome Cognome" --queries "query1" "query2" [opzioni]

Esempi:
    python main.py --target "Elon Musk" --queries "Elon Musk Tesla" "Elon Musk SpaceX"
    python main.py --target "Apple" --queries "Apple scandal" --sources news gdelt
    python main.py --target "Mario Rossi" --queries "Mario Rossi" --no-raw
"""

import argparse
from pathlib import Path

from collectors import build_registry
from exporters import JsonExporter, CsvExporter, SummaryJsonExporter
from pipeline import PipelineRunner, PipelineConfig
from pipeline.date_filter import parse_since
from storage import RawStore
from utils import now_timestamp, configure_logging

configure_logging()

BASE_DIR  = Path(__file__).parent
REGISTRY  = build_registry()
ALL_SOURCES = list(REGISTRY.keys())

# Opt-in: escluse dal default perché producono molto rumore per target non-tech.
# stackexchange: nomi propri matchano spesso in esempi di codice.
# hackernews: comunità anglofona tech-savvy, poco utile per target non-tech.
OPT_IN_SOURCES = frozenset({"stackexchange", "hackernews"})

DEFAULT_SOURCES = [s for s in ALL_SOURCES if s not in OPT_IN_SOURCES]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Web Reputational Analysis — pipeline di raccolta dati da fonti web"
    )
    parser.add_argument(
        "--target", required=True,
        help="Entità da analizzare (es. 'Elon Musk', 'Apple')",
    )
    parser.add_argument(
        "--queries", required=True, nargs="+",
        help="Una o più query di ricerca",
    )
    parser.add_argument(
        "--sources", nargs="+", default=DEFAULT_SOURCES, choices=ALL_SOURCES,
        help=(
            f"Fonti da interrogare. Default: {', '.join(DEFAULT_SOURCES)}. "
            f"Opt-in (richiede invocazione esplicita): {', '.join(sorted(OPT_IN_SOURCES))}. "
            f"Scelte valide: {', '.join(ALL_SOURCES)}."
        ),
    )
    parser.add_argument(
        "--max-results", type=int, default=20,
        help="Numero massimo di risultati per fonte/query (default: 20)",
    )
    parser.add_argument(
        "--no-raw", action="store_true",
        help="Non salvare i payload grezzi in data/raw/",
    )
    parser.add_argument(
        "--news-language", default="en", metavar="LANG",
        help="Codice lingua ISO 639-1 per NewsAPI (default: 'en').",
    )
    parser.add_argument(
        "--since", type=parse_since, default=None, metavar="YYYY-MM-DD",
        help=(
            "Scarta i record con data anteriore. Record senza data vengono mantenuti."
        ),
    )
    parser.add_argument(
        "--languages", nargs="+", default=None, metavar="LANG",
        help=(
            "Filtra i record per lingua (codici ISO 639-1, es. 'en it fr'). "
            "Applicato dopo l'enrichment. Record senza lingua rilevata vengono mantenuti. "
            "Default: nessun filtro (tutte le lingue)."
        ),
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Forza max_results=1 per fonte/query. Verifica le API senza consumare quota.",
    )
    def _positive_int(value: str) -> int:
        """Valida che il valore sia un intero >= 1."""
        try:
            n = int(value)
        except ValueError:
            raise argparse.ArgumentTypeError(f"valore non intero: {value!r}")
        if n < 1:
            raise argparse.ArgumentTypeError(
                f"--keep-raw-days deve essere >= 1, ricevuto: {n}"
            )
        return n

    parser.add_argument(
        "--keep-raw-days", type=_positive_int, default=None, metavar="N",
        help="Elimina i file raw più vecchi di N giorni dopo la pipeline.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ts   = now_timestamp()

    if args.dry_run:
        import logging as _logging
        _logging.getLogger(__name__).info(
            "[DRY RUN] max_results forzato a 1 per ogni fonte/query."
        )

    config = PipelineConfig(
        target=args.target,
        queries=args.queries,
        sources=args.sources,
        max_results=args.max_results,
        save_raw=not args.no_raw,
        collector_kwargs={
            "news": {"language": args.news_language},
        },
        since=args.since,
        languages=args.languages,
        dry_run=args.dry_run,
    )

    raw_store = RawStore(BASE_DIR)

    runner = PipelineRunner(
        registry=REGISTRY,
        raw_store=raw_store,
        exporters=[
            JsonExporter(BASE_DIR),
            CsvExporter(BASE_DIR),
        ],
        summary_exporters=[
            SummaryJsonExporter(BASE_DIR),
        ],
    )

    records, summary = runner.run(config, timestamp=ts)

    if args.keep_raw_days is not None:
        raw_store.purge_old_files(keep_days=args.keep_raw_days)
    print(f"\nRisultato: {len(records)} record finali esportati in data/final/")

    if summary is not None:
        print(f"\n--- Reputation Summary: {summary.entity} ---")
        print(f"  Reputation Score: {summary.reputation_score:.4f}")
        print(f"  Sentiment (avg):  {summary.sentiment_avg}")
        print(f"  Trend:            {summary.trend}")
        print(f"  Sources:          {summary.record_count} record da {len(summary.source_distribution)} fonti")
        print(f"  Date range:       {summary.date_range}")


if __name__ == "__main__":
    main()
