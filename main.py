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
import logging
from pathlib import Path

from collectors import build_registry
from exporters import JsonExporter, CsvExporter, SummaryJsonExporter
from pipeline import PipelineRunner, PipelineConfig
from pipeline.date_filter import parse_since
from storage import RawStore
from utils import now_timestamp

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

# Silenzia logger di terze parti rumorosi a INFO (wikipediaapi, httpx);
# WARNING+ rimane visibile per non perdere errori reali.
logging.getLogger("wikipediaapi").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

# ---------------------------------------------------------------------------
# Costanti
# ---------------------------------------------------------------------------
BASE_DIR  = Path(__file__).parent
REGISTRY  = build_registry()           # lazy: importa i collector solo qui
ALL_SOURCES = list(REGISTRY.keys())

# Fonti opt-in: non incluse nel set di default, ma richiamabili esplicitamente
# via --sources. Usate per fonti strutturalmente inadatte alla maggior parte
# dei target ma utili in casi specifici.
#   - stackexchange: full-text match sul body delle domande, quindi nomi propri
#     risultano spesso come stringhe di test in esempi di codice (rumore).
#     Ha senso solo per target tech (librerie, framework, prodotti software).
OPT_IN_SOURCES = frozenset({"stackexchange"})

DEFAULT_SOURCES = [s for s in ALL_SOURCES if s not in OPT_IN_SOURCES]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
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
        help=(
            "Codice lingua ISO 639-1 per NewsAPI (default: 'en'). "
            "Esempi: 'it', 'fr', 'de'. Attenzione: NewsAPI supporta un "
            "sottoinsieme limitato di lingue nel piano gratuito."
        ),
    )
    parser.add_argument(
        "--since", type=parse_since, default=None, metavar="YYYY-MM-DD",
        help=(
            "Scarta i record con data anteriore. Formato 'YYYY-MM-DD'. "
            "I record senza data vengono mantenuti. "
            "Utile per analisi focalizzate su finestre temporali recenti: "
            "migliora anche la significatività del campo 'trend' nel summary."
        ),
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    args = parse_args()
    ts   = now_timestamp()

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
    )

    runner = PipelineRunner(
        registry=REGISTRY,
        raw_store=RawStore(BASE_DIR),
        exporters=[
            JsonExporter(BASE_DIR),
            CsvExporter(BASE_DIR),
        ],
        summary_exporters=[
            SummaryJsonExporter(BASE_DIR),
        ],
    )

    records, summary = runner.run(config, timestamp=ts)
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
