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
import sys
from pathlib import Path

# Root del progetto nel path per import assoluti
sys.path.insert(0, str(Path(__file__).parent))

from collectors import REGISTRY
from exporters import JsonExporter, CsvExporter
from pipeline import PipelineRunner, PipelineConfig
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

# ---------------------------------------------------------------------------
# Costanti
# ---------------------------------------------------------------------------
BASE_DIR     = Path(__file__).parent
ALL_SOURCES  = list(REGISTRY.keys())


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
        "--sources", nargs="+", default=ALL_SOURCES, choices=ALL_SOURCES,
        help=f"Fonti da interrogare (default: tutte). Scelte: {', '.join(ALL_SOURCES)}",
    )
    parser.add_argument(
        "--max-results", type=int, default=20,
        help="Numero massimo di risultati per fonte/query (default: 20)",
    )
    parser.add_argument(
        "--no-raw", action="store_true",
        help="Non salvare i payload grezzi in data/raw/",
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
    )

    runner = PipelineRunner(
        registry=REGISTRY,
        raw_store=RawStore(BASE_DIR),
        exporters=[
            JsonExporter(BASE_DIR),
            CsvExporter(BASE_DIR),
        ],
    )

    records = runner.run(config, timestamp=ts)
    print(f"\nRisultato: {len(records)} record finali esportati in data/final/")


if __name__ == "__main__":
    main()
