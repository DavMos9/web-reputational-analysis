"""
Web Reputational Analysis — Pipeline principale
Uso:
    python main.py --target "Nome Cognome" --queries "query1" "query2" [opzioni]

Esempi:
    python main.py --target "Elon Musk" --queries "Elon Musk Tesla" "Elon Musk SpaceX"
    python main.py --target "Apple" --queries "Apple scandal" --sources news gdelt
    python main.py --target "Mario Rossi" --queries "Mario Rossi" --no-raw
"""

import argparse
import json
import csv
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path

# Aggiunge la root del progetto al path per import relativi
sys.path.insert(0, str(Path(__file__).parent))

from collectors import (
    news_collector,
    gdelt_collector,
    wikipedia_collector,
    youtube_collector,
    guardian_collector,
    nyt_collector,
)
from processors.normalizer import normalize_all
from processors.cleaner import clean_all
from processors.deduplicator import deduplicate
from processors.validator import validate_all

# ---------------------------------------------------------------------------
# Configurazione logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Mappa sorgente → collector
# ---------------------------------------------------------------------------
COLLECTORS = {
    "news":      news_collector,
    "gdelt":     gdelt_collector,
    "wikipedia": wikipedia_collector,
    "youtube":   youtube_collector,
    "guardian":  guardian_collector,
    "nyt":       nyt_collector,
}

ALL_SOURCES = list(COLLECTORS.keys())

# ---------------------------------------------------------------------------
# Cartelle di output
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).parent / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
FINAL_DIR = DATA_DIR / "final"


def ensure_dirs():
    for d in (RAW_DIR, PROCESSED_DIR, FINAL_DIR):
        d.mkdir(parents=True, exist_ok=True)


def timestamp_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# ---------------------------------------------------------------------------
# Salvataggio file
# ---------------------------------------------------------------------------
def save_json(records: list[dict], path: Path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)
    log.info(f"Salvato JSON: {path} ({len(records)} record)")


def save_csv(records: list[dict], path: Path):
    if not records:
        log.warning(f"Nessun record da salvare in CSV: {path}")
        return

    fieldnames = [
        "source_type", "source_name", "target_entity", "query",
        "title", "snippet", "url", "domain", "author",
        "published_at", "retrieved_at", "language", "country",
        "rank", "views_count", "likes_count", "comments_count",
        "engagement_score", "keywords_found", "sentiment_stub",
    ]

    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            row = record.copy()
            kw = row.get("keywords_found", [])
            row["keywords_found"] = ";".join(kw) if isinstance(kw, list) else ""
            writer.writerow(row)

    log.info(f"Salvato CSV: {path} ({len(records)} record)")


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def run(
    target: str,
    queries: list[str],
    sources: list[str],
    save_raw: bool = True,
    max_results: int = 20,
) -> list[dict]:
    ts = timestamp_slug()
    target_slug = target.lower().replace(" ", "_")[:30]

    log.info("=== Avvio pipeline ===")
    log.info(f"Target:  {target}")
    log.info(f"Query:   {queries}")
    log.info(f"Fonti:   {sources}")

    # ------------------------------------------------------------------
    # 1. RACCOLTA
    # ------------------------------------------------------------------
    raw_all: list[dict] = []

    for source in sources:
        collector = COLLECTORS.get(source)
        if not collector:
            log.warning(f"Fonte sconosciuta: '{source}', ignorata.")
            continue

        for query in queries:
            log.info(f"Raccolta da '{source}' per query: '{query}'")
            try:
                kwargs = {}
                if source in ("youtube", "nyt"):
                    kwargs["max_results"] = max_results
                elif source in ("news", "guardian"):
                    kwargs["page_size"] = max_results
                elif source == "gdelt":
                    kwargs["max_records"] = max_results

                results = collector.collect(
                    target_entity=target,
                    query=query,
                    **kwargs,
                )
                raw_all.extend(results)
            except Exception as e:
                log.error(f"Errore collector '{source}' / query '{query}': {e}")

    log.info(f"Raccolti {len(raw_all)} record grezzi totali")

    if save_raw and raw_all:
        save_json(raw_all, RAW_DIR / f"{target_slug}_{ts}_raw.json")

    # ------------------------------------------------------------------
    # 2. NORMALIZZAZIONE
    # ------------------------------------------------------------------
    normalized = normalize_all(raw_all)
    log.info(f"Normalizzati {len(normalized)} record")

    # ------------------------------------------------------------------
    # 3. PULIZIA
    # ------------------------------------------------------------------
    cleaned = clean_all(normalized)
    log.info(f"Puliti {len(cleaned)} record")

    if cleaned:
        save_json(cleaned, PROCESSED_DIR / f"{target_slug}_{ts}_processed.json")

    # ------------------------------------------------------------------
    # 4. DEDUPLICAZIONE
    # ------------------------------------------------------------------
    unique, removed = deduplicate(cleaned)
    log.info(f"Deduplicazione: {removed} duplicati rimossi, {len(unique)} record unici")

    # ------------------------------------------------------------------
    # 5. VALIDAZIONE
    # ------------------------------------------------------------------
    result = validate_all(unique)

    if result.errors:
        log.warning(f"Validazione: {len(result.invalid)} record scartati")
        for err in result.errors:
            log.warning(f"  {err}")

    final = result.valid
    log.info(f"Record finali validi: {len(final)}")

    # ------------------------------------------------------------------
    # 6. EXPORT
    # ------------------------------------------------------------------
    if final:
        base = FINAL_DIR / f"{target_slug}_{ts}_final"
        save_json(final, Path(str(base) + ".json"))
        save_csv(final, Path(str(base) + ".csv"))
    else:
        log.warning("Nessun record valido da esportare.")

    log.info("=== Pipeline completata ===")
    return final


# ---------------------------------------------------------------------------
# Entry point CLI
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Web Reputational Analysis — raccolta e normalizzazione dati da fonti web"
    )
    parser.add_argument(
        "--target", required=True,
        help="Entità da analizzare (es. 'Elon Musk', 'Apple')"
    )
    parser.add_argument(
        "--queries", required=True, nargs="+",
        help="Una o più query di ricerca"
    )
    parser.add_argument(
        "--sources", nargs="+", default=ALL_SOURCES,
        choices=ALL_SOURCES,
        help=f"Fonti da interrogare (default: tutte). Scelte: {', '.join(ALL_SOURCES)}"
    )
    parser.add_argument(
        "--max-results", type=int, default=20,
        help="Numero massimo di risultati per fonte/query (default: 20)"
    )
    parser.add_argument(
        "--no-raw", action="store_true",
        help="Non salvare i payload grezzi in data/raw/"
    )
    return parser.parse_args()


if __name__ == "__main__":
    ensure_dirs()
    args = parse_args()

    records = run(
        target=args.target,
        queries=args.queries,
        sources=args.sources,
        save_raw=not args.no_raw,
        max_results=args.max_results,
    )

    print(f"\nRisultato: {len(records)} record finali esportati in data/final/")
