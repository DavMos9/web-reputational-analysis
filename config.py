"""
config.py — Configurazione centralizzata della pipeline.
Tutte le costanti tunabili (soglie, pesi, strategie) vengono importate da qui.
"""

from __future__ import annotations

import os
from importlib.metadata import version as _pkg_version, PackageNotFoundError as _PackageNotFoundError
from dotenv import load_dotenv

load_dotenv()

# Versione letta da pyproject.toml; fallback "dev" in sviluppo senza pip install -e .
try:
    _APP_VERSION: str = _pkg_version("web-reputational-analysis")
except _PackageNotFoundError:
    _APP_VERSION = "dev"

APP_USER_AGENT: str = (
    f"web-reputational-analysis/{_APP_VERSION} (academic research pipeline)"
)

YOUTUBE_API_KEY:  str | None = os.getenv("YOUTUBE_API_KEY")
NEWS_API_KEY:     str | None = os.getenv("NEWS_API_KEY")
GUARDIAN_API_KEY: str | None = os.getenv("GUARDIAN_API_KEY")
NYT_API_KEY:      str | None = os.getenv("NYT_API_KEY")
STACKEXCHANGE_API_KEY: str | None = os.getenv("STACKEXCHANGE_API_KEY")

# Brave: piano gratuito 2.000 query/mese, 1 query/sec.
BRAVE_API_KEY: str | None = os.getenv("BRAVE_API_KEY")

# Bluesky: App Password (non la password principale). Formato xxxx-xxxx-xxxx-xxxx.
BLUESKY_HANDLE:       str | None = os.getenv("BLUESKY_HANDLE")
BLUESKY_APP_PASSWORD: str | None = os.getenv("BLUESKY_APP_PASSWORD")

# Mastodon: token valido SOLO sull'istanza dove è stato creato (MASTODON_TOKEN_INSTANCE).
# Senza token: fallback automatico su timeline hashtag.
MASTODON_ACCESS_TOKEN: str | None = os.getenv("MASTODON_ACCESS_TOKEN")
MASTODON_TOKEN_INSTANCE: str = os.getenv("MASTODON_TOKEN_INSTANCE", "mastodon.social")

# Istanze selezionate per copertura e presenza di ElasticSearch.
# Sovrascrivibili via env con lista separata da virgole.
_mastodon_instances_env = os.getenv("MASTODON_INSTANCES")
MASTODON_INSTANCES: tuple[str, ...] = (
    tuple(i.strip() for i in _mastodon_instances_env.split(",") if i.strip())
    if _mastodon_instances_env
    else ("mastodon.social", "mastodon.online", "techhub.social")
)

_lemmy_instances_env = os.getenv("LEMMY_INSTANCES")
LEMMY_INSTANCES: tuple[str, ...] = (
    tuple(i.strip() for i in _lemmy_instances_env.split(",") if i.strip())
    if _lemmy_instances_env
    else ("lemmy.world", "lemmy.ml", "sh.itjust.works")
)

# HuggingFace token: propagato nell'ambiente perché transformers lo rilevi automaticamente.
_hf_token = os.getenv("HF_TOKEN")
if _hf_token:
    os.environ["HF_TOKEN"] = _hf_token


# NLP — usate da pipeline/enricher.py

# XLM-RoBERTa fine-tuned su Twitter in 8 lingue.
# Ref: https://huggingface.co/cardiffnlp/twitter-xlm-roberta-base-sentiment
SENTIMENT_MODEL: str = "cardiffnlp/twitter-xlm-roberta-base-sentiment"

# Lingue supportate dal modello (aggiornare se si cambia modello).
SENTIMENT_SUPPORTED_LANGS: frozenset[str] = frozenset({
    "ar", "en", "fr", "de", "hi", "it", "pt", "es",
})

# Abbassato a 15 (da 25): i record brevi (GDELT, NYT abstract) hanno spesso 15-25 chars.
NLP_MIN_LEN_DETECT: int = 15
NLP_MIN_LEN_SENTIMENT: int = 15

# Soglia di confidenza minima per accettare il risultato di langdetect.
# Sotto questa soglia (es. testo troppo corto o ambiguo) → language=None.
# 0.80 bilancia recall (non perdere record chiari) e precisione (non classificare male
# commenti brevi che triggherano falsi positivi su lingue dominanti nel corpus).
NLP_LANG_DETECT_MIN_CONFIDENCE: float = 0.80

# Minimo record per regressione lineare statisticamente significativa.
MIN_RECORDS_FOR_TREND: int = 3


# Quality thresholds — usate da pipeline/cleaner.py

# Un record viene scartato solo se ENTRAMBI i campi sono sotto soglia.
# Un articolo con solo titolo (es. GDELT senza body) è ancora valido.
MIN_TEXT_LENGTH:  int = 30   # caratteri minimi nel campo `text`
MIN_TITLE_LENGTH: int = 5    # caratteri minimi nel campo `title`

# Consent page e aggregatori senza contenuto utile.
# consent.google.com: gate GDPR che appare su feed IT per articoli anglofoni.
BLOCKED_DOMAINS: frozenset[str] = frozenset({
    "consent.yahoo.com",
    "consent.google.com",
    "amp.google.com",
    "smartnews.com",
})


# Source weights — scala [0.0, 1.0] di autorevolezza.
# Sorgenti assenti ricevono SOURCE_WEIGHT_DEFAULT (0.50).
SOURCE_WEIGHTS: dict[str, float] = {
    "guardian":         1.00,
    "nyt":              0.95,
    "bbc":              0.90,
    "news":             0.85,
    "ansa":             0.85,
    "gdelt":            0.75,
    "gnews_it":         0.75,
    "stackexchange":    0.75,
    "wikipedia":        0.70,
    "youtube":          0.65,
    "hackernews":       0.65,
    "lemmy":            0.65,
    "bluesky":          0.60,
    "mastodon":         0.60,
    # UGC a bassa autorevolezza: qualità variabile, metadati limitati.
    "brave":            0.55,
    "wikitalk":         0.55,
    "youtube_comments": 0.55,
    "reddit":           0.55,
}

SOURCE_WEIGHT_DEFAULT: float = 0.50

# Soglia per source_trust_avg: esclude sorgenti UGC (peso 0.55) dal calcolo
# per evitare che abbassino artificialmente la media quando la copertura editoriale è alta.
# Fallback su tutti i record se tutti sono sotto soglia.
MIN_SOURCE_TRUST: float = 0.60


# Aggregation — usate da pipeline/aggregator.py

# Pesi del reputation score composito. La somma DEVE essere 1.0.
REPUTATION_WEIGHTS: dict[str, float] = {
    "sentiment":  0.40,
    "trust":      0.30,
    "recency":    0.20,
    "volume":     0.10,
}

# Decay esponenziale: dopo RECENCY_HALF_LIFE_DAYS un record contribuisce metà del peso.
RECENCY_HALF_LIFE_DAYS: int = 30

# volume_score = log(1+n) / (log(1+n) + log(1+halfsat)); vale 0.5 quando n = VOLUME_HALFSAT.
# Formula senza hard cap: discriminante anche su run molto grandi.
VOLUME_HALFSAT: int = 100

# |slope| < TREND_THRESHOLD → trend "stable".
TREND_THRESHOLD: float = 0.005


# Validazione eseguita all'import: fail-fast su valori errati in .env o config.py.

def _validate_config() -> None:
    if not (0.0 <= MIN_SOURCE_TRUST <= 1.0):
        raise ValueError(
            f"config: MIN_SOURCE_TRUST deve essere in [0.0, 1.0], "
            f"ricevuto: {MIN_SOURCE_TRUST}."
        )
    if VOLUME_HALFSAT < 1:
        raise ValueError(
            f"config: VOLUME_HALFSAT deve essere >= 1, ricevuto: {VOLUME_HALFSAT}. "
            "Valori < 1 causano divisione per zero nel volume_score."
        )
    if RECENCY_HALF_LIFE_DAYS < 1:
        raise ValueError(
            f"config: RECENCY_HALF_LIFE_DAYS deve essere >= 1, ricevuto: {RECENCY_HALF_LIFE_DAYS}. "
            "Valori < 1 causano decay esponenziale degenere."
        )
    if TREND_THRESHOLD < 0:
        raise ValueError(
            f"config: TREND_THRESHOLD deve essere >= 0, ricevuto: {TREND_THRESHOLD}."
        )
    _weight_sum = sum(REPUTATION_WEIGHTS.values())
    if not (0.999 <= _weight_sum <= 1.001):
        raise ValueError(
            f"config: REPUTATION_WEIGHTS deve sommare a 1.0, somma attuale: {_weight_sum:.4f}. "
            "Aggiustare i pesi in config.py."
        )


_validate_config()
