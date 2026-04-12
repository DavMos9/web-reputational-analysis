"""
config.py

Configurazione centralizzata della pipeline.

Tutte le costanti tunabili (soglie, pesi, strategie) vivono qui.
I moduli pipeline non devono mai avere valori hardcoded:
devono importare da questo file.

Variabili d'ambiente: caricate da .env tramite python-dotenv.
"""

from __future__ import annotations

import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# API Keys
# ---------------------------------------------------------------------------

YOUTUBE_API_KEY:  str | None = os.getenv("YOUTUBE_API_KEY")
NEWS_API_KEY:     str | None = os.getenv("NEWS_API_KEY")
GUARDIAN_API_KEY: str | None = os.getenv("GUARDIAN_API_KEY")
NYT_API_KEY:      str | None = os.getenv("NYT_API_KEY")
STACKEXCHANGE_API_KEY: str | None = os.getenv("STACKEXCHANGE_API_KEY")

# Bluesky — App Password (non la password principale dell'account).
# Crea una App Password su: https://bsky.app/settings/app-passwords
# BLUESKY_HANDLE: es. "tuo.handle.bsky.social" o solo "tuo.handle"
# BLUESKY_APP_PASSWORD: formato xxxx-xxxx-xxxx-xxxx
# Senza credenziali: il collector viene skippato con un warning.
BLUESKY_HANDLE:       str | None = os.getenv("BLUESKY_HANDLE")
BLUESKY_APP_PASSWORD: str | None = os.getenv("BLUESKY_APP_PASSWORD")

# Mastodon — access token opzionale (app-level o user-level).
# IMPORTANTE: un token Mastodon è valido SOLO sull'istanza dove è stato creato.
# MASTODON_TOKEN_INSTANCE indica a quale istanza appartiene il token.
# Senza token: la ricerca full-text non restituisce statuses;
# il collector usa automaticamente il fallback su timeline hashtag.
# Con token (sull'istanza corretta): sblocca full-text search.
MASTODON_ACCESS_TOKEN: str | None = os.getenv("MASTODON_ACCESS_TOKEN")
MASTODON_TOKEN_INSTANCE: str = os.getenv("MASTODON_TOKEN_INSTANCE", "mastodon.social")

# Istanze Mastodon da interrogare.
# Criteri di selezione per reputation analysis:
#   - mastodon.social: istanza più grande (~2M utenti), generalista, ElasticSearch attivo
#   - mastodon.online: seconda per dimensione, generalista, utenza internazionale
#   - techhub.social:  community tech/startup, utile per reputazione aziende tech
# Sovrascrivibile via env con lista separata da virgole.
_mastodon_instances_env = os.getenv("MASTODON_INSTANCES")
MASTODON_INSTANCES: tuple[str, ...] = (
    tuple(i.strip() for i in _mastodon_instances_env.split(",") if i.strip())
    if _mastodon_instances_env
    else ("mastodon.social", "mastodon.online", "techhub.social")
)

# Istanze Lemmy da interrogare.
# Criteri di selezione per reputation analysis:
#   - lemmy.world:    istanza più grande e generalista (~150k utenti)
#   - lemmy.ml:       istanza originale, community tech/FOSS
#   - sh.itjust.works: grande istanza generalista, alta attività
# Sovrascrivibile via env con lista separata da virgole.
_lemmy_instances_env = os.getenv("LEMMY_INSTANCES")
LEMMY_INSTANCES: tuple[str, ...] = (
    tuple(i.strip() for i in _lemmy_instances_env.split(",") if i.strip())
    if _lemmy_instances_env
    else ("lemmy.world", "lemmy.ml", "sh.itjust.works")
)

# HuggingFace token — opzionale.
# Propagato nell'ambiente affinché transformers lo rilevi automaticamente
# (aumenta il rate limit per il download del modello).
_hf_token = os.getenv("HF_TOKEN")
if _hf_token:
    os.environ["HF_TOKEN"] = _hf_token


# ---------------------------------------------------------------------------
# Quality thresholds — usate da pipeline/cleaner.py
# ---------------------------------------------------------------------------

# Un record viene scartato solo se ENTRAMBI i campi sono sotto soglia.
# Ratio: un articolo con titolo lungo e testo vuoto (es. GDELT) è ancora valido.
MIN_TEXT_LENGTH:  int = 30   # caratteri minimi nel campo `text`
MIN_TITLE_LENGTH: int = 5    # caratteri minimi nel campo `title`


# ---------------------------------------------------------------------------
# Source weights — usate dall'enricher per il relevance score
# ---------------------------------------------------------------------------
# Scala [0.0, 1.0]: misura l'autorevolezza/affidabilità della sorgente.
# Un peso più alto fa salire il relevance_score dei record di quella fonte.
# Sorgenti non presenti in questo dict ricevono peso 0.5 (default neutro).

SOURCE_WEIGHTS: dict[str, float] = {
    "guardian":         1.00,
    "nyt":              0.95,
    "news":             0.85,
    "gdelt":            0.75,
    "wikipedia":        0.70,
    "reddit":           0.70,  # preparato per collector Reddit
    "bluesky":          0.60,
    "youtube":          0.65,
    "youtube_comments": 0.55,
    "stackexchange":    0.75,
    "mastodon":         0.60,
    "lemmy":            0.65,
    "wikitalk":         0.55,
}

SOURCE_WEIGHT_DEFAULT: float = 0.50


# ---------------------------------------------------------------------------
# Deduplication — usate da pipeline/deduplicator.py
# ---------------------------------------------------------------------------

# Soglia di similarità per fuzzy dedup (Jaccard/cosine su token).
# 1.0 = match esatto, 0.0 = nessuna similarità.
# Valori consigliati: 0.80–0.90 per UGC, 0.85–0.95 per news.
FUZZY_DEDUP_THRESHOLD: float = 0.85
