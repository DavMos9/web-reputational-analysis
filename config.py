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

# Brave Search API — web search generalista con indice indipendente.
# Piano gratuito ("Data for AI" Free): 2.000 query/mese, 1 query/sec.
# Registrazione: https://api-dashboard.search.brave.com/
# Senza key: il collector viene skippato con un warning.
BRAVE_API_KEY: str | None = os.getenv("BRAVE_API_KEY")

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
    "bluesky":          0.60,
    "youtube":          0.65,
    "youtube_comments": 0.55,
    "stackexchange":    0.75,
    "mastodon":         0.60,
    "lemmy":            0.65,
    "wikitalk":         0.55,
    # Brave Search: web generalista (SEO-shaped). Peso inferiore alle news
    # API native perché qualità e presenza di metadati (data, autore) variano.
    "brave":            0.55,
}

SOURCE_WEIGHT_DEFAULT: float = 0.50


# ---------------------------------------------------------------------------
# Aggregation — usate da pipeline/aggregator.py
# ---------------------------------------------------------------------------

# Pesi del reputation score composito.
# Ogni componente è normalizzato in [0.0, 1.0] prima della combinazione.
# La somma DEVE essere 1.0.
REPUTATION_WEIGHTS: dict[str, float] = {
    "sentiment":  0.40,   # media pesata del sentiment dei record
    "trust":      0.30,   # media pesata dei SOURCE_WEIGHTS delle sorgenti
    "recency":    0.20,   # quanto sono recenti i record (decay esponenziale)
    "volume":     0.10,   # volume normalizzato (log-scaling)
}

# Half-life per il recency score (in giorni).
# Dopo RECENCY_HALF_LIFE_DAYS un record contribuisce metà del suo peso
# rispetto a un record di oggi. Valori bassi = più sensibile alla freschezza.
RECENCY_HALF_LIFE_DAYS: int = 30

# Half-saturation per il volume_score.
# VOLUME_HALFSAT è il numero di record a cui volume_score = 0.5.
# Formula (saturazione asintotica, senza hard cap):
#   volume_score = log(1 + count) / (log(1 + count) + log(1 + halfsat))
# Lo score tende asintoticamente a 1.0 al crescere di count: resta sempre
# discriminante anche per run molto grandi (100 vs 5000 record → score diversi),
# a differenza della vecchia formula log-normalizzata con hard cap.
VOLUME_HALFSAT: int = 100

# Soglia sulla pendenza della regressione lineare per classificare il trend.
# Se |slope| < TREND_THRESHOLD → "stable".
TREND_THRESHOLD: float = 0.005


# ---------------------------------------------------------------------------
# Deduplication — usate da pipeline/deduplicator.py
# ---------------------------------------------------------------------------

# Soglia di similarità per fuzzy dedup (Jaccard/cosine su token).
# 1.0 = match esatto, 0.0 = nessuna similarità.
# Valori consigliati: 0.80–0.90 per UGC, 0.85–0.95 per news.
#
# NOTA: il fuzzy dedup è predisposto ma NON attivo nella pipeline corrente.
# Il deduplicator usa solo match esatto su URL e titolo+dominio normalizzati.
# Il costo O(n²) e i falsi positivi su testi brevi/UGC hanno sconsigliato
# l'implementazione: i due livelli esatti coprono la maggioranza dei casi pratici.
# Questo parametro è mantenuto come punto di configurazione per un'eventuale
# implementazione futura (cfr. pipeline/deduplicator.py per la motivazione).
FUZZY_DEDUP_THRESHOLD: float = 0.85
