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
# NLP — usate da pipeline/enricher.py
# ---------------------------------------------------------------------------

# Modello HuggingFace per sentiment analysis multilingue.
# Fine-tuned su Twitter in 8 lingue.
# Ref: https://huggingface.co/cardiffnlp/twitter-xlm-roberta-base-sentiment
SENTIMENT_MODEL: str = "cardiffnlp/twitter-xlm-roberta-base-sentiment"

# Lingue supportate dal modello di sentiment (ISO 639-1).
# Aggiornare questo set se si cambia modello o si aggiungono lingue.
# Lingua non presente → sentiment non calcolato (→ None), senza errori.
SENTIMENT_SUPPORTED_LANGS: frozenset[str] = frozenset({
    "ar", "en", "fr", "de", "hi", "it", "pt", "es",
})

# Lunghezza minima del testo (caratteri) per language detection affidabile.
# Sotto questa soglia langdetect produce risultati inaffidabili e viene skippato.
# Abbassato a 15 rispetto al default 25: i record brevi (GDELT, NYT abstract)
# hanno spesso 15-25 chars. La soglia 25 scartava troppi record con solo titolo.
NLP_MIN_LEN_DETECT: int = 15

# Lunghezza minima del testo (caratteri) per sentiment analysis affidabile.
# Sotto questa soglia il modello XLM-RoBERTa produce punteggi instabili.
NLP_MIN_LEN_SENTIMENT: int = 15

# Numero minimo di record con (data, sentiment) per calcolare il trend.
# Con meno record la regressione lineare non è statisticamente significativa.
MIN_RECORDS_FOR_TREND: int = 3


# ---------------------------------------------------------------------------
# Quality thresholds — usate da pipeline/cleaner.py
# ---------------------------------------------------------------------------

# Un record viene scartato solo se ENTRAMBI i campi sono sotto soglia.
# Ratio: un articolo con titolo lungo e testo vuoto (es. GDELT) è ancora valido.
MIN_TEXT_LENGTH:  int = 30   # caratteri minimi nel campo `text`
MIN_TITLE_LENGTH: int = 5    # caratteri minimi nel campo `title`

# Domini sempre da scartare, indipendentemente dalla qualità del testo.
# Include redirect di consenso cookie, paywall wall-gate e aggregatori che
# non espongono contenuto utile. Il cleaner logga un warning per ogni record
# scartato per dominio.
BLOCKED_DOMAINS: frozenset[str] = frozenset({
    # Yahoo consent/cookie gate
    "consent.yahoo.com",
    # Google consent interstitial — può apparire come destinazione finale quando
    # Google News mostra il gate GDPR per articoli internazionali (es. target
    # anglofoni sul feed IT). Il redirect resolver usa questo come safety net
    # dopo aver già tentato il fallback all'URL originale.
    "consent.google.com",
    # Google AMP redirect (non canonical)
    "amp.google.com",
    # Siti di aggregazione puri senza testo originale
    "smartnews.com",
})


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
    # Google News IT: aggrega testate italiane autorevoli (Corriere, ANSA, ecc.).
    # Peso simile a GDELT: copertura buona, ma metadati (autore, testo) limitati
    # dal formato RSS.
    "gnews_it":         0.75,
    # Hacker News: community tech con discussioni su business, politica, scienza.
    # Peso moderato: contenuto spesso di qualità ma fortemente orientato a un
    # pubblico anglofono e tech-savvy.
    "hackernews":       0.65,
    # Reddit: UGC da community generaliste. Peso basso per l'anonimato degli
    # autori e la variabilità della qualità tra subreddit, ma alta copertura
    # su target mainstream (politici, brand, personaggi pubblici).
    "reddit":           0.55,
    # BBC News: principale emittente pubblica internazionale. Peso alto per
    # affidabilità e copertura globale. Feed RSS pubblici aggiornati ogni 15
    # minuti (world, business, technology), nessuna API key richiesta.
    # Sostituisce Reuters il cui feed feeds.reuters.com è stato dismesso.
    "bbc":              0.90,
    # ANSA: principale agenzia di stampa italiana. Peso alto per autorevolezza
    # nel contesto italiano. Feed RSS aggiornati in tempo reale.
    "ansa":             0.85,
}

SOURCE_WEIGHT_DEFAULT: float = 0.50

# Soglia minima di affidabilità per includere una sorgente nel calcolo di
# source_trust_avg. I record il cui peso sorgente è inferiore a questa soglia
# vengono esclusi dalla media di trust (ma non dalla pipeline o dagli altri
# score come sentiment, volume e recency).
#
# Motivazione: sorgenti UGC a bassa autorevolezza (reddit, brave, wikitalk,
# youtube_comments — peso 0.55) in grandi volumi abbassano artificialmente
# source_trust_avg anche quando la copertura editoriale è eccellente.
# Con questa soglia si calcola la trust solo sulle sorgenti che superano
# il livello minimo di credibilità editoriale.
#
# Valori indicativi:
#   0.55 → esclude solo il default (0.50); wikitalk/reddit/brave/yt_comments
#           inclusi perché al limite della soglia
#   0.60 → esclude wikitalk (0.55), reddit (0.55), brave (0.55),
#           youtube_comments (0.55) — include mastodon (0.60) e tutto sopra
#   0.65 → esclude anche mastodon (0.60) e youtube (0.65 — al limite)
#
# Se TUTTI i record appartengono a sorgenti sotto soglia, il calcolo
# usa tutti i record come fallback per evitare trust_avg = 0.0.
MIN_SOURCE_TRUST: float = 0.60


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
# NOT ACTIVE — riservato per uso futuro. Non viene letto da nessun modulo corrente.
FUZZY_DEDUP_THRESHOLD: float = 0.85


# ---------------------------------------------------------------------------
# Validazione parametri critici
# ---------------------------------------------------------------------------
# Eseguita al momento dell'import: un valore errato in .env o config.py
# produce un errore esplicito all'avvio invece di comportamenti anomali
# a runtime (division by zero, score infiniti, ecc.).

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
