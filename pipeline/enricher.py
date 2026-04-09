"""
pipeline/enricher.py

Step di arricchimento semantico: language detection e sentiment analysis.

Posizione nella pipeline:
    collect → normalize → clean → deduplicate → enrich → export

Motivazione del posizionamento:
    - Dopo deduplicate: evita NLP su record duplicati (operazioni costose).
    - Prima di export: i campi language e sentiment vengono inclusi nell'output finale.

Dipendenze NLP (installare via requirements.txt):
    langdetect >= 1.0.9
        Rilevamento lingua per ~55 lingue. Leggero, deterministico con seed fisso.
    transformers >= 4.35 + torch >= 2.0
        Sentiment analysis multilingue via XLM-RoBERTa (HuggingFace).
        Il modello viene scaricato automaticamente alla prima esecuzione (~1.1 GB).
        Il download avviene una sola volta e viene cachato da HuggingFace.

Note architetturali:
    - Il modello di sentiment è caricato come singleton lazy: inizializzato
      alla prima chiamata, mai ricaricato durante la stessa esecuzione.
    - Tutte le funzioni pubbliche sono pure rispetto al Record:
      il Record originale non viene mai mutato (dataclasses.replace).
    - I fallback sono espliciti: None indica "non calcolato/non disponibile",
      non "neutro" o "sconosciuto".
"""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any

from models import Record

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Costanti
# ---------------------------------------------------------------------------

# Soglie di lunghezza testo per operazioni NLP affidabili (in caratteri).
# Sotto queste soglie i risultati sono inaffidabili e vengono scartati.
# _MIN_LEN_DETECT abbassato a 15: title + text di record brevi (GDELT, NYT abstract)
# hanno spesso 15-25 chars. La soglia 25 scartava troppi record con solo titolo.
_MIN_LEN_DETECT: int = 15
_MIN_LEN_SENTIMENT: int = 15

# Modello HuggingFace per sentiment analysis multilingue.
# Fine-tuned su Twitter in 8 lingue: ar, en, fr, de, hi, it, pt, es.
# Ref: https://huggingface.co/cardiffnlp/twitter-xlm-roberta-base-sentiment
_SENTIMENT_MODEL: str = "cardiffnlp/twitter-xlm-roberta-base-sentiment"

# Lingue supportate dal modello di sentiment (ISO 639-1).
# Per lingue fuori da questo set il sentiment NON viene calcolato (→ None).
_SENTIMENT_SUPPORTED_LANGS: frozenset[str] = frozenset({
    "ar", "en", "fr", "de", "hi", "it", "pt", "es",
})

# Mapping ISO 639-3 → ISO 639-1 per i codici più comuni restituiti da GDELT
# e altre sorgenti che usano 3-letter codes.
_ISO3_TO_ISO1: dict[str, str] = {
    "ara": "ar", "zho": "zh", "nld": "nl", "eng": "en", "fra": "fr",
    "deu": "de", "ell": "el", "hin": "hi", "hun": "hu", "ind": "id",
    "ita": "it", "jpn": "ja", "kor": "ko", "nor": "no", "pol": "pl",
    "por": "pt", "rum": "ro", "ron": "ro", "rus": "ru", "spa": "es",
    "swe": "sv", "tur": "tr", "ukr": "uk", "vie": "vi",
}

# Mapping nome lingua esteso → ISO 639-1 (usato da alcune sorgenti come GDELT)
_LANG_NAME_TO_ISO1: dict[str, str] = {
    "arabic": "ar", "chinese": "zh", "dutch": "nl", "english": "en",
    "french": "fr", "german": "de", "greek": "el", "hindi": "hi",
    "hungarian": "hu", "indonesian": "id", "italian": "it",
    "japanese": "ja", "korean": "ko", "norwegian": "no", "polish": "pl",
    "portuguese": "pt", "romanian": "ro", "russian": "ru",
    "spanish": "es", "swedish": "sv", "turkish": "tr",
    "ukrainian": "uk", "vietnamese": "vi",
}


# ---------------------------------------------------------------------------
# Costruzione testo per analisi
# ---------------------------------------------------------------------------

def build_analysis_text(record: Record) -> str:
    """
    Costruisce il testo migliore disponibile per language detection e sentiment.

    Strategia: concatena title e text (se presenti e non vuoti).
    Questo garantisce che anche i record con text="" (es. GDELT) abbiano
    almeno il titolo come base per l'analisi.

    Returns:
        Testo pulito pronto per NLP. Stringa vuota se nessun contenuto disponibile.
    """
    parts = [
        part.strip()
        for part in (record.title, record.text)
        if part and part.strip()
    ]
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Normalizzazione codice lingua
# ---------------------------------------------------------------------------

def _normalize_language_code(raw_lang: str | None) -> str | None:
    """
    Normalizza un codice lingua in formato ISO 639-1 (2 lettere lowercase).

    Gestisce i seguenti formati in input:
    - ISO 639-1 standard: "en", "it", "fr"
    - ISO 639-1 con variante regionale: "en-US", "zh-CN", "pt-BR" → "en", "zh", "pt"
    - ISO 639-3 (3 lettere): "eng", "ita", "fra" → "en", "it", "fr"
    - Nome esteso (inglese): "English", "Italian" → "en", "it"

    Returns:
        Codice ISO 639-1 normalizzato, oppure None se il formato è irriconoscibile.
    """
    if not raw_lang:
        return None

    normalized = raw_lang.strip().lower()

    # Formato "en-US", "zh-CN", "pt-BR": prendi il codice primario
    primary = normalized.split("-")[0].split("_")[0]

    if len(primary) == 2:
        # Già ISO 639-1 valido
        return primary

    if len(primary) == 3:
        # Potrebbe essere ISO 639-3
        mapped = _ISO3_TO_ISO1.get(primary)
        if mapped:
            return mapped

    # Potrebbe essere un nome esteso ("english", "italian", ecc.)
    mapped = _LANG_NAME_TO_ISO1.get(normalized)
    if mapped:
        return mapped

    # Formato non riconosciuto: restituisce None anziché un codice arbitrario
    log.debug("Codice lingua non riconosciuto: '%s'", raw_lang)
    return None


# ---------------------------------------------------------------------------
# Language detection
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Inizializzazione langdetect (seed deterministico, eseguita una sola volta)
# ---------------------------------------------------------------------------

# DetectorFactory.seed viene impostato a livello modulo per due motivi:
# 1. Garantisce determinismo (stessa stringa → stesso risultato) su tutte le chiamate.
# 2. Evita di reimpostarlo ad ogni invocazione di detect_language (operazione ridondante).
# L'import è lazy: se langdetect non è installato, il blocco viene saltato silenziosamente
# e detect_language gestirà ImportError nella propria chiamata.
try:
    from langdetect import DetectorFactory as _DetectorFactory
    _DetectorFactory.seed = 0
    del _DetectorFactory  # rimuove dalla namespace del modulo (import non esportato)
except ImportError:
    pass  # langdetect non installato — detect_language restituirà None con warning


def detect_language(text: str) -> str | None:
    """
    Rileva la lingua del testo con langdetect.

    Il seed deterministico (DetectorFactory.seed = 0) è impostato a livello
    modulo: stessa stringa → stesso risultato in qualsiasi esecuzione.

    Args:
        text: testo su cui eseguire il rilevamento.

    Returns:
        Codice lingua ISO 639-1 (es. "en", "it"), oppure None se:
        - testo troppo corto (< _MIN_LEN_DETECT caratteri)
        - rilevamento fallisce per qualsiasi motivo
    """
    if len(text) < _MIN_LEN_DETECT:
        return None

    try:
        from langdetect import detect

        raw = detect(text)
        return _normalize_language_code(raw)

    except ImportError:
        log.warning(
            "langdetect non installato. Language detection disabilitata. "
            "Installare con: pip install langdetect"
        )
        return None
    except Exception as exc:
        # LangDetectException viene sollevata su testi ambigui o troppo brevi
        log.debug(
            "Language detection fallita per testo di %d chars: %s",
            len(text), exc,
        )
        return None


def resolve_language(record: Record, analysis_text: str) -> str | None:
    """
    Determina la lingua finale per il record.

    Ordine di priorità:
    1. record.language già impostato (normalizzato): la sorgente conosce
       la lingua meglio del rilevamento automatico. Se il valore è riconoscibile
       come codice valido, viene usato direttamente.
    2. Rilevamento automatico via langdetect sul testo analizzato.
    3. None se il testo è troppo corto o il rilevamento fallisce.

    Args:
        record:        Record da arricchire.
        analysis_text: testo costruito da build_analysis_text().

    Returns:
        Codice ISO 639-1 normalizzato, oppure None.
    """
    # Priorità 1: lingua dichiarata dalla sorgente (se normalizzabile)
    existing = _normalize_language_code(record.language)
    if existing:
        return existing

    # Priorità 2: rilevamento automatico
    return detect_language(analysis_text)


# ---------------------------------------------------------------------------
# Sentiment analysis — singleton lazy del modello
# ---------------------------------------------------------------------------

# Stato del singleton: None = non caricato, False = tentativo fallito, pipeline = caricato
_sentiment_pipeline: Any = None
_sentiment_pipeline_initialized: bool = False


def _get_sentiment_pipeline() -> Any | None:
    """
    Carica e restituisce il pipeline HuggingFace per il sentiment analysis.

    Utilizza un singleton: il modello viene caricato una sola volta alla
    prima chiamata e riutilizzato per tutte le chiamate successive.
    Se il caricamento fallisce, il fallimento viene registrato e None
    viene restituito per tutte le chiamate successive (no retry).

    Returns:
        Pipeline transformers configurato per sentiment-analysis,
        oppure None se transformers/torch non sono disponibili.
    """
    global _sentiment_pipeline, _sentiment_pipeline_initialized

    if _sentiment_pipeline_initialized:
        return _sentiment_pipeline

    # Segna come inizializzato prima del tentativo per evitare retry su errori
    _sentiment_pipeline_initialized = True

    try:
        from transformers import pipeline as hf_pipeline

        log.info(
            "Caricamento modello sentiment '%s' (primo utilizzo)...",
            _SENTIMENT_MODEL,
        )
        _sentiment_pipeline = hf_pipeline(
            task="sentiment-analysis",
            model=_SENTIMENT_MODEL,
            top_k=None,      # Restituisce scores per tutti i label (negative/neutral/positive)
            truncation=True,
            max_length=512,  # Limite token del modello; testi più lunghi vengono troncati
        )
        log.info("Modello sentiment caricato correttamente.")

    except ImportError:
        log.warning(
            "transformers o torch non installati. Sentiment analysis disabilitata. "
            "Installare con: pip install transformers torch"
        )
        _sentiment_pipeline = None

    except Exception as exc:
        log.error(
            "Errore nel caricamento del modello sentiment '%s': %s",
            _SENTIMENT_MODEL, exc,
        )
        _sentiment_pipeline = None

    return _sentiment_pipeline


def analyze_sentiment(text: str, language: str | None) -> float | None:
    """
    Calcola lo score di sentiment del testo con XLM-RoBERTa multilingue.

    Lingue supportate: ar, en, fr, de, hi, it, pt, es.
    Per lingue non in questo set restituisce None (non un errore).

    Score output: float in [-1.0, 1.0]
        - Calcolo: P(positive) - P(negative)
        - Positivo → sentiment favorevole
        - Negativo → sentiment critico/avverso
        - ~0.0 → neutro o bilanciato

    Questa formula è preferibile a usare direttamente la classe dominante
    perché incorpora l'incertezza del modello: un record con P(pos)=0.4,
    P(neu)=0.5, P(neg)=0.1 riceve score +0.3, non +1.0.

    Args:
        text:     testo da analizzare (idealmente almeno 15 caratteri).
        language: codice ISO 639-1. Se None → il modello viene comunque
                  invocato (XLM-RoBERTa gestisce input senza lingua esplicita).
                  Se lingua non supportata → None senza invocare il modello.

    Returns:
        Score float in [-1.0, 1.0], oppure None se:
        - testo troppo corto
        - lingua non supportata dal modello
        - modello non disponibile
        - errore durante l'inferenza
    """
    if len(text) < _MIN_LEN_SENTIMENT:
        return None

    # Se la lingua è nota e non supportata, non invocare il modello
    if language is not None and language not in _SENTIMENT_SUPPORTED_LANGS:
        log.debug(
            "Lingua '%s' non supportata dal modello sentiment. Campo non calcolato.",
            language,
        )
        return None

    pipe = _get_sentiment_pipeline()
    if pipe is None:
        return None

    try:
        # Il pipeline con top_k=None restituisce una lista di liste:
        # [[{"label": "positive", "score": 0.9}, {"label": "neutral", ...}, ...]]
        raw = pipe(text)
        label_scores: list[dict] = raw[0] if raw and isinstance(raw[0], list) else raw

        score_map: dict[str, float] = {
            item["label"].lower(): float(item["score"])
            for item in label_scores
        }

        positive = score_map.get("positive", 0.0)
        negative = score_map.get("negative", 0.0)

        # Clamping difensivo per compensare floating point imprecision
        score = max(-1.0, min(1.0, positive - negative))
        return round(score, 6)  # 6 cifre decimali sufficienti per analisi reputazionale

    except Exception as exc:
        log.error("Errore durante l'inferenza del sentiment: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Entry point pubblici
# ---------------------------------------------------------------------------

def enrich_record(record: Record) -> Record:
    """
    Arricchisce un singolo Record con language detection e sentiment analysis.

    Flusso interno:
    1. Costruisce il testo migliore disponibile (title + text).
    2. Determina la lingua (sorgente normalizzata oppure langdetect).
    3. Calcola il sentiment (se lingua supportata e testo sufficiente).
    4. Restituisce un nuovo Record aggiornato.

    Il Record originale non viene mai modificato (uso di dataclasses.replace).
    I campi già valorizzati nel Record vengono sovrascritti solo se il nuovo
    valore è diverso (evita replace() inutili).

    Args:
        record: Record normalizzato, pulito e deduplicato.

    Returns:
        Nuovo Record con language e sentiment aggiornati.
    """
    analysis_text = build_analysis_text(record)

    lang = resolve_language(record, analysis_text)
    sentiment = analyze_sentiment(analysis_text, lang) if analysis_text else None

    updates: dict = {}
    if lang != record.language:
        updates["language"] = lang
    if sentiment != record.sentiment:
        updates["sentiment"] = sentiment

    return replace(record, **updates) if updates else record


def enrich_all(records: list[Record]) -> list[Record]:
    """
    Arricchisce una lista di Record applicando enrich_record() a ciascuno.

    I record per cui language e sentiment non possono essere calcolati
    (testo troppo corto, lingua non supportata, dipendenze mancanti)
    vengono restituiti invariati, con i campi a None.

    Args:
        records: lista di Record dopo deduplicate.

    Returns:
        Lista di Record arricchiti (stesso ordine dell'input).
    """
    enriched = [enrich_record(r) for r in records]

    with_lang = sum(1 for r in enriched if r.language is not None)
    with_sentiment = sum(1 for r in enriched if r.sentiment is not None)
    log.info(
        "Enrichment: %d/%d record con language, %d/%d con sentiment.",
        with_lang, len(enriched), with_sentiment, len(enriched),
    )
    return enriched
