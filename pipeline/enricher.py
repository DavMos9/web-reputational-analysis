"""
pipeline/enricher.py

Step di arricchimento semantico: language detection e sentiment analysis.

Posizione nella pipeline:
    collect → normalize → clean → deduplicate → enrich → export

Motivazione del posizionamento:
    - Dopo deduplicate: evita NLP su record duplicati (operazioni costose).
    - Prima di export: i campi language e sentiment vengono inclusi nell'output finale.

Dipendenze NLP (installare con `pip install -e ".[nlp]"`):
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
import threading
from dataclasses import replace
from typing import Any

from config import (
    SENTIMENT_MODEL as _SENTIMENT_MODEL,
    SENTIMENT_SUPPORTED_LANGS as _SENTIMENT_SUPPORTED_LANGS,
    NLP_MIN_LEN_DETECT as _MIN_LEN_DETECT,
    NLP_MIN_LEN_SENTIMENT as _MIN_LEN_SENTIMENT,
)
from models import Record

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Costanti locali
# ---------------------------------------------------------------------------

# _MIN_LEN_DETECT, _MIN_LEN_SENTIMENT, _SENTIMENT_MODEL e
# _SENTIMENT_SUPPORTED_LANGS sono importati da config.py:
# modificare lì per cambiare soglie, modello o lingue supportate.

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

# Seed deterministico: stessa stringa → stesso risultato su ogni esecuzione.
# Verifica disponibilità a livello di modulo: il warning viene emesso una
# sola volta all'import, non ripetuto ad ogni chiamata a detect_language().
try:
    from langdetect import DetectorFactory as _DetectorFactory
    _DetectorFactory.seed = 0
    del _DetectorFactory
    _LANGDETECT_AVAILABLE: bool = True
except ImportError:
    _LANGDETECT_AVAILABLE = False
    log.warning(
        "langdetect non installato — language detection disabilitata. "
        "Installare con: pip install langdetect"
    )


def detect_language(text: str) -> str | None:
    """
    Rileva la lingua del testo con langdetect.

    Args:
        text: testo su cui eseguire il rilevamento.

    Returns:
        Codice lingua ISO 639-1 (es. "en", "it"), oppure None se:
        - testo troppo corto (< _MIN_LEN_DETECT caratteri)
        - rilevamento fallisce per qualsiasi motivo
    """
    if not _LANGDETECT_AVAILABLE:
        return None

    if len(text) < _MIN_LEN_DETECT:
        return None

    try:
        from langdetect import detect

        raw = detect(text)
        return _normalize_language_code(raw)

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
# Enricher — classe con dependency injection
# ---------------------------------------------------------------------------

# Sentinel: distingue "pipeline non fornita → lazy load" da
# "pipeline fornita esplicitamente come None → nessun modello".
_UNSET: object = object()


class Enricher:
    """
    Esegue language detection e sentiment analysis su una lista di Record.

    Design:
    - Il modello di sentiment viene caricato in modo lazy alla prima chiamata
      effettiva, non all'istanziazione della classe.
    - Se `sentiment_pipeline` viene fornito nel costruttore, viene usato
      direttamente senza caricamento (utile per test e ambienti senza GPU).
    - Il Record originale non viene mai mutato (dataclasses.replace).

    Uso in produzione:
        enricher = Enricher()
        records = enricher.enrich_all(records)

    Uso nei test (dependency injection):
        mock_pipe = lambda text: [[{"label": "positive", "score": 0.9}, ...]]
        enricher = Enricher(sentiment_pipeline=mock_pipe)
    """

    def __init__(self, sentiment_pipeline: Any = _UNSET) -> None:
        """
        Args:
            sentiment_pipeline: controlla il comportamento del modello di sentiment.
                - omesso (default):           lazy load alla prima chiamata.
                - pipeline HuggingFace reale: usata direttamente, no lazy load.
                                              Utile in produzione per condividere
                                              un modello già caricato.
                - None esplicito:             nessun modello disponibile, tutte le
                                              chiamate a analyze_sentiment() → None.
                                              Utile nei test per disabilitare NLP.
        """
        if sentiment_pipeline is _UNSET:
            self._pipeline: Any = None
            self._pipeline_initialized: bool = False   # lazy: carica al primo uso
        else:
            self._pipeline = sentiment_pipeline
            self._pipeline_initialized = True          # già deciso: no lazy load
        # Lock per il lazy loading: garantisce che il modello venga inizializzato
        # una sola volta anche se l'istanza venisse condivisa tra più thread.
        self._pipeline_lock: threading.Lock = threading.Lock()

    def _get_pipeline(self) -> Any | None:
        """
        Carica il modello HuggingFace la prima volta che viene richiesto.

        Thread-safe: usa un Lock per prevenire double-initialization se l'istanza
        viene condivisa tra thread. Il double-checked locking (DCL) garantisce
        che la sezione critica venga eseguita al massimo una volta.

        Se il caricamento fallisce, imposta il pipeline a None e non ritenta
        nelle chiamate successive (fail-fast, no retry).

        Returns:
            Pipeline transformers configurato, oppure None se non disponibile.
        """
        # Fast path: già inizializzato, nessun lock richiesto.
        if self._pipeline_initialized:
            return self._pipeline

        with self._pipeline_lock:
            # Secondo check dentro il lock: un altro thread potrebbe aver già
            # completato l'inizializzazione mentre aspettavamo il lock.
            if self._pipeline_initialized:
                return self._pipeline
            self._pipeline_initialized = True

        try:
            import logging as _logging
            from transformers import pipeline as hf_pipeline

            # XLM-RoBERTa emette un LOAD REPORT benigno su `position_ids`; lo sopprimo
            # durante il caricamento e ripristino il livello originale dopo.
            _modeling_logger = _logging.getLogger("transformers.modeling_utils")
            _prev_level = _modeling_logger.level
            _modeling_logger.setLevel(_logging.ERROR)

            log.info(
                "Caricamento modello sentiment '%s' (primo utilizzo)...",
                _SENTIMENT_MODEL,
            )
            self._pipeline = hf_pipeline(
                task="sentiment-analysis",
                model=_SENTIMENT_MODEL,
                top_k=None,      # Restituisce scores per tutti i label
                truncation=True,
                max_length=512,  # Limite token; testi più lunghi vengono troncati
            )

            _modeling_logger.setLevel(_prev_level)
            log.info("Modello sentiment caricato correttamente.")

        except ImportError:
            log.warning(
                "transformers o torch non installati. Sentiment analysis disabilitata. "
                "Installare con: pip install transformers torch"
            )
            self._pipeline = None

        except Exception as exc:
            log.error(
                "Errore nel caricamento del modello sentiment '%s': %s",
                _SENTIMENT_MODEL, exc,
            )
            self._pipeline = None

        return self._pipeline

    def analyze_sentiment(self, text: str, language: str | None) -> float | None:
        """
        Calcola lo score di sentiment del testo con XLM-RoBERTa multilingue.

        Score output: float in [-1.0, 1.0], calcolato come P(positive) - P(negative).
        Questa formula incorpora l'incertezza: un record con P(pos)=0.4, P(neg)=0.1
        riceve +0.3, non +1.0.

        Args:
            text:     testo da analizzare (≥ _MIN_LEN_SENTIMENT caratteri).
            language: codice ISO 639-1. Se lingua non supportata → None.

        Returns:
            Score float in [-1.0, 1.0], oppure None se testo troppo corto,
            lingua non supportata, modello non disponibile, o errore durante inferenza.
        """
        if len(text) < _MIN_LEN_SENTIMENT:
            return None

        if language is not None and language not in _SENTIMENT_SUPPORTED_LANGS:
            log.debug(
                "Lingua '%s' non supportata dal modello sentiment. Campo non calcolato.",
                language,
            )
            return None

        pipe = self._get_pipeline()
        if pipe is None:
            return None

        try:
            raw = pipe(text)
            label_scores: list[dict] = raw[0] if raw and isinstance(raw[0], list) else raw

            score_map: dict[str, float] = {
                item["label"].lower(): float(item["score"])
                for item in label_scores
            }

            positive = score_map.get("positive", 0.0)
            negative = score_map.get("negative", 0.0)

            return round(max(-1.0, min(1.0, positive - negative)), 6)

        except Exception as exc:
            log.error("Errore durante l'inferenza del sentiment: %s", exc)
            return None

    def enrich_record(self, record: Record) -> Record:
        """
        Arricchisce un singolo Record con language detection e sentiment analysis.

        Flusso:
        1. Costruisce il testo migliore (title + text).
        2. Determina la lingua (sorgente normalizzata oppure langdetect).
        3. Calcola il sentiment (se lingua supportata e testo sufficiente).
        4. Restituisce un nuovo Record aggiornato (originale invariato).
        """
        analysis_text = build_analysis_text(record)

        lang = resolve_language(record, analysis_text)
        sentiment = self.analyze_sentiment(analysis_text, lang) if analysis_text else None

        updates: dict = {}
        if lang != record.language:
            updates["language"] = lang
        if sentiment != record.sentiment:
            updates["sentiment"] = sentiment

        return replace(record, **updates) if updates else record

    def enrich_all(self, records: list[Record]) -> list[Record]:
        """
        Arricchisce una lista di Record, restituendo record invariati per quelli
        per cui language e sentiment non sono calcolabili.

        Args:
            records: lista di Record dopo deduplicate.

        Returns:
            Lista di Record arricchiti (stesso ordine dell'input).
        """
        enriched = [self.enrich_record(r) for r in records]

        with_lang = sum(1 for r in enriched if r.language is not None)
        with_sentiment = sum(1 for r in enriched if r.sentiment is not None)
        log.info(
            "Enrichment: %d/%d record con language, %d/%d con sentiment.",
            with_lang, len(enriched), with_sentiment, len(enriched),
        )
        return enriched


# ---------------------------------------------------------------------------
# Istanza default e funzioni module-level (backward compatibility)
# ---------------------------------------------------------------------------

# Singleton condiviso per l'uso diretto delle funzioni module-level.
# Runner e altri consumer che usano Enricher() esplicito non lo usano.
_default_enricher: Enricher = Enricher()


def analyze_sentiment(text: str, language: str | None) -> float | None:
    """Wrapper module-level → delega a _default_enricher."""
    return _default_enricher.analyze_sentiment(text, language)


# ---------------------------------------------------------------------------
# Entry point pubblici (module-level, backward compatible)
# ---------------------------------------------------------------------------

def enrich_record(record: Record) -> Record:
    """Wrapper module-level → delega a _default_enricher."""
    return _default_enricher.enrich_record(record)


def enrich_all(records: list[Record]) -> list[Record]:
    """Wrapper module-level → delega a _default_enricher."""
    return _default_enricher.enrich_all(records)
