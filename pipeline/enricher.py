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
from normalizers.utils import normalize_language_code as _normalize_language_code

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Costanti locali
# ---------------------------------------------------------------------------

# _MIN_LEN_DETECT, _MIN_LEN_SENTIMENT, _SENTIMENT_MODEL e
# _SENTIMENT_SUPPORTED_LANGS sono importati da config.py:
# modificare lì per cambiare soglie, modello o lingue supportate.

# _normalize_language_code è importata da normalizers.utils:
# la logica di normalizzazione dei codici lingua appartiene al layer
# di normalizzazione, non al layer di enrichment.


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
# Language detection
# ---------------------------------------------------------------------------

# Seed deterministico: stessa stringa → stesso risultato su ogni esecuzione.
# Verifica disponibilità a livello di modulo: il warning viene emesso una
# sola volta all'import, non ripetuto ad ogni chiamata a detect_language().
try:
    from langdetect import DetectorFactory as _DetectorFactory, detect as _langdetect_detect
    _DetectorFactory.seed = 0
    del _DetectorFactory
    _LANGDETECT_AVAILABLE: bool = True
except ImportError:
    _langdetect_detect = None  # type: ignore[assignment]
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
        raw = _langdetect_detect(text)  # type: ignore[misc]
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

            # Il caricamento del modello avviene DENTRO il lock in modo che
            # nessun altro thread possa osservare _pipeline_initialized=True
            # mentre self._pipeline è ancora None (race condition DCL).
            # _pipeline_initialized viene marcato True solo al termine,
            # indipendentemente dall'esito (successo o fallimento).
            try:
                import logging as _logging
                import os as _os
                from transformers import pipeline as hf_pipeline

                # Sopprimo tutto il rumore di caricamento HuggingFace:
                # - "The following layers were not sharded" → transformers.modeling_utils
                # - progress bar "Loading weights" → tqdm su stderr (TQDM_DISABLE)
                # - altri warning benigni → root logger di transformers
                # Ripristino tutti i livelli originali dopo il caricamento.
                _hf_loggers = [
                    _logging.getLogger("transformers"),
                    _logging.getLogger("transformers.modeling_utils"),
                ]
                _prev_levels = [lg.level for lg in _hf_loggers]
                for lg in _hf_loggers:
                    lg.setLevel(_logging.ERROR)

                _prev_tqdm = _os.environ.get("TQDM_DISABLE")
                _os.environ["TQDM_DISABLE"] = "1"

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

                for lg, prev in zip(_hf_loggers, _prev_levels):
                    lg.setLevel(prev)
                if _prev_tqdm is None:
                    _os.environ.pop("TQDM_DISABLE", None)
                else:
                    _os.environ["TQDM_DISABLE"] = _prev_tqdm

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

            finally:
                # Marcato sempre come inizializzato, anche in caso di errore:
                # non si riprova il caricamento nelle chiamate successive (fail-fast).
                self._pipeline_initialized = True

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
        Arricchisce una lista di Record con language detection e sentiment analysis.

        Usa batch inference per il modello NLP: invece di chiamare la pipeline
        HuggingFace una volta per record (overhead per ogni forward pass), raccoglie
        tutti i testi eleggibili in un'unica lista e li processa in un solo batch.
        Questo riduce il tempo di inferenza di 2–10× su CPU, ancora di più su GPU.

        Un record è eleggibile al sentiment se:
        - ha testo analizzabile (build_analysis_text non vuoto)
        - la lingua è supportata dal modello (o None → il modello tenta comunque)
        - il testo supera la soglia minima di lunghezza

        Args:
            records: lista di Record dopo deduplicate.

        Returns:
            Lista di Record arricchiti (stesso ordine dell'input).
        """
        if not records:
            return []

        pipe = self._get_pipeline()

        # --- Fase 1: language detection per tutti i record ---
        # Rapida (langdetect, CPU-bound leggero); non beneficia di batching.
        analysis_texts: list[str] = []
        resolved_langs: list[str | None] = []
        for r in records:
            text = build_analysis_text(r)
            lang = resolve_language(r, text)
            analysis_texts.append(text)
            resolved_langs.append(lang)

        # --- Fase 2: batch inference sentiment ---
        # Identifica quali record sono eleggibili per il batch.
        # batch_indices: posizioni nell'array originale dei record eleggibili.
        batch_indices: list[int] = []
        batch_texts: list[str] = []

        if pipe is not None:
            for i, (text, lang) in enumerate(zip(analysis_texts, resolved_langs)):
                if not text or len(text) < _MIN_LEN_SENTIMENT:
                    continue
                if lang is not None and lang not in _SENTIMENT_SUPPORTED_LANGS:
                    log.debug(
                        "Lingua '%s' non supportata dal modello sentiment. "
                        "Campo non calcolato per record [source=%s].",
                        lang, records[i].source,
                    )
                    continue
                batch_indices.append(i)
                batch_texts.append(text)

        # Chiama il modello una sola volta per tutti i testi eleggibili.
        # sentiment_map: indice_record → score float calcolato.
        sentiment_map: dict[int, float | None] = {}
        if batch_texts:
            try:
                batch_results = pipe(batch_texts)
                for idx, raw in zip(batch_indices, batch_results):
                    label_scores: list[dict] = (
                        raw[0] if raw and isinstance(raw[0], list) else raw
                    )
                    score_map: dict[str, float] = {
                        item["label"].lower(): float(item["score"])
                        for item in label_scores
                    }
                    positive = score_map.get("positive", 0.0)
                    negative = score_map.get("negative", 0.0)
                    sentiment_map[idx] = round(
                        max(-1.0, min(1.0, positive - negative)), 6
                    )
            except Exception as exc:
                log.error(
                    "Errore durante la batch inference del sentiment (%d record): %s",
                    len(batch_texts), exc,
                )
                # Fallback record-per-record in caso di errore batch.
                # Garantisce che un singolo testo problematico non faccia
                # perdere tutti i risultati del batch.
                for i, text in zip(batch_indices, batch_texts):
                    try:
                        raw = pipe(text)
                        label_scores = raw[0] if raw and isinstance(raw[0], list) else raw
                        score_map = {
                            item["label"].lower(): float(item["score"])
                            for item in label_scores
                        }
                        positive = score_map.get("positive", 0.0)
                        negative = score_map.get("negative", 0.0)
                        sentiment_map[i] = round(
                            max(-1.0, min(1.0, positive - negative)), 6
                        )
                    except Exception as exc2:
                        log.error(
                            "Errore sentiment record %d (fallback): %s", i, exc2
                        )
                        sentiment_map[i] = None

        # --- Fase 3: assembla i Record arricchiti ---
        enriched: list[Record] = []
        for i, (r, lang, text) in enumerate(zip(records, resolved_langs, analysis_texts)):
            sentiment = sentiment_map.get(i)  # None se non eleggibile o errore
            updates: dict = {}
            if lang != r.language:
                updates["language"] = lang
            if sentiment != r.sentiment:
                updates["sentiment"] = sentiment
            enriched.append(replace(r, **updates) if updates else r)

        with_lang = sum(1 for r in enriched if r.language is not None)
        with_sentiment = sum(1 for r in enriched if r.sentiment is not None)
        log.info(
            "Enrichment: %d/%d record con language, %d/%d con sentiment.",
            with_lang, len(enriched), with_sentiment, len(enriched),
        )
        return enriched


