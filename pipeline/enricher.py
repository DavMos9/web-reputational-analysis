"""pipeline/enricher.py — Language detection (langdetect) e sentiment analysis (XLM-RoBERTa)."""

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


def build_analysis_text(record: Record) -> str:
    """Concatena title e text per l'analisi NLP. Stringa vuota se nessun contenuto disponibile."""
    parts = [
        part.strip()
        for part in (record.title, record.text)
        if part and part.strip()
    ]
    return " ".join(parts)


# Seed fisso: deterministismo su langdetect. Warning emesso una sola volta all'import.
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
    """Rileva lingua ISO 639-1 con langdetect. None se testo troppo corto o rilevamento fallisce."""
    if not _LANGDETECT_AVAILABLE:
        return None

    if len(text) < _MIN_LEN_DETECT:
        return None

    try:
        raw = _langdetect_detect(text)  # type: ignore[misc]
        return _normalize_language_code(raw)

    except Exception as exc:
        log.debug(
            "Language detection fallita per testo di %d chars: %s",
            len(text), exc,
        )
        return None


def resolve_language(record: Record, analysis_text: str) -> str | None:
    """Priorità: lingua dichiarata dalla sorgente → langdetect. None se non determinabile."""
    existing = _normalize_language_code(record.language)
    if existing:
        return existing
    return detect_language(analysis_text)


# Sentinel: distingue "lazy load" (omesso) da "nessun modello" (None esplicito).
_UNSET: object = object()


class Enricher:
    """
    Language detection e sentiment analysis su una lista di Record.

    Il modello è caricato in modo lazy alla prima chiamata.
    Passare sentiment_pipeline=None nel costruttore per disabilitare NLP (utile nei test).
    """

    def __init__(self, sentiment_pipeline: Any = _UNSET) -> None:
        if sentiment_pipeline is _UNSET:
            self._pipeline: Any = None
            self._pipeline_initialized: bool = False   # lazy: carica al primo uso
        else:
            self._pipeline = sentiment_pipeline
            self._pipeline_initialized = True
        self._pipeline_lock: threading.Lock = threading.Lock()

    def _get_pipeline(self) -> Any | None:
        """Lazy load del modello HuggingFace con double-checked locking. Fail-fast su errore."""
        if self._pipeline_initialized:
            return self._pipeline

        with self._pipeline_lock:
            if self._pipeline_initialized:
                return self._pipeline
            try:
                import logging as _logging
                import os as _os
                from transformers import pipeline as hf_pipeline

                # Sopprimo logging e progress bar HuggingFace durante il caricamento.
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
                    top_k=None,
                    truncation=True,
                    max_length=512,
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
                self._pipeline_initialized = True

        return self._pipeline

    def analyze_sentiment(self, text: str, language: str | None) -> float | None:
        """Score float in [-1.0, 1.0] = P(positive) - P(negative). None se non disponibile."""
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
        """Arricchisce un Record con language e sentiment. Il Record originale non viene mutato."""
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
        """Arricchisce la lista con batch inference NLP (2-10x più veloce di record-by-record)."""
        if not records:
            return []

        pipe = self._get_pipeline()

        analysis_texts: list[str] = []
        resolved_langs: list[str | None] = []
        for r in records:
            text = build_analysis_text(r)
            lang = resolve_language(r, text)
            analysis_texts.append(text)
            resolved_langs.append(lang)

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
                log.error("Errore batch inference sentiment (%d record): %s", len(batch_texts), exc)
                # Fallback record-per-record: un singolo testo problematico non perde tutto il batch.
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

        enriched: list[Record] = []
        for i, (r, lang, text) in enumerate(zip(records, resolved_langs, analysis_texts)):
            sentiment = sentiment_map.get(i)
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


