"""
tests/test_enricher.py

Test per pipeline/enricher.py.

Copertura:
- build_analysis_text():         combinazioni title/text, whitespace, entrambi vuoti
- _normalize_language_code():    ISO 639-1, ISO 639-1+regione, ISO 639-3, nome esteso,
                                  codice sconosciuto, None, stringa vuota
- detect_language():             testo troppo corto → None, testo valido → codice ISO 639-1
                                  (dipende da langdetect; saltato se non installato)
- resolve_language():            priorità sorgente > detect > None
- analyze_sentiment():           testo corto → None, lingua non supportata → None,
                                  pipeline assente (mock) → None, pipeline presente (mock) → float
- enrich_record():               integrazione con mock pipeline sentiment
- enrich_all():                  lista vuota, lista mista, invarianza ordine

Nota sul design dei test:
    La pipeline HuggingFace (XLM-RoBERTa) NON viene caricata nei test:
    - è un modello da ~1.1 GB, non appropriato per CI
    - _get_sentiment_pipeline() viene mocked per restituire un callable finto
    Questo garantisce che tutti i test siano deterministici e veloci.
"""

from __future__ import annotations

import pytest

from unittest.mock import MagicMock, patch

from models import Record
from normalizers.utils import normalize_language_code as _normalize_language_code
from pipeline.enricher import (
    Enricher,
    build_analysis_text,
    detect_language,
    resolve_language,
    _MIN_LEN_DETECT,
    _MIN_LEN_SENTIMENT,
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _record(**kwargs) -> Record:
    """Costruisce un Record minimale con i campi forniti."""
    defaults = dict(
        source="news",
        title="",
        text="",
        date=None,
        url="https://example.com/article",
        query="test query",
        target="Test Target",
    )
    defaults.update(kwargs)
    return Record(**defaults)


def _mock_sentiment_pipeline(pos: float, neg: float, neu: float = None) -> MagicMock:
    """
    Crea un mock del pipeline HuggingFace che restituisce scores fissi.

    Il formato atteso da analyze_sentiment è:
        [[{"label": "positive", "score": p}, {"label": "neutral", "score": n}, ...]]
    """
    if neu is None:
        neu = max(0.0, 1.0 - pos - neg)
    pipe = MagicMock()
    pipe.return_value = [[
        {"label": "positive", "score": pos},
        {"label": "neutral",  "score": neu},
        {"label": "negative", "score": neg},
    ]]
    return pipe


# ---------------------------------------------------------------------------
# Test: build_analysis_text()
# ---------------------------------------------------------------------------

class TestBuildAnalysisText:
    def test_title_and_text(self):
        r = _record(title="Breaking News", text="Full article body here.")
        assert build_analysis_text(r) == "Breaking News Full article body here."

    def test_only_title_text_empty(self):
        r = _record(title="Only Title", text="")
        assert build_analysis_text(r) == "Only Title"

    def test_only_text_title_empty(self):
        r = _record(title="", text="Only text content available.")
        assert build_analysis_text(r) == "Only text content available."

    def test_both_empty(self):
        r = _record(title="", text="")
        assert build_analysis_text(r) == ""

    def test_whitespace_only_stripped(self):
        """Titolo/testo composti solo da spazi vengono scartati."""
        r = _record(title="   ", text="  \t  ")
        assert build_analysis_text(r) == ""

    def test_strips_leading_trailing_whitespace(self):
        r = _record(title="  Title  ", text="  Text  ")
        assert build_analysis_text(r) == "Title Text"

    def test_none_fields_handled(self):
        """language/author=None non devono influenzare build_analysis_text."""
        r = _record(title="Title", text="Text", language=None, author=None)
        assert build_analysis_text(r) == "Title Text"


# ---------------------------------------------------------------------------
# Test: _normalize_language_code()
# ---------------------------------------------------------------------------

class TestNormalizeLanguageCode:
    # ISO 639-1 standard (2 lettere)
    def test_iso1_lowercase(self):
        assert _normalize_language_code("en") == "en"

    def test_iso1_uppercase(self):
        assert _normalize_language_code("EN") == "en"

    def test_iso1_mixed_case(self):
        assert _normalize_language_code("It") == "it"

    # ISO 639-1 con variante regionale
    def test_iso1_with_region_dash(self):
        assert _normalize_language_code("en-US") == "en"

    def test_iso1_with_region_underscore(self):
        assert _normalize_language_code("pt_BR") == "pt"

    def test_iso1_zh_region(self):
        assert _normalize_language_code("zh-CN") == "zh"

    # ISO 639-3 (3 lettere)
    def test_iso3_english(self):
        assert _normalize_language_code("eng") == "en"

    def test_iso3_italian(self):
        assert _normalize_language_code("ita") == "it"

    def test_iso3_spanish(self):
        assert _normalize_language_code("spa") == "es"

    def test_iso3_german(self):
        assert _normalize_language_code("deu") == "de"

    def test_iso3_portuguese(self):
        assert _normalize_language_code("por") == "pt"

    # Nome esteso (come restituito da GDELT)
    def test_name_english(self):
        assert _normalize_language_code("English") == "en"

    def test_name_italian(self):
        assert _normalize_language_code("Italian") == "it"

    def test_name_french(self):
        assert _normalize_language_code("French") == "fr"

    def test_name_spanish(self):
        assert _normalize_language_code("spanish") == "es"  # lowercase

    # Casi di fallback
    def test_unknown_code_returns_none(self):
        assert _normalize_language_code("xyz123") is None

    def test_none_input_returns_none(self):
        assert _normalize_language_code(None) is None

    def test_empty_string_returns_none(self):
        assert _normalize_language_code("") is None

    def test_whitespace_only_returns_none(self):
        assert _normalize_language_code("   ") is None


# ---------------------------------------------------------------------------
# Test: detect_language()
# ---------------------------------------------------------------------------

# Tutti i test in questa classe richiedono langdetect installato.
langdetect_available = pytest.mark.skipif(
    not __import__("importlib").util.find_spec("langdetect"),
    reason="langdetect non installato",
)


class TestDetectLanguage:
    @langdetect_available
    def test_short_text_returns_none(self):
        """Testo più corto di _MIN_LEN_DETECT → nessun tentativo di rilevamento."""
        short = "Hi"  # 2 chars, ben sotto soglia
        assert len(short) < _MIN_LEN_DETECT
        assert detect_language(short) is None

    @langdetect_available
    def test_text_at_threshold_returns_none(self):
        """Esattamente _MIN_LEN_DETECT - 1 caratteri → None."""
        text = "x" * (_MIN_LEN_DETECT - 1)
        assert detect_language(text) is None

    @langdetect_available
    def test_english_text_detected(self):
        text = "The quick brown fox jumps over the lazy dog and runs away."
        result = detect_language(text)
        assert result == "en"

    @langdetect_available
    def test_italian_text_detected(self):
        text = "Il governo italiano ha annunciato nuove misure economiche per il paese."
        result = detect_language(text)
        assert result == "it"

    @langdetect_available
    def test_french_text_detected(self):
        text = "Le président français a annoncé de nouvelles mesures économiques importantes."
        result = detect_language(text)
        assert result == "fr"

    @langdetect_available
    def test_deterministic_across_calls(self):
        """Stessa stringa → stesso risultato in tutte le chiamate (seed fisso)."""
        text = "This is a test sentence in English language for detection purposes."
        results = {detect_language(text) for _ in range(5)}
        assert len(results) == 1, f"Risultati non deterministici: {results}"

    def test_langdetect_unavailable_returns_none(self):
        """Se langdetect non è importabile, detect_language restituisce None senza crash."""
        with patch.dict("sys.modules", {"langdetect": None}):
            result = detect_language("This is a long enough English sentence for detection.")
            assert result is None


# ---------------------------------------------------------------------------
# Test: resolve_language()
# ---------------------------------------------------------------------------

class TestResolveLanguage:
    def test_existing_valid_language_takes_priority(self):
        """Se record.language è già un codice valido, viene usato direttamente."""
        r = _record(language="en")
        result = resolve_language(r, "testo lungo abbastanza per la detection automatica")
        assert result == "en"

    def test_existing_iso3_normalized(self):
        """ISO 639-3 nel record viene normalizzato a ISO 639-1."""
        r = _record(language="eng")
        result = resolve_language(r, "some text here for detection")
        assert result == "en"

    def test_existing_name_normalized(self):
        """Nome lingua nel record viene normalizzato."""
        r = _record(language="Italian")
        result = resolve_language(r, "some text here for detection")
        assert result == "it"

    def test_none_language_falls_back_to_detection(self):
        """Se record.language è None, si prova la detection automatica."""
        r = _record(language=None)
        with patch("pipeline.enricher.detect_language", return_value="fr") as mock_detect:
            result = resolve_language(r, "texte en français suffisamment long")
            mock_detect.assert_called_once()
            assert result == "fr"

    def test_unrecognized_language_falls_back_to_detection(self):
        """Codice non riconoscibile (es. 'xyz') fa scattare il fallback."""
        r = _record(language="xyz")
        with patch("pipeline.enricher.detect_language", return_value="en") as mock_detect:
            result = resolve_language(r, "english text long enough for detection")
            mock_detect.assert_called_once()
            assert result == "en"

    def test_short_text_no_language_returns_none(self):
        """Testo corto + language=None → None (detection fallisce)."""
        r = _record(language=None)
        result = resolve_language(r, "hi")
        assert result is None


# ---------------------------------------------------------------------------
# Test: analyze_sentiment()
# ---------------------------------------------------------------------------

class TestAnalyzeSentiment:
    def test_short_text_returns_none(self):
        """Testo più corto di _MIN_LEN_SENTIMENT → None senza invocare il modello."""
        short = "x" * (_MIN_LEN_SENTIMENT - 1)
        enricher = Enricher(sentiment_pipeline=None)
        assert enricher.analyze_sentiment(short, "en") is None

    def test_unsupported_language_returns_none(self):
        """Lingua non nel set supportato → None senza invocare il modello."""
        text = "a" * 50
        enricher = Enricher(sentiment_pipeline=None)
        assert enricher.analyze_sentiment(text, "zh") is None  # cinese non supportato

    def test_pipeline_unavailable_returns_none(self):
        """Se il modello non è disponibile, analyze_sentiment restituisce None."""
        enricher = Enricher(sentiment_pipeline=None)  # None esplicito: nessun modello
        result = enricher.analyze_sentiment(
            "This is a sufficiently long text for analysis.", "en"
        )
        assert result is None

    def test_positive_text_score(self):
        """P(pos)=0.9, P(neg)=0.05 → score = 0.85."""
        text = "This is a sufficiently long positive text for sentiment analysis."
        enricher = Enricher(sentiment_pipeline=_mock_sentiment_pipeline(pos=0.9, neg=0.05))
        assert enricher.analyze_sentiment(text, "en") == pytest.approx(0.85, abs=1e-5)

    def test_negative_text_score(self):
        """P(pos)=0.05, P(neg)=0.9 → score = -0.85."""
        text = "This is a sufficiently long negative text for sentiment analysis."
        enricher = Enricher(sentiment_pipeline=_mock_sentiment_pipeline(pos=0.05, neg=0.9))
        assert enricher.analyze_sentiment(text, "en") == pytest.approx(-0.85, abs=1e-5)

    def test_neutral_text_score(self):
        """P(pos)=0.3, P(neg)=0.3 → score = 0.0."""
        text = "This is a sufficiently long neutral text for sentiment analysis."
        enricher = Enricher(
            sentiment_pipeline=_mock_sentiment_pipeline(pos=0.3, neg=0.3, neu=0.4)
        )
        assert enricher.analyze_sentiment(text, "en") == pytest.approx(0.0, abs=1e-5)

    def test_score_clamped_to_range(self):
        """Score è sempre in [-1.0, 1.0] anche con floating point estremo."""
        text = "x" * 50
        enricher = Enricher(sentiment_pipeline=_mock_sentiment_pipeline(pos=1.0, neg=0.0))
        result = enricher.analyze_sentiment(text, "en")
        assert result is not None
        assert -1.0 <= result <= 1.0

    def test_score_rounded_to_6_decimals(self):
        """Il risultato viene arrotondato a 6 cifre decimali."""
        text = "x" * 50
        enricher = Enricher(
            sentiment_pipeline=_mock_sentiment_pipeline(pos=0.123456789, neg=0.0)
        )
        result = enricher.analyze_sentiment(text, "en")
        assert result is not None
        assert result == round(result, 6)

    def test_none_language_still_invokes_model(self):
        """language=None non blocca il sentiment (XLM-RoBERTa gestisce input senza lingua)."""
        text = "x" * 50
        mock_pipe = _mock_sentiment_pipeline(pos=0.7, neg=0.2)
        enricher = Enricher(sentiment_pipeline=mock_pipe)
        result = enricher.analyze_sentiment(text, None)
        assert result is not None
        mock_pipe.assert_called_once_with(text)

    def test_pipeline_exception_returns_none(self):
        """Eccezione durante l'inferenza → None senza propagare."""
        text = "x" * 50
        crashing_pipe = MagicMock(side_effect=RuntimeError("CUDA out of memory"))
        enricher = Enricher(sentiment_pipeline=crashing_pipe)
        assert enricher.analyze_sentiment(text, "en") is None


# ---------------------------------------------------------------------------
# Test: enrich_record()
# ---------------------------------------------------------------------------

class TestEnrichRecord:
    def test_record_without_text_returns_unchanged(self):
        """Record con title e text vuoti: language e sentiment rimangono None."""
        r = _record(title="", text="", language=None)
        enricher = Enricher(sentiment_pipeline=None)
        enriched = enricher.enrich_record(r)
        assert enriched.language is None
        assert enriched.sentiment is None

    def test_record_not_mutated(self):
        """Il Record originale non deve essere modificato (dataclasses.replace).

        detect_language viene mockato per restituire "en": garantisce che
        language passi da None a "en", che replace() venga invocato e che
        enriched sia un oggetto distinto da r — indipendentemente da langdetect.
        """
        r = _record(title="Original", text="x" * 50, language=None)
        enricher = Enricher(sentiment_pipeline=None)
        with patch("pipeline.enricher.detect_language", return_value="en"):
            enriched = enricher.enrich_record(r)
        assert r.title == "Original"     # originale invariato
        assert r.language is None        # originale non mutato
        assert enriched is not r         # replace() chiamato: language None→"en"

    def test_language_detected_from_text(self):
        """language=None + testo → language assegnato dopo enrichment."""
        text = "The quick brown fox jumps over the lazy dog in english language."
        r = _record(title="", text=text, language=None)
        enricher = Enricher(sentiment_pipeline=None)
        with patch("pipeline.enricher.detect_language", return_value="en"):
            enriched = enricher.enrich_record(r)
        assert enriched.language == "en"

    def test_language_from_source_respected(self):
        """language già valorizzato nel record → mantenuto senza detect."""
        r = _record(title="Titre", text="Contenu court.", language="fr")
        enricher = Enricher(sentiment_pipeline=None)
        with patch("pipeline.enricher.detect_language") as mock_detect:
            enriched = enricher.enrich_record(r)
            mock_detect.assert_not_called()
        assert enriched.language == "fr"

    def test_sentiment_calculated_with_mock_pipeline(self):
        """Con mock pipeline, sentiment viene calcolato e assegnato al record."""
        text = "This is a sufficiently long and very positive text for sentiment analysis."
        r = _record(title="", text=text, language="en")
        enricher = Enricher(sentiment_pipeline=_mock_sentiment_pipeline(pos=0.8, neg=0.1))
        enriched = enricher.enrich_record(r)
        assert enriched.sentiment is not None
        assert enriched.sentiment == pytest.approx(0.7, abs=1e-5)

    def test_no_replace_when_fields_unchanged(self):
        """Se language e sentiment non cambiano, il record originale viene restituito."""
        r = _record(title="", text="", language=None, sentiment=None)
        enricher = Enricher(sentiment_pipeline=None)
        enriched = enricher.enrich_record(r)
        # Con testo vuoto non ci sono aggiornamenti → stesso oggetto
        assert enriched is r

    def test_sentiment_none_for_unsupported_language(self):
        """Lingua non supportata da XLM-RoBERTa → sentiment None."""
        text = "这是中文文本内容，包含足够多的字符用于测试情感分析的支持程度。"
        r = _record(title="", text=text, language="zh")
        enricher = Enricher(
            sentiment_pipeline=_mock_sentiment_pipeline(pos=0.5, neg=0.3)
        )
        enriched = enricher.enrich_record(r)
        # zh non è nel set supportato → sentiment non calcolato
        assert enriched.sentiment is None


# ---------------------------------------------------------------------------
# Test: enrich_all()
# ---------------------------------------------------------------------------

class TestEnrichAll:
    def test_empty_list_returns_empty(self):
        enricher = Enricher(sentiment_pipeline=None)
        assert enricher.enrich_all([]) == []

    def test_preserves_order(self):
        """L'ordine dei record nell'output coincide con l'input."""
        records = [
            _record(title=f"Article {i}", text="x" * 50, language="en")
            for i in range(5)
        ]
        enricher = Enricher(sentiment_pipeline=None)
        enriched = enricher.enrich_all(records)
        assert len(enriched) == 5
        assert [r.title for r in enriched] == [r.title for r in records]

    def test_all_records_processed(self):
        """Ogni record riceve enrich_record() anche se il risultato è invariato."""
        records = [_record(title="", text="", language=None) for _ in range(3)]
        enricher = Enricher(sentiment_pipeline=None)
        enriched = enricher.enrich_all(records)
        assert len(enriched) == 3
        assert all(r.language is None for r in enriched)

    def test_partial_enrichment_does_not_affect_others(self):
        """Record con testo corto non compromette l'enrichment degli altri.

        detect_language viene mockato con un side_effect che replica il
        comportamento della soglia di lunghezza (_MIN_LEN_DETECT): testi brevi
        restituiscono None, testi sufficientemente lunghi restituiscono "en".
        Questo isola il test da langdetect mantenendo la semantica corretta.
        """
        short_record = _record(title="", text="Hi", language=None)
        long_record  = _record(
            title="",
            text="The quick brown fox jumps over the lazy dog repeatedly.",
            language=None,
        )

        def _mock_detect(text: str) -> str | None:
            return "en" if len(text) >= _MIN_LEN_DETECT else None

        enricher = Enricher(sentiment_pipeline=None)
        with patch("pipeline.enricher.detect_language", side_effect=_mock_detect):
            enriched = enricher.enrich_all([short_record, long_record])

        assert enriched[0].language is None  # "Hi" → 2 chars, sotto soglia
        assert enriched[1].language == "en"  # testo lungo → rilevato correttamente

    def test_returns_list_not_generator(self):
        """enrich_all deve restituire una lista, non un generatore."""
        records = [_record(title="T", text="x" * 20, language="en")]
        enricher = Enricher(sentiment_pipeline=None)
        result = enricher.enrich_all(records)
        assert isinstance(result, list)
