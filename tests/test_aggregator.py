"""
tests/test_aggregator.py

Test per pipeline/aggregator.py.

Copertura:
- aggregate(): lista vuota → ValueError
- aggregate(): singolo record senza sentiment → score coerente
- aggregate(): record con sentiment → media pesata corretta
- aggregate(): source_distribution ordinata correttamente
- aggregate(): recency_score con record recenti vs vecchi
- aggregate(): volume_score con saturazione asintotica (Hill-on-log)
- aggregate(): trend "up", "down", "stable", "unknown"
- aggregate(): date_range estrazione corretta
- aggregate(): sentiment_std calcolata correttamente
- aggregate(): EntitySummary.to_dict() serializzazione coerente
- _sentiment_to_unit(): mapping [-1,1] → [0,1]
- _compute_reputation_score(): combinazione pesata corretta
"""

from __future__ import annotations

import math
from datetime import date, timedelta

import pytest

from models import Record
from pipeline.aggregator import (
    aggregate,
    EntitySummary,
    _compute_source_distribution,
    _compute_weighted_sentiment,
    _compute_source_trust,
    _compute_recency_score,
    _compute_volume_score,
    _compute_trend,
    _compute_date_range,
    _sentiment_to_unit,
    _compute_reputation_score,
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _record(
    source: str = "news",
    sentiment: float | None = None,
    date_str: str | None = "2026-04-10",
    url: str = "https://example.com/1",
    target: str = "TestEntity",
) -> Record:
    """Crea un Record minimale per i test."""
    return Record(
        source=source,
        query="test query",
        target=target,
        title="Test Title",
        text="Test text content for analysis.",
        date=date_str,
        url=url,
        sentiment=sentiment,
    )


# ---------------------------------------------------------------------------
# Test: aggregate() — validazione input
# ---------------------------------------------------------------------------

class TestAggregateValidation:

    def test_empty_list_raises(self):
        with pytest.raises(ValueError, match="almeno un record"):
            aggregate([])

    def test_heterogeneous_targets_uses_first(self):
        """Record con target diversi: usa il primo, non crasha."""
        r1 = _record(target="Alpha", url="https://a.com/1")
        r2 = _record(target="Beta", url="https://b.com/2")
        summary = aggregate([r1, r2])
        assert summary.entity == "Alpha"


# ---------------------------------------------------------------------------
# Test: source_distribution
# ---------------------------------------------------------------------------

class TestSourceDistribution:

    def test_single_source(self):
        records = [_record(source="news", url=f"https://example.com/{i}") for i in range(3)]
        dist = _compute_source_distribution(records)
        assert dist == {"news": 3}

    def test_multiple_sources_ordered_desc(self):
        records = [
            _record(source="news", url="https://a.com/1"),
            _record(source="news", url="https://a.com/2"),
            _record(source="guardian", url="https://b.com/1"),
        ]
        dist = _compute_source_distribution(records)
        keys = list(dist.keys())
        assert keys[0] == "news"  # 2 record → primo
        assert dist["news"] == 2
        assert dist["guardian"] == 1


# ---------------------------------------------------------------------------
# Test: weighted sentiment
# ---------------------------------------------------------------------------

class TestWeightedSentiment:

    def test_no_sentiment_returns_none(self):
        records = [_record(sentiment=None)]
        avg, std, count = _compute_weighted_sentiment(records)
        assert avg is None
        assert std is None
        assert count == 0

    def test_single_record_no_std(self):
        """Con un solo record con sentiment, std deve essere None."""
        records = [_record(sentiment=0.5, source="news")]
        avg, std, count = _compute_weighted_sentiment(records)
        assert avg is not None
        assert abs(avg - 0.5) < 1e-4
        assert std is None
        assert count == 1

    def test_weighted_average(self):
        """Guardian (1.0) e youtube_comments (0.55) con sentiment diverso."""
        r1 = _record(source="guardian", sentiment=0.8, url="https://a.com/1")
        r2 = _record(source="youtube_comments", sentiment=-0.2, url="https://b.com/2")
        avg, std, count = _compute_weighted_sentiment([r1, r2])

        # avg = (0.8*1.0 + (-0.2)*0.55) / (1.0 + 0.55)
        expected = (0.8 * 1.0 + (-0.2) * 0.55) / (1.0 + 0.55)
        assert avg is not None
        assert abs(avg - expected) < 1e-4
        assert count == 2

    def test_std_with_multiple_records(self):
        """Std calcolata con almeno 2 record."""
        r1 = _record(source="news", sentiment=0.8, url="https://a.com/1")
        r2 = _record(source="news", sentiment=-0.4, url="https://a.com/2")
        avg, std, count = _compute_weighted_sentiment([r1, r2])
        assert std is not None
        # std = sqrt(((0.8-0.2)^2 + (-0.4-0.2)^2) / 2) = sqrt(0.36) = 0.6
        mean_raw = (0.8 + (-0.4)) / 2  # = 0.2
        expected_std = math.sqrt(((0.8 - mean_raw)**2 + (-0.4 - mean_raw)**2) / 2)
        assert abs(std - expected_std) < 1e-4


# ---------------------------------------------------------------------------
# Test: source trust
# ---------------------------------------------------------------------------

class TestSourceTrust:

    def test_empty_records(self):
        assert _compute_source_trust([]) == 0.0

    def test_single_known_source(self):
        records = [_record(source="guardian")]
        trust = _compute_source_trust(records)
        assert abs(trust - 1.0) < 1e-4

    def test_mixed_sources(self):
        r1 = _record(source="guardian", url="https://a.com/1")  # 1.00
        r2 = _record(source="youtube_comments", url="https://b.com/2")  # 0.55
        trust = _compute_source_trust([r1, r2])
        expected = (1.00 + 0.55) / 2
        assert abs(trust - expected) < 1e-4


# ---------------------------------------------------------------------------
# Test: recency score
# ---------------------------------------------------------------------------

class TestRecencyScore:

    def test_all_today(self):
        """Record di oggi → recency_score ≈ 1.0."""
        today = date.today().isoformat()
        records = [_record(date_str=today)]
        score = _compute_recency_score(records, reference_date=date.today())
        assert abs(score - 1.0) < 1e-4

    def test_one_half_life_ago(self):
        """Record di 30 giorni fa (= half_life) → weight ≈ 0.5."""
        ref = date(2026, 4, 10)
        old_date = (ref - timedelta(days=30)).isoformat()
        records = [_record(date_str=old_date)]
        score = _compute_recency_score(records, reference_date=ref)
        assert abs(score - 0.5) < 1e-4

    def test_no_dates_returns_zero(self):
        records = [_record(date_str=None)]
        score = _compute_recency_score(records, reference_date=date(2026, 4, 10))
        assert score == 0.0

    def test_future_date_treated_as_today(self):
        """Record con data futura → trattato come age=0 → weight=1.0."""
        ref = date(2026, 4, 10)
        future = (ref + timedelta(days=5)).isoformat()
        records = [_record(date_str=future)]
        score = _compute_recency_score(records, reference_date=ref)
        assert abs(score - 1.0) < 1e-4


# ---------------------------------------------------------------------------
# Test: volume score
# ---------------------------------------------------------------------------

class TestVolumeScore:
    """
    Saturazione asintotica: volume_score = l / (l + log(1+halfsat)),
    con l = log(1+count). Proprietà:
      - count=0 → 0.0
      - count=VOLUME_HALFSAT → 0.5
      - count→∞ → 1.0 (asintoto, mai raggiunto)
      - monotona crescente in count
    """

    def test_zero_records(self):
        assert _compute_volume_score(0) == 0.0

    def test_halfsat_gives_half(self):
        """VOLUME_HALFSAT record → volume_score = 0.5 (per definizione)."""
        from config import VOLUME_HALFSAT
        score = _compute_volume_score(VOLUME_HALFSAT)
        assert abs(score - 0.5) < 1e-4

    def test_above_halfsat_is_greater_than_half(self):
        """Oltre halfsat lo score supera 0.5 ma resta < 1.0."""
        from config import VOLUME_HALFSAT
        score = _compute_volume_score(VOLUME_HALFSAT * 5)
        assert 0.5 < score < 1.0

    def test_asymptotic_upper_bound(self):
        """
        Anche per count enormi lo score non raggiunge 1.0 (asintoto).
        La saturazione della Hill-on-log è deliberatamente lenta:
        con halfsat=100, n=10M dà ~0.78 — la formula resta discriminante
        fino a ordini di grandezza molto alti.
        """
        score = _compute_volume_score(10_000_000)
        assert score < 1.0
        # Molto sopra il punto di half-saturation (0.5), ma asintotico
        assert score > 0.7

    def test_no_hard_cap(self):
        """La nuova formula NON satura: count diversi → score diversi."""
        s1 = _compute_volume_score(200)
        s2 = _compute_volume_score(5000)
        assert s2 > s1  # la vecchia formula con hard cap avrebbe dato s1 == s2 == 1.0

    def test_monotonic_increase(self):
        """Più record → score più alto."""
        s1 = _compute_volume_score(5)
        s2 = _compute_volume_score(50)
        s3 = _compute_volume_score(500)
        assert s1 < s2 < s3

    def test_score_range(self):
        """Lo score è sempre in [0.0, 1.0) per ogni count non-negativo."""
        for n in (0, 1, 10, 100, 1000, 100_000):
            s = _compute_volume_score(n)
            assert 0.0 <= s < 1.0


# ---------------------------------------------------------------------------
# Test: trend
# ---------------------------------------------------------------------------

class TestTrend:

    def test_insufficient_data_unknown(self):
        """< 3 record con data+sentiment → unknown."""
        records = [_record(sentiment=0.5, date_str="2026-04-01")]
        assert _compute_trend(records) == "unknown"

    def test_increasing_sentiment_up(self):
        records = [
            _record(sentiment=-0.5, date_str="2026-04-01", url="https://a.com/1"),
            _record(sentiment=0.0,  date_str="2026-04-05", url="https://a.com/2"),
            _record(sentiment=0.5,  date_str="2026-04-10", url="https://a.com/3"),
        ]
        assert _compute_trend(records) == "up"

    def test_decreasing_sentiment_down(self):
        records = [
            _record(sentiment=0.8,  date_str="2026-04-01", url="https://a.com/1"),
            _record(sentiment=0.2,  date_str="2026-04-05", url="https://a.com/2"),
            _record(sentiment=-0.4, date_str="2026-04-10", url="https://a.com/3"),
        ]
        assert _compute_trend(records) == "down"

    def test_flat_sentiment_stable(self):
        records = [
            _record(sentiment=0.5, date_str="2026-04-01", url="https://a.com/1"),
            _record(sentiment=0.5, date_str="2026-04-05", url="https://a.com/2"),
            _record(sentiment=0.5, date_str="2026-04-10", url="https://a.com/3"),
        ]
        assert _compute_trend(records) == "stable"

    def test_no_sentiment_records_unknown(self):
        records = [
            _record(sentiment=None, date_str="2026-04-01", url="https://a.com/1"),
            _record(sentiment=None, date_str="2026-04-05", url="https://a.com/2"),
            _record(sentiment=None, date_str="2026-04-10", url="https://a.com/3"),
        ]
        assert _compute_trend(records) == "unknown"


# ---------------------------------------------------------------------------
# Test: date_range
# ---------------------------------------------------------------------------

class TestDateRange:

    def test_no_dates(self):
        records = [_record(date_str=None)]
        assert _compute_date_range(records) is None

    def test_single_date(self):
        records = [_record(date_str="2026-04-10")]
        assert _compute_date_range(records) == ("2026-04-10", "2026-04-10")

    def test_range_extraction(self):
        records = [
            _record(date_str="2026-04-10", url="https://a.com/1"),
            _record(date_str="2026-03-01", url="https://a.com/2"),
            _record(date_str="2026-04-05", url="https://a.com/3"),
        ]
        assert _compute_date_range(records) == ("2026-03-01", "2026-04-10")


# ---------------------------------------------------------------------------
# Test: sentiment_to_unit mapping
# ---------------------------------------------------------------------------

class TestSentimentToUnit:

    def test_negative_one(self):
        assert abs(_sentiment_to_unit(-1.0) - 0.0) < 1e-6

    def test_zero(self):
        assert abs(_sentiment_to_unit(0.0) - 0.5) < 1e-6

    def test_positive_one(self):
        assert abs(_sentiment_to_unit(1.0) - 1.0) < 1e-6

    def test_none_returns_neutral(self):
        assert abs(_sentiment_to_unit(None) - 0.5) < 1e-6


# ---------------------------------------------------------------------------
# Test: reputation score composito
# ---------------------------------------------------------------------------

class TestReputationScore:

    def test_all_max(self):
        """Tutti i componenti a 1.0 → score = 1.0 (perché pesi sommano a 1.0)."""
        score = _compute_reputation_score(
            sentiment_avg=1.0,  # → 1.0 dopo mapping
            source_trust=1.0,
            recency=1.0,
            volume=1.0,
        )
        assert abs(score - 1.0) < 1e-4

    def test_all_min(self):
        """Sentiment -1.0 → 0.0, tutto il resto 0.0 → score = 0.0."""
        score = _compute_reputation_score(
            sentiment_avg=-1.0,
            source_trust=0.0,
            recency=0.0,
            volume=0.0,
        )
        assert abs(score - 0.0) < 1e-4

    def test_neutral_sentiment_none(self):
        """sentiment_avg=None → mappato a 0.5 (neutro)."""
        score = _compute_reputation_score(
            sentiment_avg=None,
            source_trust=0.5,
            recency=0.5,
            volume=0.5,
        )
        # 0.4*0.5 + 0.3*0.5 + 0.2*0.5 + 0.1*0.5 = 0.5
        assert abs(score - 0.5) < 1e-4


# ---------------------------------------------------------------------------
# Test: EntitySummary.to_dict()
# ---------------------------------------------------------------------------

class TestEntitySummaryToDict:

    def test_serialization(self):
        summary = aggregate([
            _record(source="news", sentiment=0.5, date_str="2026-04-10"),
        ])
        d = summary.to_dict()
        assert d["entity"] == "TestEntity"
        assert d["record_count"] == 1
        assert "reputation_score" in d
        assert isinstance(d["source_distribution"], dict)

    def test_date_range_format(self):
        summary = aggregate([
            _record(source="news", sentiment=0.5, date_str="2026-04-01", url="https://a.com/1"),
            _record(source="news", sentiment=0.3, date_str="2026-04-10", url="https://a.com/2"),
        ])
        d = summary.to_dict()
        assert d["date_range"] == {"from": "2026-04-01", "to": "2026-04-10"}

    def test_none_date_range(self):
        summary = aggregate([_record(date_str=None)])
        d = summary.to_dict()
        assert d["date_range"] is None
