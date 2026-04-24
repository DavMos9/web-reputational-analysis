"""pipeline/aggregator.py — Aggregazione entity-level: list[Record] → EntitySummary."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timezone
from typing import Any

from models import Record
from config import (
    SOURCE_WEIGHTS,
    SOURCE_WEIGHT_DEFAULT,
    MIN_SOURCE_TRUST,
    REPUTATION_WEIGHTS,
    RECENCY_HALF_LIFE_DAYS,
    VOLUME_HALFSAT,
    TREND_THRESHOLD,
    MIN_RECORDS_FOR_TREND,
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class EntitySummary:
    """
    Riepilogo reputazionale aggregato per un'entità.

    Score in [0.0, 1.0] tranne sentiment_avg in [-1.0, 1.0].
    sentiment_std misura la polarizzazione (alta = opinioni divergenti).
    source_trust_avg esclude sorgenti UGC sotto MIN_SOURCE_TRUST dalla media.
    """

    entity: str
    queries: list[str]          # query usate nella raccolta, ordinate alfabeticamente
    record_count: int
    records_with_sentiment: int
    source_distribution: dict[str, int]

    sentiment_avg: float | None
    sentiment_std: float | None

    source_trust_avg: float
    recency_score: float
    volume_score: float

    reputation_score: float

    trend: str   # "up" | "down" | "stable" | "unknown"

    date_range: tuple[str, str] | None
    computed_at: str

    def to_dict(self) -> dict[str, Any]:
        """Converte in dizionario JSON-serializzabile."""
        d = asdict(self)
        if self.date_range is not None:
            d["date_range"] = {"from": self.date_range[0], "to": self.date_range[1]}
        return d


def _get_source_weight(source: str) -> float:
    if source not in SOURCE_WEIGHTS:
        log.debug(
            "Sorgente '%s' non in SOURCE_WEIGHTS — usato peso default %.2f. "
            "Aggiornare config.SOURCE_WEIGHTS se questa sorgente è permanente.",
            source, SOURCE_WEIGHT_DEFAULT,
        )
    return SOURCE_WEIGHTS.get(source, SOURCE_WEIGHT_DEFAULT)


def _compute_source_distribution(records: list[Record]) -> dict[str, int]:
    """Conteggio record per sorgente, ordinato decrescente per volume."""
    dist: dict[str, int] = {}
    for r in records:
        dist[r.source] = dist.get(r.source, 0) + 1
    return dict(sorted(dist.items(), key=lambda x: x[1], reverse=True))


def _compute_weighted_sentiment(records: list[Record]) -> tuple[float | None, float | None, int]:
    """Calcola media pesata e std del sentiment. Pesi = SOURCE_WEIGHTS."""
    pairs: list[tuple[float, float]] = []  # (sentiment, weight)
    for r in records:
        if r.sentiment is not None:
            w = _get_source_weight(r.source)
            pairs.append((r.sentiment, w))

    count = len(pairs)
    if count == 0:
        return None, None, 0

    total_weight = sum(w for _, w in pairs)
    if total_weight == 0:
        return None, None, 0

    avg = sum(s * w for s, w in pairs) / total_weight
    avg = max(-1.0, min(1.0, avg))

    # Varianza pesata: Σ w_i*(s_i - avg)² / Σ w_i
    std: float | None = None
    if count >= 2:
        weighted_variance = sum(w * (s - avg) ** 2 for s, w in pairs) / total_weight
        std = math.sqrt(weighted_variance)

    return round(avg, 6), round(std, 6) if std is not None else None, count


def _compute_source_trust(records: list[Record]) -> float:
    """
    Media semplice dei pesi di autorevolezza.

    Esclude sorgenti con peso < MIN_SOURCE_TRUST per evitare che UGC ad alto volume
    abbassino la media quando la copertura editoriale è alta.
    Fallback su tutti i record se TUTTI i pesi sono sotto soglia.
    """
    if not records:
        return 0.0

    all_weights = [(r, _get_source_weight(r.source)) for r in records]
    trusted = [(r, w) for r, w in all_weights if w >= MIN_SOURCE_TRUST]

    if trusted:
        selected = trusted
    else:
        selected = all_weights

    weights = [w for _, w in selected]
    return round(sum(weights) / len(weights), 6)


def _compute_recency_score(records: list[Record], reference_date: date | None = None) -> float:
    """
    Score di recency con decay esponenziale: weight_i = 2^(-age_days / half_life).
    half_life = RECENCY_HALF_LIFE_DAYS. Score = media dei weight_i.
    """
    if reference_date is None:
        reference_date = datetime.now(timezone.utc).date()

    half_life = RECENCY_HALF_LIFE_DAYS
    if half_life <= 0:
        half_life = 30  # fallback difensivo

    decay_weights: list[float] = []
    for r in records:
        if r.date is None:
            continue
        try:
            record_date = date.fromisoformat(r.date)
        except (ValueError, TypeError):
            continue

        age_days = (reference_date - record_date).days
        if age_days < 0:
            age_days = 0

        w = 2.0 ** (-age_days / half_life)
        decay_weights.append(w)

    if not decay_weights:
        return 0.0

    return round(sum(decay_weights) / len(decay_weights), 6)


def _compute_volume_score(record_count: int) -> float:
    """
    Volume con saturazione asintotica: l / (l + log(1 + VOLUME_HALFSAT)), l = log(1 + count).
    Vale 0.5 a count = VOLUME_HALFSAT; tende asintoticamente a 1.0. Mai satura con hard cap.
    """
    if record_count <= 0:
        return 0.0

    halfsat = max(1, VOLUME_HALFSAT)
    l = math.log(1 + record_count)
    denom = l + math.log(1 + halfsat)
    return round(l / denom, 6)


def _compute_trend(records: list[Record]) -> str:
    """Trend del sentiment via regressione lineare. Richiede >= MIN_RECORDS_FOR_TREND record con data+sentiment."""
    dated: list[tuple[str, float]] = []
    for r in records:
        if r.date is not None and r.sentiment is not None:
            dated.append((r.date, r.sentiment))

    if len(dated) < MIN_RECORDS_FOR_TREND:
        log.debug(
            "Trend non calcolabile: %d record con data+sentiment, richiesti >= %d.",
            len(dated), MIN_RECORDS_FOR_TREND,
        )
        return "unknown"

    dated.sort(key=lambda x: x[0])

    n = len(dated)
    # x = distanza in giorni dalla data minima (non indice ordinale):
    # preserva il peso temporale reale anche con distribuzione irregolare.
    min_date = date.fromisoformat(dated[0][0])
    x_vals = [(date.fromisoformat(d) - min_date).days for d, _ in dated]
    y_vals = [s for _, s in dated]

    sum_x = sum(x_vals)
    sum_y = sum(y_vals)
    sum_xy = sum(x * y for x, y in zip(x_vals, y_vals))
    sum_x2 = sum(x * x for x in x_vals)

    denominator = n * sum_x2 - sum_x * sum_x
    if denominator == 0:
        log.warning("[aggregator] Trend: denominatore zero (%d record). Fallback 'stable'.", n)
        return "stable"

    slope = (n * sum_xy - sum_x * sum_y) / denominator

    threshold = TREND_THRESHOLD
    if slope > threshold:
        return "up"
    elif slope < -threshold:
        return "down"
    else:
        return "stable"


def _compute_date_range(records: list[Record]) -> tuple[str, str] | None:
    """Estrae la data minima e massima dai record con data valida."""
    dates: list[str] = [r.date for r in records if r.date is not None]
    if not dates:
        return None
    dates.sort()
    return (dates[0], dates[-1])


def _sentiment_to_unit(sentiment_avg: float | None) -> float:
    """Mappa sentiment [-1,1] → [0,1]: (s+1)/2. None → 0.5 (neutro)."""
    if sentiment_avg is None:
        return 0.5
    return (sentiment_avg + 1.0) / 2.0


def _compute_reputation_score(
    sentiment_avg: float | None,
    source_trust: float,
    recency: float,
    volume: float,
) -> float:
    """Media pesata dei componenti (pesi da config.REPUTATION_WEIGHTS). Score in [0.0, 1.0]."""
    components = {
        "sentiment": _sentiment_to_unit(sentiment_avg),
        "trust":     source_trust,
        "recency":   recency,
        "volume":    volume,
    }

    score = sum(
        REPUTATION_WEIGHTS.get(key, 0.0) * value
        for key, value in components.items()
    )

    return round(max(0.0, min(1.0, score)), 4)


def aggregate(records: list[Record]) -> EntitySummary:
    """Aggrega una lista di Record in un EntitySummary. Lancia ValueError se vuota."""
    if not records:
        raise ValueError("aggregate() richiede almeno un record.")

    targets = {r.target for r in records}
    entity = records[0].target
    if len(targets) > 1:
        log.warning(
            "aggregate() ricevuto record con target eterogenei: %s. "
            "Viene usato '%s' come entità.",
            targets, entity,
        )

    queries = sorted({r.query for r in records if r.query})

    source_dist = _compute_source_distribution(records)
    sentiment_avg, sentiment_std, sentiment_count = _compute_weighted_sentiment(records)
    source_trust = _compute_source_trust(records)
    recency = _compute_recency_score(records)
    volume = _compute_volume_score(len(records))
    trend = _compute_trend(records)
    date_range = _compute_date_range(records)

    reputation = _compute_reputation_score(sentiment_avg, source_trust, recency, volume)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    summary = EntitySummary(
        entity=entity,
        queries=queries,
        record_count=len(records),
        records_with_sentiment=sentiment_count,
        source_distribution=source_dist,
        sentiment_avg=sentiment_avg,
        sentiment_std=sentiment_std,
        source_trust_avg=source_trust,
        recency_score=recency,
        volume_score=volume,
        reputation_score=reputation,
        trend=trend,
        date_range=date_range,
        computed_at=now,
    )

    log.info(
        "Aggregazione completata per '%s': %d record, reputation=%.4f, trend=%s",
        entity, len(records), reputation, trend,
    )

    return summary
