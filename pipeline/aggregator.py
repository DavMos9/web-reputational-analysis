"""
pipeline/aggregator.py

Aggregazione entity-level: trasforma una lista di Record arricchiti
in un EntitySummary con metriche reputazionali composite.

Posizione nella pipeline:
    collect → normalize → clean → deduplicate → enrich → **aggregate** → export

Motivazione del posizionamento:
    - Dopo enrich: richiede language, sentiment e source già calcolati.
    - Prima di export: il summary viene esportato come file separato.

Input:  list[Record]  — record arricchiti, tutti con lo stesso target.
Output: EntitySummary — dataclass immutabile con metriche aggregate.

Principi:
    - Nessuna mutazione dei Record in input.
    - Tutti i parametri tunabili importati da config.py.
    - Gestione esplicita di edge case (0 record, 0 sentiment, 0 date).
    - Nessuna dipendenza NLP: lavora solo su campi già calcolati.
"""

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
    REPUTATION_WEIGHTS,
    RECENCY_HALF_LIFE_DAYS,
    VOLUME_HALFSAT,
    TREND_THRESHOLD,
    MIN_RECORDS_FOR_TREND,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# EntitySummary — output dell'aggregazione
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EntitySummary:
    """
    Riepilogo reputazionale aggregato per una singola entità.

    Tutti gli score sono in [0.0, 1.0] tranne sentiment_avg che è in [-1.0, 1.0].

    Attributi:
        entity              Nome dell'entità analizzata (target).
        record_count        Numero totale di record aggregati.
        records_with_sentiment  Quanti record avevano un sentiment calcolato.
        source_distribution Conteggio record per sorgente, ordinato decrescente.

        sentiment_avg       Media pesata del sentiment (pesi = SOURCE_WEIGHTS).
                            Range [-1.0, 1.0]. None se nessun record ha sentiment.
        sentiment_std       Deviazione standard pesata del sentiment.
                            Usa gli stessi pesi SOURCE_WEIGHTS della media.
                            Misura la polarizzazione: alta = opinioni divergenti.
                            None se < 2 record con sentiment.

        source_trust_avg    Media pesata dell'autorevolezza delle sorgenti.
                            Range [0.0, 1.0].
        recency_score       Quanto sono recenti i record. Range [0.0, 1.0].
                            1.0 = tutti pubblicati oggi, 0.0 = tutti molto vecchi.
        volume_score        Volume normalizzato con saturazione asintotica.
                            Range [0.0, 1.0): tende a 1.0 al crescere del numero
                            di record, vale 0.5 a VOLUME_HALFSAT (default 100).
                            Non satura con hard cap: resta discriminante anche
                            su run molto grandi.

        reputation_score    Score composito finale. Range [0.0, 1.0].
                            Combinazione pesata di sentiment (mappato in [0,1]),
                            trust, recency e volume.

        trend               Direzione del sentiment nel tempo: "up", "down", "stable".
                            Basato sulla pendenza di una regressione lineare.
                            "unknown" se non ci sono dati temporali sufficienti.

        date_range          Tupla (data_min, data_max) dei record con data valida.
                            None se nessun record ha data.
        computed_at         Timestamp ISO 8601 del calcolo.
    """

    entity: str
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
        # asdict converte le tuple in liste; date_range va preservata leggibile
        if self.date_range is not None:
            d["date_range"] = {"from": self.date_range[0], "to": self.date_range[1]}
        return d


# ---------------------------------------------------------------------------
# Funzioni di calcolo delle singole componenti
# ---------------------------------------------------------------------------

def _get_source_weight(source: str) -> float:
    """Restituisce il peso di autorevolezza per una sorgente."""
    return SOURCE_WEIGHTS.get(source, SOURCE_WEIGHT_DEFAULT)


def _compute_source_distribution(records: list[Record]) -> dict[str, int]:
    """Conteggio record per sorgente, ordinato decrescente per volume."""
    dist: dict[str, int] = {}
    for r in records:
        dist[r.source] = dist.get(r.source, 0) + 1
    return dict(sorted(dist.items(), key=lambda x: x[1], reverse=True))


def _compute_weighted_sentiment(records: list[Record]) -> tuple[float | None, float | None, int]:
    """
    Calcola media pesata e deviazione standard del sentiment.

    La media e la deviazione standard sono entrambe pesate per SOURCE_WEIGHTS:
    un articolo del Guardian pesa più di un commento YouTube.
    Questo garantisce coerenza tra i due indicatori: la std misura la
    dispersione intorno alla stessa media pesata.

    Returns:
        (media_pesata, std_pesata, conteggio_record_con_sentiment)
        Media e std sono None se non ci sono record con sentiment.
        Std è None se < 2 record con sentiment.
    """
    pairs: list[tuple[float, float]] = []  # (sentiment, weight)
    for r in records:
        if r.sentiment is not None:
            w = _get_source_weight(r.source)
            pairs.append((r.sentiment, w))

    count = len(pairs)
    if count == 0:
        return None, None, 0

    # Media pesata
    total_weight = sum(w for _, w in pairs)
    if total_weight == 0:
        return None, None, 0

    avg = sum(s * w for s, w in pairs) / total_weight

    # Clamping difensivo
    avg = max(-1.0, min(1.0, avg))

    # Varianza pesata: Σ w_i*(s_i - avg)² / Σ w_i
    # Stessi pesi della media → std coerente con avg.
    std: float | None = None
    if count >= 2:
        weighted_variance = sum(w * (s - avg) ** 2 for s, w in pairs) / total_weight
        std = math.sqrt(weighted_variance)

    return round(avg, 6), round(std, 6) if std is not None else None, count


def _compute_source_trust(records: list[Record]) -> float:
    """
    Media pesata dell'autorevolezza delle sorgenti.

    Ogni record contribuisce con il peso della sua sorgente.
    Il risultato è la media semplice dei pesi — misura la qualità
    complessiva del mix di fonti raccolte.

    Returns:
        Score in [0.0, 1.0].
    """
    if not records:
        return 0.0

    weights = [_get_source_weight(r.source) for r in records]
    return round(sum(weights) / len(weights), 6)


def _compute_recency_score(records: list[Record], reference_date: date | None = None) -> float:
    """
    Quanto sono recenti i record, con decay esponenziale.

    Formula per singolo record:
        weight_i = 2^(-age_days / half_life)

    Score finale = media dei weight_i dei record con data valida.

    Un half_life di 30 giorni significa che un record di 30 giorni fa
    contribuisce con peso 0.5, uno di 60 giorni con 0.25, ecc.

    Args:
        records:        lista di Record.
        reference_date: data di riferimento per calcolare l'età.
                        Default: oggi (UTC).

    Returns:
        Score in [0.0, 1.0]. 0.0 se nessun record ha data valida.
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
            age_days = 0  # record con data futura → trattato come "oggi"

        w = 2.0 ** (-age_days / half_life)
        decay_weights.append(w)

    if not decay_weights:
        return 0.0

    return round(sum(decay_weights) / len(decay_weights), 6)


def _compute_volume_score(record_count: int) -> float:
    """
    Volume normalizzato con saturazione asintotica (Hill-on-log).

    Formula:
        l = log(1 + count)
        volume_score = l / (l + log(1 + VOLUME_HALFSAT))

    Proprietà:
        - count = 0                → 0.0
        - count = VOLUME_HALFSAT   → 0.5   (punto di half-saturation)
        - count → +∞               → 1.0   (asintoto, mai raggiunto)

    Rispetto al vecchio `min(1.0, log(1+c)/log(1+ref))`, questa formula non
    satura con un hard cap: run da 100 e da 5000 record producono
    `volume_score` diversi, preservando la discriminabilità.

    Il log preserva i rendimenti decrescenti (passare da 10 a 20 conta più
    che passare da 90 a 100).

    Returns:
        Score in [0.0, 1.0).
    """
    if record_count <= 0:
        return 0.0

    halfsat = max(1, VOLUME_HALFSAT)
    l = math.log(1 + record_count)
    denom = l + math.log(1 + halfsat)
    return round(l / denom, 6)


def _compute_trend(records: list[Record]) -> str:
    """
    Direzione del sentiment nel tempo via regressione lineare.

    Ordina i record per data, assegna x = indice ordinale (0, 1, 2, ...),
    y = sentiment. Calcola la pendenza della retta di regressione.

    - slope > +TREND_THRESHOLD  → "up"   (sentiment in miglioramento)
    - slope < -TREND_THRESHOLD  → "down" (sentiment in peggioramento)
    - |slope| <= TREND_THRESHOLD → "stable"

    Servono almeno 3 record con data E sentiment per un trend significativo.

    Returns:
        "up", "down", "stable", o "unknown" se dati insufficienti.
    """
    # Filtra record con data e sentiment validi
    dated: list[tuple[str, float]] = []
    for r in records:
        if r.date is not None and r.sentiment is not None:
            dated.append((r.date, r.sentiment))

    if len(dated) < MIN_RECORDS_FOR_TREND:
        return "unknown"

    # Ordina per data
    dated.sort(key=lambda x: x[0])

    n = len(dated)
    # x = indice ordinale, y = sentiment
    x_vals = list(range(n))
    y_vals = [s for _, s in dated]

    # Regressione lineare: slope = (n*Σxy - Σx*Σy) / (n*Σx² - (Σx)²)
    sum_x = sum(x_vals)
    sum_y = sum(y_vals)
    sum_xy = sum(x * y for x, y in zip(x_vals, y_vals))
    sum_x2 = sum(x * x for x in x_vals)

    denominator = n * sum_x2 - sum_x * sum_x
    if denominator == 0:
        log.warning(
            "[aggregator] Trend: denominatore zero con %d record "
            "(tutti con lo stesso ordinal o dataset degenere). "
            "Restituito 'stable' come fallback — verificare qualità dei dati.",
            n,
        )
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
    """
    Mappa il sentiment medio da [-1.0, 1.0] a [0.0, 1.0] per la
    combinazione nel reputation score.

    Formula: (sentiment + 1) / 2
        -1.0 → 0.0  (completamente negativo)
         0.0 → 0.5  (neutro)
        +1.0 → 1.0  (completamente positivo)

    Se sentiment è None, restituisce 0.5 (neutro — nessuna informazione).
    """
    if sentiment_avg is None:
        return 0.5
    return (sentiment_avg + 1.0) / 2.0


def _compute_reputation_score(
    sentiment_avg: float | None,
    source_trust: float,
    recency: float,
    volume: float,
) -> float:
    """
    Calcola il reputation score composito come media pesata.

    Tutti i componenti sono in [0.0, 1.0] (sentiment viene mappato).
    I pesi sono definiti in config.REPUTATION_WEIGHTS.

    Returns:
        Score in [0.0, 1.0].
    """
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

    # Clamping difensivo (non dovrebbe mai servire con pesi normalizzati)
    return round(max(0.0, min(1.0, score)), 4)


# ---------------------------------------------------------------------------
# Entry point pubblico
# ---------------------------------------------------------------------------

def aggregate(records: list[Record]) -> EntitySummary:
    """
    Aggrega una lista di Record in un EntitySummary.

    Tutti i record devono avere lo stesso target. Se i target sono
    eterogenei, viene usato il target del primo record e viene emesso
    un warning.

    Args:
        records: lista di Record arricchiti (post-enricher).

    Returns:
        EntitySummary con metriche reputazionali aggregate.

    Raises:
        ValueError: se la lista è vuota.
    """
    if not records:
        raise ValueError("aggregate() richiede almeno un record.")

    # Verifica coerenza target
    targets = {r.target for r in records}
    entity = records[0].target
    if len(targets) > 1:
        log.warning(
            "aggregate() ricevuto record con target eterogenei: %s. "
            "Viene usato '%s' come entità.",
            targets, entity,
        )

    # Calcolo componenti
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
