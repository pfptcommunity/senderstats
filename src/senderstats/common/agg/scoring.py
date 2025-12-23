from __future__ import annotations

from math import exp, log2
from typing import List, NamedTuple


class SenderScore(NamedTuple):
    p_template: float
    p_volume: float
    p_app_like: float
    p_human: float
    p_final: float
    p_rank: float
    label: str
    sort_score: float


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = exp(-x)
        return 1.0 / (1.0 + z)
    else:
        z = exp(x)
        return z / (1.0 + z)


def _clamp01(x: float) -> float:
    return 0.0 if x < 0.0 else 1.0 if x > 1.0 else x


def classify_sender(
        rows_per_day: float,
        reply_ratio: float,
        p_human: float,
        top1_ratio: float,
        *,
        top1_template: str = "",
) -> tuple[str, float]:
    if p_human >= 0.40:
        return "Likely Human", 0.05

    if rows_per_day >= 20.0:
        return "High Probability App", 0.90

    if rows_per_day < 1.0 and reply_ratio <= 0.02 and top1_ratio >= 0.95:
        return "Low-Volume Automated Source", 0.55

    if rows_per_day >= 1.0 and reply_ratio <= 0.02:
        return "Medium Probability App", 0.70

    return "Unknown/Ambiguous", 0.30


def normalized_entropy(counts: List[int], total: int) -> float:
    """
    Normalized Shannon entropy in [0,1], with a tail bucket for untracked mass.
    """
    if total <= 0:
        return 0.0

    tracked_sum = sum(counts)
    tail = total - tracked_sum

    probs = []
    for c in counts:
        if c > 0:
            probs.append(c / total)
    if tail > 0:
        probs.append(tail / total)

    if len(probs) <= 1:
        return 0.0

    H = 0.0
    for p in probs:
        H -= p * log2(p)

    Hmax = log2(len(probs))
    return (H / Hmax) if Hmax > 0 else 0.0


def app_probability(
        n_effective_rows: int,
        top_mass: float,
        top3_mass: float,
        top1_ratio: float,
        ent_norm: float,
        reply_ratio: float,
) -> float:
    if n_effective_rows <= 0:
        return 0.0

    top_mass = _clamp01(top_mass)
    top3_mass = _clamp01(top3_mass)
    top1_ratio = _clamp01(top1_ratio)
    ent_norm = _clamp01(ent_norm)
    reply_ratio = _clamp01(reply_ratio)

    score = (
            3.0 * (top_mass - 0.60) +
            2.0 * (top3_mass - 0.75) +
            1.5 * (top1_ratio - 0.30) +
            2.5 * ((1.0 - ent_norm) - 0.35)
    )

    # Conversation penalty (kept)
    score += -3.0 * (reply_ratio - 0.20)

    p_signal = _sigmoid(4.0 * score)

    # Low-N shrinkage
    PRIOR = 0.20
    MIN_MESSAGES = 25
    confidence = n_effective_rows / MIN_MESSAGES
    if confidence > 1.0:
        confidence = 1.0

    return PRIOR * (1.0 - confidence) + p_signal * confidence


def volume_prior(rows_per_day: float) -> float:
    r = max(0.0, rows_per_day)

    pts = [
        (0.0, 0.05),
        (5.0, 0.10),
        (25.0, 0.35),
        (50.0, 0.90),
        (75.0, 0.97),
        (100.0, 0.99),
    ]

    if r <= pts[0][0]:
        return pts[0][1]
    if r >= pts[-1][0]:
        return pts[-1][1]

    for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
        if x0 <= r <= x1:
            t = (r - x0) / (x1 - x0) if x1 > x0 else 0.0
            return y0 + t * (y1 - y0)

    return pts[-1][1]


def combine_probabilities(p_template: float, p_volume: float) -> float:
    p_template = _clamp01(p_template)
    p_volume = _clamp01(p_volume)
    return p_template + (1.0 - p_template) * (p_volume ** 2) * 0.35


def human_probability(reply_ratio: float, rows_per_day: float) -> float:
    rr = _clamp01(reply_ratio)
    rpd = max(0.0, rows_per_day)

    rr_score = _sigmoid(14.0 * (rr - 0.30))
    vol_gate = 1.0 - _sigmoid(0.18 * (rpd - 25.0))
    return rr_score * vol_gate


def autonomy_score(
        *,
        p_app_like: float,
        p_human: float,
        rows_per_day: float,
        reply_ratio: float,
        top1_ratio: float,
) -> float:
    # 0..1 clamps
    p_app_like = max(0.0, min(1.0, p_app_like))
    p_human = max(0.0, min(1.0, p_human))

    # Strong human suppression (labels treat human as "not app")
    base = p_app_like * (1.0 - p_human)

    # Label alignment boosts:
    # - high volume => more "app"
    # - very low reply => more "app"
    # - low-volume + top1_ratio high => "automated source"
    vol_boost = min(1.0, rows_per_day / 20.0)  # hits 1 around your "High Probability App" cutoff
    rr_boost = min(1.0, max(0.0, (0.30 - reply_ratio) / 0.30))  # 1 when rr=0, 0 when rr>=0.30
    lowvol_auto = 1.0 if (rows_per_day < 1.0 and reply_ratio == 0.0 and top1_ratio >= 0.95) else 0.0

    # Combine: base drives most ordering, boosts improve label alignment
    score = base * 0.75 + vol_boost * 0.15 + rr_boost * 0.10

    # Guarantee low-volume automated sources rank above ambiguous low-volume humans
    if lowvol_auto:
        score = max(score, 0.60)

    # If human-likely, push it down hard so it sorts to bottom
    if p_human >= 0.40:
        score = min(score, 0.10)

    return max(0.0, min(1.0, score))


def compute_sender_scores_and_label(
        *,
        total_messages: int,
        messages_per_day: float,
        reply_ratio: float,
        top_mass: float,
        top3_mass: float,
        top1_ratio: float,
        ent: float,
) -> SenderScore:
    p_template = app_probability(
        n_effective_rows=total_messages,
        top_mass=top_mass,
        top3_mass=top3_mass,
        top1_ratio=top1_ratio,
        ent_norm=ent,
        reply_ratio=reply_ratio,
    )

    p_volume = volume_prior(messages_per_day)
    p_app_like = combine_probabilities(p_template, p_volume)
    p_human = human_probability(reply_ratio=reply_ratio, rows_per_day=messages_per_day)

    p_final = p_app_like * (1.0 - p_human)

    # caps (kept)
    if reply_ratio >= 0.40:
        p_final = min(p_final, 0.10)
    elif reply_ratio >= 0.30:
        p_final = min(p_final, 0.20)

    label, base = classify_sender(
        rows_per_day=messages_per_day,
        reply_ratio=reply_ratio,
        p_human=p_human,
        top1_ratio=top1_ratio,
    )

    p_rank = autonomy_score(
        p_app_like=p_app_like,
        p_human=p_human,
        rows_per_day=messages_per_day,
        reply_ratio=reply_ratio,
        top1_ratio=top1_ratio,
    )

    sort_score = base + (max(0.0, min(1.0, float(p_rank))) * 0.099)

    return SenderScore(
        p_template=p_template,
        p_volume=p_volume,
        p_app_like=p_app_like,
        p_human=p_human,
        p_final=p_final,
        p_rank=p_rank,
        label=label,
        sort_score=sort_score,
    )
