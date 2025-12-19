from __future__ import annotations

from dataclasses import dataclass, field
from math import sqrt, exp, log2
from typing import Dict, Optional, Iterator, Tuple, List
from datetime import datetime

from senderstats.common.subject_normalizer import normalize_subject
from senderstats.data.message_data import MessageData
from senderstats.interfaces.processor import Processor
from senderstats.interfaces.reportable import Reportable

def welford_update(n: int, mean: float, M2: float, x: float):
    delta = x - mean
    mean += delta / n
    delta2 = x - mean
    M2 += delta * delta2
    return mean, M2


@dataclass
class RunningStats:
    n: int = 0
    mean: float = 0.0
    M2: float = 0.0

    def add(self, x: float):
        self.n += 1
        self.mean, self.M2 = welford_update(self.n, self.mean, self.M2, x)

    def std(self) -> float:
        return sqrt(self.M2 / (self.n - 1)) if self.n > 1 else 0.0

    def cv(self) -> float:
        return (self.std() / self.mean) if self.mean > 0 else 0.0


# ----------------------------
# Heavy hitters (Space-Saving)
# ----------------------------
@dataclass
class PatternEntry:
    count: int
    sample: str  # representative original subject


@dataclass
class TopKNormalizedPatterns:
    """
    Space-Saving heavy hitters for normalized_subject patterns.

    Stores a bounded map:
      normalized_subject -> {count, sample_original_subject}

    Counts are approximate, but very accurate for the most frequent patterns.
    Sample is "first-seen" while the key is tracked.
    """
    k: int = 64
    patterns: Dict[str, PatternEntry] = field(default_factory=dict)

    def add(self, normalized: str, sample_subject: str):
        if not normalized:
            return

        p = self.patterns
        entry = p.get(normalized)
        if entry is not None:
            entry.count += 1
            return

        if len(p) < self.k:
            p[normalized] = PatternEntry(count=1, sample=sample_subject or normalized)
            return

        # Space-Saving eviction: replace the current minimum-count key
        min_key = min(p, key=lambda kk: p[kk].count)
        min_val = p[min_key].count
        del p[min_key]

        # New key inherits min+1 count (carry-forward error)
        p[normalized] = PatternEntry(count=min_val + 1, sample=sample_subject or normalized)

    def top_items(self, n: int = 10) -> List[Tuple[str, PatternEntry]]:
        return sorted(self.patterns.items(), key=lambda kv: kv[1].count, reverse=True)[:n]


# ----------------------------
# Scoring helpers
# ----------------------------
def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + exp(-x))


def normalized_entropy(counts: List[int], total: int) -> float:
    """
    Normalized Shannon entropy in [0,1].
    Includes a tail bucket for everything outside tracked counts.
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


def is_response(subject: str) -> bool:
    if not subject:
        return False
    s = subject.lstrip().lower()
    return s.startswith(("{r}"))


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

    def clamp01(x: float) -> float:
        return 0.0 if x < 0.0 else 1.0 if x > 1.0 else x

    top_mass = clamp01(top_mass)
    top3_mass = clamp01(top3_mass)
    top1_ratio = clamp01(top1_ratio)
    ent_norm = clamp01(ent_norm)
    reply_ratio = clamp01(reply_ratio)

    score = (
        3.0 * (top_mass - 0.60) +
        2.0 * (top3_mass - 0.75) +
        1.5 * (top1_ratio - 0.30) +
        2.5 * ((1.0 - ent_norm) - 0.35)
    )

    # Conversation penalty (kept)
    score += -3.0 * (reply_ratio - 0.20)

    p_signal = sigmoid(4.0 * score)

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
        (0.0,   0.05),
        (5.0,   0.10),
        (25.0,  0.35),
        (50.0,  0.90),
        (75.0,  0.97),
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
    """
    Template-first combination:
      - template evidence is required for a high final probability
      - volume can boost confidence but cannot "create" app-ness alone
    """
    p_template = max(0.0, min(1.0, p_template))
    p_volume = max(0.0, min(1.0, p_volume))
    return p_template + (1.0 - p_template) * (p_volume ** 2) * 0.35


def human_probability(reply_ratio: float, rows_per_day: float) -> float:
    """
    Language-agnostic "interactive mailbox" likelihood.

    High when:
      - reply_ratio is substantial (interactive)
      - and volume is not extreme (true apps can be high-volume)
    """
    rr = max(0.0, min(1.0, reply_ratio))
    rpd = max(0.0, rows_per_day)

    # Reply signal: starts kicking in around 0.30
    rr_score = sigmoid(14.0 * (rr - 0.30))

    # Volume gate: higher rpd => less likely human mailbox
    # midpoint ~25/day
    vol_gate = 1.0 - sigmoid(0.18 * (rpd - 25.0))

    return rr_score * vol_gate


# ----------------------------
# Aggregation per sender
# ----------------------------
@dataclass
class MFromAgg:
    # Original messages
    messages: int = 0
    total_bytes_original: int = 0

    # Recipient aggregate data (for SER)
    total_recipients: int = 0
    total_recipients_bytes: int = 0

    # Reply/forward activity
    responses: int = 0

    # Burstiness / timing
    last_date: Optional[datetime] = None
    gap_stats: RunningStats = field(default_factory=RunningStats)

    # Size stats (per original message)
    size_stats: RunningStats = field(default_factory=RunningStats)

    # Patterns
    norm_patterns: TopKNormalizedPatterns = field(
        default_factory=lambda: TopKNormalizedPatterns(k=64)
    )

    def add_message(
            self,
            msgsz: int,
            subject: str,
            normalized_subject: str,
            is_response: bool,
            rcpts: List[str],
            msg_date: Optional[datetime],
    ):
        # Original message
        self.messages += 1 # 1 row, 1 message
        self.total_bytes_original += msgsz
        self.size_stats.add(float(msgsz))

        if is_response:
            self.responses += 1

        # Timing gaps
        if msg_date is not None:
            if self.last_date is not None:
                delta = (msg_date - self.last_date).total_seconds()
                if delta >= 0:
                    self.gap_stats.add(float(delta))
            self.last_date = msg_date

        # Recipient aggregate
        if rcpts:
            n = len(rcpts)
            self.total_recipients += n
            self.total_recipients_bytes += msgsz * n

        # Subject patterns
        self.norm_patterns.add(normalized_subject or "", subject or "")


# ----------------------------
# Processor
# ----------------------------
class MFromProcessor(Processor[MessageData], Reportable):
    __mfrom_data: Dict[str, MFromAgg]
    __expand_recipients: bool
    __sample_subject: bool
    __topk_subjects: int
    __report_top_n: int
    __debug: bool

    def __init__(
        self,
        sample_subject: bool = False,
        expand_recipients: bool = False,
        topk_subjects: int = 64,
        report_top_n: int = 50,
        debug: bool = True,
    ):
        super().__init__()
        self.__mfrom_data = {}
        self.__sample_subject = sample_subject
        self.__expand_recipients = expand_recipients
        self.__topk_subjects = topk_subjects
        self.__report_top_n = report_top_n
        self.__debug = debug

    def execute(self, data: MessageData) -> None:
        subj = data.subject or ""
        snorm, is_response = normalize_subject(subj)
        mfrom = data.mfrom

        agg = self.__mfrom_data.get(data.mfrom)
        if agg is None:
            agg = MFromAgg(norm_patterns=TopKNormalizedPatterns(k=self.__topk_subjects))
            self.__mfrom_data[mfrom] = agg

        agg.add_message(
            msgsz=int(data.msgsz),
            subject=subj,
            normalized_subject=snorm,
            is_response=is_response,
            rcpts=data.rcpts or [],
            msg_date=data.date or None
        )

    def report(self, context: Optional = None) -> Iterator[Tuple[str, Iterator[list]]]:
        def get_report_name():
            return "Envelope Senders"

        def get_report_data():
            headers = [
                'MFrom',
                'Messages',
                'Avg Msg Size',
                'Messages Per Day',
                'Total Bytes',
                'Total Recipients',
                'Delivery Bytes',
                'App Probability',
                'Label',
            ]

            if self.__debug:
                headers.extend([
                    'Reply/Fwd Ratio',
                    'TopN Mass',
                    'Top3 Mass',
                    'Top1 Ratio',
                    'Entropy',
                    'P Template',
                    'P Volume',
                    'P AppLike',
                    'P Human',
                    'Avg Rcpts/Msg',
                    'Avg Ext Rcpts/Msg',
                    'Gap Mean (s)',
                    'Gap CV',
                    'Top Normalized Subjects',
                ])

            if self.__sample_subject:
                headers.append('Sample Subjects')

            yield headers

            days = float(context) if context else 0.0

            for mfrom, agg in self.__mfrom_data.items():
                # -------------------------
                # Base counts (original msgs)
                # -------------------------
                total_messages = agg.messages  # original message rows
                messages_per_day = (total_messages / days) if days > 0 else 0.0

                # Bytes: Size of single message in bytes
                total_bytes = agg.total_bytes_original
                avg_size = (total_bytes / total_messages) if total_messages > 0 else 0.0

                total_recipients = agg.total_recipients
                # If you have agg.delivery_bytes tracked already, use that instead.
                delivery_bytes = agg.total_recipients_bytes

                # -------------------------
                # Behavior stats
                # -------------------------
                reply_ratio = (agg.responses / total_messages) if total_messages > 0 else 0.0

                # Template distribution stats
                top_items = agg.norm_patterns.top_items(self.__report_top_n)
                top_counts = [entry.count for _, entry in top_items]
                top_sum = sum(top_counts)

                top_mass = (top_sum / total_messages) if total_messages > 0 else 0.0
                top1_ratio = (top_counts[0] / total_messages) if (total_messages > 0 and top_counts) else 0.0
                top3_mass = (sum(top_counts[:3]) / total_messages) if total_messages > 0 else 0.0

                tracked_counts = [entry.count for _, entry in agg.norm_patterns.patterns.items()]
                ent = normalized_entropy(tracked_counts, total_messages)

                # Core probabilities (UNCHANGED inputs)
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

                # Optional caps (kept)
                if reply_ratio >= 0.40:
                    p_final = min(p_final, 0.10)
                elif reply_ratio >= 0.30:
                    p_final = min(p_final, 0.20)

                label = classify_sender(
                    rows_per_day=messages_per_day,
                    reply_ratio=reply_ratio,
                    p_human=p_human,
                    top1_ratio=top1_ratio,
                )

                row = [
                    mfrom,
                    total_messages,
                    avg_size,
                    messages_per_day,
                    total_bytes,
                    total_recipients,
                    delivery_bytes,
                    p_final,
                    label,
                ]

                if self.__debug:
                    gap_mean = agg.gap_stats.mean if agg.gap_stats.n > 0 else 0.0
                    gap_cv = agg.gap_stats.cv() if agg.gap_stats.n > 1 else 0.0

                    avg_rcpts = (total_recipients / total_messages) if total_messages > 0 else 0.0
                    avg_ext_rcpts = avg_rcpts  # per your export semantics

                    top_snorm = agg.norm_patterns.top_items(self.__report_top_n)

                    snorm_summary = "\n".join(
                        f"[{entry.count}] {snorm}"
                        for snorm, entry in top_snorm
                    )

                    row.extend([
                        reply_ratio,
                        top_mass,
                        top3_mass,
                        top1_ratio,
                        ent,
                        p_template,
                        p_volume,
                        p_app_like,
                        p_human,
                        avg_rcpts,
                        avg_ext_rcpts,
                        gap_mean,
                        gap_cv,
                        snorm_summary,
                    ])

                if self.__sample_subject:
                    top_items_all = agg.norm_patterns.top_items(self.__report_top_n)
                    lines = [f"[{entry.count}] {entry.sample}" for _, entry in top_items_all]
                    row.append("\n".join(lines))

                yield row

        yield get_report_name(), get_report_data()

    @property
    def create_data_table(self) -> bool:
        return True


def classify_sender(
    rows_per_day: float,
    reply_ratio: float,
    p_human: float,
    top1_ratio: float,
) -> str:
    """
    Deterministic sender classification for reporting/debugging.
    """
    if p_human >= 0.40:
        return "Likely Human"

    if rows_per_day >= 20.0:
        return "High Probability App"

    if rows_per_day < 1.0 and reply_ratio == 0.0 and top1_ratio >= 0.95:
        return "Low-Volume Automated Source"

    if rows_per_day >= 1.0 and reply_ratio == 0.0:
        return "Medium Probability App"

    return "Unknown/Ambiguous"
