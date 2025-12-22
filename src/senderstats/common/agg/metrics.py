from dataclasses import dataclass
from typing import List, Tuple

from .message import MessageAgg, PatternEntry
from .scoring import normalized_entropy

@dataclass(frozen=True)
class MessageAggMetrics:
    total_messages: int
    messages_per_day: float
    total_bytes: int
    avg_size: float
    total_recipients: int
    delivery_bytes: int
    reply_ratio: float
    top_items: List[Tuple[str, PatternEntry]]
    top_mass: float
    top3_mass: float
    top1_ratio: float
    entropy: float
    gap_mean: float
    gap_cv: float
    avg_rcpts: float
    avg_ext_rcpts: float

def compute_message_agg_metrics(
    agg: MessageAgg,
    *,
    days: float,
    report_top_n: int,
) -> MessageAggMetrics:
    total_messages = agg.messages
    messages_per_day = (total_messages / days) if days > 0 else 0.0

    total_bytes = agg.total_bytes_original
    avg_size = (total_bytes / total_messages) if total_messages > 0 else 0.0

    total_recipients = agg.total_recipients
    delivery_bytes = agg.total_recipients_bytes

    reply_ratio = (agg.responses / total_messages) if total_messages > 0 else 0.0

    top_items = agg.norm_patterns.top_items(report_top_n)
    top_counts = [e.count for _, e in top_items]
    top_sum = sum(top_counts)

    top_mass = (top_sum / total_messages) if total_messages > 0 else 0.0
    top1_ratio = (top_counts[0] / total_messages) if (total_messages > 0 and top_counts) else 0.0
    top3_mass = (sum(top_counts[:3]) / total_messages) if total_messages > 0 else 0.0

    tracked_counts = [e.count for e in agg.norm_patterns.patterns.values()]
    ent = normalized_entropy(tracked_counts, total_messages)

    gap_mean = agg.gap_stats.mean if agg.gap_stats.n > 0 else 0.0
    gap_cv = agg.gap_stats.cv() if agg.gap_stats.n > 1 else 0.0

    avg_rcpts = (total_recipients / total_messages) if total_messages > 0 else 0.0
    avg_ext_rcpts = avg_rcpts  # your semantics

    return MessageAggMetrics(
        total_messages=total_messages,
        messages_per_day=messages_per_day,
        total_bytes=total_bytes,
        avg_size=avg_size,
        total_recipients=total_recipients,
        delivery_bytes=delivery_bytes,
        reply_ratio=reply_ratio,
        top_items=top_items,
        top_mass=top_mass,
        top3_mass=top3_mass,
        top1_ratio=top1_ratio,
        entropy=ent,
        gap_mean=gap_mean,
        gap_cv=gap_cv,
        avg_rcpts=avg_rcpts,
        avg_ext_rcpts=avg_ext_rcpts,
    )
