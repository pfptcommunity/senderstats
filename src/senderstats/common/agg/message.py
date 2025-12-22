from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from math import sqrt

@dataclass
class PatternEntry:
    count: int
    sample: str


@dataclass
class TopKNormalizedPatterns:
    """
    Used to aggregate subject lines and heavy hitting subjects

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

@dataclass
class RunningStats:
    n: int = 0
    mean: float = 0.0
    M2: float = 0.0

    def add(self, x: float) -> None:
        n = self.n + 1
        mean = self.mean
        M2 = self.M2

        # Inline welford
        delta = x - mean
        mean += delta / n
        delta2 = x - mean
        M2 += delta * delta2

        self.n = n
        self.mean = mean
        self.M2 = M2

    def std(self) -> float:
        return sqrt(self.M2 / (self.n - 1)) if self.n > 1 else 0.0

    def cv(self) -> float:
        return (self.std() / self.mean) if self.mean > 0 else 0.0

@dataclass
class MessageAgg:
    # Original messages
    messages: int = 0
    total_bytes_original: int = 0

    # Recipient aggregate
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
            msg_date: Optional[datetime],
            *,
            rcpt_count: int = 1,
    ) -> None:
        if msgsz < 0:
            return

        if rcpt_count <= 0:
            rcpt_count = 1

        # Message-unit stats for non-expanded
        self.messages += 1
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

        # Delivery stats recipient expanded
        self.total_recipients += rcpt_count
        self.total_recipients_bytes += msgsz * rcpt_count

        # Subject patterns per message
        self.norm_patterns.add(normalized_subject or "", subject or "")
