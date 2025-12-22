from __future__ import annotations

from typing import Dict, Iterator, List, Optional, Tuple

from senderstats.common.agg.report import KeyedAggReport
from senderstats.common.agg.aggregator import KeyedAggregator
from senderstats.common.agg.message import MessageAgg, TopKNormalizedPatterns
from senderstats.data.message_data import MessageData
from senderstats.interfaces.processor import Processor
from senderstats.interfaces.reportable import Reportable


class MFromProcessor(Processor[MessageData], Reportable):
    """
    Aggregates per-envelope-sender (MFrom) stats.

    Aggregation: KeyedAggregator[str, MessageAgg]
    Reporting: derives template metrics + probabilities using scoring.py
    """

    def __init__(
        self,
        sample_subject: bool = False,
        with_probability: bool = False,
        expand_recipients: bool = False,
        topk_subjects: int = 64,
        report_top_n: int = 50,
        debug: bool = False,
    ):
        super().__init__()
        self.__sample_subject = sample_subject
        self.__with_probability = with_probability
        self.__expand_recipients = expand_recipients
        self.__topk_subjects = topk_subjects
        self.__report_top_n = report_top_n
        self.__debug = debug

        # Keyed buckets for per-sender aggregation
        self.__by_mfrom: KeyedAggregator[str, MessageAgg] = KeyedAggregator(
            agg_factory=lambda: MessageAgg(
                # ensure per-processor top-k size is applied
                norm_patterns=TopKNormalizedPatterns(k=self.__topk_subjects)
            )
        )
        self.__reporter = KeyedAggReport[str](
            title="Envelope Senders",
            key_columns=["MFrom"],
            key_to_cells=lambda k: [k],
            report_top_n=self.__report_top_n,
            sample_subject=self.__sample_subject,
            with_probability=self.__with_probability,
            debug=self.__debug,
        )

    def execute(self, data: MessageData) -> None:
        if self.__expand_recipients:
            count = len(data.rcpts)
        else:
            count = 1

        if self.__sample_subject:
            subject = data.subject
            snorm = data.subject_norm
            is_response = data.subject_is_response
        else:
            subject = ""
            snorm = ""
            is_response = ""

        agg = self.__by_mfrom.get(data.mfrom)
        agg.add_message(
            msgsz=int(data.msgsz),
            subject=subject,
            normalized_subject=snorm,
            is_response=is_response,
            msg_date=data.date or None,
            rcpt_count=count
        )

    def report(self, context: Optional = None) -> Iterator[Tuple[str, Iterator[list]]]:
        days = float(context) if context else 0.0
        return self.__reporter.report(self.__by_mfrom.items(), days=days)

    @property
    def create_data_table(self) -> bool:
        return True
