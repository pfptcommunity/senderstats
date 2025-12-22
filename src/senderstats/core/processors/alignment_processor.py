from __future__ import annotations

from typing import Iterator, Optional, Tuple

from senderstats.common.agg.report import KeyedAggReport
from senderstats.common.agg.aggregator import KeyedAggregator
from senderstats.common.agg.message import MessageAgg, TopKNormalizedPatterns
from senderstats.data.message_data import MessageData
from senderstats.interfaces.processor import Processor
from senderstats.interfaces.reportable import Reportable


AlignKey = tuple[str, str]  # (mfrom, hfrom)


class AlignmentProcessor(Processor[MessageData], Reportable):
    """
    Aggregates per (MFrom, HFrom) alignment stats.

    Aggregation: KeyedAggregator[AlignKey, MessageAgg]
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

        self.__by_alignment: KeyedAggregator[AlignKey, MessageAgg] = KeyedAggregator(
            agg_factory=lambda: MessageAgg(
                norm_patterns=TopKNormalizedPatterns(k=self.__topk_subjects)
            )
        )

        self.__reporter = KeyedAggReport[AlignKey](
            title="MFrom + HFrom (Alignment)",
            key_columns=["MFrom", "HFrom"],
            key_to_cells=lambda k: [k[0], k[1]],
            report_top_n=self.__report_top_n,
            sample_subject=self.__sample_subject,
            with_probability=self.__with_probability,
            debug=self.__debug,
        )

    def execute(self, data: MessageData) -> None:
        key: AlignKey = (data.mfrom, data.hfrom)

        if self.__expand_recipients:
            count = len(data.rcpts)
        else:
            count = 1

        agg = self.__by_alignment.get(key)
        agg.add_message(
            msgsz=int(data.msgsz),
            subject=data.subject,
            normalized_subject=data.subject_norm,
            is_response=data.subject_is_response,
            msg_date=data.date or None,
            rcpt_count=count
        )


    def report(self, context: Optional = None) -> Iterator[Tuple[str, Iterator[list]]]:
        days = float(context) if context else 0.0
        return self.__reporter.report(self.__by_alignment.items(), days=days)

    @property
    def create_data_table(self) -> bool:
        return True
