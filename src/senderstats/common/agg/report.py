from __future__ import annotations

from typing import Iterable, Iterator, List, Tuple, TypeVar, Generic, Callable

from senderstats.common.agg.message import MessageAgg
from senderstats.common.agg.metrics import compute_message_agg_metrics
from senderstats.common.agg.scoring import compute_sender_scores_and_label

K = TypeVar("K")


class KeyedAggReport(Generic[K]):
    def __init__(
            self,
            *,
            title: str,
            key_columns: List[str],
            key_to_cells: Callable[[K], List[object]],
            report_top_n: int,
            sample_subject: bool,
            with_probability: bool,  # NEW
            debug: bool,
    ):
        self._title = title
        self._key_columns = key_columns
        self._key_to_cells = key_to_cells
        self._report_top_n = report_top_n
        self._sample_subject = sample_subject
        self._with_probability = with_probability
        self._debug = debug

    def report(
            self,
            items: Iterable[Tuple[K, MessageAgg]],
            *,
            days: float,
    ) -> Iterator[Tuple[str, Iterator[list]]]:

        def get_report_name() -> str:
            return self._title

        def get_report_data() -> Iterator[list]:
            headers: List[object] = [
                *self._key_columns,
                "Messages",
                "Avg Msg Size",
                "Messages Per Day",
                "Total Bytes",
                "Total Recipients",
                "Delivery Bytes",
            ]

            # Only include these when probability is enabled
            if self._with_probability:
                headers.extend(["Autonomy Score (%)", "Label"])

            if self._sample_subject:
                headers.append("Sample Subjects")

            if self._sample_subject and self._debug:
                headers += ["Top Normalized Subjects"]

                if self._with_probability:
                    headers += [
                        "App Probability",
                        "Reply/Fwd Ratio",
                        "TopN Mass",
                        "Top3 Mass",
                        "Top1 Ratio",
                        "Entropy",
                        "P Template",
                        "P Volume",
                        "P AppLike",
                        "P Human",
                        "Avg Rcpts/Msg",
                        "Avg Ext Rcpts/Msg",
                        "Gap Mean (s)",
                        "Gap CV",
                    ]

            yield headers

            for key, agg in items:
                m = compute_message_agg_metrics(
                    agg,
                    days=days,
                    report_top_n=self._report_top_n,
                )

                # Only compute probabilistic scoring when enabled
                s = None
                if self._with_probability:
                    s = compute_sender_scores_and_label(
                        total_messages=m.total_messages,
                        messages_per_day=m.messages_per_day,
                        reply_ratio=m.reply_ratio,
                        top_mass=m.top_mass,
                        top3_mass=m.top3_mass,
                        top1_ratio=m.top1_ratio,
                        ent=m.entropy,
                    )

                row: List[object] = []
                row.extend(self._key_to_cells(key))
                row.extend([
                    m.total_messages,
                    m.avg_size,
                    m.messages_per_day,
                    m.total_bytes,
                    m.total_recipients,
                    m.delivery_bytes,
                ])

                if self._with_probability:
                    row.extend([round(s.sort_score * 100, 2), s.label])

                if self._sample_subject:
                    row.append("\n".join(
                        f"[{entry.count}] {entry.sample}" for _, entry in m.top_items
                    ))

                if self._sample_subject and self._debug:
                    snorm_summary = "\n".join(
                        f"[{entry.count}] {snorm}" for snorm, entry in m.top_items
                    )
                    row.append(snorm_summary)

                    if self._with_probability:
                        row.extend([
                            s.p_final,
                            m.reply_ratio,
                            m.top_mass,
                            m.top3_mass,
                            m.top1_ratio,
                            m.entropy,
                            s.p_template,
                            s.p_volume,
                            s.p_app_like,
                            s.p_human,
                            m.avg_rcpts,
                            m.avg_ext_rcpts,
                            m.gap_mean,
                            m.gap_cv,
                        ])

                yield row

        yield get_report_name(), get_report_data()
