from random import random
from typing import Dict, Optional, Iterator, Tuple

from senderstats.common.utils import average
from senderstats.data.message_data import MessageData
from senderstats.interfaces.processor import Processor
from senderstats.interfaces.reportable import Reportable


class AlignmentProcessor(Processor[MessageData], Reportable):
    __alignment_data: Dict[tuple, Dict]
    __sample_subject: bool
    __expand_recipients: bool

    def __init__(self, sample_subject=False, expand_recipients=False):
        super().__init__()
        self.__alignment_data = dict()
        self.__sample_subject = sample_subject
        self.__expand_recipients = expand_recipients

    def execute(self, data: MessageData) -> None:
        sender_header_index = (data.mfrom, data.hfrom)
        self.__alignment_data.setdefault(sender_header_index, {})

        alignment_data = self.__alignment_data[sender_header_index]

        if self.__expand_recipients:
            alignment_data.setdefault("message_size", []).extend([data.msgsz] * len(data.rcpts))
        else:
            alignment_data.setdefault("message_size", []).append(data.msgsz)

        if self.__sample_subject:
            alignment_data.setdefault("subjects", [])
            if data.subject:
                probability = 1 / len(alignment_data['message_size'])
                if not alignment_data['subjects'] or random() < probability:
                    alignment_data['subjects'].append(data.subject)

    def report(self, context: Optional = None) -> Iterator[Tuple[str, Iterator[list]]]:
        # Yield the report name and the data generator together
        def get_report_name():
            return "MFrom + HFrom (Alignment)"

        def get_report_data():
            headers = ['MFrom', 'HFrom', 'Messages', 'Size', 'Messages Per Day', 'Total Bytes']
            if self.__sample_subject:
                headers.append('Subjects')
            yield headers
            for k, v in self.__alignment_data.items():
                messages_per_sender = len(v['message_size'])
                total_bytes = sum(v['message_size'])
                average_message_size = average(v['message_size'])
                messages_per_sender_per_day = messages_per_sender / context
                row = [k[0], k[1], messages_per_sender, average_message_size, messages_per_sender_per_day, total_bytes]
                if self.__sample_subject:
                    row.append('\n'.join(v['subjects']))
                yield row

        yield get_report_name(), get_report_data()

    @property
    def create_data_table(self) -> bool:
        return True
