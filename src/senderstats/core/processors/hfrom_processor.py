from random import random
from typing import TypeVar, Generic, Dict, Optional, Iterator, Tuple

from senderstats.common.utils import average
from senderstats.data.message_data import MessageData
from senderstats.interfaces.processor import Processor
from senderstats.interfaces.reportable import Reportable

TMessageData = TypeVar('TMessageData', bound=MessageData)


# HFromProcessor.py
class HFromProcessor(Processor[MessageData], Reportable, Generic[TMessageData]):
    __hfrom_data: Dict[str, Dict]
    __sample_subject: bool
    __expand_recipients: bool

    def __init__(self, sample_subject=False, expand_recipients=False):
        super().__init__()
        self.__hfrom_data = dict()
        self.__sample_subject = sample_subject
        self.__expand_recipients = expand_recipients

    def execute(self, data: TMessageData) -> None:
        self.__hfrom_data.setdefault(data.hfrom, {})

        hfrom_data = self.__hfrom_data[data.hfrom]

        if self.__expand_recipients:
            hfrom_data.setdefault("message_size", []).extend([data.msgsz] * len(data.rcpts))
        else:
            hfrom_data.setdefault("message_size", []).append(data.msgsz)

        if self.__sample_subject:
            hfrom_data.setdefault("subjects", [])
            if data.subject:
                probability = 1 / len(hfrom_data['message_size'])
                if not hfrom_data['subjects'] or random() < probability:
                    hfrom_data['subjects'].append(data.subject)

    def report(self, context: Optional = None) -> Iterator[Tuple[str, Iterator[list]]]:
        # Yield the report name and the data generator together
        def get_report_name():
            return "Header From"

        def get_report_data():
            headers = ['HFrom', 'Messages', 'Size', 'Messages Per Day', 'Total Bytes']
            if self.__sample_subject:
                headers.append('Subjects')
            yield headers
            for k, v in self.__hfrom_data.items():
                messages_per_sender = len(v['message_size'])
                total_bytes = sum(v['message_size'])
                average_message_size = average(v['message_size'])
                messages_per_sender_per_day = messages_per_sender / context
                row = [k, messages_per_sender, average_message_size, messages_per_sender_per_day, total_bytes]
                if self.__sample_subject:
                    row.append('\n'.join(v['subjects']))
                yield row

        yield get_report_name(), get_report_data()

    @property
    def create_data_table(self) -> bool:
        return True
