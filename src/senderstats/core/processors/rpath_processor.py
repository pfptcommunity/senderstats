from random import random
from typing import Dict, Optional, Iterator, Tuple

from senderstats.common.utils import average
from senderstats.data.message_data import MessageData
from senderstats.interfaces.processor import Processor
from senderstats.interfaces.reportable import Reportable


class RPathProcessor(Processor[MessageData], Reportable):
    sheet_name = "Return Path"
    headers = ['RPath', 'Messages', 'Size', 'Messages Per Day', 'Total Bytes']
    __rpath_data: Dict[str, Dict]
    __sample_subject: bool
    __expand_recipients: bool

    def __init__(self, sample_subject=False, expand_recipients=False):
        super().__init__()
        self.__rpath_data = dict()
        self.__sample_subject = sample_subject
        self.__expand_recipients = expand_recipients

    def execute(self, data: MessageData) -> None:
        self.__rpath_data.setdefault(data.rpath, {})

        rpath_data = self.__rpath_data[data.rpath]

        if self.__expand_recipients:
            rpath_data.setdefault("message_size", []).extend([data.msgsz] * len(data.rcpts))
        else:
            rpath_data.setdefault("message_size", []).append(data.msgsz)

        if self.__sample_subject:
            rpath_data.setdefault("subjects", [])
            # Avoid storing empty subject lines
            if data.subject:
                # Calculate probability based on the number of processed records
                probability = 1 / len(rpath_data['message_size'])

                # Ensure at least one subject is added if subjects array is empty
                if not rpath_data['subjects'] or random() < probability:
                    rpath_data['subjects'].append(data.subject)

    def report(self, context: Optional = None) -> Iterator[Tuple[str, Iterator[list]]]:
        # Yield the report name and the data generator together
        def get_report_name():
            return "Return Path"

        def get_report_data():
            headers = ['RPath', 'Messages', 'Size', 'Messages Per Day', 'Total Bytes']
            if self.__sample_subject:
                headers.append('Subjects')
            yield headers
            for k, v in self.__rpath_data.items():
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
