from random import random
from typing import Dict, Optional, Iterator, Tuple

from senderstats.common.utils import average
from senderstats.data.message_data import MessageData
from senderstats.interfaces.processor import Processor
from senderstats.interfaces.reportable import Reportable


# MIDProcessor.py
class MIDProcessor(Processor[MessageData], Reportable):
    sheet_name = "MFrom + Message ID"
    headers = ['MFrom', 'Message ID Host', 'Message ID Domain', 'Messages', 'Size', 'Messages Per Day', 'Total Bytes']
    __msgid_data: Dict[tuple, Dict]
    __sample_subject: bool
    __expand_recipients: bool

    def __init__(self, sample_subject=False, expand_recipients=False):
        super().__init__()
        self.__msgid_data = dict()
        self.__sample_subject = sample_subject
        self.__expand_recipients = expand_recipients

    def execute(self, data: MessageData) -> None:
        mid_host_domain_index = (data.mfrom, data.msgid_host, data.msgid_domain)
        self.__msgid_data.setdefault(mid_host_domain_index, {})

        msgid_data = self.__msgid_data[mid_host_domain_index]

        if self.__expand_recipients:
            msgid_data.setdefault("message_size", []).extend([data.msgsz] * len(data.rcpts))
        else:
            msgid_data.setdefault("message_size", []).append(data.msgsz)

        if self.__sample_subject:
            msgid_data.setdefault("subjects", [])
            if data.subject:
                probability = 1 / len(msgid_data['message_size'])
                if not msgid_data['subjects'] or random() < probability:
                    msgid_data['subjects'].append(data.subject)

    def report(self, context: Optional = None) -> Iterator[Tuple[str, Iterator[list]]]:
        # Yield the report name and the data generator together
        def get_report_name():
            return "MFrom + Message ID"

        def get_report_data():
            headers = ['MFrom', 'Message ID Host', 'Message ID Domain', 'Messages', 'Size', 'Messages Per Day',
                       'Total Bytes']
            if self.__sample_subject:
                headers.append('Subjects')
            yield headers
            for k, v in self.__msgid_data.items():
                messages_per_sender = len(v['message_size'])
                total_bytes = sum(v['message_size'])
                average_message_size = average(v['message_size'])
                messages_per_sender_per_day = messages_per_sender / context
                row = [k[0], k[1], k[2], messages_per_sender, average_message_size, messages_per_sender_per_day,
                       total_bytes]
                if self.__sample_subject:
                    row.append('\n'.join(v['subjects']))
                yield row

        yield get_report_name(), get_report_data()

    @property
    def create_data_table(self) -> bool:
        return True
