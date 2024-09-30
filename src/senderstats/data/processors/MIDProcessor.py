from random import random
from typing import Dict

from senderstats.data.MessageData import MessageData
from senderstats.data.common.Processor import Processor


# MIDProcessor.py
class MIDProcessor(Processor[MessageData]):
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

    def is_sample_subject(self) -> bool:
        return self.__sample_subject

    def get_data(self) -> Dict:
        return self.__msgid_data
