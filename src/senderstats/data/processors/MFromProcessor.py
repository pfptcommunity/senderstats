# MFromProcessor.py
from random import random
from typing import Dict

from senderstats.data.MessageData import MessageData
from senderstats.data.common.Processor import Processor


class MFromProcessor(Processor[MessageData]):
    sheet_name = "Envelope Senders"
    headers = ['MFrom', 'Messages', 'Size', 'Messages Per Day', 'Total Bytes']
    __mfrom_data: Dict[str, Dict]
    __sample_subject: bool
    __expand_recipients: bool

    def __init__(self, sample_subject=False, expand_recipients=False):
        super().__init__()
        self.__mfrom_data = dict()
        self.__sample_subject = sample_subject
        self.__expand_recipients = expand_recipients

    def execute(self, data: MessageData) -> None:
        self.__mfrom_data.setdefault(data.mfrom, {})

        mfrom_data = self.__mfrom_data[data.mfrom]

        if self.__expand_recipients:
            mfrom_data.setdefault("message_size", []).extend([data.msgsz] * len(data.rcpts))
        else:
            mfrom_data.setdefault("message_size", []).append(data.msgsz)

        if self.__sample_subject:
            mfrom_data.setdefault("subjects", [])
            if data.subject:
                probability = 1 / len(mfrom_data['message_size'])
                if not mfrom_data['subjects'] or random() < probability:
                    mfrom_data['subjects'].append(data.subject)

    def is_sample_subject(self) -> bool:
        return self.__sample_subject

    def get_data(self) -> Dict:
        return self.__mfrom_data
