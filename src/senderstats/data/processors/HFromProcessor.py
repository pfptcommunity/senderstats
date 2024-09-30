from random import random
from typing import TypeVar, Generic, Dict

from senderstats.data.MessageData import MessageData
from senderstats.data.common.Processor import Processor

TMessageData = TypeVar('TMessageData', bound=MessageData)


# HFromProcessor.py
class HFromProcessor(Processor[MessageData], Generic[TMessageData]):
    sheet_name = "Header From"
    headers = ['HFrom', 'Messages', 'Size', 'Messages Per Day', 'Total Bytes']
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

    def is_sample_subject(self) -> bool:
        return self.__sample_subject

    def get_data(self) -> Dict:
        return self.__hfrom_data
