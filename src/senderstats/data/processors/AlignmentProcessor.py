from random import random
from typing import Dict

from senderstats.data.MessageData import MessageData
from senderstats.data.common.Processor import Processor


class AlignmentProcessor(Processor[MessageData]):
    sheet_name = "MFrom + HFrom (Alignment)"
    headers = ['MFrom', 'HFrom', 'Messages', 'Size', 'Messages Per Day', 'Total Bytes']
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

    def is_sample_subject(self) -> bool:
        return self.__sample_subject

    def get_data(self) -> Dict:
        return self.__alignment_data
