from random import random
from typing import Dict

from senderstats.data.MessageData import MessageData
from senderstats.data.common.Processor import Processor


class RPathProcessor(Processor[MessageData]):
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

    def is_sample_subject(self) -> bool:
        return self.__sample_subject

    def get_data(self) -> Dict:
        return self.__rpath_data
