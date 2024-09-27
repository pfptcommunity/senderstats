from random import random
from typing import Dict

from data.MessageData import MessageData
from data.common.Processor import Processor


class RPathProcessor(Processor[MessageData]):
    sheet_name = "Return Path"
    headers = ['RPath', 'Messages', 'Size', 'Messages Per Day', 'Total Bytes']
    __rpath_data: Dict[str, Dict]
    __sample_subject: bool

    def __init__(self, sample_subject=False):
        super().__init__()
        self.__rpath_data = dict()
        self.__sample_subject = sample_subject

    def execute(self, data: MessageData) -> None:
        self.__rpath_data.setdefault(data.rpath, {})

        rpath_data = self.__rpath_data[data.rpath]

        rpath_data.setdefault("message_size", []).append(data.message_size)

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
