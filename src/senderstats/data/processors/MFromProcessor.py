from random import random
from typing import Dict

from data.MessageData import MessageData
from data.common.Processor import Processor


class MFromProcessor(Processor[MessageData]):
    sheet_name = "Envelope Senders"
    headers = ['MFrom', 'Messages', 'Size', 'Messages Per Day', 'Total Bytes']
    __mfrom_data: Dict[str, Dict]
    __sample_subject: bool

    def __init__(self, sample_subject=False):
        super().__init__()
        self.__mfrom_data = dict()
        self.__sample_subject = sample_subject

    def execute(self, data: MessageData) -> None:
        self.__mfrom_data.setdefault(data.mfrom, {})

        mfrom_data = self.__mfrom_data[data.mfrom]

        mfrom_data.setdefault("message_size", []).append(data.message_size)

        if self.__sample_subject:
            mfrom_data.setdefault("subjects", [])
            # Avoid storing empty subject lines
            if data.subject:
                # Calculate probability based on the number of processed records
                probability = 1 / len(mfrom_data['message_size'])

                # Ensure at least one subject is added if subjects array is empty
                if not mfrom_data['subjects'] or random() < probability:
                    mfrom_data['subjects'].append(data.subject)

    def is_sample_subject(self) -> bool:
        return self.__sample_subject

    def get_data(self) -> Dict:
        return self.__mfrom_data
