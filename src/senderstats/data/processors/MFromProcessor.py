from random import random
from typing import TypeVar, Generic, Dict

from data.MessageData import MessageData
from data.common.Processor import Processor

TMessageData = TypeVar('TMessageData', bound=MessageData)


class MFromProcessor(Processor[MessageData], Generic[TMessageData]):
    __mfrom_data: Dict[str, Dict]
    __sample_subject: bool

    def __init__(self, sample_subject=False):
        super().__init__()
        self.__mfrom_data = dict()
        self.__sample_subject = sample_subject

    def execute(self, data: TMessageData) -> TMessageData:
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

        return data
