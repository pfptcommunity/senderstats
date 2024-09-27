from random import random
from typing import TypeVar, Generic, Dict

from data.MessageData import MessageData
from data.common.Processor import Processor

TMessageData = TypeVar('TMessageData', bound=MessageData)


class HFromProcessor(Processor[MessageData], Generic[TMessageData]):
    sheet_name = "Header From"
    headers = ['HFrom', 'Messages', 'Size', 'Messages Per Day', 'Total Bytes']
    __hfrom_data: Dict[str, Dict]
    __sample_subject: bool

    def __init__(self, sample_subject=False):
        super().__init__()
        self.__hfrom_data = dict()
        self.__sample_subject = sample_subject

    def execute(self, data: TMessageData) -> None:
        self.__hfrom_data.setdefault(data.hfrom, {})

        hfrom_data = self.__hfrom_data[data.hfrom]

        hfrom_data.setdefault("message_size", []).append(data.message_size)

        if self.__sample_subject:
            hfrom_data.setdefault("subjects", [])
            # Avoid storing empty subject lines
            if data.subject:
                # Calculate probability based on the number of processed records
                probability = 1 / len(hfrom_data['message_size'])

                # Ensure at least one subject is added if subjects array is empty
                if not hfrom_data['subjects'] or random() < probability:
                    hfrom_data['subjects'].append(data.subject)

    def is_sample_subject(self) -> bool:
        return self.__sample_subject

    def get_data(self) -> Dict:
        return self.__hfrom_data
