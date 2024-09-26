from random import random
from typing import TypeVar, Generic, Dict

from data.MessageData import MessageData
from data.common.Processor import Processor

TMessageData = TypeVar('TMessageData', bound=MessageData)


class MIDProcessor(Processor[MessageData], Generic[TMessageData]):
    sheet_name = "MFrom + Message ID"
    headers = ['MFrom', 'Message ID Host', 'Message ID Domain', 'Messages', 'Size', 'Messages Per Day', 'Total Bytes']
    __msgid_data: Dict[tuple, Dict]
    __sample_subject: bool

    def __init__(self, sample_subject=False):
        super().__init__()
        self.__msgid_data = dict()
        self.__sample_subject = sample_subject

    def execute(self, data: TMessageData) -> TMessageData:
        mid_host_domain_index = (data.mfrom, data.msgid_host, data.msgid_domain)
        self.__msgid_data.setdefault(mid_host_domain_index, {})

        msgid_data = self.__msgid_data[mid_host_domain_index]

        msgid_data.setdefault("message_size", []).append(data.message_size)

        if self.__sample_subject:
            msgid_data.setdefault("subjects", [])
            # Avoid storing empty subject lines
            if data.subject:
                # Calculate probability based on the number of processed records
                probability = 1 / len(msgid_data['message_size'])

                # Ensure at least one subject is added if subjects array is empty
                if not msgid_data['subjects'] or random() < probability:
                    msgid_data['subjects'].append(data.subject)

        return data

    def is_sample_subject(self) -> bool:
        return self.__sample_subject

    def get_data(self) -> Dict:
        return self.__msgid_data
