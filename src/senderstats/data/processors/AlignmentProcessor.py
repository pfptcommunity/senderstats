from random import random
from typing import TypeVar, Generic, Dict

from data.MessageData import MessageData
from data.common.Processor import Processor

TMessageData = TypeVar('TMessageData', bound=MessageData)


class AlignmentProcessor(Processor[MessageData], Generic[TMessageData]):
    __alignment_data: Dict[tuple, Dict]
    __sample_subject: bool

    def __init__(self, sample_subject=False):
        super().__init__()
        self.__alignment_data = dict()
        self.__sample_subject = sample_subject

    def execute(self, data: TMessageData) -> TMessageData:
        # Fat index for binding commonality
        sender_header_index = (data.mfrom, data.hfrom)

        self.__alignment_data.setdefault(sender_header_index, {})

        alignment_data = self.__alignment_data[sender_header_index]

        alignment_data.setdefault("message_size", []).append(data.message_size)

        if self.__sample_subject:
            alignment_data.setdefault("subjects", [])
            # Avoid storing empty subject lines
            if data.subject:
                # Calculate probability based on the number of processed records
                probability = 1 / len(alignment_data['message_size'])

                # Ensure at least one subject is added if subjects array is empty
                if not alignment_data['subjects'] or random() < probability:
                    alignment_data['subjects'].append(data.subject)

        return data
