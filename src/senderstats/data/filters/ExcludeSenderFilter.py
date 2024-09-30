from typing import List, Set

from senderstats.data.MessageData import MessageData
from senderstats.data.common.Filter import Filter


class ExcludeSenderFilter(Filter[MessageData]):
    __excluded_senders: Set[str]

    def __init__(self, excluded_senders: List[str]):
        super().__init__()
        self.__excluded_senders = set(excluded_senders)

    def filter(self, data: MessageData) -> bool:
        if data.mfrom in self.__excluded_senders:
            return False  # Exclude record
        return True
