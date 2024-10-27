from typing import List, Set

from senderstats.data.message_data import MessageData
from senderstats.interfaces.filter import Filter


class ExcludeSenderFilter(Filter[MessageData]):
    __excluded_senders: Set[str]

    def __init__(self, excluded_senders: List[str]):
        super().__init__()
        self.__excluded_senders = set(excluded_senders)
        self.__excluded_count = 0

    def filter(self, data: MessageData) -> bool:
        if data.mfrom in self.__excluded_senders:
            self.__excluded_count += 1
            return False
        return True

    def get_excluded_count(self) -> int:
        return self.__excluded_count
