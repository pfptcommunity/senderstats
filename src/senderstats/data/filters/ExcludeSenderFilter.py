from typing import List, Set, TypeVar, Generic

from data.MessageData import MessageData
from data.common.Filter import Filter

TMessageData = TypeVar('TMessageData', bound=MessageData)


class ExcludeSenderFilter(Filter[MessageData], Generic[TMessageData]):
    __excluded_senders: Set[str]

    def __init__(self, excluded_senders: List[str]):
        super().__init__()
        self.__excluded_senders = set(excluded_senders)

    def filter(self, data: TMessageData) -> bool:
        if data.mfrom in self.__excluded_senders:
            return False  # Exclude record
        return True
