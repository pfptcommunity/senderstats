from typing import TypeVar, Generic

from data.MessageData import MessageData
from data.common.Filter import Filter

TMessageData = TypeVar('TMessageData', bound=MessageData)


class ExcludeEmptySenderFilter(Filter[MessageData], Generic[TMessageData]):
    def __init__(self):
        super().__init__()

    def filter(self, data: TMessageData) -> bool:
        if not data.mfrom:
            return False
        return True
