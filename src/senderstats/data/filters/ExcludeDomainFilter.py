from typing import List, TypeVar, Generic

from common.utils import compile_domains_pattern
from data.MessageData import MessageData
from data.common.Filter import Filter

TMessageData = TypeVar('TMessageData', bound=MessageData)


class ExcludeDomainFilter(Filter[MessageData], Generic[TMessageData]):
    def __init__(self, excluded_domains: List[str]):
        super().__init__()
        self.__excluded_domains = compile_domains_pattern(excluded_domains)

    def filter(self, data: TMessageData) -> bool:
        if self.__excluded_domains.search(data.mfrom):
            return False
        return True
