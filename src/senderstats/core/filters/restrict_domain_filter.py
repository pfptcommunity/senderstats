from typing import List, TypeVar

import regex as re

from senderstats.common.utils import compile_domains_pattern
from senderstats.data.message_data import MessageData
from senderstats.interfaces.filter import Filter

TMessageData = TypeVar('TMessageData', bound=MessageData)


class RestrictDomainFilter(Filter[MessageData]):
    __restricted_domains: re.Pattern

    def __init__(self, restricted_domains: List[str]):
        super().__init__()
        self.__restricted_domains = compile_domains_pattern(restricted_domains)
        self.__excluded_count = 0

    def filter(self, data: MessageData) -> bool:
        if not self.__restricted_domains.search(data.mfrom):
            self.__excluded_count += 1
            return False
        return True

    def get_excluded_count(self) -> int:
        return self.__excluded_count
