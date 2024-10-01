from typing import List, TypeVar

import regex as re

from senderstats.common.utils import compile_domains_pattern
from senderstats.data.MessageData import MessageData
from senderstats.data.common.Filter import Filter

TMessageData = TypeVar('TMessageData', bound=MessageData)


class RestrictDomainFilter(Filter[MessageData]):
    __restricted_domains: re.Pattern

    def __init__(self, restricted_domains: List[str]):
        super().__init__()
        self.__restricted_domains = compile_domains_pattern(restricted_domains)

    def filter(self, data: MessageData) -> bool:
        if not self.__restricted_domains.search(data.mfrom):
            return False
        return True
