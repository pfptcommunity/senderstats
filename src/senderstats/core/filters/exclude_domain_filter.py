from typing import List

from senderstats.common.utils import compile_domains_pattern
from senderstats.data.message_data import MessageData
from senderstats.interfaces.filter import Filter


# ExcludeDomainFilter inherits from filter and works with MessageData
class ExcludeDomainFilter(Filter[MessageData]):
    def __init__(self, excluded_domains: List[str]):
        super().__init__()
        self.__excluded_domains = compile_domains_pattern(excluded_domains)
        self.__excluded_count = 0

    def filter(self, data: MessageData) -> bool:
        if self.__excluded_domains.search(data.mfrom):
            self.__excluded_count += 1
            return False
        return True

    def get_excluded_count(self) -> int:
        return self.__excluded_count
