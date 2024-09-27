from typing import List

from common.utils import compile_domains_pattern
from data.MessageData import MessageData
from data.common.Filter import Filter


# ExcludeDomainFilter inherits from Filter and works with MessageData
class ExcludeDomainFilter(Filter[MessageData]):
    def __init__(self, excluded_domains: List[str]):
        super().__init__()
        self.__excluded_domains = compile_domains_pattern(excluded_domains)

    def filter(self, data: MessageData) -> bool:
        return not self.__excluded_domains.search(data.mfrom)
