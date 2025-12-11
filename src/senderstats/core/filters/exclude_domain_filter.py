from typing import List

import pandas

from senderstats.common.utils import compile_domains_pattern
from senderstats.interfaces.filter import Filter


# ExcludeDomainFilter inherits from filter
class ExcludeDomainFilter(Filter[pandas.DataFrame]):
    def __init__(self, excluded_domains: List[str]):
        super().__init__()
        self.__excluded_domains = compile_domains_pattern(excluded_domains)
        self.__excluded_count = 0

    def filter(self, data: pandas.DataFrame) -> bool:
        if data.empty:
            return False

        before_count = len(data)
        data = data[~data['mfrom'].str.contains(self.__excluded_domains, regex=True, na=False)]
        removed = before_count - len(data)
        self.__excluded_count += removed
        return True

    def get_excluded_count(self) -> int:
        return self.__excluded_count
