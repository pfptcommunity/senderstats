from typing import List, Optional

import pandas as pd

from senderstats.common.utils import compile_domains_pattern
from senderstats.interfaces.filter import Filter


# ExcludeDomainFilter inherits from filter
class ExcludeDomainFilter(Filter[pd.DataFrame]):
    def __init__(self, excluded_domains: List[str]):
        super().__init__()
        self.__excluded_domains = compile_domains_pattern(excluded_domains)
        self.__excluded_count = 0

    def filter(self, data: pd.DataFrame) -> Optional[pd.DataFrame]:
        if data.empty:
            return None

        before_count = len(data)
        data = data[~data['mfrom'].str.contains(self.__excluded_domains, regex=True, na=False)]
        removed = before_count - len(data)
        self.__excluded_count += removed
        return data

    def get_excluded_count(self) -> int:
        return self.__excluded_count
