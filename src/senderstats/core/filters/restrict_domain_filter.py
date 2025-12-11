from typing import List

import pandas
import regex as re

from senderstats.common.utils import compile_domains_pattern
from senderstats.interfaces.filter import Filter


class RestrictDomainFilter(Filter[pandas.DataFrame]):
    __restricted_domains: re.Pattern

    def __init__(self, restricted_domains: List[str]):
        super().__init__()
        self.__restricted_domains = compile_domains_pattern(restricted_domains)
        self.__excluded_count = 0

    def filter(self, data: pandas.DataFrame) -> bool:
        if data.empty:
            return False

        before_count = len(data)
        data_filtered = data[data['mfrom'].str.contains(
            self.__restricted_domains,
            regex=True,
            na=False
        )]

        removed = before_count - len(data_filtered)
        self.__excluded_count += removed

        data.drop(data.index, inplace=True)
        data_filtered_copy = data_filtered.copy()
        for col in data.columns:
            data[col] = data_filtered_copy[col]

        return True

    def get_excluded_count(self) -> int:
        return self.__excluded_count
