from typing import List, Set

import pandas

from senderstats.interfaces.filter import Filter


class ExcludeSenderFilter(Filter[pandas.DataFrame]):
    __excluded_senders: Set[str]

    def __init__(self, excluded_senders: List[str]):
        super().__init__()
        self.__excluded_senders = set(excluded_senders)
        self.__excluded_count = 0

    def filter(self, data: pandas.DataFrame) -> bool:
        if data.empty:
            return False

        before_count = len(data)
        data = data[~data['mfrom'].isin(self.__excluded_senders)]
        removed = before_count - len(data)
        self.__excluded_count += removed
        return True

    def get_excluded_count(self) -> int:
        return self.__excluded_count
