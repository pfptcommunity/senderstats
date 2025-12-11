from typing import List, Set

import pandas

from senderstats.interfaces.filter import Filter


class ExcludeIPFilter(Filter[pandas.DataFrame]):
    __excluded_ips: Set[str]

    def __init__(self, excluded_ips: List[str]):
        super().__init__()
        self.__excluded_ips = set(excluded_ips)
        self.__excluded_count = 0

    def filter(self, data: pandas.DataFrame) -> bool:
        if data.empty:
            return False

        before_count = len(data)
        data = data[~data['ip'].isin(self.__excluded_ips)]
        removed = before_count - len(data)
        self.__excluded_count += removed
        return True

    def get_excluded_count(self) -> int:
        return self.__excluded_count
