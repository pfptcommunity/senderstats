from typing import List, Set, Optional

import pandas as pd

from senderstats.interfaces.filter import Filter


class ExcludeIPFilter(Filter[pd.DataFrame]):
    __excluded_ips: Set[str]

    def __init__(self, excluded_ips: List[str]):
        super().__init__()
        self.__excluded_ips = set(excluded_ips)
        self.__excluded_count = 0

    def filter(self, data: pd.DataFrame) -> Optional[pd.DataFrame]:
        if data.empty:
            return None

        before_count = len(data)
        data = data[~data['ip'].isin(self.__excluded_ips)]
        removed = before_count - len(data)
        self.__excluded_count += removed
        return data

    def get_excluded_count(self) -> int:
        return self.__excluded_count
