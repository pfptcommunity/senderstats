from typing import List, Set, Optional

import pandas as pd

from senderstats.interfaces.filter import Filter


class ExcludeSenderFilter(Filter[pd.DataFrame]):
    __excluded_senders: Set[str]

    def __init__(self, excluded_senders: List[str]):
        super().__init__()
        self.__excluded_senders = set(excluded_senders)
        self.__excluded_count = 0

    def filter(self, data: pd.DataFrame) -> Optional[pd.DataFrame]:
        if data.empty:
            return None

        before_count = len(data)
        data = data[~data['mfrom'].isin(self.__excluded_senders)]
        removed = before_count - len(data)
        self.__excluded_count += removed
        return data

    def get_excluded_count(self) -> int:
        return self.__excluded_count
