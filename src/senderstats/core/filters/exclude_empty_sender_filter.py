from typing import Optional

import pandas as pd

from senderstats.interfaces.filter import Filter


class ExcludeEmptySenderFilter(Filter[pd.DataFrame]):
    def __init__(self):
        super().__init__()
        self.__excluded_count = 0

    def filter(self, data: pd.DataFrame) -> Optional[pd.DataFrame]:
        if data.empty:
            return False

        before_count = len(data)
        # Remove and count all removed envelope senders
        data.dropna(subset=['mfrom'], inplace=True)
        removed = before_count - len(data)
        self.__excluded_count += removed
        return data

    def get_excluded_count(self) -> int:
        return self.__excluded_count
