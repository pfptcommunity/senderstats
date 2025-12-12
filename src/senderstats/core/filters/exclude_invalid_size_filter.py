from typing import Optional

import pandas as pd

from senderstats.interfaces.filter import Filter


class ExcludeInvalidSizeFilter(Filter[pd.DataFrame]):
    def __init__(self):
        super().__init__()
        self.__excluded_count = 0

    def filter(self, data: pd.DataFrame) -> Optional[pd.DataFrame]:
        if data.empty:
            return None

        before = len(data)
        result = data.dropna(subset=['msgsz'])
        self.__excluded_count += (before - len(result))
        return data

    def get_excluded_count(self) -> int:
        return self.__excluded_count
