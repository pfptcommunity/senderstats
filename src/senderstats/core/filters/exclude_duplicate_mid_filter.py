from typing import Optional

import pandas as pd

from senderstats.interfaces.filter import Filter


class ExcludeDuplicateMessageIdFilter(Filter[pd.DataFrame]):
    def __init__(self):
        super().__init__()
        self.__seen_msgids = set()
        self.__excluded_count = 0

    def filter(self, data: pd.DataFrame) -> Optional[pd.DataFrame]:
        if data.empty:
            return None

        before_count = len(data)
        already_seen_mask = data['msgid'].isin(self.__seen_msgids)
        batch_dup_mask = data['msgid'].duplicated(keep='first')
        to_remove = already_seen_mask | batch_dup_mask
        self.__seen_msgids.update(data['msgid'].dropna())
        data.drop(index=data.index[to_remove], inplace=True)
        removed = int(to_remove.sum())
        self.__excluded_count += removed
        return data

    def get_excluded_count(self) -> int:
        return self.__excluded_count
