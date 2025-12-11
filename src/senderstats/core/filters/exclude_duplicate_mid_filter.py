import pandas

from senderstats.interfaces.filter import Filter


class ExcludeDuplicateMessageIdFilter(Filter[pandas.DataFrame]):
    def __init__(self):
        super().__init__()
        self.__seen_msgids = set()
        self.__excluded_count = 0

    def filter(self, data: pandas.DataFrame) -> bool:
        if data.empty:
            return False

        before_count = len(data)
        already_seen_mask = data['msgid'].isin(self.__seen_msgids)
        batch_dup_mask = data['msgid'].duplicated(keep='first')
        to_remove = already_seen_mask | batch_dup_mask
        self.__seen_msgids.update(data['msgid'].dropna())
        data.drop(index=data.index[to_remove], inplace=True)
        removed = int(to_remove.sum())
        self.__excluded_count += removed
        return True

    def get_excluded_count(self) -> int:
        return self.__excluded_count
