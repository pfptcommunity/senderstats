import pandas

from senderstats.interfaces.filter import Filter


class ExcludeInvalidSizeFilter(Filter[pandas.DataFrame]):
    def __init__(self):
        super().__init__()
        self.__excluded_count = 0

    def filter(self, data: pandas.DataFrame) -> bool:
        if data.empty:
            return False

        before = len(data)
        result = data.dropna(subset=['msgsz'])
        self.__excluded_count += (before - len(result))
        return True

    def get_excluded_count(self) -> int:
        return self.__excluded_count
