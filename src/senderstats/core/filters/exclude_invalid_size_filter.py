import pandas

from senderstats.interfaces.filter import Filter


class ExcludeInvalidSizeFilter(Filter[pandas.DataFrame]):
    def __init__(self):
        super().__init__()
        self.__excluded_count = 0

    def filter(self, data: pandas.DataFrame) -> bool:
        if data.empty:
            return False

        before_count = len(data)
        data = data[data['msgsz'] < 0]
        removed = before_count - len(data)
        self.__excluded_count += removed
        return True

    def get_excluded_count(self) -> int:
        return self.__excluded_count
