from senderstats.data.message_data import MessageData
from senderstats.interfaces.filter import Filter


class ExcludeInvalidSizeFilter(Filter[MessageData]):
    def __init__(self):
        super().__init__()
        self.__excluded_count = 0

    def filter(self, data: MessageData) -> bool:
        if data.msgsz == -1:
            self.__excluded_count += 1
            return False
        return True

    def get_excluded_count(self) -> int:
        return self.__excluded_count
