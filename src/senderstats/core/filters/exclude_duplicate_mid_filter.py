from senderstats.data.message_data import MessageData
from senderstats.interfaces.filter import Filter


class ExcludeDuplicateMessageIdFilter(Filter[MessageData]):
    def __init__(self):
        super().__init__()
        self.__seen_msgids = set()
        self.__excluded_count = 0

    def filter(self, data: MessageData) -> bool:
        if data.msgid in self.__seen_msgids:
            self.__excluded_count += 1
            return False
        self.__seen_msgids.add(data.msgid)
        return True

    def get_excluded_count(self) -> int:
        return self.__excluded_count
