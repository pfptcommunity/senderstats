from typing import List, Set

from senderstats.data.message_data import MessageData
from senderstats.interfaces.filter import Filter


class ExcludeIPFilter(Filter[MessageData]):
    __excluded_ips: Set[str]

    def __init__(self, excluded_ips: List[str]):
        super().__init__()
        self.__excluded_ips = set(excluded_ips)
        self.__excluded_count = 0

    def filter(self, data: MessageData) -> bool:
        if data.ip in self.__excluded_ips:
            self.__excluded_count += 1
            return False
        return True

    def get_excluded_count(self) -> int:
        return self.__excluded_count
