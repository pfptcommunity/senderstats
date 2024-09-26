import datetime
from collections import defaultdict
from typing import TypeVar, Generic, Dict, DefaultDict

from data.MessageData import MessageData
from data.common.Processor import Processor

TMessageData = TypeVar('TMessageData', bound=MessageData)


class DateProcessor(Processor[MessageData], Generic[TMessageData]):
    __date_counter: DefaultDict[str, int]
    __volume_counter: DefaultDict[str, int]
    __date_format: str

    def __init__(self, date_format: str):
        super().__init__()
        self.__date_counter = defaultdict(int)
        self.__hourly_counter = defaultdict(int)
        self.__date_format = date_format

    def execute(self, data: TMessageData) -> TMessageData:
        date = datetime.datetime.strptime(data.date, self.__date_format)
        str_date = date.strftime('%Y-%m-%d')
        str_hourly_date = date.strftime('%Y-%m-%d %Y-%m-%d %H:00:00')
        self.__date_counter[str_date] += 1
        self.__hourly_counter[str_hourly_date] += 1
        return data

    def get_date_counter(self) -> DefaultDict[str, int]:
        return self.__date_counter

    def get_hourly_counter(self) -> DefaultDict[str, int]:
        return self.__hourly_counter