import datetime
from collections import defaultdict
from typing import DefaultDict

from senderstats.data.MessageData import MessageData
from senderstats.data.common.Processor import Processor


class DateProcessor(Processor[MessageData]):
    __date_counter: DefaultDict[str, int]
    __hourly_counter: DefaultDict[str, int]
    __date_format: str
    __expand_recipients: bool

    def __init__(self, date_format: str, expand_recipients: bool = False):
        super().__init__()
        self.__date_counter = defaultdict(int)
        self.__hourly_counter = defaultdict(int)
        self.__date_format = date_format
        self.__expand_recipients = expand_recipients

    def execute(self, data: MessageData) -> None:
        date = datetime.datetime.strptime(data.date, self.__date_format)
        str_date = date.strftime('%Y-%m-%d')
        str_hourly_date = date.strftime('%Y-%m-%d %H:00:00')
        if self.__expand_recipients:
            self.__date_counter[str_date] += len(data.rcpts)
            self.__hourly_counter[str_hourly_date] += len(data.rcpts)
        else:
            self.__date_counter[str_date] += 1
            self.__hourly_counter[str_hourly_date] += 1

    def get_date_counter(self) -> DefaultDict[str, int]:
        return self.__date_counter

    def get_hourly_counter(self) -> DefaultDict[str, int]:
        return self.__hourly_counter
