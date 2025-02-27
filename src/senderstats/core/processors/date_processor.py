from collections import defaultdict
from typing import DefaultDict, Optional, Iterator, Tuple

from senderstats.data.message_data import MessageData
from senderstats.interfaces.processor import Processor
from senderstats.interfaces.reportable import Reportable


class DateProcessor(Processor[MessageData], Reportable):
    __date_counter: DefaultDict[str, int]
    __hourly_counter: DefaultDict[str, int]
    __expand_recipients: bool

    def __init__(self, expand_recipients: bool = False):
        super().__init__()
        self.__date_counter = defaultdict(int)
        self.__hourly_counter = defaultdict(int)
        self.__expand_recipients = expand_recipients

    def execute(self, data: MessageData) -> None:
        # Strftime takes too long
        str_date = "{:04d}-{:02d}-{:02d}".format(data.date.year, data.date.month, data.date.day)
        str_hourly_date = "{:04d}-{:02d}-{:02d} {:02d}:00:00".format(data.date.year, data.date.month, data.date.day,
                                                                     data.date.hour)
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

    def report(self, context: Optional = None) -> Iterator[Tuple[str, Iterator[list]]]:
        # Yield the report name and the data generator together
        def get_report_name():
            return "Hourly Metrics"

        def get_report_data():
            yield ['Date', 'Messages']
            for k, v in self.__hourly_counter.items():
                yield [k, v]

        yield get_report_name(), get_report_data()

    @property
    def create_data_table(self) -> bool:
        return False
