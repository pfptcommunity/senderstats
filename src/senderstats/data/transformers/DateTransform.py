from datetime import datetime

from senderstats.data.MessageData import MessageData
from senderstats.data.common.Transform import Transform


class DateTransform(Transform[MessageData, MessageData]):
    def __init__(self, date_format: str):
        super().__init__()
        self.__date_format = date_format

    def transform(self, data: MessageData) -> MessageData:
        # This is a massive bottle neck. Need to test different date parsers.
        data.date = datetime.strptime(data.date, self.__date_format)
        return data
