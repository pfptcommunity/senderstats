from datetime import datetime
from ciso8601 import parse_datetime
from senderstats.data.MessageData import MessageData
from senderstats.data.common.Transform import Transform


class DateTransform(Transform[MessageData, MessageData]):
    def __init__(self, date_format: str):
        super().__init__()
        self.__date_format = date_format

    def transform(self, data: MessageData) -> MessageData:
        data.date = parse_datetime(data.date)
        return data
