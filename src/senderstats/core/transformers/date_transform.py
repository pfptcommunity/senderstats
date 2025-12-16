from datetime import datetime

import ciso8601

from senderstats.data.message_data import MessageData
from senderstats.interfaces.transform import Transform


class DateTransform(Transform[MessageData, MessageData]):
    def __init__(self, date_format: str):
        super().__init__()
        self.__date_format = date_format

    def transform(self, data: MessageData) -> MessageData:
        try:
            # Try ISO date first for fastest parsing
            data.date = ciso8601.parse_datetime(data.date)
        except ValueError as e:
            # If ISO date parsing fails, try custom date parse
            data.date = datetime.strptime(data.date, self.__date_format)

        return data
