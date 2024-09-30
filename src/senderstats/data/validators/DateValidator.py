from datetime import datetime

from senderstats.data.MessageData import MessageData
from senderstats.data.common.Validator import Validator


class DateValidator(Validator[MessageData]):
    def __init__(self, date_format: str):
        super().__init__()
        self.__date_format = date_format

    def validate(self, data: MessageData) -> bool:
        try:
            datetime.strptime(data.date, self.__date_format)
        except ValueError:
            return False
        return True
