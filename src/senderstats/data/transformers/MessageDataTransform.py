from typing import List

from data.Mapper import Mapper
from data.MessageData import MessageData
from data.common.Transform import Transform


# MessageDataTransform inherits from Transform with List[str] as input and MessageData as output
class MessageDataTransform(Transform[List[str], MessageData]):
    _field_mapper: Mapper
    __data: MessageData

    def __init__(self, field_mapper: Mapper):
        super().__init__()
        self._field_mapper = field_mapper
        self.__data = MessageData()

    def transform(self, data: List[str]) -> MessageData:  # Input is List[str], output is MessageData
        self.__data.message_size = int(self._field_mapper.get_field(data, 'msgsz')) if self._field_mapper.get_field(
            data, 'msgsz').isdigit() else 0
        self.__data.mfrom = self._field_mapper.get_field(data, 'mfrom').casefold().strip()
        self.__data.hfrom = self._field_mapper.get_field(data, 'hfrom').casefold().strip()
        self.__data.rpath = self._field_mapper.get_field(data, 'rpath').casefold().strip()
        self.__data.rcpts = self._field_mapper.get_field(data, 'rcpts').casefold().strip().split(',')
        self.__data.msgid = self._field_mapper.get_field(data, 'msgid').casefold().strip('<>[] ')
        self.__data.msgid_domain = ''
        self.__data.msgid_host = ''
        self.__data.subject = self._field_mapper.get_field(data, 'subject').strip()
        self.__data.date = self._field_mapper.get_field(data, 'date').strip()
        return self.__data
