from typing import List

from senderstats.data.MessageData import MessageData
from senderstats.data.common.Transform import Transform
from senderstats.data.mappers.Mapper import Mapper


# MessageDataTransform inherits from Transform with List[str] as input and MessageData as output
class MessageDataTransform(Transform[List[str], MessageData]):
    _field_mapper: Mapper
    __data: MessageData

    def __init__(self, field_mapper: Mapper):
        super().__init__()
        self._field_mapper = field_mapper
        self.__data = MessageData()

    def transform(self, data: List[str]) -> MessageData:
        for field in self._field_mapper._index_map.keys():
            value = self._field_mapper.get_field(data, field)
            if field == 'msgsz':
                value = int(value) if value.isdigit() else 0
            elif field == 'rcpts':
                value = value.casefold().strip().split(',')
            else:
                value = value.casefold().strip()
            setattr(self.__data, field, value)

        # Dump fields mapped
        # print(self.__data)
        return self.__data
