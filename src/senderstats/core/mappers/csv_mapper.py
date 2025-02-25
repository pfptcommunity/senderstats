from typing import List, Dict

from senderstats.data.message_data import MessageData
from senderstats.interfaces.field_mapper import FieldMapper


class CSVMapper(FieldMapper):
    def __init__(self, default_mappings: Dict[str, str]):
        self.__mappings = default_mappings
        self._index_map = {}

    def reindex(self, headers: List[str]):
        error = False
        self._index_map = {}
        for key, value in self.__mappings.items():
            if value in headers:
                self._index_map[key] = headers.index(value)
            else:
                print(f"Required header '{value}' not found in provided headers.")
                error = True
        if error:
            print("Please make sure the required headers exist or are mapped, and try again.")
            exit(1)

    def extract_value(self, row: List[str], field_name: str) -> str:
        if field_name in self._index_map:
            index = self._index_map[field_name]
            return row[index]
        else:
            raise ValueError(f"Field '{field_name}' not found or not mapped correctly.")

    def map_fields(self, row: List[str]) -> MessageData:
        message_data = MessageData()
        for field in self._index_map.keys():
            value = self.extract_value(row, field)
            if field == 'msgsz':
                value = int(value) if value.isdigit() else -1
            elif field == 'rcpts':
                value = value.casefold().strip().split(',')
            elif field == 'subject':
                value = value.strip()
            else:
                value = value.casefold().strip()
            setattr(message_data, field, value)
        return message_data

    def add_mapping(self, field_name: str, csv_field_name: str):
        self.__mappings[field_name] = csv_field_name

    def delete_mapping(self, field_name: str) -> bool:
        if field_name in self.__mappings:
            del self.__mappings[field_name]
            if field_name in self._index_map:
                del self._index_map[field_name]
            return True
        return False

    def set_field(self, csv_row: List[str], field_name: str, field_value: str):
        if field_name in self._index_map:
            index = self._index_map[field_name]
            csv_row[index] = field_value
        else:
            raise ValueError(f"Field '{field_name}' not found or not mapped correctly.")

    def __repr__(self):
        return f"CSVMapper(mappings={self.__mappings}, index_map={self._index_map})"
