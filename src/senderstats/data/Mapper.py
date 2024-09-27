from typing import List, Dict


class Mapper:
    def __init__(self, default_mappings: Dict[str, str]):
        self.__mappings = default_mappings
        self.__index_map = {}

    def configure(self, headers: List[str]):
        self.__index_map = {}
        for key, value in self.__mappings.items():
            if value in headers:
                self.__index_map[key] = headers.index(value)
            else:
                print(f"Warning: Expected header '{value}' not found in provided headers.")

    def add_mapping(self, field_name: str, csv_field_name: str, headers: List[str]):
        self.__mappings[field_name] = csv_field_name
        if csv_field_name in headers:
            self.__index_map[field_name] = headers.index(csv_field_name)
        else:
            print(f"Warning: Added header '{csv_field_name}' not found in provided headers.")

    def delete_mapping(self, field_name: str) -> bool:
        if field_name in self.__mappings:
            del self.__mappings[field_name]
            if field_name in self.__index_map:
                del self.__index_map[field_name]
            return True
        return False

    def get_field(self, csv_row: List[str], field_name: str) -> str:
        if field_name in self.__index_map:
            index = self.__index_map[field_name]
            return csv_row[index]
        else:
            raise ValueError(f"Field '{field_name}' not found or not mapped correctly.")

    def set_field(self, csv_row: List[str], field_name: str, field_value: str):
        if field_name in self.__index_map:
            index = self.__index_map[field_name]
            csv_row[index] = field_value
        else:
            raise ValueError(f"Field '{field_name}' not found or not mapped correctly.")

    def __repr__(self):
        return f"FieldMapper(mappings={self.__mappings}, index_map={self.__index_map})"