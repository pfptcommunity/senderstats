from typing import List, Dict

DEFAULT_MFROM_FIELD = 'Sender'
DEFAULT_HFROM_FIELD = 'Header_From'
DEFAULT_RPATH_FIELD = 'Header_Return-Path'
DEFAULT_MSGID_FIELD = 'Message_ID'
DEFAULT_MSGSZ_FIELD = 'Message_Size'
DEFAULT_SUBJECT_FIELD = 'Subject'
DEFAULT_DATE_FIELD = 'Date'
DEFAULT_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%f%z'


class FieldMapper:
    __mappings: Dict[str, str]
    __index_map: Dict[str, int]

    def __init__(self):
        self.__mappings = {
            'mfrom': DEFAULT_MFROM_FIELD,
            'hfrom': DEFAULT_HFROM_FIELD,
            'rpath': DEFAULT_RPATH_FIELD,
            'msgsz': DEFAULT_MSGSZ_FIELD,
            'msgid': DEFAULT_MSGID_FIELD,
            'subject': DEFAULT_SUBJECT_FIELD,
            'date': DEFAULT_DATE_FIELD
        }

    def configure(self, headers: List[str]):
        # Initialize index_map checking if headers are present in the provided headers list
        self.__index_map = {}
        for key, value in self.__mappings.items():
            if value in headers:
                self.__index_map[key] = headers.index(value)
            else:
                # Optionally print a warning or log that the expected header is missing
                print(f"Warning: Expected header '{value}' not found in provided headers.")

    def add_mapping(self, field_name: str, csv_field_name: str, headers: List[str]):
        self.__mappings[field_name] = csv_field_name
        if csv_field_name in headers:
            self.__index_map[field_name] = headers.index(csv_field_name)
        else:
            # Optionally handle or log missing new mapping headers similarly
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

    def __repr__(self):
        return f"CSVFieldMapper(mappings={self.__mappings}, index_map={self.__index_map})"
