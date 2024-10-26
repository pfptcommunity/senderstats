from typing import Dict, List, Callable, Any

from senderstats.interfaces.FieldMapper import FieldMapper


class JSONMapper(FieldMapper):
    def __init__(self, mappings: Dict[str, List[str]], decoders: Dict[str, Callable] = None):
        self.mappings = mappings
        self.decoders = decoders or {}

    def extract_value(self, data: Dict[str, Any], field_name: str) -> Any:
        path = self.mappings.get(field_name)
        if not path:
            raise ValueError(f"Field '{field_name}' not found in mappings.")

        for key in path:
            data = data.get(key, {})
            if not isinstance(data, dict) and key != path[-1]:
                return None
            if data == {}:  # If any key in the path is missing, return None
                return None
        return data

    def map_fields(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        normalized_data = {}
        for field, path in self.mappings.items():
            value = self.extract_value(json_data, field)
            # Apply decoder if one is specified for the field
            if field in self.decoders and value is not None:
                value = self.decoders[field](value)
            normalized_data[field] = value
        return normalized_data

    def add_mapping(self, field_name: str, json_path: List[str]):
        self.mappings[field_name] = json_path

    def delete_mapping(self, field_name: str) -> bool:
        if field_name in self.mappings:
            del self.mappings[field_name]
            if field_name in self.decoders:
                del self.decoders[field_name]
            return True
        return False

    def __repr__(self):
        return f"JSONMapper(mappings={self.mappings}, decoders={self.decoders})"
