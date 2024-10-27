from abc import ABC, abstractmethod
from typing import Dict, Any


class FieldMapper(ABC):
    @abstractmethod
    def extract_value(self, data: Any, field_name: str) -> Any:
        pass

    @abstractmethod
    def map_fields(self, data: Any) -> Dict[str, Any]:
        pass
