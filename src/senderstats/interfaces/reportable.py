from abc import abstractmethod, ABC
from typing import Optional


class Reportable(ABC):
    @abstractmethod
    def report(self, context: Optional = None) -> dict:
        pass
