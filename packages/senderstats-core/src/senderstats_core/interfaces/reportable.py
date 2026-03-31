from abc import abstractmethod, ABC
from typing import Optional, Iterator, Tuple


class Reportable(ABC):
    @abstractmethod
    def report(self, context: Optional = None) -> Iterator[Tuple[str, Iterator[list]]]:
        pass

    @property
    @abstractmethod
    def create_data_table(self) -> bool:
        pass
