from abc import abstractmethod
from typing import Optional, final, Generic

from data.common.Handler import AbstractHandler, TData


class Filter(AbstractHandler[TData], Generic[TData]):
    @final
    def handle(self, data: TData) -> Optional[TData]:
        if self.filter(data):
            return super().handle(data)
        return None

    @abstractmethod
    def filter(self, data: TData) -> bool:
        pass
