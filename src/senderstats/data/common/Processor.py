from abc import abstractmethod
from typing import Optional, final, Generic

from data.common.Handler import AbstractHandler, TData


class Processor(AbstractHandler[TData], Generic[TData]):
    @final
    def handle(self, data: TData) -> Optional[TData]:
        self.execute(data)
        return super().handle(data)

    @abstractmethod
    def execute(self, data: TData) -> None:
        pass
