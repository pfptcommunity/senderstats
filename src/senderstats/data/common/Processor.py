from abc import abstractmethod
from typing import Optional, final, Generic

from senderstats.data.common.Handler import AbstractHandler, TInput


class Processor(AbstractHandler[TInput, TInput], Generic[TInput]):
    @final
    def handle(self, data: TInput) -> Optional[TInput]:
        self.execute(data)
        return super().handle(data)

    @abstractmethod
    def execute(self, data: TInput) -> None:
        pass
