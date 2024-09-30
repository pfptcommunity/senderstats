from abc import abstractmethod
from typing import Optional, final, Generic

from senderstats.data.common.Handler import AbstractHandler, TInput, TOutput


# Transform now extends AbstractHandler with separate input and output types
class Transform(AbstractHandler[TInput, TOutput], Generic[TInput, TOutput]):
    @final
    def handle(self, data: TInput) -> Optional[TOutput]:
        transformed_data = self.transform(data)
        return super().handle(transformed_data)

    @abstractmethod
    def transform(self, data: TInput) -> TOutput:
        pass
