from abc import abstractmethod
from typing import Optional, final, Generic

from senderstats.data.common.Handler import AbstractHandler, TInput


# Validator class validates the data and passes it down the chain if valid
class Validator(AbstractHandler[TInput, TInput], Generic[TInput]):
    @final
    def handle(self, data: TInput) -> Optional[TInput]:
        if self.validate(data):
            return super().handle(data)  # Pass the data to the next handler
        return None  # Stop the chain if validation fails

    @abstractmethod
    def validate(self, data: TInput) -> bool:
        pass
