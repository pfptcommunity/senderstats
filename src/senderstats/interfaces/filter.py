from abc import abstractmethod
from typing import Optional, final, Generic

from senderstats.interfaces.handler import AbstractHandler, TInput

# Filter class filters the data and passes it down the chain if it meets the condition
class Filter(AbstractHandler[TInput, TInput], Generic[TInput]):
    @final
    def handle(self, data: TInput) -> Optional[TInput]:
        """
        Apply the filter and pass the filtered data to the next handler.
        If the filter returns None, stop the chain.
        """
        filtered = self.filter(data)
        if filtered is None:
            return None
        return super().handle(filtered)

    @abstractmethod
    def filter(self, data: TInput) -> Optional[TInput]:
        """
        Apply the filter. Return filtered data, or None to stop the chain.
        """
