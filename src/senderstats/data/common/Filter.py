from abc import abstractmethod
from typing import Optional, final, Generic

from senderstats.data.common.Handler import AbstractHandler, TInput


# Filter class filters the data and passes it down the chain if it meets the condition
class Filter(AbstractHandler[TInput, TInput], Generic[TInput]):
    @final
    def handle(self, data: TInput) -> Optional[TInput]:
        """Apply the filter. If data passes the filter, pass it to the next handler."""
        if self.filter(data):
            return super().handle(data)  # Pass the data to the next handler if the filter passes
        return None  # Stop the chain if the data fails the filter

    @abstractmethod
    def filter(self, data: TInput) -> bool:
        """Abstract method to apply the filter."""
        pass
