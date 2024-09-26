from abc import abstractmethod
from typing import Optional, final, Generic

from data.common.Handler import AbstractHandler, TData


class Transform(AbstractHandler[TData], Generic[TData]):
    @final
    def handle(self, data: TData) -> Optional[TData]:
        transformed_data = self.transform(data)
        return super().handle(transformed_data)

    @abstractmethod
    def transform(self, data: TData) -> TData:
        pass
