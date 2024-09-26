from abc import ABC, abstractmethod
from typing import Optional, Generic, TypeVar, Any

TData = TypeVar('TData', bound=Any)
THandler = TypeVar('THandler', bound='Handler')


class Handler(ABC, Generic[TData]):
    @abstractmethod
    def set_next(self, handler: THandler) -> THandler:
        pass

    @abstractmethod
    def get_next(self) -> THandler:
        pass

    @abstractmethod
    def handle(self, data: TData) -> Optional[TData]:
        pass


class AbstractHandler(Handler[TData], Generic[TData]):
    _next_handler: Optional[Handler[TData]] = None

    def set_next(self, handler: Handler[TData]) -> Handler[TData]:
        if self._next_handler is None:
            self._next_handler = handler
        else:
            last_handler = self._next_handler
            while last_handler._next_handler is not None:
                if last_handler == handler:
                    raise ValueError("Circular reference detected in handler chain.")
                last_handler = last_handler._next_handler
            last_handler._next_handler = handler
        return self

    def get_next(self) -> Handler[TData]:
        return self._next_handler

    def handle(self, data: TData) -> Optional[TData]:
        if self._next_handler:
            return self._next_handler.handle(data)
        return None  # If no handler can process the request
