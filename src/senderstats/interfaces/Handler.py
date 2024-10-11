from abc import ABC, abstractmethod
from typing import Optional, Generic, TypeVar, Any

# Define type variables for input and output types
TInput = TypeVar('TInput', bound=Any)
TOutput = TypeVar('TOutput', bound=Any)

# Define THandler, bound to a Handler that outputs TOutput and accepts any for further chaining
THandler = TypeVar('THandler', bound='Handler')


class Handler(ABC, Generic[TInput, TOutput]):
    @abstractmethod
    def set_next(self, handler: THandler) -> THandler:
        pass

    @abstractmethod
    def get_next(self) -> Optional[THandler]:
        pass

    @abstractmethod
    def handle(self, data: TInput) -> Optional[TOutput]:
        pass


class AbstractHandler(Handler[TInput, TOutput], Generic[TInput, TOutput]):
    _next_handler: Optional[THandler] = None

    def set_next(self, handler: THandler) -> THandler:
        if self._next_handler is None:
            self._next_handler = handler
        else:
            last_handler = self._next_handler
            while last_handler.get_next() is not None:
                if last_handler == handler:
                    raise ValueError("Circular reference detected in handler chain.")
                last_handler = last_handler.get_next()
            last_handler.set_next(handler)
        return self

    def get_next(self) -> Optional[THandler]:
        return self._next_handler

    def handle(self, data: TInput) -> Optional[TOutput]:
        if self._next_handler:
            return self._next_handler.handle(data)
