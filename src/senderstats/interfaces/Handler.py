from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, Generic, TypeVar, Any

# Define type variables for input and output types
TInput = TypeVar('TInput', bound=Any)
TOutput = TypeVar('TOutput', bound=Any)


class Handler(ABC, Generic[TInput, TOutput]):
    @abstractmethod
    def set_next(self, handler: Handler[TOutput, Any]) -> Handler[TInput, TOutput]:
        pass

    @abstractmethod
    def get_next(self) -> Optional[Handler[TOutput, Any]]:
        pass

    @abstractmethod
    def handle(self, data: TInput) -> Optional[TOutput]:
        pass


class AbstractHandler(Handler[TInput, TOutput], Generic[TInput, TOutput]):
    _next_handler: Optional[Handler[TOutput, Any]] = None

    def set_next(self, handler: Handler[TOutput, Any]) -> Handler[TInput, TOutput]:
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

    def get_next(self) -> Optional[Handler[TOutput, Any]]:
        return self._next_handler

    def handle(self, data: TInput) -> Optional[TOutput]:
        if self._next_handler:
            return self._next_handler.handle(data)
