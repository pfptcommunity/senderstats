from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

# Define generic type for the data
TData = TypeVar('TData')


# Criteria base class
class Criteria(ABC, Generic[TData]):
    @abstractmethod
    def is_satisfied(self, data: TData) -> bool:
        pass

    # Define AND operation
    def __and__(self, other: Criteria[TData]) -> AndCriteria[TData]:
        return AndCriteria(self, other)

    # Define OR operation
    def __or__(self, other: Criteria[TData]) -> OrCriteria[TData]:
        return OrCriteria(self, other)

    # Define NOT operation (negation)
    def __invert__(self) -> NotCriteria[TData]:
        return NotCriteria(self)


# AND Criteria
class AndCriteria(Criteria[TData]):
    def __init__(self, first: Criteria[TData], second: Criteria[TData]):
        self.first = first
        self.second = second

    def is_satisfied(self, data: TData) -> bool:
        return self.first.is_satisfied(data) and self.second.is_satisfied(data)


# OR Criteria
class OrCriteria(Criteria[TData]):
    def __init__(self, first: Criteria[TData], second: Criteria[TData]):
        self.first = first
        self.second = second

    def is_satisfied(self, data: TData) -> bool:
        return self.first.is_satisfied(data) or self.second.is_satisfied(data)


# NOT Criteria (negation)
class NotCriteria(Criteria[TData]):
    def __init__(self, criteria: Criteria[TData]):
        self.criteria = criteria

    def is_satisfied(self, data: TData) -> bool:
        return not self.criteria.is_satisfied(data)
