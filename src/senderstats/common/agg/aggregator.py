from dataclasses import dataclass, field
from typing import Callable, Dict, Generic, Hashable, Iterator, Tuple, TypeVar

K = TypeVar("K", bound=Hashable)
AggT = TypeVar("AggT")


@dataclass
class KeyedAggregator(Generic[K, AggT]):
    agg_factory: Callable[[], AggT]
    data: Dict[K, AggT] = field(default_factory=dict)

    def get(self, key: K) -> AggT:
        agg = self.data.get(key)
        if agg is None:
            agg = self.agg_factory()
            self.data[key] = agg
        return agg

    def items(self) -> Iterator[Tuple[K, AggT]]:
        return self.data.items()
