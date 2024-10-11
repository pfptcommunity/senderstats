from abc import abstractmethod, ABC


class Reportable(ABC):
    @abstractmethod
    def report(self) -> dict:
        pass
