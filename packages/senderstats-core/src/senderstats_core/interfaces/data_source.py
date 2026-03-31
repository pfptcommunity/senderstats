from abc import ABC, abstractmethod


class DataSource(ABC):
    @abstractmethod
    def read_data(self):
        pass
