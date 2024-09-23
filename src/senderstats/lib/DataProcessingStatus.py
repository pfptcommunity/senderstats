from enum import Enum


class DataProcessingStatus(Enum):
    CONTINUE = 1
    SKIP = 2
    STOP = 3