from senderstats.data.MessageData import MessageData
from senderstats.data.common.Filter import Filter


class ExcludeEmptySenderFilter(Filter[MessageData]):
    def __init__(self):
        super().__init__()

    def filter(self, data: MessageData) -> bool:
        if not data.mfrom:
            return False
        return True
