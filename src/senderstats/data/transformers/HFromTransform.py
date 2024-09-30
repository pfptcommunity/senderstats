from senderstats.common.utils import parse_email_details
from senderstats.data.MessageData import MessageData
from senderstats.data.common.Transform import Transform


class HFromTransform(Transform[MessageData, MessageData]):
    def __init__(self, no_display: bool = False, empty_from: bool = False):
        super().__init__()
        self.__no_display = no_display
        self.__empty_from = empty_from

    def transform(self, data: MessageData) -> MessageData:
        hfrom = data.hfrom

        if self.__no_display:
            hfrom_parts = parse_email_details(hfrom)
            hfrom = hfrom_parts['email_address']

        # If header from is empty, we will use env_sender
        if self.__empty_from and not data.hfrom:
            hfrom = data.mfrom

        data.hfrom = hfrom
        return data
