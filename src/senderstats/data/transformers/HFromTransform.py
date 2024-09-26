from typing import TypeVar, Generic

from common.utils import parse_email_details
from data.MessageData import MessageData
from data.common.Transform import Transform

TMessageData = TypeVar('TMessageData', bound=MessageData)


class HFromTransform(Transform[MessageData], Generic[TMessageData]):
    def __init__(self, no_display: bool = False, empty_from: bool = False):
        super().__init__()
        self.__no_display = no_display
        self.__empty_from = empty_from

    def transform(self, data: TMessageData) -> TMessageData:
        hfrom = data.hfrom

        if self.__no_display:
            hfrom_parts = parse_email_details(hfrom)
            hfrom = hfrom_parts['email_address']

        # If header from is empty, we will use env_sender
        if self.__empty_from and not data.hfrom:
            hfrom = data.mfrom

        data.hfrom = hfrom
        return data
