from typing import TypeVar, Generic

from common.utils import parse_email_details, convert_srs, remove_prvs
from data.MessageData import MessageData
from data.common.Transform import Transform

TMessageData = TypeVar('TMessageData', bound=MessageData)


class MFromTransform(Transform[MessageData], Generic[TMessageData]):
    def __init__(self, decode_srs: bool = False, remove_prvs: bool = False):
        super().__init__()
        self.__decode_srs = decode_srs
        self.__remove_prvs = remove_prvs

    def transform(self, data: TMessageData) -> TMessageData:
        # If sender is not empty, we will extract parts of the email
        mfrom_parts = parse_email_details(data.mfrom)
        mfrom = mfrom_parts['email_address']

        if self.__decode_srs:
            mfrom = convert_srs(mfrom)

        if self.__remove_prvs:
            mfrom = remove_prvs(mfrom)

        data.mfrom = mfrom
        return data
