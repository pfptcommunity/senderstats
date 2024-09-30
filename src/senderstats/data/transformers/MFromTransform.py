from senderstats.common.utils import parse_email_details, convert_srs, remove_prvs
from senderstats.data.MessageData import MessageData
from senderstats.data.common.Transform import Transform


class MFromTransform(Transform[MessageData, MessageData]):
    def __init__(self, decode_srs: bool = False, remove_prvs: bool = False):
        super().__init__()
        self.__decode_srs = decode_srs
        self.__remove_prvs = remove_prvs

    def transform(self, data: MessageData) -> MessageData:
        # If sender is not empty, we will extract parts of the email
        mfrom_parts = parse_email_details(data.mfrom)
        mfrom = mfrom_parts['email_address']

        if self.__decode_srs:
            mfrom = convert_srs(mfrom)

        if self.__remove_prvs:
            mfrom = remove_prvs(mfrom)

        data.mfrom = mfrom
        return data
