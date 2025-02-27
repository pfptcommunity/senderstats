from senderstats.common.utils import parse_email_details, convert_srs, remove_prvs, normalize_bounces, \
    normalize_entropy
from senderstats.data.message_data import MessageData
from senderstats.interfaces.transform import Transform


class MFromTransform(Transform[MessageData, MessageData]):
    def __init__(self, decode_srs: bool = False, remove_prvs: bool = False, normalize_bounces: bool = False,
                 normalize_entropy: bool = False):
        super().__init__()
        self.__decode_srs = decode_srs
        self.__remove_prvs = remove_prvs
        self.__normalize_bounces = normalize_bounces
        self.__normalize_entropy = normalize_entropy

    def transform(self, data: MessageData) -> MessageData:
        # If sender is not empty, we will extract parts of the email
        mfrom_parts = parse_email_details(data.mfrom)
        mfrom = mfrom_parts['email_address']

        if self.__decode_srs:
            mfrom = convert_srs(mfrom)

        if self.__remove_prvs:
            mfrom = remove_prvs(mfrom)

        if self.__normalize_bounces:
            mfrom = normalize_bounces(mfrom)

        if self.__normalize_entropy:
            mfrom = normalize_entropy(mfrom)

        data.mfrom = mfrom
        return data
