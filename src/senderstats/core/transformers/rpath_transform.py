from senderstats.common.address_parser import parse_email_details
from senderstats.common.address_tools import convert_srs, remove_prvs, normalize_bounces, normalize_entropy
from senderstats.data.message_data import MessageData
from senderstats.interfaces.transform import Transform


class RPathTransform(Transform[MessageData, MessageData]):
    def __init__(self, decode_srs: bool = False, remove_prvs: bool = False):
        super().__init__()
        self.__decode_srs = decode_srs
        self.__remove_prvs = remove_prvs
        self.__normalize_bounces = True
        self.__normalize_entropy = True

    def transform(self, data: MessageData) -> MessageData:
        # If sender is not empty, we will extract parts of the email
        rpath_parts = parse_email_details(data.rpath)
        rpath = rpath_parts['email_address']

        if self.__decode_srs:
            rpath, has_srs = convert_srs(rpath)
            setattr(data, 'rpath_had_srs', has_srs)

        if self.__remove_prvs:
            rpath, had_prvs = remove_prvs(rpath)
            setattr(data, 'rpath_had_prvs', had_prvs)

        if self.__normalize_bounces:
            rpath, has_bounce = normalize_bounces(rpath)
            setattr(data, 'rpath_had_bounces', has_bounce)

        if self.__normalize_entropy:
            rpath, has_entropy = normalize_entropy(rpath)
            setattr(data, 'rpath_had_bounces', has_entropy)

        data.rpath = rpath
        return data
