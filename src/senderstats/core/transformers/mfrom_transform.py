import pandas

from senderstats.common.utils import convert_srs_col, extract_email_address_col, remove_prvs_col, normalize_bounces_col, \
    normalize_entropy_col
from senderstats.interfaces.transform import Transform


def normalize_entropy_col(col):
    pass


class MFromTransform(Transform[pandas.DataFrame, pandas.DataFrame]):
    def __init__(self, decode_srs: bool = False, remove_prvs: bool = False, normalize_bounces: bool = False,
                 normalize_entropy: bool = False):
        super().__init__()
        self.__decode_srs = decode_srs
        self.__remove_prvs = remove_prvs
        self.__normalize_bounces = normalize_bounces
        self.__normalize_entropy = normalize_entropy

    def transform(self, data: pandas.DataFrame) -> pandas.DataFrame:
        col = data["mfrom"].astype("string")
        col = extract_email_address_col(col)

        if self.__decode_srs:
            col = convert_srs_col(col)

        if self.__remove_prvs:
            col = remove_prvs_col(col)

        if self.__normalize_bounces:
            col = normalize_bounces_col(col)

        if self.__normalize_entropy:
            col = normalize_entropy_col(col)

        data["mfrom"] = col

        return data
