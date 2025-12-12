import pandas

from senderstats.common.utils import extract_email_address_col, convert_srs_col, remove_prvs_col
from senderstats.interfaces.transform import Transform


class RPathTransform(Transform[pandas.DataFrame, pandas.DataFrame]):
    def __init__(self, decode_srs: bool = False, remove_prvs: bool = False):
        super().__init__()
        self.__decode_srs = decode_srs
        self.__remove_prvs = remove_prvs

    def transform(self, data: pandas.DataFrame) -> pandas.DataFrame:
        col = data["rpath"].astype("string")

        col = extract_email_address_col(col)

        if self.__decode_srs:
            col = convert_srs_col(col)

        if self.__remove_prvs:
            col = remove_prvs_col(col)

        data['rpath'] = col
        return data
