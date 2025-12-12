import pandas as pd

from senderstats.common.utils import email_re
from senderstats.interfaces.transform import Transform


class HFromTransform(Transform[pd.DataFrame, pd.DataFrame]):
    def __init__(self, no_display: bool = False, empty_from: bool = False):
        super().__init__()
        self.__no_display = no_display
        self.__empty_from = empty_from

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        if "hfrom" not in data.columns or "mfrom" not in data.columns:
            return data

        # Strip display name first, using the precompiled regex
        if self.__no_display:
            extracted = (
                data["hfrom"]
                .astype("string")
                .str.extract(email_re)
            )
            # extracted[1] is the email address group
            data["hfrom"] = extracted[1].fillna(data["hfrom"])

        # If header From is empty after that, carry mfrom into hfrom
        if self.__empty_from:
            mask = (
                data["hfrom"]
                .astype("string")
                .fillna("")
                .str.strip()
                .eq("")
            )
            data.loc[mask, "hfrom"] = data.loc[mask, "mfrom"]

        return data
