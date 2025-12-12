import warnings

import pandas

from senderstats.interfaces.transform import Transform


class DateTransform(Transform[pandas.DataFrame, pandas.DataFrame]):
    def __init__(self, date_format: str):
        super().__init__()
        self.__date_format = date_format

    def transform(self, data: pandas.DataFrame) -> pandas.DataFrame:
        if "date" not in data.columns:
            return data

        col = data["date"]

        if self.__date_format:
            # First try the user-supplied format for speed
            parsed = pandas.to_datetime(
                col,
                format=self.__date_format,
                errors="coerce",
                utc=True,
            )
            mask = parsed.isna() & col.notna()
            if mask.any():
                bad_count = mask.sum()
                total = len(col)
                warnings.warn(
                    f"User-specified date format '{self.__date_format}' "
                    f"did not match {bad_count} of {total} date values. "
                    "Falling back to Pandas date inference for those entries.",
                    RuntimeWarning
                )
                parsed.loc[mask] = pandas.to_datetime(
                    col.loc[mask],
                    errors="coerce",
                    utc=True,
                )
        else:
            # no user format, just start with generic parse
            parsed = pandas.to_datetime(
                col,
                errors="raise",
                utc=True,
            )

        data["date"] = parsed
        return data
