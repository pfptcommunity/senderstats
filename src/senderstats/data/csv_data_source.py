from typing import List

import pandas as pd

from senderstats.core.mappers.csv_mapper import CSVMapper
from senderstats.interfaces.data_source import DataSource


class CSVDataSource(DataSource):
    CHUNK_SIZE = 100000  # Adjust as needed

    def __init__(self, input_files: List[str], field_mapper: CSVMapper):
        self.__input_files = input_files
        self.__field_mapper = field_mapper

    def read_data(self):
        f_total = len(self.__input_files)
        for f_current, input_file in enumerate(self.__input_files, start=1):
            print(f"Processing: {input_file} ({f_current} of {f_total})")
            try:
                used_columns = self.__field_mapper.get_used_columns()
                chunk_reader = pd.read_csv(input_file, chunksize=self.CHUNK_SIZE, dtype=str, encoding="utf-8-sig",
                                           usecols=used_columns)
                rename_map = self.__field_mapper.get_pandas_rename_map()
                transformations = self.__field_mapper.get_pandas_transformations()

                first_chunk = True
                for chunk in chunk_reader:
                    chunk = chunk.rename(columns=rename_map)  # Rename to internal keys

                    # Apply transformations
                    for field, transform in transformations.items():
                        if field in chunk.columns:
                            chunk[field] = chunk[field].apply(transform)

                    yield chunk

            except Exception as e:
                print(f"Error reading file {input_file}: {e}")
