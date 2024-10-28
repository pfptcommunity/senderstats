import csv
from typing import List

from senderstats.core.mappers.csv_mapper import CSVMapper
from senderstats.interfaces.data_source import DataSource


class CSVDataSource(DataSource):
    def __init__(self, input_files: List[str], field_mapper: CSVMapper):
        self.__input_files = input_files
        self.__field_mapper = field_mapper

    def read_data(self):
        f_total = len(self.__input_files)
        for f_current, input_file in enumerate(self.__input_files, start=1):
            print(f"Processing: {input_file} ({f_current} of {f_total})")
            try:
                with open(input_file, mode="r", encoding="utf-8-sig") as file:
                    reader = csv.reader(file)
                    headers = next(reader)
                    self.__field_mapper.reindex(headers)
                    for row in reader:
                        normalized_row = self.__field_mapper.map_fields(row)
                        yield normalized_row

            except Exception as e:
                print(f"Error reading file {input_file}: {e}")
