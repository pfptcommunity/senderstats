import os
from glob import glob

from senderstats.processing.CSVMapperManager import CSVMapperManager
from senderstats.data.WebSocketDataSource import WebSocketDataSource
from senderstats.data.CSVDataSource import CSVDataSource
from senderstats.common.Config import Config
from enum import Enum

class SourceType(Enum):
    CSV = "CSV"
    JSON = "JSON"

class DataSourceManager:
    def __init__(self, config: Config):
        if config.source_type == SourceType.CSV:
            self.__mapper_manager = CSVMapperManager(config)
            self.__data_source = CSVDataSource(config.input_files, self.__mapper_manager.get_mapper())
        elif config.source_type == SourceType.JSON:
            self.__data_source = WebSocketDataSource()
        else:
            raise ValueError("Unsupported source type. Use SourceType.CSV or SourceType.JSON.")

    def get_data_source(self):
        return self.__data_source