from senderstats.data.csv_data_source import CSVDataSource
from senderstats.data.data_source_type import DataSourceType
from senderstats.processing.config_manager import ConfigManager
from senderstats.processing.csv_mapper_manager import CSVMapperManager


class DataSourceManager:
    def __init__(self, config: ConfigManager):
        if config.source_type == DataSourceType.CSV:
            self.__mapper_manager = CSVMapperManager(config)
            self.__data_source = CSVDataSource(config.input_files, self.__mapper_manager.get_mapper())
        else:
            raise ValueError("Unsupported source type. Use SourceType.CSV")

    def get_data_source(self):
        return self.__data_source
