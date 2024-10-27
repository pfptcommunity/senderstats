from senderstats.processing.json_mapper_manager import JSONMapperManager
from senderstats.data.csv_data_source import CSVDataSource
from senderstats.data.data_source_type import DataSourceType
from senderstats.data.web_socket_data_source import WebSocketDataSource
from senderstats.processing.config_manager import ConfigManager
from senderstats.processing.csv_mapper_manager import CSVMapperManager


class DataSourceManager:
    def __init__(self, config: ConfigManager):
        if config.source_type == DataSourceType.CSV:
            self.__mapper_manager = CSVMapperManager(config)
            self.__data_source = CSVDataSource(config.input_files, self.__mapper_manager.get_mapper())
        elif config.source_type == DataSourceType.JSON:
            self.__mapper_manager = JSONMapperManager(config)
            self.__data_source = WebSocketDataSource( self.__mapper_manager.get_mapper(), config.cluster_id, config.token, "message", 300)
        else:
            raise ValueError("Unsupported source type. Use SourceType.CSV or SourceType.JSON.")

    def get_data_source(self):
        return self.__data_source
