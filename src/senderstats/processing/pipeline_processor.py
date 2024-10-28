from senderstats.processing.data_source_manager import DataSourceManager
from senderstats.processing.pipeline_manager import PipelineManager


class PipelineProcessor:
    def __init__(self, data_source_manager: DataSourceManager, pipeline_builder: PipelineManager):
        self.__data_source = data_source_manager.get_data_source()
        self.__pipeline = pipeline_builder.get_pipeline()

    def process_data(self):
        for message_data in self.__data_source.read_data():
            self.__pipeline.handle(message_data)
