from senderstats.common.Config import Config
from senderstats.interfaces import Processor
from senderstats.processing.DataSourceManager import DataSourceManager
from senderstats.processing.FilterManager import FilterManager
from senderstats.processing.PipelineBuilder import PipelineBuilder
from senderstats.processing.ProcessorManager import ProcessorManager
from senderstats.processing.TransformManager import TransformManager


class PipelineProcessor:
    def __init__(self, config: Config, data_source_manager: DataSourceManager):
        self.__config = config
        self.__data_source = data_source_manager.get_data_source()
        self._filter_manager = FilterManager(config)
        self._transform_manager = TransformManager(config)
        self._processor_manager = ProcessorManager(config)
        self.__pipeline = PipelineBuilder(
            self._transform_manager,
            self._filter_manager,
            self._processor_manager
        ).build_pipeline(config)

    async def process_data(self):
        async for message_data in self.__data_source.read_data():
            # print("Processed Row:", vars(normalized_data))
            self.__pipeline.handle(message_data)

    def filter_summary(self):
        print()
        print("Messages excluded by empty sender:",
              self._filter_manager.exclude_empty_sender_filter.get_excluded_count())
        print("Messages excluded by invalid size:",
              self._filter_manager.exclude_invalid_size_filter.get_excluded_count())
        print("Messages excluded by IP address:",
              self._filter_manager.exclude_ip_filter.get_excluded_count())
        print("Messages excluded by domain:", self._filter_manager.exclude_domain_filter.get_excluded_count())
        print("Messages excluded by sender:", self._filter_manager.exclude_senders_filter.get_excluded_count())
        print("Messages excluded by constraint:", self._filter_manager.restrict_senders_filter.get_excluded_count())

    def get_processors(self) -> list:
        processors = []
        current = self.__pipeline
        while current is not None:
            if isinstance(current, Processor):
                processors.append(current)
            current = current.get_next()
        return processors
