from senderstats.common.utils import print_list_with_title
from senderstats.interfaces import Processor
from senderstats.processing.CSVProcessor import CSVProcessor
from senderstats.processing.ExclusionManager import ExclusionManager
from senderstats.processing.FilterManager import FilterManager
from senderstats.processing.InputFileManager import InputFileManager
from senderstats.processing.MapperManager import MapperManager
from senderstats.processing.PipelineBuilder import PipelineBuilder
from senderstats.processing.ProcessorManager import ProcessorManager
from senderstats.processing.TransformManager import TransformManager


class PipelineProcessor:
    def __init__(self, args):
        self.__input_file_manager = InputFileManager(args)
        self.__mapper_manager = MapperManager(args)
        self.__exclusion_manager = ExclusionManager(args)
        self._filter_manager = FilterManager(self.__exclusion_manager)
        self._transform_manager = TransformManager(args, self.__mapper_manager)
        self._processor_manager = ProcessorManager(args)

        self.__pipeline = PipelineBuilder(
            self._transform_manager,
            self._filter_manager,
            self._processor_manager
        ).build_pipeline(args)

    def process_files(self):
        csv_processor = CSVProcessor(self.__mapper_manager)
        f_total = len(self.__input_file_manager.input_files)
        for f_current, input_file in enumerate(self.__input_file_manager.input_files, start=1):
            print(f"Processing: {input_file} ({f_current} of {f_total})")
            csv_processor.process(input_file, self.__pipeline)

    def exclusion_summary(self):
        print()
        print_list_with_title("Files to be processed:", self.__input_file_manager.input_files)
        print_list_with_title("Senders excluded from processing:", self.__exclusion_manager.excluded_senders)
        print_list_with_title("Domains excluded from processing:", self.__exclusion_manager.excluded_domains)
        print_list_with_title("Domains constrained for processing:", self.__exclusion_manager.restricted_domains)

    def filter_summary(self):
        print()
        print("Messages excluded by empty senders:",
              self._filter_manager.exclude_empty_sender_filter.get_excluded_count())
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
