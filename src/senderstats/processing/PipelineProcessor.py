import csv
import os
from glob import glob

from senderstats.processing.PipelineBuilder import PipelineBuilder
from senderstats.processing.FilterManager import FilterManager
from senderstats.processing.ProcessorManager import ProcessorManager
from senderstats.processing.TransformManager import TransformManager
from senderstats.processing.ExclusionManager import ExclusionManager
from senderstats.processing.MapperManager import MapperManager
from senderstats.common.utils import print_list_with_title
from senderstats.core.processors import *
from senderstats.interfaces import Processor


class PipelineProcessor:
    def __init__(self, args):
        self.__input_files = self.__process_input_files(args.input_files)
        self.__mapper_manager = MapperManager(args)
        self.__exclusion_manager = ExclusionManager(args)
        self.__filter_manager = FilterManager(self.__exclusion_manager)
        self.__transform_manager = TransformManager(args, self.__mapper_manager)
        self.__processor_manager = ProcessorManager(args)

        self.__pipeline = PipelineBuilder(
            self.__transform_manager,
            self.__filter_manager,
            self.__processor_manager
        ).build_pipeline(args)

        #self.__pipeline = self.__build_pipeline(args)

    def __build_pipeline(self, args):
        pipeline = (self.__transform_manager.csv_to_message_data_transform.set_next(self.__filter_manager.exclude_empty_sender_filter)
                    .set_next(self.__transform_manager.mfrom_transform)
                    .set_next(self.__filter_manager.exclude_domain_filter)
                    .set_next(self.__filter_manager.exclude_senders_filter)
                    .set_next(self.__filter_manager.restrict_senders_filter)
                    .set_next(self.__transform_manager.date_transform)
                    .set_next(self.__processor_manager.mfrom_processor))

        if args.gen_hfrom or args.gen_alignment:
            pipeline.set_next(self.__transform_manager.hfrom_transform)
        if args.gen_hfrom:
            pipeline.set_next(self.__processor_manager.hfrom_processor)
        if args.gen_rpath:
            pipeline.set_next(self.__transform_manager.rpath_transform)
            pipeline.set_next(self.__processor_manager.rpath_processor)
        if args.gen_msgid:
            pipeline.set_next(self.__transform_manager.msgid_transform)
            pipeline.set_next(self.__processor_manager.msgid_processor)
        if args.gen_alignment:
            pipeline.set_next(self.__processor_manager.align_processor)

        pipeline.set_next(self.__processor_manager.date_processor)

        return pipeline

    def process_files(self):
        f_current = 1
        f_total = len(self.__input_files)
        for input_file in self.__input_files:
            print("Processing:", input_file, f'({f_current} of {f_total})')
            try:
                with open(input_file, 'r', encoding='utf-8-sig') as file:
                    reader = csv.reader(file)
                    headers = next(reader)
                    self.__mapper_manager.field_mapper.reindex(headers)
                    for csv_line in reader:
                        self.__pipeline.handle(csv_line)
            except Exception as e:
                print(f"Error processing file {input_file}: {e}")
            f_current += 1

    def __process_input_files(self, input_files):
        file_names = []
        for f in input_files:
            file_names += glob(f)
        file_names = set(file_names)
        return [file for file in file_names if os.path.isfile(file)]

    def exclusion_summary(self):
        print_list_with_title("Files to be processed:", self.__input_files)
        print_list_with_title("Senders excluded from processing:", self.__exclusion_manager.excluded_senders)
        print_list_with_title("Domains excluded from processing:", self.__exclusion_manager.excluded_domains)
        print_list_with_title("Domains constrained for processing:", self.__exclusion_manager.restricted_domains)

    def filter_summary(self):
        print("Messages Excluded by Empty Senders:",self.__filter_manager.exclude_empty_sender_filter.get_excluded_count())
        print("Messages Excluded by Domain:",self.__filter_manager.exclude_domain_filter.get_excluded_count())
        print("Messages Excluded by Sender:", self.__filter_manager.exclude_senders_filter.get_excluded_count())
        print("Messages Excluded by Restriction:", self.__filter_manager.restrict_senders_filter.get_excluded_count())

    def get_date_count(self) -> int:
        return len(self.__processor_manager.date_processor.get_date_counter())

    def get_processors(self) -> list:
        processors = []
        current = self.__pipeline
        while current is not None:
            if isinstance(current, Processor):
                processors.append(current)
            current = current.get_next()
        return processors

    def get_date_processor(self) -> DateProcessor:
        return self.__processor_manager.date_processor
