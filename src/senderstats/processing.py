import csv
import os
from glob import glob

from senderstats.common.defaults import *
from senderstats.common.utils import print_list_with_title
from senderstats.data.common import Processor
from senderstats.data.filters import *
from senderstats.data.mappers import *
from senderstats.data.processors import *
from senderstats.data.transformers import *


class PipelineProcessor:
    def __init__(self, args):
        self.__field_mapper = self.__configure_field_mapper(args)
        self.__input_files = self.__process_input_files(args.input_files)
        self.__excluded_senders = self.__prepare_exclusions(args.excluded_senders)
        self.__excluded_domains = self.__prepare_exclusions(DEFAULT_DOMAIN_EXCLUSIONS + args.excluded_domains)
        self.__restricted_domains = self.__prepare_exclusions(args.restricted_domains)

        self.__initialize_transforms(args)
        self.__initialize_processors(args)
        self.__pipeline = self.__build_pipeline(args)

    def __prepare_exclusions(self, exclusions):
        return sorted(list({item.casefold() for item in exclusions}))

    def __initialize_transforms(self, args):
        self.__csv_to_message_data_transform = MessageDataTransform(self.__field_mapper)
        self.__date_transform = DateTransform(args.date_format)
        self.__exclude_empty_sender_filter = ExcludeEmptySenderFilter()
        self.__exclude_domain_filter = ExcludeDomainFilter(self.__excluded_domains)
        self.__exclude_senders_filter = ExcludeSenderFilter(self.__excluded_senders)
        self.__restrict_senders_filter = RestrictDomainFilter(self.__restricted_domains)
        self.__mfrom_transform = MFromTransform(args.decode_srs, args.remove_prvs)
        self.__hfrom_transform = HFromTransform(args.no_display, args.no_empty_hfrom)
        self.__msgid_transform = MIDTransform()
        self.__rpath_transform = RPathTransform(args.decode_srs, args.remove_prvs)

    def __initialize_processors(self, args):
        self.__mfrom_processor = MFromProcessor(args.sample_subject, args.expand_recipients)
        self.__hfrom_processor = HFromProcessor(args.sample_subject, args.expand_recipients)
        self.__msgid_processor = MIDProcessor(args.sample_subject, args.expand_recipients)
        self.__rpath_processor = RPathProcessor(args.sample_subject, args.expand_recipients)
        self.__align_processor = AlignmentProcessor(args.sample_subject, args.expand_recipients)
        self.__date_processor = DateProcessor(args.expand_recipients)

    def __configure_field_mapper(self, args):
        default_field_mappings = {
            'mfrom': DEFAULT_MFROM_FIELD,
            'hfrom': DEFAULT_HFROM_FIELD,
            'rpath': DEFAULT_RPATH_FIELD,
            'rcpts': DEFAULT_RCPTS_FIELD,
            'msgsz': DEFAULT_MSGSZ_FIELD,
            'msgid': DEFAULT_MSGID_FIELD,
            'subject': DEFAULT_SUBJECT_FIELD,
            'date': DEFAULT_DATE_FIELD
        }
        field_mapper = Mapper(default_field_mappings)
        self.__add_custom_mappings(field_mapper, args)
        self.__remove_unnecessary_mappings(field_mapper, args)
        return field_mapper

    def __add_custom_mappings(self, field_mapper, args):
        if args.mfrom_field:
            field_mapper.add_mapping('mfrom', args.mfrom_field)
        if args.hfrom_field:
            field_mapper.add_mapping('hfrom', args.hfrom_field)
        if args.rcpts_field:
            field_mapper.add_mapping('rcpts', args.rcpts_field)
        if args.rpath_field:
            field_mapper.add_mapping('rpath', args.rpath_field)
        if args.msgid_field:
            field_mapper.add_mapping('msgid', args.msgid_field)
        if args.msgsz_field:
            field_mapper.add_mapping('msgsz', args.msgsz_field)
        if args.subject_field:
            field_mapper.add_mapping('subject', args.subject_field)
        if args.date_field:
            field_mapper.add_mapping('date', args.date_field)

    def __remove_unnecessary_mappings(self, field_mapper, args):
        if not (args.gen_hfrom or args.gen_alignment):
            field_mapper.delete_mapping('hfrom')
        if not args.gen_rpath:
            field_mapper.delete_mapping('rpath')
        if not args.sample_subject:
            field_mapper.delete_mapping('subject')
        if not args.gen_msgid:
            field_mapper.delete_mapping('msgid')
        if not args.expand_recipients:
            field_mapper.delete_mapping('rcpts')

    def __build_pipeline(self, args):
        pipeline = (self.__csv_to_message_data_transform.set_next(self.__exclude_empty_sender_filter)
                    .set_next(self.__mfrom_transform)
                    .set_next(self.__exclude_domain_filter)
                    .set_next(self.__exclude_senders_filter)
                    .set_next(self.__restrict_senders_filter)
                    .set_next(self.__date_transform)
                    .set_next(self.__mfrom_processor))

        if args.gen_hfrom or args.gen_alignment:
            pipeline.set_next(self.__hfrom_transform)
        if args.gen_hfrom:
            pipeline.set_next(self.__hfrom_processor)
        if args.gen_rpath:
            pipeline.set_next(self.__rpath_transform)
            pipeline.set_next(self.__rpath_processor)
        if args.gen_msgid:
            pipeline.set_next(self.__msgid_transform)
            pipeline.set_next(self.__msgid_processor)
        if args.gen_alignment:
            pipeline.set_next(self.__align_processor)

        pipeline.set_next(self.__date_processor)

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
                    self.__csv_to_message_data_transform._field_mapper.reindex(headers)
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
        print_list_with_title("Senders excluded from processing:", self.__excluded_senders)
        print_list_with_title("Domains excluded from processing:", self.__excluded_domains)
        print_list_with_title("Domains constrained or processing:", self.__restricted_domains)

    def get_date_count(self) -> int:
        return len(self.__date_processor.get_date_counter())

    def get_processors(self) -> list:
        processors = []
        current = self.__pipeline
        while current is not None:
            if isinstance(current, Processor):
                processors.append(current)
            current = current.get_next()
        return processors

    def get_date_processor(self) -> DateProcessor:
        return self.__date_processor
