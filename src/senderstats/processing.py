import csv
import os
from glob import glob

from data.Mapper import Mapper
from data.MessageData import MessageData
from data.common.Processor import Processor
from data.filters import *
from data.processors import *
from data.transformers import *
from senderstats.common.defaults import *


def process_input_files(input_files):
    file_names = []
    for f in input_files:
        file_names += glob(f)
    file_names = set(file_names)
    return [file for file in file_names if os.path.isfile(file)]


def process_exclusions(args):
    args.excluded_senders = sorted(list({sender.casefold() for sender in args.excluded_senders}))
    args.excluded_domains = sorted(
        list({domain.casefold() for domain in DEFAULT_DOMAIN_EXCLUSIONS + args.excluded_domains}))
    args.restricted_domains = sorted(list({domain.casefold() for domain in args.restricted_domains}))
    return args


def configure_field_mapper(args):
    default_field_mappings = {
        'mfrom': DEFAULT_MFROM_FIELD,
        'hfrom': DEFAULT_HFROM_FIELD,
        'rpath': DEFAULT_RPATH_FIELD,
        'msgsz': DEFAULT_MSGSZ_FIELD,
        'msgid': DEFAULT_MSGID_FIELD,
        'subject': DEFAULT_SUBJECT_FIELD,
        'date': DEFAULT_DATE_FIELD
    }
    field_mapper = Mapper(default_field_mappings)
    if args.mfrom_field:
        field_mapper.add_mapping('mfrom', args.mfrom_field)
    if args.hfrom_field:
        field_mapper.add_mapping('hfrom', args.hfrom_field)
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
    return field_mapper


def build_pipeline(args):
    exclude_empty_sender_filter = ExcludeEmptySenderFilter()
    exclude_domain_filter = ExcludeDomainFilter(args.excluded_domains)
    exclude_senders_filter = ExcludeSenderFilter(args.excluded_senders)
    restrict_senders_filter = RestrictDomainFilter(args.restricted_domains)
    mfrom_transform = MFromTransform(args.decode_srs, args.remove_prvs)
    hfrom_transform = HFromTransform(args.no_display, args.no_empty_hfrom)
    msgid_transform = MIDTransform()
    rpath_transform = RPathTransform(args.decode_srs, args.remove_prvs)
    mfrom_processor = MFromProcessor(args.sample_subject)
    hfrom_processor = HFromProcessor(args.sample_subject)
    msgid_processor = MIDProcessor(args.sample_subject)
    rpath_processor = RPathProcessor(args.sample_subject)
    align_processor = AlignmentProcessor(args.sample_subject)
    pipeline = (exclude_empty_sender_filter
                .set_next(mfrom_transform)
                .set_next(exclude_domain_filter)
                .set_next(exclude_senders_filter)
                .set_next(restrict_senders_filter)
                .set_next(mfrom_processor))
    if args.gen_hfrom or args.gen_alignment:
        pipeline.set_next(hfrom_transform)
    if args.gen_hfrom:
        pipeline.set_next(hfrom_processor)
    if args.gen_rpath:
        pipeline.set_next(rpath_transform)
        pipeline.set_next(rpath_processor)
    if args.gen_msgid:
        pipeline.set_next(msgid_transform)
        pipeline.set_next(msgid_processor)
    if args.gen_alignment:
        pipeline.set_next(align_processor)
    return pipeline

def get_processors(pipeline) -> []:
    processors = []
    current = pipeline
    while current is not None:
        if isinstance(current, Processor):
            processors.append(current)
        current = current.get_next()
    return processors

def process_files(file_names, field_mapper, pipeline):
    message_data = MessageData(field_mapper)
    f_current = 1
    f_total = len(file_names)
    for input_file in file_names:
        print("Processing:", input_file, f'({f_current} of {f_total})')
        with open(input_file, 'r', encoding='utf-8-sig') as file:
            reader = csv.reader(file)
            headers = next(reader)
            field_mapper.configure(headers)
            for csv_line in reader:
                message_data.load(csv_line)
                pipeline.handle(message_data)
        f_current += 1
