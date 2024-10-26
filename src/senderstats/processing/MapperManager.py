from enum import Enum
from senderstats.common.defaults import *
from senderstats.core.mappers.CSVMapper import CSVMapper
from senderstats.core.mappers.JSONMapper import JSONMapper


class SourceType(Enum):
    CSV = "CSV"
    JSON = "JSON"


class MapperManager:
    def __init__(self, args, source_type: SourceType = SourceType.CSV):
        self.source_type = source_type
        self.field_mapper = self.__configure_field_mapper(args)

    def __configure_field_mapper(self, args):
        # Define default field mappings for CSV and JSON sources
        if self.source_type == SourceType.CSV:
            default_field_mappings = {
                'mfrom': DEFAULT_MFROM_FIELD,
                'hfrom': DEFAULT_HFROM_FIELD,
                'rpath': DEFAULT_RPATH_FIELD,
                'rcpts': DEFAULT_RCPTS_FIELD,
                'msgsz': DEFAULT_MSGSZ_FIELD,
                'msgid': DEFAULT_MSGID_FIELD,
                'subject': DEFAULT_SUBJECT_FIELD,
                'date': DEFAULT_DATE_FIELD,
                'ip': DEFAULT_IP_FIELD
            }
            field_mapper = CSVMapper(default_field_mappings)

        elif self.source_type == SourceType.JSON:
            default_field_mappings = {
                'mfrom': ['envelope', 'from'],
                'rcpts': ['envelope', 'rcpts'],
                'subject': ['msg', 'normalizedHeader', 'subject'],
                'msgid': ['msg', 'normalizedHeader', 'message-id'],
                'msgsz': ['msg', 'sizeBytes'],
                'hfrom': ['msg', 'normalizedHeader', 'from']
            }
            field_mapper = JSONMapper(default_field_mappings)

        else:
            raise ValueError("Unsupported source type. Use SourceType.CSV or SourceType.JSON.")

        # Apply custom mappings and exclusions as needed
        self.__add_custom_mappings(field_mapper, args)
        self.__remove_unnecessary_mappings(field_mapper, args)

        return field_mapper

    def __add_custom_mappings(self, field_mapper, args):
        if self.source_type == SourceType.CSV:
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
            if args.ip_field:
                field_mapper.add_mapping('ip', args.ip_field)

        elif self.source_type == SourceType.JSON:
            if args.mfrom_path:
                field_mapper.add_mapping('mfrom', args.mfrom_path)
            if args.rcpts_path:
                field_mapper.add_mapping('rcpts', args.rcpts_path)
            if args.subject_path:
                field_mapper.add_mapping('subject', args.subject_path)
            if args.msgid_path:
                field_mapper.add_mapping('msgid', args.msgid_path)
            if args.msgsz_path:
                field_mapper.add_mapping('msgsz', args.msgsz_path)
            if args.hfrom_path:
                field_mapper.add_mapping('hfrom', args.hfrom_path)

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
        if not args.exclude_ips:
            field_mapper.delete_mapping('ip')
