from senderstats.common.defaults import *
from senderstats.core.mappers import *


class MapperManager:
    def __init__(self, args):
        self.field_mapper = self.__configure_field_mapper(args)

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
