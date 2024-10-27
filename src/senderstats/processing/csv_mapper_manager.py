from senderstats.common.defaults import *
from senderstats.core.mappers import CSVMapper
from senderstats.processing.config_manager import ConfigManager


class CSVMapperManager:
    def __init__(self, config: ConfigManager):
        self.__config = config
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
        self.__mapper = CSVMapper(default_field_mappings)
        self.__add_custom_mappings()
        self.__remove_unnecessary_mappings()

    def get_mapper(self) -> CSVMapper:
        return self.__mapper

    def __add_custom_mappings(self):
        if self.__config.mfrom_field:
            self.__mapper.add_mapping('mfrom', self.__config.mfrom_field)
        if self.__config.hfrom_field:
            self.__mapper.add_mapping('hfrom', self.__config.hfrom_field)
        if self.__config.rcpts_field:
            self.__mapper.add_mapping('rcpts', self.__config.rcpts_field)
        if self.__config.rpath_field:
            self.__mapper.add_mapping('rpath', self.__config.rpath_field)
        if self.__config.msgid_field:
            self.__mapper.add_mapping('msgid', self.__config.msgid_field)
        if self.__config.msgsz_field:
            self.__mapper.add_mapping('msgsz', self.__config.msgsz_field)
        if self.__config.subject_field:
            self.__mapper.add_mapping('subject', self.__config.subject_field)
        if self.__config.date_field:
            self.__mapper.add_mapping('date', self.__config.date_field)
        if self.__config.ip_field:
            self.__mapper.add_mapping('ip', self.__config.ip_field)

    def __remove_unnecessary_mappings(self):
        if not (self.__config.gen_hfrom or self.__config.gen_alignment):
            self.__mapper.delete_mapping('hfrom')
        if not self.__config.gen_rpath:
            self.__mapper.delete_mapping('rpath')
        if not self.__config.sample_subject:
            self.__mapper.delete_mapping('subject')
        if not self.__config.gen_msgid:
            self.__mapper.delete_mapping('msgid')
        if not self.__config.expand_recipients:
            self.__mapper.delete_mapping('rcpts')
        if not self.__config.exclude_ips:
            self.__mapper.delete_mapping('ip')
