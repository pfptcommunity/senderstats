from email.header import decode_header
from typing import List

from senderstats.core.mappers.json_mapper import JSONMapper
from senderstats.processing.config_manager import ConfigManager
from senderstats.common.defaults import *


class JSONMapperManager:
    def __init__(self, config: ConfigManager):
        self.__config = config
        default_field_mappings = {
            'direction': ["filter", "routeDirection"],
            'mfrom': ["envelope", "from"],
            'hfrom': ["msg", "normalizedHeader", "from"],
            'rpath': ["envelope", "from"],
            'rcpts': ["envelope", "rcpts"],
            'msgsz': ["msg", "sizeBytes"],
            'msgid': ["msg", "normalizedHeader", "message-id"],
            'subject': ["msg", "normalizedHeader", "subject"],
            'date': ["ts"],
            'ip': ["connection", "ip"]
        }
        decoders = {
            'hfrom': JSONMapperManager.decode_array,
            'msgid': JSONMapperManager.decode_array,
            'subject': JSONMapperManager.decode_array
        }
        self.__mapper = JSONMapper(default_field_mappings,decoders)
        self.__add_custom_mappings()
        self.__remove_unnecessary_mappings()

    def get_mapper(self) -> JSONMapper:
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

    @staticmethod
    def decode_mime_strings(encoded_strings) -> List[str]:
        decoded_strings = []
        for encoded_string in encoded_strings:
            try:
                # Decode the MIME-encoded string
                decoded_parts = decode_header(encoded_string)
                decoded_string = ""
                # Process each part of the decoded header
                for part, encoding in decoded_parts:
                    if isinstance(part, bytes):
                        decoded_string += part.decode(encoding or 'utf-8')
                    else:
                        decoded_string += part
                decoded_strings.append(decoded_string)
            except Exception as e:
                print(f"Error decoding string: {encoded_string} - {e}")
                decoded_strings.append(encoded_string)  # Add None or a placeholder for failed decoding
        return decoded_strings

    @staticmethod
    def decode_array(hfrom_data: List[str]):
        return ";".join(JSONMapperManager.decode_mime_strings(hfrom_data))

