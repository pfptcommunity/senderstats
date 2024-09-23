import re
from collections import defaultdict
from typing import Dict, List, Set, DefaultDict
from lib.DataProcessingStatus import DataProcessingStatus
from common.utils import parse_email_details, convert_srs, remove_prvs, compile_domains_pattern
from lib.DataProcessor import DataProcessor
from lib.DataRecord import DataRecord


class DataProcessorMFrom(DataProcessor):
    __mfrom_data: Dict[str, Dict]
    __decode_srs: bool
    __remove_prvs: bool
    __excluded_senders: Set[str]
    __excluded_domains: re.Pattern
    __restricted_domains: re.Pattern
    __excluded_sender_count: DefaultDict[str, int]
    __excluded_domain_count: DefaultDict[str, int]
    __restricted_domains_count: DefaultDict[str, int]

    def __init__(self,excluded_senders: List[str], excluded_domains: List[str],
                 restricted_domains: List[str], decode_srs: bool = False, remove_prvs: bool = False):
        super().__init__()
        self.__mfrom_data: Dict[str, Dict] = {}
        self.__decode_srs = decode_srs
        self.__remove_prvs = remove_prvs
        self.__excluded_senders = set(excluded_senders)
        self.__excluded_domains = compile_domains_pattern(excluded_domains)
        self.__restricted_domains = compile_domains_pattern(restricted_domains)
        self.__excluded_sender_count = defaultdict(int)
        self.__excluded_domain_count = defaultdict(int)
        self.__restricted_domains_count = defaultdict(int)

    def process(self, data_record: DataRecord) -> DataProcessingStatus:
        if not data_record.mfrom:
            return DataProcessingStatus.SKIP

        # If sender is not empty, we will extract parts of the email
        mfrom_parts = parse_email_details(data_record.mfrom)
        mfrom = mfrom_parts['email_address']

        # Determine the original sender
        if self.__decode_srs:
            mfrom = convert_srs(mfrom)

        if self.__remove_prvs:
            mfrom = remove_prvs(mfrom)

        # Exclude a specific sender highest priority
        if mfrom in self.__excluded_senders:
            self.__excluded_sender_count[mfrom] += 1
            return DataProcessingStatus.SKIP

        # Deal with all the records we don't want to process based on sender.
        if self.__excluded_domains.search(mfrom):
            domain = mfrom_parts['domain']
            self.__excluded_domain_count[domain] += 1
            return DataProcessingStatus.SKIP

        # Limit processing to only domains on in a list
        if not self.__restricted_domains.search(mfrom):
            domain = mfrom_parts['domain']
            self.__restricted_domains_count[domain] += 1
            return DataProcessingStatus.SKIP

        self.__mfrom_data.setdefault(mfrom, {})

        self.update_message_size_and_subjects(self.__mfrom_data[mfrom], data_record.message_size, data_record.subject)

        # Store the preprocessed data back
        data_record.mfrom = mfrom

        return DataProcessingStatus.CONTINUE
