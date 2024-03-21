import csv
import datetime
import re
from collections import defaultdict
from typing import DefaultDict, Any, Dict, Set, List

from tldextract import tldextract

from utils import convert_srs, remove_prvs, get_email_domain, get_message_id_host, strip_display_names, \
    compile_domains_pattern

# Constants for the class
DEFAULT_DOMAIN_EXCLUSIONS = ['ppops.net', 'pphosted.com', 'knowledgefront.com']
DEFAULT_MFROM_FIELD = 'Sender'
DEFAULT_HFROM_FIELD = 'Header_From'
DEFAULT_RPATH_FIELD = 'Header_Return-Path'
DEFAULT_MSGID_FIELD = 'Message_ID'
DEFAULT_MSGSZ_FIELD = 'Message_Size'
DEFAULT_DATE_FIELD = 'Date'
DEFAULT_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%f%z'

# Thresholds and limits
DEFAULT_THRESHOLD = 100


class MessageDataProcessor:
    # Data processing information
    __mfrom_data: Dict[str, List[Any]]
    __hfrom_data: Dict[str, List[Any]]
    __rpath_data: Dict[str, List[Any]]
    __mfrom_hfrom_data: Dict[tuple, List[Any]]
    __msgid_data: Dict[tuple, List[Any]]
    # Counters
    __date_counter: DefaultDict[str, int]
    __total_processed_count: int
    __empty_sender_count: int
    __excluded_sender_count: DefaultDict[str, int]
    __excluded_domain_count: DefaultDict[str, int]
    __restricted_domains_count: DefaultDict[str, int]
    # Define CSV Fields
    __mfrom_field: str
    __hfrom_field: str
    __rpath_field: str
    __msgid_field: str
    __msgsz_field: str
    __date_field: str
    __date_format: str
    # Processing Option Flags
    __opt_no_display: bool
    __opt_decode_srs: bool
    __opt_remove_prvs: bool
    __opt_empty_from: bool
    # Restrictions
    __excluded_senders: Set[str]
    __excluded_domains: re.Pattern
    __restricted_domains: re.Pattern

    def __init__(self, excluded_senders: List[str], excluded_domains: List[str], restricted_domains: List[str]):
        # Default field mappings based on smart search output
        self.__mfrom_field = DEFAULT_MFROM_FIELD
        self.__hfrom_field = DEFAULT_HFROM_FIELD
        self.__rpath_field = DEFAULT_RPATH_FIELD
        self.__msgid_field = DEFAULT_MSGID_FIELD
        self.__msgsz_field = DEFAULT_MSGSZ_FIELD
        self.__date_field = DEFAULT_DATE_FIELD
        self.__date_format = DEFAULT_DATE_FORMAT
        # Initialize counters
        self.__date_counter = defaultdict(int)
        self.__total_processed_count = 0
        self.__empty_sender_count = 0
        self.__excluded_sender_count = defaultdict(int)
        self.__excluded_domain_count = defaultdict(int)
        self.__restricted_domains_count = defaultdict(int)
        # Initialize processing flags
        self.__opt_no_display = False
        self.__opt_decode_srs = False
        self.__opt_remove_prvs = False
        self.__opt_empty_from = False
        # Used to match the patterns
        self.__excluded_senders = set(excluded_senders)
        self.__excluded_domains = compile_domains_pattern(excluded_domains)
        self.__restricted_domains = compile_domains_pattern(restricted_domains)
        # Initialize data collections
        self.__mfrom_data = dict()
        self.__hfrom_data = dict()
        self.__rpath_data = dict()
        self.__mfrom_hfrom_data = dict()
        self.__msgid_data = dict()

    def process_file(self, input_file):
        with open(input_file, 'r', encoding='utf-8-sig') as input_file:
            reader = csv.DictReader(input_file)
            for csv_line in reader:
                self.__total_processed_count += 1
                env_sender = csv_line[self.__mfrom_field].casefold().strip()

                # Check for empy sender
                if not env_sender:
                    self.__empty_sender_count += 1
                    continue

                if self.__opt_decode_srs:
                    env_sender = convert_srs(env_sender)

                if self.__opt_remove_prvs:
                    env_sender = remove_prvs(env_sender)

                # Exclude a specific sender highest priority
                if env_sender in self.__excluded_senders:
                    self.__excluded_sender_count[env_sender] += 1
                    continue

                # Deal with all the records we don't want to process based on sender.
                if self.__excluded_domains.search(env_sender):
                    domain = get_email_domain(env_sender)
                    self.__excluded_domain_count[domain] += 1
                    continue

                # Limit processing to only domains on in a list
                if not self.__restricted_domains.search(env_sender):
                    domain = get_email_domain(env_sender)
                    self.__restricted_domains_count[domain] += 1
                    continue

                header_from = csv_line[self.__hfrom_field].casefold().strip()

                return_path = csv_line[self.__rpath_field].casefold().strip()

                if self.__opt_decode_srs:
                    return_path = remove_prvs(return_path)

                if self.__opt_decode_srs:
                    return_path = convert_srs(return_path)

                # Message ID is unique but often the sending host behind the @ symbol is unique to the application
                message_id = csv_line[self.__msgid_field].casefold().strip()
                message_id_domain = get_message_id_host(message_id)
                message_id_domain_extract = tldextract.extract(message_id_domain)
                message_id_host = message_id_domain_extract.subdomain
                message_id_domain = message_id_domain_extract.domain
                message_id_domain_suffix = message_id_domain_extract.suffix

                # If header from is empty, we will use env_sender
                if self.__opt_empty_from and not header_from:
                    header_from = env_sender

                # Add domain suffix to TLD
                if message_id_domain_suffix:
                    message_id_domain += '.' + message_id_domain_suffix

                if not message_id_host and not message_id_domain_suffix:
                    message_id_host = message_id_domain
                    message_id_domain = ''

                if self.__opt_no_display:
                    header_from = strip_display_names(header_from)

                # Determine distinct dates of data, and count number of messages on that day
                date = datetime.datetime.strptime(csv_line[self.__date_field], self.__date_format)

                str_date = date.strftime('%Y-%m-%d')
                self.__date_counter[str_date] += 1

                message_size = csv_line[self.__msgsz_field]

                # Make sure cast to int is valid, else 0
                if message_size.isdigit():
                    message_size = int(message_size)
                else:
                    message_size = 0

                self.__mfrom_data.setdefault(env_sender, []).append(message_size)
                self.__hfrom_data.setdefault(header_from, []).append(message_size)
                self.__rpath_data.setdefault(return_path, []).append(message_size)

                # Fat index for binding commonality
                mid_host_domain_index = (header_from, message_id_host, message_id_domain)
                self.__msgid_data.setdefault(mid_host_domain_index, []).append(message_size)

                # Fat index for binding commonality
                sender_header_index = (env_sender, header_from)
                self.__mfrom_hfrom_data.setdefault(sender_header_index, []).append(message_size)

    # Getter for total_processed_count
    def get_total_processed_count(self) -> int:
        return self.__total_processed_count

    # Getter for empty_sender_count
    def get_empty_sender_count(self) -> int:
        return self.__empty_sender_count

    # Getter for excluded_sender_count
    def get_excluded_sender_count(self) -> Dict[Any, int]:
        return dict(self.__excluded_sender_count)

    # Getter for excluded_domain_count
    def get_excluded_domain_count(self) -> Dict[Any, int]:
        return dict(self.__excluded_domain_count)

    # Getter for restricted_domains_count
    def get_restricted_domains_count(self) -> Dict[Any, int]:
        return dict(self.__restricted_domains_count)

    # Getter for date_counter
    def get_date_counter(self) -> Dict[str, int]:
        return dict(self.__date_counter)

    # Getter for sender_data
    def get_mfrom_data(self) -> Dict[str, List[int]]:
        return self.__mfrom_data

    # Getter for from_data
    def get_hfrom_data(self) -> Dict[str, List[int]]:
        return self.__hfrom_data

    # Getter for return_data
    def get_rpath_data(self) -> Dict[str, List[int]]:
        return self.__rpath_data

    # Getter for sender_from_data
    def get_mfrom_hfrom_data(self) -> Dict[tuple, List[int]]:
        return self.__mfrom_hfrom_data

    # Getter for mid_data
    def get_msgid_data(self) -> Dict[tuple, List[int]]:
        return self.__msgid_data

    # Setter for opt_no_display
    def set_opt_no_display(self, value: bool) -> None:
        if isinstance(value, bool):
            self.__opt_no_display = value
        else:
            raise ValueError("opt_no_display must be a boolean.")

    # Setter for opt_decode_srs
    def set_opt_decode_srs(self, value: bool) -> None:
        if isinstance(value, bool):
            self.__opt_decode_srs = value
        else:
            raise ValueError("opt_decode_srs must be a boolean.")

    # Setter for opt_remove_prvs
    def set_opt_remove_prvs(self, value: bool) -> None:
        if isinstance(value, bool):
            self.__opt_remove_prvs = value
        else:
            raise ValueError("opt_remove_prvs must be a boolean.")

    # Setter for opt_empty_from
    def set_opt_empty_from(self, value: bool) -> None:
        if isinstance(value, bool):
            self.__opt_empty_from = value
        else:
            raise ValueError("opt_empty_from must be a boolean.")

    # Setter for mfrom_field
    def set_mfrom_field(self, value: str) -> None:
        if isinstance(value, str):
            self.__mfrom_field = value
        else:
            raise ValueError("mfrom_field must be a string.")

    # Setter for hfrom_field
    def set_hfrom_field(self, value: str) -> None:
        if isinstance(value, str):
            self.__hfrom_field = value
        else:
            raise ValueError("hfrom_field must be a string.")

    # Setter for rpath_field
    def set_rpath_field(self, value: str) -> None:
        if isinstance(value, str):
            self.__rpath_field = value
        else:
            raise ValueError("rpath_field must be a string.")

    # Setter for msgid_field
    def set_msgid_field(self, value: str) -> None:
        if isinstance(value, str):
            self.__msgid_field = value
        else:
            raise ValueError("msgid_field must be a string.")

    # Setter for msgsz_field
    def set_msgsz_field(self, value: str) -> None:
        if isinstance(value, str):
            self.__msgsz_field = value
        else:
            raise ValueError("msgsz_field must be a string.")

    # Setter for date_field
    def set_date_field(self, value: str) -> None:
        if isinstance(value, str):
            self.__date_field = value
        else:
            raise ValueError("date_field must be a string.")

    # Setter for date_format
    def set_date_format(self, value: str) -> None:
        if isinstance(value, str):
            self.__date_format = value
        else:
            raise ValueError("date_format must be a string.")
