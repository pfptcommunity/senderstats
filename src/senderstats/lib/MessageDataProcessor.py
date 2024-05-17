import csv
import datetime
import re
from collections import defaultdict
from random import random
from typing import DefaultDict, Any, Dict, Set, List, Optional
from tldextract import tldextract
from senderstats.common.utils import (convert_srs, remove_prvs, compile_domains_pattern, find_ip_in_text,
                                      parse_email_details)
from .FieldMapper import FieldMapper

DEFAULT_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%f%z'


class MessageDataProcessor:
    # Data processing information
    __mfrom_data: Dict[str, Dict]
    __hfrom_data: Dict[str, Dict]
    __rpath_data: Dict[str, Dict]
    __mfrom_hfrom_data: Dict[tuple, Dict]
    __msgid_data: Dict[tuple, Dict]
    __subject_data: Dict[Any, Dict]
    # Counters
    __date_counter: DefaultDict[str, int]
    __total_processed_count: int
    __empty_sender_count: int
    __excluded_sender_count: DefaultDict[str, int]
    __excluded_domain_count: DefaultDict[str, int]
    __restricted_domains_count: DefaultDict[str, int]
    # Define CSV Fields
    __field_mapper: FieldMapper
    __date_format: str
    # Processing Option Flags
    __opt_no_display: bool
    __opt_decode_srs: bool
    __opt_remove_prvs: bool
    __opt_empty_from: bool
    __opt_sample_subject: bool
    # Report generation options
    __opt_gen_hfrom: bool
    __opt_gen_alignment: bool
    __opt_gen_rpath: bool
    __opt_gen_msgid: bool
    # Restrictions
    __excluded_senders: Set[str]
    __excluded_domains: re.Pattern
    __restricted_domains: re.Pattern

    def __init__(self, field_mapper: FieldMapper, excluded_senders: List[str], excluded_domains: List[str],
                 restricted_domains: List[str]):
        # Default field mappings based on smart search output
        self.__field_mapper = field_mapper
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
        self.__opt_sample_subject = False
        # Reports we want to generate
        self.__opt_gen_hfrom = False
        self.__opt_gen_alignment = False
        self.__opt_gen_rpath = False
        self.__opt_gen_msgid = False
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

    def __process_mfrom_data(self, mfrom: str) -> Optional[str]:
        # Check for empy sender
        if not mfrom:
            self.__empty_sender_count += 1
            return

        # If sender is not empty, we will extract parts of the email
        mfrom_parts = parse_email_details(mfrom)
        mfrom = mfrom_parts['email_address']

        # Determine the original sender
        if self.__opt_decode_srs:
            mfrom = convert_srs(mfrom)

        if self.__opt_remove_prvs:
            mfrom = remove_prvs(mfrom)

        # Exclude a specific sender highest priority
        if mfrom in self.__excluded_senders:
            self.__excluded_sender_count[mfrom] += 1
            return

        # Deal with all the records we don't want to process based on sender.
        if self.__excluded_domains.search(mfrom):
            domain = mfrom_parts['domain']
            self.__excluded_domain_count[domain] += 1
            return

        # Limit processing to only domains on in a list
        if not self.__restricted_domains.search(mfrom):
            domain = mfrom_parts['domain']
            self.__restricted_domains_count[domain] += 1
            return

        return mfrom

    def __process_hfrom_data(self, hfrom: str, mfrom: str) -> str:
        hfrom_parts = parse_email_details(hfrom)

        if self.__opt_no_display:
            hfrom = hfrom_parts['email_address']

        # If header from is empty, we will use env_sender
        if self.__opt_empty_from and not hfrom:
            hfrom = mfrom

        return hfrom

    def __process_rpath_data(self, rpath: str) -> str:
        rpath_parts = parse_email_details(rpath)
        rpath = rpath_parts['email_address']

        if self.__opt_decode_srs:
            rpath = remove_prvs(rpath)

        if self.__opt_decode_srs:
            rpath = convert_srs(rpath)

        return rpath

    def __process_alignment_data(self, hfrom: str, mfrom: str, message_size: int, subject: str):
        # Fat index for binding commonality
        sender_header_index = (mfrom, hfrom)
        self.__mfrom_hfrom_data.setdefault(sender_header_index, {})
        self.__update_message_size_and_subjects(self.__mfrom_hfrom_data[sender_header_index], message_size, subject)

    def __process_msgid_data(self, msgid: str, mfrom: str, message_size: int, subject: str) -> str:
        # Message ID is unique but often the sending host behind the @ symbol is unique to the application
        msgid_parts = parse_email_details(msgid)
        msgid_domain = ''
        msgid_host = ''

        if msgid_parts['email_address'] or '@' in msgid:
            # Use the extracted domain if available; otherwise, split the msgid
            domain = msgid_parts['domain'] if msgid_parts['domain'] else msgid.split('@')[-1]
            msgid_host = find_ip_in_text(domain)
            if not msgid_host:
                # Extract the components using tldextract
                extracted = tldextract.extract(domain)
                # Combine domain and suffix if the suffix is present
                msgid_domain = f"{extracted.domain}.{extracted.suffix}"
                msgid_host = extracted.subdomain

                # Adjust msgid_host and msgid_domain based on the presence of subdomain
                if not msgid_host and not extracted.suffix:
                    msgid_host = msgid_domain
                    msgid_domain = ''

        # Fat index for binding commonality
        mid_host_domain_index = (mfrom, msgid_host, msgid_domain)
        self.__msgid_data.setdefault(mid_host_domain_index, {})
        self.__update_message_size_and_subjects(self.__msgid_data[mid_host_domain_index], message_size, subject)

        return msgid

    def __update_message_size_and_subjects(self, data: Dict, message_size: int, subject: str):
        # Ensure the message_size list exists and append the new message size
        data.setdefault("message_size", []).append(message_size)

        if not self.__opt_sample_subject:
            return

        data.setdefault("subjects", [])

        # Avoid storing empty subject lines
        if not subject:
            return

        # Calculate probability based on the number of processed records
        probability = 1 / len(data['message_size'])

        # Ensure at least one subject is added if subjects array is empty
        if not data['subjects'] or random() < probability:
            data['subjects'].append(subject)

    def process_file(self, input_file):
        with (open(input_file, 'r', encoding='utf-8-sig') as input_file):
            reader = csv.reader(input_file)
            headers = next(reader)  # Read the first line which contains the headers
            self.__field_mapper.configure(headers)
            for csv_line in reader:
                self.__total_processed_count += 1

                # Make sure cast to int is valid, else 0 (size is required)
                message_size = self.__field_mapper.get_field(csv_line, 'msgsz')

                if message_size.isdigit():
                    message_size = int(message_size)
                else:
                    message_size = 0

                mfrom = self.__field_mapper.get_field(csv_line, 'mfrom').casefold().strip()
                mfrom = self.__process_mfrom_data(mfrom)

                # mfrom will be None, unless the filtering criteria applied properly
                if not mfrom:
                    continue

                subject = ''
                # Not all CSV files will contain a subject field.
                if self.__opt_sample_subject:
                    subject = self.__field_mapper.get_field(csv_line, 'subject').casefold().strip()

                # Track the cleaned, filtered mfrom data for our report
                self.__mfrom_data.setdefault(mfrom, {})
                self.__update_message_size_and_subjects(self.__mfrom_data[mfrom], message_size, subject)

                # Alignment will require that we have hfrom
                if self.__opt_gen_hfrom or self.__opt_gen_alignment:
                    hfrom = self.__field_mapper.get_field(csv_line, 'hfrom').casefold().strip()
                    hfrom = self.__process_hfrom_data(hfrom, mfrom)

                    # Generate data for HFrom
                    if self.__opt_gen_hfrom:
                        self.__hfrom_data.setdefault(hfrom, {})
                        self.__update_message_size_and_subjects(self.__hfrom_data[hfrom], message_size, subject)

                    # Generate data for HFrom and MFrom Alignment
                    if self.__opt_gen_alignment:
                        self.__process_alignment_data(hfrom, mfrom, message_size, subject)

                # Generate data for return path
                if self.__opt_gen_rpath:
                    rpath = self.__field_mapper.get_field(csv_line, 'rpath').casefold().strip()
                    rpath = self.__process_rpath_data(rpath)
                    if self.__opt_gen_rpath:
                        self.__rpath_data.setdefault(rpath, {})
                        self.__update_message_size_and_subjects(self.__rpath_data[rpath], message_size, subject)

                # Generate data for parsed message ID
                if self.__opt_gen_msgid:
                    # Generate data for Message ID information
                    msgid = self.__field_mapper.get_field(csv_line, 'msgid').casefold().strip('<>[] ')
                    self.__process_msgid_data(msgid, mfrom, message_size, subject)

                # Determine distinct dates of data, and count number of messages on that day
                date_str = self.__field_mapper.get_field(csv_line, 'date')
                date = datetime.datetime.strptime(date_str, self.__date_format)
                str_date = date.strftime('%Y-%m-%d')
                self.__date_counter[str_date] += 1

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

    # Setter for opt_gen_hfrom
    def set_opt_gen_hfrom(self, value):
        if isinstance(value, bool):
            self.__opt_gen_hfrom = value
        else:
            raise ValueError("opt_gen_hfrom must be a boolean")

    # Setter for opt_gen_alignment
    def set_opt_gen_alignment(self, value):
        if isinstance(value, bool):
            self.__opt_gen_alignment = value
        else:
            raise ValueError("opt_gen_alignment must be a boolean")

    # Setter for opt_gen_rpath
    def set_opt_gen_rpath(self, value):
        if isinstance(value, bool):
            self.__opt_gen_rpath = value
        else:
            raise ValueError("opt_gen_rpath must be a boolean")

    # Setter for opt_gen_msgid
    def set_opt_gen_msgid(self, value):
        if isinstance(value, bool):
            self.__opt_gen_msgid = value
        else:
            raise ValueError("opt_gen_msgid must be a boolean")

    # Setter for opt_sample_subject
    def set_opt_sample_subject(self, value):
        if isinstance(value, bool):
            self.__opt_sample_subject = value
        else:
            raise ValueError("opt_sample_subject must be a boolean")

    # Getter for opt_sample_subject
    def get_opt_sample_subject(self) -> bool:
        return self.__opt_sample_subject

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

    def set_subject_field(self, value: str) -> None:
        if isinstance(value, str):
            self.__subject_field = value
        else:
            raise ValueError("subject_field must be a string.")

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
