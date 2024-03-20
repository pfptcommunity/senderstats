import csv
import datetime
from collections import defaultdict

from tldextract import tldextract

from utils import convert_srs, strip_prvs, get_email_domain, get_message_id_host, \
    strip_display_names  # Assuming these functions are defined


class MessageDataProcessor:
    def __init__(self):
        self.days = None
        self.date_format = None
        self.date_field = None
        self.size_field = None
        self.no_display = None
        self.return_field = None
        self.from_field = None
        self.skipped_domain_set = None
        self.restricted_domains_pattern = None
        self.excluded_domains_pattern = None
        self.no_empty_from = None
        self.mid_field = None
        self.decode_srs = None
        self.sender_field = None
        self.strip_prvs = None
        self.total = 0
        self.dates = {}
        self.sender_data = {}
        self.from_data = {}
        self.return_data = {}
        self.sender_from_data = {}
        self.mid_data = {}
        self.excluded_senders = defaultdict(int)
        self.excluded_domains = defaultdict(int)
        self.restricted_domains = defaultdict(int)
        self.empty_senders = 0

    def process_file(self, input_file):
        with open(input_file, 'r', encoding='utf-8-sig') as input_file:
            reader = csv.DictReader(input_file)
            for line in reader:
                self.total += 1
                env_sender = line[self.sender_field].casefold().strip()

                # Skip Empty Sender
                if not env_sender:
                    self.empty_senders += 1
                    continue

                if self.decode_srs:
                    env_sender = convert_srs(env_sender)

                if self.strip_prvs:
                    env_sender = strip_prvs(env_sender)

                # Exclude a specific sender highest priority
                if env_sender in self.skipped_domain_set:
                    self.excluded_senders[env_sender] += 1
                    continue

                # Deal with all the records we don't want to process based on sender.
                if self.excluded_domains_pattern.search(env_sender):
                    domain = get_email_domain(env_sender)
                    self.excluded_domains[domain] += 1
                    continue

                # Limit processing to only domains on in a list
                if not self.restricted_domains_pattern.search(env_sender):
                    domain = get_email_domain(env_sender)
                    self.restricted_domains[domain] += 1
                    continue

                header_from = line[self.from_field].casefold().strip()

                return_path = line[self.return_field].casefold().strip()

                if self.decode_srs:
                    return_path = strip_prvs(return_path)

                if self.strip_prvs:
                    return_path = convert_srs(return_path)

                # Message ID is unique but often the sending host behind the @ symbol is unique to the application
                message_id = line[self.mid_field].casefold().strip()
                message_id_domain = get_message_id_host(message_id)
                message_id_domain_extract = tldextract.extract(message_id_domain)
                message_id_host = message_id_domain_extract.subdomain
                message_id_domain = message_id_domain_extract.domain
                message_id_domain_suffix = message_id_domain_extract.suffix

                # If header from is empty, we will use env_sender
                if self.no_empty_from and not header_from:
                    header_from = env_sender

                # Add domain suffix to TLD
                if message_id_domain_suffix:
                    message_id_domain += '.' + message_id_domain_suffix

                if not message_id_host and not message_id_domain_suffix:
                    message_id_host = message_id_domain
                    message_id_domain = ''

                if self.no_display:
                    header_from = strip_display_names(header_from)

                # Determine distinct dates of data, and count number of messages on that day
                date = datetime.datetime.strptime(line[self.date_field], self.date_format)
                if date.strftime('%Y-%m-%d') not in self.dates:
                    self.dates[date.strftime('%Y-%m-%d')] = 0

                self.dates[date.strftime('%Y-%m-%d')] += 1

                message_size = line[self.size_field]

                # Make sure cast to int is valid, else 0
                if message_size.isdigit():
                    message_size = int(message_size)
                else:
                    message_size = 0

                self.sender_data.setdefault(env_sender, []).append(message_size)
                self.from_data.setdefault(header_from, []).append(message_size)
                self.return_data.setdefault(return_path, []).append(message_size)

                # Fat index for binding commonality
                mid_host_domain_index = (header_from, message_id_host, message_id_domain)
                self.mid_data.setdefault(mid_host_domain_index, []).append(message_size)

                # Fat index for binding commonality
                sender_header_index = (env_sender, header_from)
                self.sender_from_data.setdefault(sender_header_index, []).append(message_size)

        self.days = len(self.dates)
