from senderstats.core.filters import *
from senderstats.processing.config_manager import ConfigManager


class FilterManager:
    def __init__(self, config: ConfigManager):
        self.exclude_empty_sender_filter = ExcludeEmptySenderFilter()
        self.exclude_invalid_size_filter = ExcludeInvalidSizeFilter()
        self.exclude_domain_filter = ExcludeDomainFilter(config.exclude_domains)
        self.exclude_ip_filter = ExcludeIPFilter(config.exclude_ips)
        self.exclude_senders_filter = ExcludeSenderFilter(config.exclude_senders)
        self.restrict_senders_filter = RestrictDomainFilter(config.restrict_domains)

    def display_summary(self):
        print()
        print("Messages excluded by empty sender:", self.exclude_empty_sender_filter.get_excluded_count())
        print("Messages excluded by invalid size:", self.exclude_invalid_size_filter.get_excluded_count())
        print("Messages excluded by IP address:", self.exclude_ip_filter.get_excluded_count())
        print("Messages excluded by domain:", self.exclude_domain_filter.get_excluded_count())
        print("Messages excluded by sender:", self.exclude_senders_filter.get_excluded_count())
        print("Messages excluded by constraint:", self.restrict_senders_filter.get_excluded_count())
