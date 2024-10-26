from senderstats.common.Config import Config
from senderstats.core.filters import *


class FilterManager:
    def __init__(self, config: Config):
        self.exclude_empty_sender_filter = ExcludeEmptySenderFilter()
        self.exclude_invalid_size_filter = ExcludeInvalidSizeFilter()
        self.exclude_domain_filter = ExcludeDomainFilter(config.exclude_domains)
        self.exclude_ip_filter = ExcludeIPFilter(config.exclude_ips)
        self.exclude_senders_filter = ExcludeSenderFilter(config.exclude_senders)
        self.restrict_senders_filter = RestrictDomainFilter(config.restrict_domains)
