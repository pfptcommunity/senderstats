from senderstats.core.filters import *
from senderstats.processing.ExclusionManager import ExclusionManager


class FilterManager:
    def __init__(self, exclusion_manager: ExclusionManager):
        self.exclude_empty_sender_filter = ExcludeEmptySenderFilter()
        self.exclude_domain_filter = ExcludeDomainFilter(exclusion_manager.excluded_domains)
        self.exclude_senders_filter = ExcludeSenderFilter(exclusion_manager.excluded_senders)
        self.restrict_senders_filter = RestrictDomainFilter(exclusion_manager.restricted_domains)
