from senderstats.common.defaults import DEFAULT_DOMAIN_EXCLUSIONS


class ExclusionManager:
    def __init__(self, args):
        self.excluded_senders = self.__prepare_exclusions(args.excluded_senders)
        self.excluded_domains = self.__prepare_exclusions(DEFAULT_DOMAIN_EXCLUSIONS + args.excluded_domains)
        self.restricted_domains = self.__prepare_exclusions(args.restricted_domains)

    def __prepare_exclusions(self, exclusions):
        return sorted(list({item.casefold() for item in exclusions}))