from senderstats.common.defaults import DEFAULT_DOMAIN_EXCLUSIONS


class ExclusionManager:
    def __init__(self, args):
        self.excluded_senders = self.__prepare_exclusions(args.exclude_senders)
        if not args.no_default_exclude_domains:
            args.excluded_domains = DEFAULT_DOMAIN_EXCLUSIONS + args.exclude_domains
        self.excluded_ips = self.__prepare_exclusions(args.exclude_ips)
        self.excluded_domains = self.__prepare_exclusions(args.exclude_domains)
        self.restricted_domains = self.__prepare_exclusions(args.restrict_domains)

    def __prepare_exclusions(self, exclusions):
        return sorted(list({item.casefold() for item in exclusions}))
