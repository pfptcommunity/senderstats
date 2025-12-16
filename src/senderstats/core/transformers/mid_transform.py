from senderstats.common.mid_parser import MIDParser
from senderstats.common.tld_parser import TLDParser
from senderstats.data.message_data import MessageData
from senderstats.interfaces.transform import Transform


class MIDTransform(Transform[MessageData, MessageData]):
    def __init__(self):
        super().__init__()
        self._mid_parser = MIDParser(TLDParser.load_default())

    def transform(self, data: MessageData) -> MessageData:
        mid_host_label, mid_subdomain, mid_domain, _ = self._mid_parser.parse(data.msgid)
        setattr(data, 'msgid_host', ".".join(s for s in [mid_host_label, mid_subdomain] if s))
        setattr(data, 'msgid_domain', mid_domain)
        return data
