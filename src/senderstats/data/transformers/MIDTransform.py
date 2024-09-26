from typing import TypeVar, Generic

from tldextract import tldextract

from common.utils import parse_email_details, find_ip_in_text
from data.MessageData import MessageData
from data.common.Transform import Transform

TMessageData = TypeVar('TMessageData', bound=MessageData)


class MIDTransform(Transform[MessageData], Generic[TMessageData]):
    def __init__(self):
        super().__init__()

    def transform(self, data: TMessageData) -> TMessageData:
        msgid = data.msgid

        # Message ID is unique but often the sending host behind the @ symbol is unique to the application
        msgid_parts = parse_email_details(msgid)

        if msgid_parts['email_address'] or '@' in msgid:
            # Use the extracted domain if available; otherwise, split the msgid
            domain = msgid_parts['domain'] if msgid_parts['domain'] else msgid.split('@')[-1]
            data.msgid_host = find_ip_in_text(domain)
            if not data.msgid_host:
                # Extract the components using tldextract
                extracted = tldextract.extract(domain)
                # Combine domain and suffix if the suffix is present
                data.msgid_domain = f"{extracted.domain}.{extracted.suffix}"
                data.msgid_host = extracted.subdomain

                # Adjust msgid_host and msgid_domain based on the presence of subdomain
                if not data.msgid_host and not extracted.suffix:
                    data.msgid_host = data.msgid_domain
                    data.msgid_domain = ''

        return data
