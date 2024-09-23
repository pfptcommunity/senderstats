from typing import Dict

from tldextract import tldextract

from lib.DataProcessingStatus import DataProcessingStatus
from common.utils import parse_email_details, find_ip_in_text
from lib.DataProcessor import DataProcessor
from lib.DataRecord import DataRecord


class DataProcessorMessageID(DataProcessor):
    __msgid_data: Dict[tuple, Dict]

    def __init__(self):
        super().__init__()
        self.__msgid_data = dict()

    def process(self, data_record: DataRecord) -> DataProcessingStatus:
        msgid = data_record.msgid

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
        mid_host_domain_index = (data_record.mfrom, msgid_host, msgid_domain)
        self.__msgid_data.setdefault(mid_host_domain_index, {})

        self.update_message_size_and_subjects(self.__msgid_data[mid_host_domain_index], data_record.message_size, data_record.subject)

        return DataProcessingStatus.CONTINUE
