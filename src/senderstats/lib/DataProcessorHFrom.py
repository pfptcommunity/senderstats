from typing import Dict
from lib.DataProcessingStatus import DataProcessingStatus
from common.utils import parse_email_details
from lib.DataProcessor import DataProcessor
from lib.DataRecord import DataRecord


class DataProcessorHFrom(DataProcessor):
    __hfrom_data: Dict[str, Dict]
    __no_display: bool
    __empty_from: bool

    def __init__(self, no_display: bool = False, empty_from: bool = False):
        super().__init__()
        self.__hfrom_data: Dict[str, Dict] = {}
        self.__no_display = no_display
        self.__empty_from = empty_from

    def process(self, data_record: DataRecord) -> DataProcessingStatus:
        hfrom = data_record.hfrom
        hfrom_parts = parse_email_details(hfrom)

        if self.__no_display:
            hfrom = hfrom_parts['email_address']

        # If header from is empty, we will use env_sender
        if self.__empty_from and not data_record.hfrom:
            hfrom = data_record.mfrom

        # Generate data for HFrom
        self.__hfrom_data.setdefault(hfrom, {})
        self.update_message_size_and_subjects(self.__hfrom_data[hfrom], data_record.message_size, data_record.subject)

        data_record.hfrom = hfrom

        return DataProcessingStatus.CONTINUE
