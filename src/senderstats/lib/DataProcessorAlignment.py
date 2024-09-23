from typing import Dict
from lib.DataProcessingStatus import DataProcessingStatus
from lib.DataProcessor import DataProcessor
from lib.DataRecord import DataRecord


class DataProcessorAlignment(DataProcessor):
    __alignment_data: Dict[tuple, Dict]

    def __init__(self):
        super().__init__()
        self.__alignment_data = dict()

    def process(self, data_record: DataRecord) -> DataProcessingStatus:
        # Fat index for binding commonality
        sender_header_index = (data_record.mfrom, data_record.hfrom)
        self.__alignment_data.setdefault(sender_header_index, {})
        self.update_message_size_and_subjects(self.__alignment_data[sender_header_index], data_record.message_size, data_record.subject)

        return DataProcessingStatus.CONTINUE
