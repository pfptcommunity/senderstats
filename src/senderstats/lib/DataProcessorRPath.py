from typing import Dict
from lib.DataProcessingStatus import DataProcessingStatus
from common.utils import parse_email_details, remove_prvs, convert_srs
from lib.DataProcessor import DataProcessor
from lib.DataRecord import DataRecord


class DataProcessorRPath(DataProcessor):
    __rpath_data: Dict[str, Dict]
    __decode_srs: bool
    __remove_prvs: bool

    def __init__(self, decode_srs: bool = False, remove_prvs: bool = False):
        super().__init__()
        self.__rpath_data: Dict[str, Dict] = {}
        self.__decode_srs = decode_srs
        self.__remove_prvs = remove_prvs

    def process(self, data_record: DataRecord) -> DataProcessingStatus:
        rpath = data_record.rpath

        rpath_parts = parse_email_details(rpath)
        rpath = rpath_parts['email_address']

        if self.__decode_srs:
            rpath = remove_prvs(rpath)

        if self.__decode_srs:
            rpath = convert_srs(rpath)

        self.__rpath_data.setdefault(rpath, {})

        self.update_message_size_and_subjects(self.__rpath_data[rpath], data_record.message_size, data_record.subject)

        data_record.rpath = rpath

        return DataProcessingStatus.CONTINUE
