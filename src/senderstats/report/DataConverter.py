from collections.abc import dict_items
from typing import TypeVar, Generic

from data.common.Transform import Transform

TProcessorData = TypeVar('TProcessorData', bound=dict_items)


class DataConverter(Transform[TProcessorData], Generic[TProcessorData]):
    def __init__(self):
        super().__init__()

    def transform(self, data: TProcessorData) -> TProcessorData:

        # If it contains a fat index
        if isinstance(k, tuple):
            # Write the fat index to columns
            for data in k:
                worksheet.write_string(row, col, data, self.__data_cell_format)
                col += 1
        else:
            worksheet.write_string(row, col, k, self.__data_cell_format)
            col += 1

        messages_per_sender = len(v['message_size'])
        total_bytes = sum(v['message_size'])
        average_message_size = average(v['message_size'])
        messages_per_sender_per_day = messages_per_sender / self.__days

        worksheet.write_number(row, col, messages_per_sender, self.__data_cell_format)
        col += 1
        worksheet.write_number(row, col, average_message_size, self.__data_cell_format)
        col += 1
        worksheet.write_number(row, col, messages_per_sender_per_day, self.__data_cell_format)
        col += 1
        worksheet.write_number(row, col, total_bytes, self.__data_cell_format)
        if 'subjects' in v:
            col += 1
            worksheet.write_string(row, col, '\n'.join(v['subjects']), self.__subject_format)

        return data
