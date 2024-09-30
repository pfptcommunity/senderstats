from typing import TypeVar

from xlsxwriter import Workbook
from xlsxwriter.format import Format
from xlsxwriter.worksheet import Worksheet

from senderstats.common.utils import average
from senderstats.data.processors import AlignmentProcessor, HFromProcessor, MFromProcessor, MIDProcessor, RPathProcessor
from senderstats.data.processors.DateProcessor import DateProcessor

TMessageProcessor = TypeVar('TMessageProcessor', AlignmentProcessor, HFromProcessor, MFromProcessor, MIDProcessor,
                            RPathProcessor)


class MessageDataReport:
    __threshold: int
    __days: int
    __workbook: Workbook
    __header_format: Format
    __summary_format: Format
    __summary_values_format: Format
    __summary_highlight_format: Format
    __summary_highlight_values: Format
    __subject_format: Format
    __data_cell_format: Format
    __field_values_format: Format

    def __init__(self, output_file: str, threshold: int, days: int):
        self.__threshold = threshold
        self.__days = days
        self.__workbook = Workbook(output_file)
        self.__initialize_formats()

    def __initialize_formats(self):
        self.__header_format = self.__workbook.add_format({'bold': True})
        self.__summary_format = self.__workbook.add_format({'bold': True, 'align': 'right', 'hidden': True})
        self.__summary_highlight_format = self.__workbook.add_format(
            {'bold': True, 'align': 'right', 'hidden': True, 'bg_color': '#FFFF00'})
        self.__summary_values_format = self.__workbook.add_format({'align': 'right', 'hidden': True})
        self.__summary_highlight_values = self.__workbook.add_format(
            {'align': 'right', 'hidden': True, 'bg_color': '#FFFF00'})
        self.__field_values_format = self.__workbook.add_format({'locked': False, 'hidden': True})
        self.__data_cell_format = self.__workbook.add_format({'valign': 'top'})
        self.__subject_format = self.__workbook.add_format({'text_wrap': True})

    def close(self):
        self.__workbook.close()

    def __write_headers(self, worksheet: Worksheet, headers: list):
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, self.__header_format)

    def __write_data(self, worksheet: Worksheet, data: dict):
        row = 1
        for k, v in data.items():
            col = 0
            if isinstance(k, tuple):
                for item in k:
                    worksheet.write_string(row, col, item, self.__data_cell_format)
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
            row += 1

    def create_sizing_summary(self):
        summary = self.__workbook.add_worksheet("Summary")
        summary.protect()

        summary.write(0, 0, f"Estimated App Data ({self.__days} days)", self.__summary_format)
        summary.write(1, 0, f"Estimated App Messages ({self.__days} days)", self.__summary_format)
        summary.write(2, 0, f"Estimated App Average Message Size ({self.__days} days)", self.__summary_format)
        summary.write(3, 0, f"Estimated App Peak Hourly Volume ({self.__days} days)", self.__summary_format)

        summary.write(5, 0, "Estimated Monthly App Data", self.__summary_highlight_format)
        summary.write(6, 0, "Estimated Monthly App Messages", self.__summary_highlight_format)

        summary.write(8, 0, "Total Outbound Data", self.__summary_format)
        summary.write(9, 0, "Total Messages", self.__summary_format)
        summary.write(10, 0, "Total Average Message Size", self.__summary_format)
        summary.write(11, 0, "Total Peak Hourly Volume", self.__summary_format)

        summary.write(13, 0, 'App Email Threshold (Enter number between 1 and 10):', self.__summary_format)
        summary.write_number(13, 1, self.__threshold, self.__field_values_format)
        summary.set_column(1, 1, 25)

        summary.data_validation(13, 1, 13, 1, {'validate': 'integer', 'criteria': '>', 'value': 0})

        summary.write_formula(0, 1, self.__get_data_formula('E', 'B', 'B14', '1024', 'KB', 'MB', 'GB'),
                              self.__summary_values_format)
        summary.write_formula(1, 1, f"=SUMIF('Envelope Senders'!D:D,\">=\"&B14,'Envelope Senders'!B:B)",
                              self.__summary_values_format)
        summary.write_formula(2, 1, self.__get_average_message_size_formula('E', 'B', 'B14', '1024', 'KB'),
                              self.__summary_values_format)
        summary.write_formula(3, 1,
                              f"=ROUNDUP(SUMIF('Envelope Senders'!D:D,\">=\"&B14,'Envelope Senders'!B:B)/{self.__days}/8,0)",
                              self.__summary_values_format)
        summary.write_formula(5, 1, self.__get_data_formula('E', 'B', 'B14', '1024', 'KB', 'MB', 'GB', monthly=True),
                              self.__summary_highlight_values)
        summary.write_formula(6, 1,
                              f"=ROUNDUP(SUMIF('Envelope Senders'!D:D,\">=\"&B14,'Envelope Senders'!B:B)/{self.__days}*30,0)",
                              self.__summary_highlight_values)
        summary.write_formula(8, 1, self.__get_total_data_formula('E', '1024', 'KB', 'MB', 'GB'),
                              self.__summary_values_format)
        summary.write_formula(9, 1, "=SUM('Envelope Senders'!B:B)", self.__summary_values_format)
        summary.write_formula(10, 1, self.__get_average_message_size_formula('E', 'B', 'B14', '1024', 'KB'),
                              self.__summary_values_format)
        summary.write_formula(11, 1, "=MAX('Hourly Metrics'!B:B)", self.__summary_values_format)
        summary.autofit()

    def __get_data_formula(self, col_data, col_messages, threshold_cell, unit, unit_kb, unit_mb, unit_gb,
                           monthly=False):
        days_multiplier = f"/{self.__days}*30" if monthly else ""
        return f"""=IF(SUMIF('Envelope Senders'!D:D,\">=\"&{threshold_cell},'Envelope Senders'!{col_data}:{col_data}){days_multiplier}<1024,
                        SUMIF('Envelope Senders'!D:D,\">=\"&{threshold_cell},'Envelope Senders'!{col_data}:{col_data}){days_multiplier}&" B",
                        IF(AND(SUMIF('Envelope Senders'!D:D,\">=\"&{threshold_cell},'Envelope Senders'!{col_data}:{col_data}){days_multiplier}>=1024,
                               SUMIF('Envelope Senders'!D:D,\">=\"&{threshold_cell},'Envelope Senders'!{col_data}:{col_data}){days_multiplier}<POWER(1024,2)),
                           (ROUND((SUMIF('Envelope Senders'!D:D,\">=\"&{threshold_cell},'Envelope Senders'!{col_data}:{col_data}){days_multiplier}/1024),1)&" {unit_kb}"),
                           IF(AND(SUMIF('Envelope Senders'!D:D,\">=\"&{threshold_cell},'Envelope Senders'!{col_data}:{col_data}){days_multiplier}>=POWER(1024,2),
                                  SUMIF('Envelope Senders'!D:D,\">=\"&{threshold_cell},'Envelope Senders'!{col_data}:{col_data}){days_multiplier}<POWER(1024,3)),
                               (ROUND((SUMIF('Envelope Senders'!D:D,\">=\"&{threshold_cell},'Envelope Senders'!{col_data}:{col_data}){days_multiplier}/POWER(1024,2)),1)&" {unit_mb}"),
                               (ROUND((SUMIF('Envelope Senders'!D:D,\">=\"&{threshold_cell},'Envelope Senders'!{col_data}:{col_data}){days_multiplier}/POWER(1024,3)),1)&" {unit_gb}"))))"""

    def __get_average_message_size_formula(self, col_data, col_messages, threshold_cell, unit, unit_kb):
        return f"""=ROUNDUP((SUMIF('Envelope Senders'!D:D,\">=\"&{threshold_cell},'Envelope Senders'!{col_data}:{col_data})/
                              SUMIF('Envelope Senders'!D:D,\">=\"&{threshold_cell},'Envelope Senders'!{col_messages}:{col_messages}))/1024,0)&" {unit_kb}" """

    def __get_total_data_formula(self, col_data, unit, unit_kb, unit_mb, unit_gb):
        return f"""=IF(SUM('Envelope Senders'!{col_data}:{col_data})<1024,
                        SUM('Envelope Senders'!{col_data}:{col_data})&" B",
                        IF(AND(SUM('Envelope Senders'!{col_data}:{col_data})>=1024,
                               SUM('Envelope Senders'!{col_data}:{col_data})<POWER(1024,2)),
                           (ROUND((SUM('Envelope Senders'!{col_data}:{col_data})/1024),1)&" {unit_kb}"),
                           IF(AND(SUM('Envelope Senders'!{col_data}:{col_data})>=POWER(1024,2),
                                  SUM('Envelope Senders'!{col_data}:{col_data})<POWER(1024,3)),
                               (ROUND((SUM('Envelope Senders'!{col_data}:{col_data})/POWER(1024,2)),1)&" {unit_mb}"),
                               (ROUND((SUM('Envelope Senders'!{col_data}:{col_data})/POWER(1024,3)),1)&" {unit_gb}"))))"""

    def create_summary(self, processor: TMessageProcessor):
        if hasattr(processor, 'sheet_name') and hasattr(processor, 'headers') and hasattr(processor,
                                                                                          'is_sample_subject'):
            sheet = self.__workbook.add_worksheet(processor.sheet_name)

            if processor.is_sample_subject():
                processor.headers.append('Subjects')

            self.__write_headers(sheet, processor.headers)
            self.__write_data(sheet, processor.get_data())
            sheet.autofit()

    def create_hourly_summary(self, processor: DateProcessor):
        sheet = self.__workbook.add_worksheet("Hourly Metrics")
        self.__write_headers(sheet, ['Date', 'Messages'])
        row = 1
        for k, v in processor.get_hourly_counter().items():
            sheet.write_string(row, 0, k, self.__data_cell_format)
            sheet.write_number(row, 1, v, self.__data_cell_format)
            row += 1
        sheet.autofit()
