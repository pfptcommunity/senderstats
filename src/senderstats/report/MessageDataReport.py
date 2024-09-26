from typing import TypeVar

from xlsxwriter import Workbook
from xlsxwriter.format import Format
from xlsxwriter.worksheet import Worksheet

from data.processors import AlignmentProcessor, HFromProcessor, MFromProcessor, MIDProcessor, RPathProcessor
from data.processors.DateProcessor import DateProcessor
from senderstats.common.utils import average

TMessageProcessor = TypeVar('TMessageProcessor', AlignmentProcessor, HFromProcessor, MFromProcessor, MIDProcessor,
                            RPathProcessor)


class MessageDataReport:
    __threshold: int
    __days: int
    __workbook: Workbook
    __header_format: Format
    __summary_format: Format
    __summary_values_format: Format
    __subject_format: Format
    __data_cell_format: Format

    def __init__(self, output_file: str, threshold: int, days: int):
        #self.__threshold = threshold
        self.__days = days
        self.__threshold = threshold

        self.__workbook = Workbook(output_file)
        self.__summary_format = self.__workbook.add_format()
        self.__summary_format.set_bold()
        self.__summary_format.set_align('right')

        self.__summary_values_format = self.__workbook.add_format()
        self.__summary_values_format.set_align('right')

        self.__header_format = self.__workbook.add_format()
        self.__header_format.set_bold()

        self.__data_cell_format = self.__workbook.add_format({'valign': 'top'})

        self.__subject_format = self.__workbook.add_format({'text_wrap': True})

    def close(self):
        self.__workbook.close()

    def __write_headers(self, worksheet: Worksheet, headers: list):
        """
        Writes data to an Excel worksheet with given headers and applies formatting.

        :param worksheet: The worksheet object to write data into.
        :param headers: The list of header fields to create
        :param header_format: Formatting for the cell eg. bold, italic, underline
        """
        row = 0
        for col, header in enumerate(headers):
            worksheet.write(row, col, header, self.__header_format)

    def __write_data(self, worksheet: Worksheet, data: dict):
        """
        Writes data to an Excel worksheet with given headers and applies formatting.

        :param worksheet: The worksheet object to write data into.
        :param data: The data to write, expected to be a dictionary.
        :param days: Number of days, used for some calculations in cells.
        """
        row = 1
        for k, v in data.items():
            col = 0
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
            row += 1

    def create_sizing_summary(self):
        summary = self.__workbook.add_worksheet("Summary")

        summary.write(0, 0, "Estimated App Data ({} days)".format(self.__days), self.__summary_format)
        summary.write(1, 0, "Estimated App Messages ({} days)".format(self.__days), self.__summary_format)
        summary.write(2, 0, "Estimated App Average Message Size ({} days)".format(self.__days), self.__summary_format)
        summary.write(3, 0, "Estimated App Peak Hourly Volume ({} days)".format(self.__days), self.__summary_format)

        summary.write(5, 0, "Estimated Monthly App Data", self.__summary_format)
        summary.write(6, 0, "Estimated Monthly App Messages", self.__summary_format)

        summary.write(8, 0, "Total Outbound Data", self.__summary_format)
        summary.write(9, 0, "Total Messages", self.__summary_format)
        summary.write(10, 0, "Total Average Message Size", self.__summary_format)
        summary.write(11, 0, "Total Peak Hourly Volume", self.__summary_format)

        # Total summary of App data
        summary.write_formula(0, 1,
                              "=IF(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)<1024,SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)&\" B\",IF(AND(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)>=1024,SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)<POWER(1024,2)),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/1024),1)&\" KB\"),IF(AND(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)>=POWER(1024,2),SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)<POWER(1024,3)),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/POWER(1024,2)),1)&\" MB\"),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/POWER(1024,3)),1)&\" GB\"))))".format(
                                  threshold=self.__threshold),
                              self.__summary_values_format)

        summary.write_formula(1, 1, "=SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!B:B)".format(
            threshold=self.__threshold),
                              self.__summary_values_format)

        summary.write_formula(2, 1,
                              "=ROUNDUP((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!B:B))/1024,0)&\" KB\"".format(
                                  threshold=self.__threshold),
                              self.__summary_values_format)

        summary.write_formula(3, 1,
                              "=ROUNDUP(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!B:B)/{days}/8,0)".format(
                                  days=self.__days,
                                  threshold=self.__threshold),
                              self.__summary_values_format)

        # 30 day calculation divide total days * 30
        summary.write_formula(5, 1,
                              "=IF(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30<1024,SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30&\" B\",IF(AND(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30>=1024,SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30<POWER(1024,2)),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30/1024),1)&\" KB\"),IF(AND(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30>=POWER(1024,2),SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30<POWER(1024,3)),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30/POWER(1024,2)),1)&\" MB\"),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30/POWER(1024,3)),1)&\" GB\"))))".format(
                                  days=self.__days,
                                  threshold=self.__threshold),
                              self.__summary_values_format)

        summary.write_formula(6, 1,
                              "=ROUNDUP(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!B:B)/{days}*30,0)".format(
                                  days=self.__days,
                                  threshold=self.__threshold),
                              self.__summary_values_format)

        # Total message volume data
        summary.write_formula(8, 1,
                              "=IF(SUM('Envelope Senders'!E:E)<1024,SUM('Envelope Senders'!E:E)&\" B\",IF(AND(SUM('Envelope Senders'!E:E)>=1024,SUM('Envelope Senders'!E:E)<POWER(1024,2)),(ROUND((SUM('Envelope Senders'!E:E)/1024),1)&\" KB\"),IF(AND(SUM('Envelope Senders'!E:E)>=POWER(1024,2),SUM('Envelope Senders'!E:E)<POWER(1024,3)),(ROUND((SUM('Envelope Senders'!E:E)/POWER(1024,2)),1)&\" MB\"),(ROUND((SUM('Envelope Senders'!E:E)/POWER(1024,3)),1)&\" GB\"))))",
                              self.__summary_values_format)
        summary.write_formula(9, 1, "=SUM('Envelope Senders'!B:B)", self.__summary_values_format)

        summary.write_formula(10, 1,
                              "=ROUNDUP((SUM('Envelope Senders'!E:E)/SUM('Envelope Senders'!B:B))/1024,0)&\" KB\"".format(
                                  threshold=self.__threshold),
                              self.__summary_values_format)

        summary.write_formula(11, 1,
                              "=MAX('Hourly Metrics'!B:B)".format(
                                  days=self.__days,
                                  threshold=self.__threshold),
                              self.__summary_values_format)
        summary.autofit()

    def create_summary(self, processor: TMessageProcessor):
        if hasattr(processor, 'sheet_name') and hasattr(processor, 'headers') and hasattr(processor, 'is_sample_subject'):
            sheet = self.__workbook.add_worksheet(processor.sheet_name)

            if processor.is_sample_subject():
                processor.headers.append('Subjects')

            self.__write_headers(sheet, processor.headers)
            self.__write_data(sheet, processor.get_data())
            sheet.autofit()

    def create_hourly_summary(self, processor: DateProcessor):
            sheet = self.__workbook.add_worksheet("Hourly Metrics")
            self.__write_headers(sheet, ['Date','Messages'])
            row = 1
            for k, v in processor.get_hourly_counter().items():
                sheet.write_string(row, 0, k, self.__data_cell_format)
                sheet.write_number(row, 1, v, self.__data_cell_format)
                row += 1
            sheet.autofit()