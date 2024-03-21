from xlsxwriter import Workbook
from xlsxwriter.format import Format
from xlsxwriter.worksheet import Worksheet

from senderstats.common.utils import average
from .MessageDataProcessor import MessageDataProcessor


class MessageDataReport:
    __threshold: int
    __data_processor: MessageDataProcessor
    __workbook: Workbook
    __header_format: Format
    __summary_format: Format
    __summary_values_format: Format
    __days: int

    def __init__(self, output_file: str, data_processor: MessageDataProcessor, threshold: int):
        self.__data_processor = data_processor
        self.__days = len(data_processor.get_date_counter())
        self.__threshold = threshold

        self.__workbook = Workbook(output_file)
        self.__summary_format = self.__workbook.add_format()
        self.__summary_format.set_bold()
        self.__summary_format.set_align('right')

        self.__summary_values_format = self.__workbook.add_format()
        self.__summary_values_format.set_align("right")

        self.__header_format = self.__workbook.add_format()
        self.__header_format.set_bold()

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
                    worksheet.write_string(row, col, data)
                    col += 1
            else:
                worksheet.write_string(row, col, k)
                col += 1

            messages_per_sender = len(v)
            total_bytes = sum(v)
            average_message_size = average(v)
            messages_per_sender_per_day = messages_per_sender / self.__days

            worksheet.write_number(row, col, messages_per_sender)
            col += 1
            worksheet.write_number(row, col, average_message_size)
            col += 1
            worksheet.write_number(row, col, messages_per_sender_per_day)
            col += 1
            worksheet.write_number(row, col, total_bytes)
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
        summary.write(9, 0, "Total Messages Message", self.__summary_format)
        summary.write(10, 0, "Total Average Message Size", self.__summary_format)
        summary.write(11, 0, "Total Peak Hourly Volume", self.__summary_format)

        # Total summary of App data
        summary.write_formula(0, 1,
                              "=IF(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)<1024,SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)&\" bytes\",IF(AND(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)>=1024,SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)<POWER(1024,2)),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/1024),1)&\" Kb\"),IF(AND(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)>=POWER(1024,2),SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)<POWER(1024,3)),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/POWER(1024,2)),1)&\" Mb\"),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/POWER(1024,3)),1)&\" Gb\"))))".format(
                                  threshold=self.__threshold),
                              self.__summary_values_format)

        summary.write_formula(1, 1, "=SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!B:B)".format(
            threshold=self.__threshold),
                              self.__summary_values_format)

        summary.write_formula(2, 1,
                              "=ROUNDUP((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!B:B))/1024,0)&\" Kb\"".format(
                                  threshold=self.__threshold),
                              self.__summary_values_format)

        summary.write_formula(3, 1,
                              "=ROUNDUP(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!B:B)/{days}/8,0)".format(
                                  days=self.__days,
                                  threshold=self.__threshold),
                              self.__summary_values_format)

        # 30 day calculation divide total days * 30
        summary.write_formula(5, 1,
                              "=IF(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30<1024,SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30&\" bytes\",IF(AND(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30>=1024,SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30<POWER(1024,2)),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30/1024),1)&\" Kb\"),IF(AND(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30>=POWER(1024,2),SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30<POWER(1024,3)),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30/POWER(1024,2)),1)&\" Mb\"),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30/POWER(1024,3)),1)&\" Gb\"))))".format(
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
                              "=IF(SUM('Envelope Senders'!E:E)<1024,SUM('Envelope Senders'!E:E)&\" bytes\",IF(AND(SUM('Envelope Senders'!E:E)>=1024,SUM('Envelope Senders'!E:E)<POWER(1024,2)),(ROUND((SUM('Envelope Senders'!E:E)/1024),1)&\" Kb\"),IF(AND(SUM('Envelope Senders'!E:E)>=POWER(1024,2),SUM('Envelope Senders'!E:E)<POWER(1024,3)),(ROUND((SUM('Envelope Senders'!E:E)/POWER(1024,2)),1)&\" Mb\"),(ROUND((SUM('Envelope Senders'!E:E)/POWER(1024,3)),1)&\" Gb\"))))",
                              self.__summary_values_format)
        summary.write_formula(9, 1, "=SUM('Envelope Senders'!B:B)", self.__summary_values_format)

        summary.write_formula(10, 1,
                              "=ROUNDUP((SUM('Envelope Senders'!E:E)/SUM('Envelope Senders'!B:B))/1024,0)&\" Kb\"".format(
                                  threshold=self.__threshold),
                              self.__summary_values_format)

        summary.write_formula(11, 1,
                              "=ROUNDUP(SUM('Envelope Senders'!B:B)/{days}/8,0)".format(
                                  days=self.__days,
                                  threshold=self.__threshold),
                              self.__summary_values_format)
        summary.autofit()

    def create_mfrom_summary(self):
        sender_sheet = self.__workbook.add_worksheet("Envelope Senders")
        self.__write_headers(sender_sheet,
                             ['MFrom', 'Messages', 'Size', 'Messages Per Day', 'Total Bytes'])
        self.__write_data(sender_sheet, self.__data_processor.get_mfrom_data())
        sender_sheet.autofit()

    def create_hfrom_summary(self):
        from_sheet = self.__workbook.add_worksheet("Header From")
        self.__write_headers(from_sheet,
                             ['HFrom', 'Messages', 'Size', 'Messages Per Day', 'Total Bytes'])
        self.__write_data(from_sheet, self.__data_processor.get_hfrom_data())
        from_sheet.autofit()

    def create_rpath_summary(self):
        return_sheet = self.__workbook.add_worksheet("Return Path")
        self.__write_headers(return_sheet,
                             ['RPath', 'Messages', 'Size', 'Messages Per Day', 'Total Bytes'])
        self.__write_data(return_sheet, self.__data_processor.get_rpath_data())
        return_sheet.autofit()

    def create_msgid_summary(self):
        mid_sheet = self.__workbook.add_worksheet("MFrom + Message ID")
        self.__write_headers(mid_sheet,
                             ['MFrom', 'Message ID Host', 'Message ID Domain', 'Messages', 'Size',
                              'Messages Per Day', 'Total Bytes'])
        self.__write_data(mid_sheet, self.__data_processor.get_msgid_data())
        mid_sheet.autofit()

    def create_mfrom_hfrom_summary(self):
        sender_from_sheet = self.__workbook.add_worksheet("MFrom + HFrom (Alignment)")
        self.__write_headers(sender_from_sheet,
                             ['MFrom', 'HFrom', 'Messages', 'Size', 'Messages Per Day', 'Total Bytes'])
        self.__write_data(sender_from_sheet, self.__data_processor.get_mfrom_hfrom_data())
        sender_from_sheet.autofit()

    def generate_report(self):
        self.create_sizing_summary()
        # Only create a worksheet for data that exists
        if self.__data_processor.get_mfrom_data():
            self.create_mfrom_summary()
        if self.__data_processor.get_hfrom_data():
            self.create_hfrom_summary()
        if self.__data_processor.get_rpath_data():
            self.create_rpath_summary()
        if self.__data_processor.get_msgid_data():
            self.create_msgid_summary()
        if self.__data_processor.get_mfrom_hfrom_data():
            self.create_mfrom_hfrom_summary()
