from xlsxwriter import Workbook
from xlsxwriter.format import Format
from xlsxwriter.worksheet import Worksheet

from senderstats.reporting.FormatManager import FormatManager
from senderstats.common.utils import average
from senderstats.core.processors import DateProcessor
from senderstats.processing import PipelineProcessor


class PipelineProcessorReport:
    def __init__(self, output_file: str, pipeline_processor: PipelineProcessor):
        self.__threshold = 100
        self.__output_file = output_file
        self.__workbook = Workbook(output_file)
        self.__format_manager = FormatManager(self.__workbook)
        self.__pipeline_processor = pipeline_processor
        self.__days = len(pipeline_processor._processor_manager.date_processor.get_date_counter())


    def close(self):
        self.__workbook.close()
        print()
        print("Please see report: {}".format(self.__output_file))

    def __write_headers(self, worksheet: Worksheet, headers: list):
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, self.__format_manager.header_format)

    def __write_data(self, worksheet: Worksheet, data: dict):
        row = 1
        for k, v in data.items():
            col = 0
            if isinstance(k, tuple):
                for item in k:
                    worksheet.write_string(row, col, item, self.__format_manager.data_cell_format)
                    col += 1
            else:
                worksheet.write_string(row, col, k, self.__format_manager.data_cell_format)
                col += 1

            messages_per_sender = len(v['message_size'])
            total_bytes = sum(v['message_size'])
            average_message_size = average(v['message_size'])
            messages_per_sender_per_day = messages_per_sender / self.__days

            worksheet.write_number(row, col, messages_per_sender, self.__format_manager.data_cell_format)
            col += 1
            worksheet.write_number(row, col, average_message_size, self.__format_manager.data_cell_format)
            col += 1
            worksheet.write_number(row, col, messages_per_sender_per_day, self.__format_manager.data_cell_format)
            col += 1
            worksheet.write_number(row, col, total_bytes, self.__format_manager.data_cell_format)
            if 'subjects' in v:
                col += 1
                worksheet.write_string(row, col, '\n'.join(v['subjects']), self.__format_manager.subject_format)
            row += 1

    def create_sizing_summary(self):
        summary = self.__workbook.add_worksheet("Summary")
        summary.protect()

        summary.write(0, 0, f"Estimated App Data ({self.__days} days)", self.__format_manager.summary_format)
        summary.write(1, 0, f"Estimated App Messages ({self.__days} days)", self.__format_manager.summary_format)
        summary.write(2, 0, f"Estimated App Average Message Size ({self.__days} days)", self.__format_manager.summary_format)

        summary.write(4, 0, "Estimated Monthly App Data", self.__format_manager.summary_highlight_format)
        summary.write(5, 0, "Estimated Monthly App Messages", self.__format_manager.summary_highlight_format)
        summary.write(6, 0, "Estimated Monthly App Message Size", self.__format_manager.summary_highlight_format)

        summary.write(8, 0, "Total Data", self.__format_manager.summary_format)
        summary.write(9, 0, "Total Messages", self.__format_manager.summary_format)
        summary.write(10, 0, "Total Average Message Size", self.__format_manager.summary_format)
        summary.write(11, 0, "Total Peak Hourly Volume", self.__format_manager.summary_format)

        summary.write(13, 0, 'App Email Threshold (Number must be >= 0):', self.__format_manager.summary_format)
        summary.write_number(13, 1, self.__threshold, self.__format_manager.field_values_format)
        summary.set_column(1, 1, 25)

        summary.data_validation(13, 1, 13, 1, {'validate': 'integer', 'criteria': '>=', 'value': 0})

        # Based on daily message volume being over a threshold N
        summary.write_formula(0, 1, self.__get_conditional_size('Envelope Senders', 'D', 'E', 'B14'),
                              self.__format_manager.summary_values_format)

        summary.write_formula(1, 1, self.__get_conditional_count('Envelope Senders', 'D', 'B', 'B14'),
                              self.__format_manager.summary_values_format)

        summary.write_formula(2, 1, self.__get_conditional_average('Envelope Senders', 'D', 'E', 'B', 'B14'),
                              self.__format_manager.summary_values_format)

        # Based on daily volumes scaled for a 30 day period
        summary.write_formula(4, 1, self.__get_conditional_size('Envelope Senders', 'D', 'E', 'B14', True),
                              self.__format_manager.summary_highlight_values_format)

        summary.write_formula(5, 1,
                              self.__get_conditional_count('Envelope Senders', 'D', 'B', 'B14', True),
                              self.__format_manager.summary_highlight_values_format)

        summary.write_formula(6, 1,
                              self.__get_conditional_average('Envelope Senders', 'D', 'E', 'B', 'B14', True),
                              self.__format_manager.summary_highlight_values_format)

        # These are total volumes for the complete data set, excluding any data that was filtered out.
        summary.write_formula(8, 1, self.__get_total_size('Envelope Senders', 'E'),
                              self.__format_manager.summary_values_format)

        summary.write_formula(9, 1, self.__get_total_count('Envelope Senders', 'B'),
                              self.__format_manager.summary_values_format)

        summary.write_formula(10, 1, self.__get_total_average('Envelope Senders', 'E', 'B'),
                              self.__format_manager.summary_values_format)
        summary.write_formula(11, 1, "=MAX('Hourly Metrics'!B:B)", self.__format_manager.summary_values_format)
        summary.autofit()

    def __get_conditional_size(self, sheet_name, col_cond, col_data, threshold_cell, monthly=False):
        days_multiplier = f"/{self.__days}*30" if monthly else ""
        return f"""=IF(SUMIF('{sheet_name}'!{col_cond}:{col_cond},\">=\"&{threshold_cell},'{sheet_name}'!{col_data}:{col_data}){days_multiplier}<1024,
                        SUMIF('{sheet_name}'!{col_cond}:{col_cond},\">=\"&{threshold_cell},'{sheet_name}'!{col_data}:{col_data}){days_multiplier}&" B",
                        IF(AND(SUMIF('{sheet_name}'!{col_cond}:{col_cond},\">=\"&{threshold_cell},'{sheet_name}'!{col_data}:{col_data}){days_multiplier}>=1024,
                               SUMIF('{sheet_name}'!{col_cond}:{col_cond},\">=\"&{threshold_cell},'{sheet_name}'!{col_data}:{col_data}){days_multiplier}<POWER(1024,2)),
                           (ROUND((SUMIF('{sheet_name}'!{col_cond}:{col_cond},\">=\"&{threshold_cell},'{sheet_name}'!{col_data}:{col_data}){days_multiplier}/1024),1)&" KB"),
                           IF(AND(SUMIF('{sheet_name}'!{col_cond}:{col_cond},\">=\"&{threshold_cell},'{sheet_name}'!{col_data}:{col_data}){days_multiplier}>=POWER(1024,2),
                                  SUMIF('{sheet_name}'!{col_cond}:{col_cond},\">=\"&{threshold_cell},'{sheet_name}'!{col_data}:{col_data}){days_multiplier}<POWER(1024,3)),
                               (ROUND((SUMIF('{sheet_name}'!{col_cond}:{col_cond},\">=\"&{threshold_cell},'{sheet_name}'!{col_data}:{col_data}){days_multiplier}/POWER(1024,2)),1)&" MB"),
                               (ROUND((SUMIF('{sheet_name}'!{col_cond}:{col_cond},\">=\"&{threshold_cell},'{sheet_name}'!{col_data}:{col_data}){days_multiplier}/POWER(1024,3)),1)&" GB"))))"""

    def __get_conditional_count(self, sheet_name, col_cond, col_data, threshold_cell, monthly=False):
        days_multiplier = f"/{self.__days}*30" if monthly else ""
        return f"""=ROUNDUP(SUMIF('{sheet_name}'!{col_cond}:{col_cond},\">=\"&{threshold_cell},'{sheet_name}'!{col_data}:{col_data}){days_multiplier}, 0)"""

    def __get_conditional_average(self, sheet_name, col_cond, col_data, col_messages, threshold_cell, monthly=False):
        days_multiplier = f"/{self.__days}*30" if monthly else ""
        return f"""ROUNDUP(
(SUMIF('{sheet_name}'!{col_cond}:{col_cond},\">=\"&{threshold_cell},'{sheet_name}'!{col_data}:{col_data}){days_multiplier})/
(SUMIF('{sheet_name}'!{col_cond}:{col_cond},\">=\"&{threshold_cell},'{sheet_name}'!{col_messages}:{col_messages}){days_multiplier})/1024
,0)&" KB" """

    def __get_total_size(self, sheet_name, col_data):
        return f"""=IF(SUM('{sheet_name}'!{col_data}:{col_data})<1024,
                        SUM('{sheet_name}'!{col_data}:{col_data})&" B",
                        IF(AND(SUM('{sheet_name}'!{col_data}:{col_data})>=1024,
                               SUM('{sheet_name}'!{col_data}:{col_data})<POWER(1024,2)),
                           (ROUND((SUM('{sheet_name}'!{col_data}:{col_data})/1024),1)&" KB"),
                           IF(AND(SUM('{sheet_name}'!{col_data}:{col_data})>=POWER(1024,2),
                                  SUM('{sheet_name}'!{col_data}:{col_data})<POWER(1024,3)),
                               (ROUND((SUM('{sheet_name}'!{col_data}:{col_data})/POWER(1024,2)),1)&" MB"),
                               (ROUND((SUM('{sheet_name}'!{col_data}:{col_data})/POWER(1024,3)),1)&" GB"))))"""

    def __get_total_count(self, sheet_name, col_data):
        return f"""=SUM('{sheet_name}'!{col_data}:{col_data})"""

    def __get_total_average(self, sheet_name, col_data, col_messages):
        return f"""=ROUNDUP((SUM('{sheet_name}'!{col_data}:{col_data})/SUM('{sheet_name}'!{col_messages}:{col_messages}))/1024,0)&" KB" """

    def create_summary(self, processor):
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
            sheet.write_string(row, 0, k, self.__format_manager.data_cell_format)
            sheet.write_number(row, 1, v, self.__format_manager.data_cell_format)
            row += 1
        sheet.autofit()

    def create(self):
        self.create_sizing_summary()

        for proc in self.__pipeline_processor.get_processors():
            self.create_summary(proc)

        self.create_hourly_summary(self.__pipeline_processor._processor_manager.date_processor)

        self.close()
