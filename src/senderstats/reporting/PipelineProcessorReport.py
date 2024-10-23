from xlsxwriter import Workbook

from senderstats.interfaces.Reportable import Reportable
from senderstats.processing import PipelineProcessor
from senderstats.reporting.FormatManager import FormatManager


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

    def create_sizing_summary(self):
        summary = self.__workbook.add_worksheet("Summary")
        summary.protect()

        summary.write(0, 0, f"Estimated App Data ({self.__days} days)", self.__format_manager.summary_format)
        summary.write(1, 0, f"Estimated App Messages ({self.__days} days)", self.__format_manager.summary_format)
        summary.write(2, 0, f"Estimated App Average Message Size ({self.__days} days)",
                      self.__format_manager.summary_format)

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

    def __report(self, processor):
        if isinstance(processor, Reportable):
            for report_name, data in processor.report(self.__days):
                sheet = self.__workbook.add_worksheet(report_name)
                for r_index, row in enumerate(data, start=0):
                    format = self.__format_manager.data_cell_format
                    if r_index == 0:
                        format = self.__format_manager.header_format
                    for c_index, value in enumerate(row, start=0):
                        if isinstance(value, str):
                            sheet.write_string(r_index, c_index, value, format)
                        if isinstance(value, int) or isinstance(value, float):
                            sheet.write_number(r_index, c_index, value, format)
                sheet.autofit()

    def generate(self):
        print()
        print("Generating report, please wait.")
        self.create_sizing_summary()

        for proc in self.__pipeline_processor.get_processors():
            self.__report(proc)
