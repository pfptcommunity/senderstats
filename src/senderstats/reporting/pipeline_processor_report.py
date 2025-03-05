from xlsxwriter import Workbook

from senderstats.common.defaults import DEFAULT_THRESHOLD
from senderstats.interfaces.reportable import Reportable
from senderstats.processing.pipeline_manager import PipelineManager
from senderstats.reporting.format_manager import FormatManager


class PipelineProcessorReport:
    def __init__(self, output_file: str, pipeline_manager: PipelineManager):
        self.__threshold = DEFAULT_THRESHOLD
        self.__output_file = output_file
        self.__workbook = Workbook(output_file)
        self.__format_manager = FormatManager(self.__workbook)
        self.__pipeline_manager = pipeline_manager
        self.__days = len(self.__pipeline_manager.get_processor_manager().date_processor.get_date_counter())

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

        data_tables = []
        for proc in self.__pipeline_manager.get_active_processors():
            if isinstance(proc, Reportable):
                if proc.create_data_table:
                    for report_name, data_generator in proc.report(self.__days):
                        data_tables.append(self.__sanitize_table_name(report_name))

        default_selection = data_tables[0] if data_tables else ''
        summary.write(14, 0, 'Select Data Source:', self.__format_manager.summary_format)
        summary.write(14, 1, default_selection, self.__format_manager.field_values_format)

        summary.data_validation(14, 1, 14, 1, {
            'validate': 'list',
            'source': data_tables,  # List of table names as strings
            'input_title': 'Select Table',
            'input_message': 'Choose a table from the list'
        })

        # Based on daily message volume being over a threshold N
        summary.write_formula(0, 1, self.__get_conditional_size('B15', 'Messages Per Day', 'Total Bytes', 'B14'),
                              self.__format_manager.summary_values_format)

        summary.write_formula(1, 1, self.__get_conditional_count('B15', 'Messages Per Day', 'Messages', 'B14'),
                              self.__format_manager.summary_values_format)

        summary.write_formula(2, 1, self.__get_conditional_average('B15', 'Messages Per Day', 'Total Bytes', 'Messages',
                                                                   'B14'),
                              self.__format_manager.summary_values_format)

        # Based on daily volumes scaled for a 30 day period
        summary.write_formula(4, 1, self.__get_conditional_size('B15', 'Messages Per Day', 'Total Bytes', 'B14', True),
                              self.__format_manager.summary_highlight_values_format)

        summary.write_formula(5, 1,
                              self.__get_conditional_count('B15', 'Messages Per Day', 'Messages', 'B14', True),
                              self.__format_manager.summary_highlight_values_format)

        summary.write_formula(6, 1,
                              self.__get_conditional_average('B15', 'Messages Per Day', 'Total Bytes', 'Messages',
                                                             'B14', True),
                              self.__format_manager.summary_highlight_values_format)

        # These are total volumes for the complete data set, excluding any data that was filtered out.
        summary.write_formula(8, 1, self.__get_total_size('B15', 'Total Bytes'),
                              self.__format_manager.summary_values_format)

        summary.write_formula(9, 1, self.__get_total_count('B15', 'Messages'),
                              self.__format_manager.summary_values_format)

        summary.write_formula(10, 1, self.__get_total_average('B15', 'Total Bytes', 'Messages'),
                              self.__format_manager.summary_values_format)

        summary.write_formula(11, 1, "=MAX('Hourly Metrics'!B:B)", self.__format_manager.summary_values_format)
        summary.autofit()

    def __get_conditional_size(self, table_id, col_cond, col_data, threshold_cell, monthly=False):
        days_multiplier = f"/{self.__days}*30" if monthly else ""
        return f"""=IF(SUMIF(INDIRECT({table_id}&"[{col_cond}]"),">="&{threshold_cell},INDIRECT({table_id}&"[{col_data}]")){days_multiplier}<1024,
                    SUMIF(INDIRECT({table_id}&"[{col_cond}]"),">="&{threshold_cell},INDIRECT({table_id}&"[{col_data}]")){days_multiplier}&" B",
                    IF(AND(SUMIF(INDIRECT({table_id}&"[{col_cond}]"),">="&{threshold_cell},INDIRECT({table_id}&"[{col_data}]")){days_multiplier}>=1024,
                           SUMIF(INDIRECT({table_id}&"[{col_cond}]"),">="&{threshold_cell},INDIRECT({table_id}&"[{col_data}]")){days_multiplier}<POWER(1024,2)),
                       (ROUND((SUMIF(INDIRECT({table_id}&"[{col_cond}]"),">="&{threshold_cell},INDIRECT({table_id}&"[{col_data}]")){days_multiplier}/1024),1)&" KB"),
                       IF(AND(SUMIF(INDIRECT({table_id}&"[{col_cond}]"),">="&{threshold_cell},INDIRECT({table_id}&"[{col_data}]")){days_multiplier}>=POWER(1024,2),
                              SUMIF(INDIRECT({table_id}&"[{col_cond}]"),">="&{threshold_cell},INDIRECT({table_id}&"[{col_data}]")){days_multiplier}<POWER(1024,3)),
                           (ROUND((SUMIF(INDIRECT({table_id}&"[{col_cond}]"),">="&{threshold_cell},INDIRECT({table_id}&"[{col_data}]")){days_multiplier}/POWER(1024,2)),1)&" MB"),
                           (ROUND((SUMIF(INDIRECT({table_id}&"[{col_cond}]"),">="&{threshold_cell},INDIRECT({table_id}&"[{col_data}]")){days_multiplier}/POWER(1024,3)),1)&" GB"))))"""

    def __get_conditional_count(self, table_id, col_cond, col_data, threshold_cell, monthly=False):
        days_multiplier = f"/{self.__days}*30" if monthly else ""
        return f"""=ROUNDUP(SUMIF(INDIRECT({table_id}&"[{col_cond}]"),\">=\"&{threshold_cell},INDIRECT({table_id}&"[{col_data}]")){days_multiplier}, 0)"""

    def __get_conditional_average(self, table_id, col_cond, col_data, col_messages, threshold_cell, monthly=False):
        days_multiplier = f"/{self.__days}*30" if monthly else ""
        return f"""ROUNDUP(
    (SUMIF(INDIRECT({table_id}&"[{col_cond}]"),\">=\"&{threshold_cell},INDIRECT({table_id}&"[{col_data}]")){days_multiplier})/
    (SUMIF(INDIRECT({table_id}&"[{col_cond}]"),\">=\"&{threshold_cell},INDIRECT({table_id}&"[{col_messages}]")){days_multiplier})/1024
    ,0)&" KB" """

    def __get_total_size(self, table_id, col_data):
        return f"""=IF(SUM(INDIRECT({table_id}&"[{col_data}]"))<1024,
                    SUM(INDIRECT({table_id}&"[{col_data}]"))&" B",
                    IF(AND(SUM(INDIRECT({table_id}&"[{col_data}]"))>=1024,
                           SUM(INDIRECT({table_id}&"[{col_data}]"))<POWER(1024,2)),
                       (ROUND((SUM(INDIRECT({table_id}&"[{col_data}]"))/1024),1)&" KB"),
                       IF(AND(SUM(INDIRECT({table_id}&"[{col_data}]"))>=POWER(1024,2),
                              SUM(INDIRECT({table_id}&"[{col_data}]"))<POWER(1024,3)),
                           (ROUND((SUM(INDIRECT({table_id}&"[{col_data}]"))/POWER(1024,2)),1)&" MB"),
                           (ROUND((SUM(INDIRECT({table_id}&"[{col_data}]"))/POWER(1024,3)),1)&" GB"))))"""

    def __get_total_count(self, table_id, col_data):
        return f"""=SUM(INDIRECT({table_id}&"[{col_data}]"))"""

    def __get_total_average(self, table_id, col_data, col_messages):
        return f"""=ROUNDUP(SUM(INDIRECT({table_id}&"[{col_data}]"))/SUM(INDIRECT({table_id}&"[{col_messages}]"))/1024,0)&" KB" """

    # Sanitize table name
    def __sanitize_table_name(self, name):
        invalid_chars = ' +-*[]:/\\&()'
        for char in invalid_chars:
            name = name.replace(char, '')
        if not name[0].isalpha():
            name = 'T_' + name
        return name[:255]

    def __report(self, processor):
        if isinstance(processor, Reportable):
            for report_name, data_generator in processor.report(self.__days):
                sheet = self.__workbook.add_worksheet(report_name)

                r_index = 0
                headers = None
                for row in data_generator:
                    format = self.__format_manager.data_cell_format
                    if r_index == 0:
                        format = self.__format_manager.header_format
                        headers = row  # Capture headers from the first row
                    # Write each cell based on its type
                    for c_index, value in enumerate(row):
                        if isinstance(value, (int, float)):
                            sheet.write_number(r_index, c_index, value, format)
                        else:
                            # Treat all non-numbers as strings (including None, str, etc.)
                            sheet.write_string(r_index, c_index, str(value), format)
                    r_index += 1

                if processor.create_data_table and r_index > 0:
                    # Create table
                    num_rows = r_index
                    num_cols = len(headers)
                    sanitized_name = self.__sanitize_table_name(report_name)
                    sheet.add_table(0, 0, num_rows - 1, num_cols - 1, {
                        'columns': [{'header': str(col)} for col in headers],
                        'name': sanitized_name,
                        'style': 'Table Style Medium 9'
                    })

                sheet.autofit()

    def generate(self):
        print()
        print("Generating report, please wait.")
        self.create_sizing_summary()

        for proc in self.__pipeline_manager.get_active_processors():
            self.__report(proc)
