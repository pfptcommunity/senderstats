from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, List, Optional, Sequence, Tuple

from xlsxwriter import Workbook
from xlsxwriter.worksheet import Worksheet

from senderstats.common.defaults import DEFAULT_THRESHOLD
from senderstats.common.utils import prepare_string_for_excel
from senderstats.interfaces.reportable import Reportable
from senderstats.processing.pipeline_manager import PipelineManager
from senderstats.reporting.format_manager import FormatManager


@dataclass(frozen=True)
class ReportContext:
    workbook: Workbook
    formats: FormatManager
    days: int
    with_probability: bool


class ExcelFormulas:
    def __init__(self, *, days: int):
        self._days = days

    @staticmethod
    def _col(table_cell: str, col_name: str) -> str:
        # INDIRECT($B$16&"[Messages]")
        return f'INDIRECT({table_cell}&"[{col_name}]")'

    @staticmethod
    def _sumif(cond_rng: str, threshold_cell: str, data_rng: str) -> str:
        return f'SUMIF({cond_rng},">="&{threshold_cell},{data_rng})'

    @staticmethod
    def format_bytes(numeric_expr: str) -> str:
        """
        Formats bytes into B/KB/MB/GB using your nested IF logic.
        numeric_expr must evaluate to a NUMBER of bytes.
        """
        x = f'({numeric_expr})'
        return (
            f'=IF({x}<1024,'
            f'{x}&" B",'
            f'IF(AND({x}>=1024,{x}<POWER(1024,2)),'
            f'ROUND({x}/1024,1)&" KB",'
            f'IF(AND({x}>=POWER(1024,2),{x}<POWER(1024,3)),'
            f'ROUND({x}/POWER(1024,2),1)&" MB",'
            f'ROUND({x}/POWER(1024,3),1)&" GB"))'
            f')'
        )

    def monthly_scale(self, numeric_expr: str) -> str:
        # (value)/days*30
        return f'(({numeric_expr}))/{self._days}*30'

    def conditional_bytes_raw(self, *, table_cell: str, threshold_cell: str) -> str:
        cond = self._col(table_cell, "Messages Per Day")
        data = self._col(table_cell, "Total Bytes")
        return f'={self._sumif(cond, threshold_cell, data)}'

    def conditional_messages_raw(self, *, table_cell: str, threshold_cell: str) -> str:
        cond = self._col(table_cell, "Messages Per Day")
        data = self._col(table_cell, "Messages")
        return f'=ROUNDUP({self._sumif(cond, threshold_cell, data)},0)'

    def conditional_avg_kb_display(self, *, table_cell: str, threshold_cell: str) -> str:
        # Your style: (sum bytes)/(sum msgs)/1024 rounded up and appended " KB"
        cond = self._col(table_cell, "Messages Per Day")
        bytes_rng = self._col(table_cell, "Total Bytes")
        msgs_rng = self._col(table_cell, "Messages")
        sum_bytes = self._sumif(cond, threshold_cell, bytes_rng)
        sum_msgs = self._sumif(cond, threshold_cell, msgs_rng)
        return f'=ROUNDUP(({sum_bytes})/({sum_msgs})/1024,0)&" KB"'

    def conditional_pct_of_total_display(self, *, table_cell: str, threshold_cell: str) -> str:
        cond = self._col(table_cell, "Messages Per Day")
        msgs = self._col(table_cell, "Messages")
        num = self._sumif(cond, threshold_cell, msgs)
        den = f'SUM({msgs})'
        return f'=ROUNDUP(({num})/({den})*100,1)&"%"'

    def total_bytes_raw(self, *, table_cell: str) -> str:
        return f'=SUM({self._col(table_cell, "Total Bytes")})'

    def total_messages_raw(self, *, table_cell: str) -> str:
        return f'=SUM({self._col(table_cell, "Messages")})'

    def total_avg_kb_display(self, *, table_cell: str) -> str:
        sum_bytes = f'SUM({self._col(table_cell, "Total Bytes")})'
        sum_msgs = f'SUM({self._col(table_cell, "Messages")})'
        return f'=ROUNDUP(({sum_bytes})/({sum_msgs})/1024,0)&" KB"'

    def conditional_delivery_bytes_raw(self, *, table_cell: str, threshold_cell: str) -> str:
        cond = self._col(table_cell, "Messages Per Day")
        data = self._col(table_cell, "Delivery Bytes")
        return f'={self._sumif(cond, threshold_cell, data)}'

    def total_delivery_bytes_raw(self, *, table_cell: str) -> str:
        return f'=SUM({self._col(table_cell, "Delivery Bytes")})'

    def prob_bytes_raw(self, *, table_cell: str, threshold_cell: str) -> str:
        cond = self._col(table_cell, "Autonomy Score (%)")
        data = self._col(table_cell, "Total Bytes")
        return f'={self._sumif(cond, threshold_cell, data)}'

    def prob_messages_raw(self, *, table_cell: str, threshold_cell: str) -> str:
        cond = self._col(table_cell, "Autonomy Score (%)")
        data = self._col(table_cell, "Messages")
        return f'=ROUNDUP({self._sumif(cond, threshold_cell, data)},0)'

    def prob_delivery_bytes_raw(self, *, table_cell: str, threshold_cell: str) -> str:
        cond = self._col(table_cell, "Autonomy Score (%)")
        data = self._col(table_cell, "Delivery Bytes")
        return f'={self._sumif(cond, threshold_cell, data)}'

    def prob_avg_kb_display(self, *, table_cell: str, threshold_cell: str) -> str:
        cond = self._col(table_cell, "Autonomy Score (%)")
        bytes_rng = self._col(table_cell, "Total Bytes")
        msgs_rng = self._col(table_cell, "Messages")
        sum_bytes = self._sumif(cond, threshold_cell, bytes_rng)
        sum_msgs = self._sumif(cond, threshold_cell, msgs_rng)
        return f'=IF(({sum_msgs})=0,"",ROUNDUP(({sum_bytes})/({sum_msgs})/1024,0)&" KB")'

    def prob_pct_of_total_display(self, *, table_cell: str, threshold_cell: str) -> str:
        cond = self._col(table_cell, "Autonomy Score (%)")
        bytes_rng = self._col(table_cell, "Total Bytes")
        num = self._sumif(cond, threshold_cell, bytes_rng)
        den = f'SUM({bytes_rng})'
        return f'=IF(({den})=0,"",ROUNDUP(({num})/({den})*100,1)&"%")'


class ExcelSheetWriter:
    def __init__(self, ctx: ReportContext, *, table_style: str = "Table Style Medium 9"):
        self._ctx = ctx
        self._table_style = table_style

    @staticmethod
    def sanitize_table_name(name: str) -> str:
        invalid_chars = ' +-*[]:/\\&()'
        for ch in invalid_chars:
            name = name.replace(ch, '')
        if not name or not name[0].isalpha():
            name = 'T_' + name
        return name[:255]

    def write_report_sheet(
        self,
        *,
        sheet_name: str,
        rows: Iterable[Sequence[Any]],
        create_table: bool,
        table_name: str,
    ) -> None:
        fm = self._ctx.formats
        sheet = self._ctx.workbook.add_worksheet(sheet_name)

        r_index = 0
        headers: Optional[Sequence[Any]] = None

        for row in rows:
            cell_format = fm.data_cell_format
            if r_index == 0:
                cell_format = fm.header_format
                headers = row

            for c_index, value in enumerate(row):
                if isinstance(value, (int, float)):
                    sheet.write_number(r_index, c_index, value, cell_format)
                else:
                    sheet.write_string(r_index, c_index, prepare_string_for_excel(value), cell_format)
            r_index += 1

        if create_table and r_index > 0 and headers:
            num_rows = r_index
            num_cols = len(headers)
            sheet.add_table(0, 0, num_rows - 1, num_cols - 1, {
                "columns": [{"header": str(col)} for col in headers],
                "name": table_name,
                "style": self._table_style,
            })

        sheet.autofit()


class SummarySheetBuilder:
    def __init__(self, ctx: ReportContext, *, threshold: int):
        self._ctx = ctx
        self._threshold = threshold
        self._fx = ExcelFormulas(days=ctx.days)
        self._cell_threshold = "Summary!$B$20"  # row 19, col 1
        self._cell_prob_threshold = "Summary!$E$20"
        self._cell_selected_table = "Summary!$B$21"  # row 20, col 1


    def build(self, *, table_names: List[str]) -> None:
        # Pick a default
        default_selection = table_names[0] if table_names else ""

        # Visible Summary sheet FIRST (we'll stash the dropdown list on it)
        summary = self._ctx.workbook.add_worksheet("Summary")


        list_col = 50

        if table_names:
            for i, t in enumerate(table_names):
                summary.write_string(i, list_col, t)
            last_row = len(table_names)  # Excel rows are 1-based in A1 notation
            # Name a range on Summary that points to the hidden list
            self._ctx.workbook.define_name("DataTables", f"=Summary!$AY$1:$AY${last_row}")
        else:
            summary.write_string(0, list_col, "")
            self._ctx.workbook.define_name("DataTables", "=Summary!$AY$1:$AY$1")

        # Hide the list column
        summary.set_column(list_col, list_col, None, None, {"hidden": True})

        # Hidden _SummaryCalc sheet: one row per table
        calc = self._ctx.workbook.add_worksheet("_SummaryCalc")
        calc.hide()  # or calc.set_hidden(2) for very hidden
        self._write_summary_calc(calc, table_names)

        # Hidden _ProbCalc sheet (only when probability enabled)
        if self._ctx.with_probability:
            pcalc = self._ctx.workbook.add_worksheet("_ProbCalc")
            pcalc.hide()
            self._write_prob_calc(pcalc, table_names)

        # Now write the Summary content (labels, validations, formulas)
        self._write_summary_sheet(summary, default_selection=default_selection)

    def _apply_summary_geometry(self, summary: Worksheet) -> None:
        # A: labels (left block)
        summary.set_column("A:A", 38)

        # B: values (left block)
        summary.set_column("B:B", 18)

        # C: gutter / spacing
        summary.set_column("C:C", 3)

        # D: labels (right block)
        summary.set_column("D:D", 38)

        # E: values (right block)
        summary.set_column("E:E", 22)

        # F+: keep unused area narrow
        summary.set_column("F:Z", 2)

        # Give section headers a little breathing room
        summary.set_row(0, 20)
        summary.set_row(6, 20)
        summary.set_row(12, 20)
        summary.set_row(18, 20)

        # Optional: legend header + a few rows below it
        summary.set_row(22, 18)
        for r in range(23, 30):
            summary.set_row(r, 16)

    def _write_summary_calc(self, calc: Worksheet, table_names: List[str]) -> None:
        fm = self._ctx.formats
        fx = self._fx

        headers = [
            "Table",  # A
            "CondBytesRaw",  # B
            "CondBytesDisp",  # C
            "CondMsgsRaw",  # D
            "CondAvgKBDisp",  # E
            "CondPctDisp",  # F
            "MonthlyCondBytesDisp",  # G
            "MonthlyCondMsgsRaw",  # H
            "MonthlyCondAvgKBDisp",  # I
            "TotalBytesRaw",  # J
            "TotalBytesDisp",  # K
            "TotalMsgsRaw",  # L
            "TotalAvgKBDisp",  # M
            "MonthlyCondDeliveryBytesDisp",  # N
            "TotalDeliveryBytesDisp",  # O
        ]
        for c, h in enumerate(headers):
            calc.write_string(0, c, h, fm.header_format)

        for i, table_name in enumerate(table_names, start=1):
            r = i  # row 0 is header
            calc.write_string(r, 0, table_name, fm.data_cell_format)
            table_cell = f"$A{r + 1}"  # Excel row is 1-based

            # ---- Conditional (threshold-based) ----

            # B: Conditional bytes raw
            calc.write_formula(
                r, 1,
                fx.conditional_bytes_raw(table_cell=table_cell, threshold_cell=self._cell_threshold),
                fm.data_cell_format
            )
            # C: Conditional bytes display
            calc.write_formula(r, 2, fx.format_bytes(f"_SummaryCalc!$B{r + 1}"), fm.data_cell_format)

            # D: Conditional messages raw
            calc.write_formula(
                r, 3,
                fx.conditional_messages_raw(table_cell=table_cell, threshold_cell=self._cell_threshold),
                fm.data_cell_format
            )

            # E: Conditional avg size (KB) display
            calc.write_formula(
                r, 4,
                fx.conditional_avg_kb_display(table_cell=table_cell, threshold_cell=self._cell_threshold),
                fm.data_cell_format
            )

            # F: Conditional percent of total display
            calc.write_formula(
                r, 5,
                fx.conditional_pct_of_total_display(table_cell=table_cell, threshold_cell=self._cell_threshold),
                fm.data_cell_format
            )

            # ---- Monthly (threshold-based, scaled) ----

            # G: Monthly conditional bytes display (scale raw bytes)
            monthly_bytes_expr = fx.monthly_scale(f"_SummaryCalc!$B{r + 1}")
            calc.write_formula(r, 6, fx.format_bytes(monthly_bytes_expr), fm.data_cell_format)

            # N: Monthly conditional delivery bytes display (scale conditional delivery bytes)
            cond_deliv_raw_expr = fx.conditional_delivery_bytes_raw(
                table_cell=table_cell,
                threshold_cell=self._cell_threshold
            )[1:]  # strip leading "=" to embed in expression
            monthly_deliv_expr = fx.monthly_scale(f"({cond_deliv_raw_expr})")
            calc.write_formula(r, 13, fx.format_bytes(monthly_deliv_expr), fm.data_cell_format)

            # H: Monthly conditional msgs raw (scale msgs)
            monthly_msgs_expr = fx.monthly_scale(f"_SummaryCalc!$D{r + 1}")
            calc.write_formula(r, 7, f"={monthly_msgs_expr}", fm.data_cell_format)

            # I: Monthly avg KB display (avg isn't scaled by days)
            calc.write_formula(r, 8, f"=_SummaryCalc!$E{r + 1}", fm.data_cell_format)

            # ---- Totals (whole dataset, not threshold-based) ----

            # J: Total bytes raw
            calc.write_formula(r, 9, fx.total_bytes_raw(table_cell=table_cell), fm.data_cell_format)
            # K: Total bytes display
            calc.write_formula(r, 10, fx.format_bytes(f"_SummaryCalc!$J{r + 1}"), fm.data_cell_format)
            # L: Total messages raw
            calc.write_formula(r, 11, fx.total_messages_raw(table_cell=table_cell), fm.data_cell_format)
            # M: Total avg KB display
            calc.write_formula(r, 12, fx.total_avg_kb_display(table_cell=table_cell), fm.data_cell_format)

            # O: Total delivery bytes display
            total_deliv_raw_expr = fx.total_delivery_bytes_raw(table_cell=table_cell)[1:]  # strip leading "="
            calc.write_formula(r, 14, fx.format_bytes(total_deliv_raw_expr), fm.data_cell_format)

        calc.autofit()

    def _write_prob_calc(self, calc: Worksheet, table_names: List[str]) -> None:
        fm = self._ctx.formats
        fx = self._fx

        headers = [
            "Table",                 # A
            "ProbBytesRaw",          # B
            "ProbBytesDisp",         # C
            "ProbMsgsRaw",           # D
            "ProbAvgKBDisp",         # E
            "ProbPctDisp",           # F
            "MonthlyProbBytesDisp",  # G
            "MonthlyProbMsgsRaw",    # H
            "MonthlyProbAvgKBDisp",  # I
            "MonthlyProbDelivDisp",  # J
        ]
        for c, h in enumerate(headers):
            calc.write_string(0, c, h, fm.header_format)

        for i, table_name in enumerate(table_names, start=1):
            r = i
            calc.write_string(r, 0, table_name, fm.data_cell_format)
            table_cell = f"$A{r + 1}"  # 1-based row

            # B: Prob bytes raw
            calc.write_formula(
                r, 1,
                fx.prob_bytes_raw(table_cell=table_cell, threshold_cell=self._cell_prob_threshold),
                fm.data_cell_format
            )
            # C: Prob bytes display
            calc.write_formula(r, 2, fx.format_bytes(f"_ProbCalc!$B{r + 1}"), fm.data_cell_format)

            # D: Prob messages raw
            calc.write_formula(
                r, 3,
                fx.prob_messages_raw(table_cell=table_cell, threshold_cell=self._cell_prob_threshold),
                fm.data_cell_format
            )

            # E: Prob avg size KB display
            calc.write_formula(
                r, 4,
                fx.prob_avg_kb_display(table_cell=table_cell, threshold_cell=self._cell_prob_threshold),
                fm.data_cell_format
            )

            # F: Prob % of total bytes display
            calc.write_formula(
                r, 5,
                fx.prob_pct_of_total_display(table_cell=table_cell, threshold_cell=self._cell_prob_threshold),
                fm.data_cell_format
            )

            # G: Monthly prob bytes display
            monthly_bytes_expr = fx.monthly_scale(f"_ProbCalc!$B{r + 1}")
            calc.write_formula(r, 6, fx.format_bytes(monthly_bytes_expr), fm.data_cell_format)

            # H: Monthly prob msgs raw
            monthly_msgs_expr = fx.monthly_scale(f"_ProbCalc!$D{r + 1}")
            calc.write_formula(r, 7, f"={monthly_msgs_expr}", fm.data_cell_format)

            # I: Monthly prob avg KB display (same as E)
            calc.write_formula(r, 8, f"=_ProbCalc!$E{r + 1}", fm.data_cell_format)

            # J: Monthly prob delivery bytes display
            cond_deliv_raw_expr = fx.prob_delivery_bytes_raw(
                table_cell=table_cell,
                threshold_cell=self._cell_prob_threshold
            )[1:]  # strip '='
            monthly_deliv_expr = fx.monthly_scale(f"({cond_deliv_raw_expr})")
            calc.write_formula(r, 9, fx.format_bytes(monthly_deliv_expr), fm.data_cell_format)

        calc.autofit()


    @staticmethod
    def _index_match(sheet_name: str, selected_table_cell: str, return_col_letter: str) -> str:
        return (
            f'=IFERROR('
            f'INDEX({sheet_name}!${return_col_letter}:${return_col_letter},'
            f'MATCH({selected_table_cell},{sheet_name}!$A:$A,0)'
            f'),""'
            f')'
        )


    def _write_summary_sheet(self, summary: Worksheet, *, default_selection: str) -> None:
        fm = self._ctx.formats
        days = self._ctx.days

        summary.protect()
        summary.merge_range(0, 0, 0, 1, f"Total Data Summary", fm.grouped_header_format)
        summary.write(1, 0, "Data", fm.summary_format)
        summary.write(2, 0, "Messages", fm.summary_format)
        summary.write(3, 0, "Average Message Size", fm.summary_format)
        summary.write(4, 0, "Total Peak Hourly Volume", fm.summary_format)
        #5 - Space
        summary.merge_range(6,0,6,1, f"Volumetric Summary ({days} days)", fm.grouped_header_format)
        summary.write(7, 0, f"App Message Data", fm.summary_format)
        summary.write(8, 0, f"App Messages", fm.summary_format)
        summary.write(9, 0, f"App Average Message Size", fm.summary_format)
        summary.write(10, 0, f"App Volume Percentage", fm.summary_format)
        #11 - Space
        summary.merge_range(12, 0, 12, 1, f"Volumetric Monthly Summary", fm.grouped_header_format)
        summary.write(13, 0, "Estimated App Data", fm.summary_format)
        summary.write(14, 0, "Estimated App Messages", fm.summary_format)
        summary.write(15, 0, "Estimated App Message Size", fm.summary_format)
        summary.write(16, 0, "Estimated App Delivery Data", fm.summary_highlight_format)
        # 17 - Space
        summary.merge_range(18, 0, 18, 1, f"Configuration Options", fm.grouped_header_format)
        summary.write(19, 0, "Messages Per Day Threshold (Number must be >= 0):", fm.summary_format)
        summary.write_number(19, 1, self._threshold, fm.field_values_format)
        summary.set_column(1, 1, 25)
        summary.data_validation(19, 1, 19, 1, {"validate": "integer", "criteria": ">=", "value": 0})

        summary.write(20, 0, "Select Data Source:", fm.summary_format)
        summary.write(20, 1, default_selection, fm.field_values_format)
        summary.data_validation(20, 1, 20, 1, {
            "validate": "list",
            "source": "=DataTables",
            "input_title": "Select Table",
            "input_message": "Choose a table from the list",
        })

        # Row 0 - Header
        summary.write_formula(1, 1, self._index_match("_SummaryCalc",self._cell_selected_table, "K"), fm.summary_values_format)
        summary.write_formula(2, 1, self._index_match("_SummaryCalc",self._cell_selected_table, "L"), fm.summary_values_format)
        summary.write_formula(3, 1, self._index_match("_SummaryCalc",self._cell_selected_table, "M"), fm.summary_values_format)
        summary.write_formula(4, 1, "=MAX('Hourly Metrics'!B:B)", fm.summary_values_format)
        # 5 - Space
        # Row 6 - Header
        summary.write_formula(7, 1, self._index_match("_SummaryCalc",self._cell_selected_table, "C"), fm.summary_values_format)
        summary.write_formula(8, 1, self._index_match("_SummaryCalc",self._cell_selected_table, "D"), fm.summary_values_format)
        summary.write_formula(9, 1, self._index_match("_SummaryCalc",self._cell_selected_table, "E"), fm.summary_values_format)
        summary.write_formula(10, 1, self._index_match("_SummaryCalc",self._cell_selected_table, "F"), fm.summary_values_format)
        # 11 - Space
        # Row 12 - Header
        summary.write_formula(13, 1, self._index_match("_SummaryCalc",self._cell_selected_table, "G"), fm.summary_values_format)
        summary.write_formula(14, 1, self._index_match("_SummaryCalc",self._cell_selected_table, "H"), fm.summary_values_format)
        summary.write_formula(15, 1, self._index_match("_SummaryCalc",self._cell_selected_table, "I"), fm.summary_values_format)
        summary.write_formula(16, 1, self._index_match("_SummaryCalc",self._cell_selected_table, "N"), fm.summary_highlight_values_format)
        # 17 - Space
        # Row 18 - Header all configs are defined above

        # ----------------------------
        # Probabilistic side (D/E)
        # ----------------------------
        if self._ctx.with_probability:
            # headings
            summary.merge_range(6, 3, 6, 4, f"Probabilistic Summary ({days} days)", fm.grouped_header_format)
            summary.write(7, 3, "App Message Data", fm.summary_format)
            summary.write(8, 3, "App Messages", fm.summary_format)
            summary.write(9, 3, "App Average Message Size", fm.summary_format)
            summary.write(10, 3, "App Volume Percentage", fm.summary_format)

            summary.merge_range(12, 3, 12, 4, "Probabilistic Monthly Summary", fm.grouped_header_format)
            summary.write(13, 3, "Estimated App Data", fm.summary_format)
            summary.write(14, 3, "Estimated App Messages", fm.summary_format)
            summary.write(15, 3, "Estimated App Message Size", fm.summary_format)
            summary.write(16, 3, "Estimated App Delivery Data", fm.summary_highlight_format)

            summary.merge_range(18, 3, 18, 4, "Probabilistic Options", fm.grouped_header_format)
            summary.write(19, 3, "Autonomy Threshold (Number must be >= 0):", fm.summary_format)
            summary.write_number(19, 4, 70, fm.field_values_format)
            summary.set_column(4, 4, 25)
            summary.data_validation(19, 4, 19, 4, {"validate": "integer", "criteria": ">=", "value": 0})

            # Live label (uses the threshold in E20)
            summary.write(20, 3, "Threshold Label:", fm.summary_format)
            summary.write_formula(
                20, 4,
                '=IF($E$20>=90,"High Probability App",'
                'IF($E$20>=70,"Medium Probability App",'
                'IF($E$20>=55,"Low-Volume Automated Source",'
                'IF($E$20>=30,"Unknown/Ambiguous","Likely Human"))))',
                fm.summary_values_format
            )

            # values via _ProbCalc
            summary.write_formula(7, 4, self._index_match("_ProbCalc", self._cell_selected_table, "C"), fm.summary_values_format)
            summary.write_formula(8, 4, self._index_match("_ProbCalc", self._cell_selected_table, "D"), fm.summary_values_format)
            summary.write_formula(9, 4, self._index_match("_ProbCalc", self._cell_selected_table, "E"), fm.summary_values_format)
            summary.write_formula(10, 4, self._index_match("_ProbCalc", self._cell_selected_table, "F"), fm.summary_values_format)

            summary.write_formula(13, 4, self._index_match("_ProbCalc", self._cell_selected_table, "G"), fm.summary_values_format)
            summary.write_formula(14, 4, self._index_match("_ProbCalc", self._cell_selected_table, "H"), fm.summary_values_format)
            summary.write_formula(15, 4, self._index_match("_ProbCalc", self._cell_selected_table, "I"), fm.summary_values_format)
            summary.write_formula(16, 4, self._index_match("_ProbCalc", self._cell_selected_table, "J"), fm.summary_highlight_values_format)

            # Legend block (starts at row 21, col 3)
            legend_r = 22
            legend_c = 3

            summary.merge_range(legend_r, legend_c, legend_r, legend_c + 1,
                                "Legend (Autonomy Score → Label)", fm.grouped_header_format)

            rows = [
                ("90–100", "High Probability App"),
                ("70–89.99", "Medium Probability App"),
                ("55–69.99", "Low-Volume Automated Source"),
                ("30–54.99", "Unknown/Ambiguous"),
                ("0–29.99", "Likely Human"),
            ]
            for i, (rng, label) in enumerate(rows, start=1):
                summary.write_string(legend_r + i, legend_c, rng, fm.summary_format)
                summary.write_string(legend_r + i, legend_c + 1, label, fm.summary_values_format)

        self._apply_summary_geometry(summary)


class PipelineProcessorReport:
    def __init__(self, output_file: str, pipeline_manager: PipelineManager, with_probability: bool = True):
        self.__threshold = DEFAULT_THRESHOLD
        self.__output_file = output_file
        self.__workbook = Workbook(output_file)
        self.__format_manager = FormatManager(self.__workbook)
        self.__pipeline_manager = pipeline_manager
        self.__days = len(self.__pipeline_manager.get_processor_manager().date_processor.get_date_counter())
        self.__with_probability = with_probability
        self._ctx = ReportContext(self.__workbook, self.__format_manager, self.__days, self.__with_probability)
        self._writer = ExcelSheetWriter(self._ctx)
        self._summary_builder = SummarySheetBuilder(self._ctx, threshold=self.__threshold)

    def close(self):
        self.__workbook.close()
        print()
        print("Please see report: {}".format(self.__output_file))

    def __collect_table_names(self) -> List[str]:
        data_tables: List[str] = []
        for proc in self.__pipeline_manager.get_active_processors():
            if isinstance(proc, Reportable) and getattr(proc, "create_data_table", False):
                for report_name, _data_generator in proc.report(self.__days):
                    data_tables.append(self._writer.sanitize_table_name(report_name))
        return data_tables

    def create_sizing_summary(self):
        table_names = self.__collect_table_names()
        self._summary_builder.build(table_names=table_names)

    def __report(self, processor: Any) -> None:
        if isinstance(processor, Reportable):
            for report_name, data_generator in processor.report(self.__days):
                table_name = self._writer.sanitize_table_name(report_name)
                self._writer.write_report_sheet(
                    sheet_name=report_name,
                    rows=data_generator,
                    create_table=getattr(processor, "create_data_table", False),
                    table_name=table_name,
                )

    def generate(self):
        print()
        print("Generating report, please wait.")

        self.create_sizing_summary()

        for proc in self.__pipeline_manager.get_active_processors():
            self.__report(proc)
