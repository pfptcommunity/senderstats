import xlsxwriter

from log_processor import LogProcessor
from utils import average  # Assuming average calculation might be reused


def write_data_to_sheet(worksheet, data, headers, days, cell_format=None):
    """
    Writes data to an Excel worksheet with given headers and applies formatting.

    :param worksheet: The worksheet object to write data into.
    :param data: The data to write, expected to be a dictionary.
    :param headers: A list of headers for the columns.
    :param days: Number of days, used for some calculations in cells.
    :param cell_format: The cell format object for header styling.
    """
    row = 0
    for col, header in enumerate(headers):
        worksheet.write(row, col, header, cell_format)
    for k, v in data.items():
        row += 1
        col = 0
        if isinstance(k, tuple):
            for f in k:
                worksheet.write(row, col, f)
                col += 1
        else:
            worksheet.write(row, col, k)
            col += 1
        worksheet.write(row, col, len(v))
        col += 1
        worksheet.write(row, col, average(v))
        col += 1
        worksheet.write_formula(row, col, f"=C{row + 1}/{days}")
        col += 1
        worksheet.write_formula(row, col, f"=C{row + 1}*D{row + 1}")
    worksheet.autofit()


def generate_report(output_file: str, processor: LogProcessor, threshold: int):
    """
    Generates the report based on the provided arguments and processed data.
    This is a placeholder
    function; implementation details depend on how data is processed and stored.

    :param threshold: int
    :param output_file: str
    :param processor: LogProcessor
    """
    workbook = xlsxwriter.Workbook(output_file)
    # Formatting Data
    summary_field = workbook.add_format()
    summary_field.set_bold()
    summary_field.set_align('right')
    summary_values = workbook.add_format()
    summary_values.set_align("right")

    cell_format = workbook.add_format()
    cell_format.set_bold()

    summary = workbook.add_worksheet("Summary")
    summary.write(0, 0, "Estimated App Data ({} days)".format(processor.days), summary_field)
    summary.write(1, 0, "Estimated App Messages ({} days)".format(processor.days), summary_field)
    summary.write(2, 0, "Estimated App Average Message Size ({} days)".format(processor.days), summary_field)
    summary.write(3, 0, "Estimated App Peak Hourly Volume ({} days)".format(processor.days), summary_field)

    summary.write(5, 0, "Estimated Monthly App Data", summary_field)
    summary.write(6, 0, "Estimated Monthly App Messages", summary_field)

    summary.write(8, 0, "Total Outbound Data", summary_field)
    summary.write(9, 0, "Total Messages Message", summary_field)
    summary.write(10, 0, "Total Average Message Size", summary_field)
    summary.write(11, 0, "Total Peak Hourly Volume", summary_field)

    # Total summary of App data
    summary.write_formula(0, 1,
                          "=IF(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)<1024,SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)&\" bytes\",IF(AND(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)>=1024,SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)<POWER(1024,2)),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/1024),1)&\" Kb\"),IF(AND(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)>=POWER(1024,2),SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)<POWER(1024,3)),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/POWER(1024,2)),1)&\" Mb\"),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/POWER(1024,3)),1)&\" Gb\"))))".format(
                              threshold=threshold),
                          summary_values)

    summary.write_formula(1, 1, "=SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!B:B)".format(
        threshold=threshold),
                          summary_values)

    summary.write_formula(2, 1,
                          "=ROUNDUP((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!B:B))/1024,0)&\" Kb\"".format(
                              threshold=threshold),
                          summary_values)

    summary.write_formula(3, 1,
                          "=ROUNDUP(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!B:B)/{days}/8,0)".format(
                              days=processor.days,
                              threshold=threshold),
                          summary_values)

    # 30 day calculation divide total days * 30
    summary.write_formula(5, 1,
                          "=IF(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30<1024,SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30&\" bytes\",IF(AND(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30>=1024,SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30<POWER(1024,2)),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30/1024),1)&\" Kb\"),IF(AND(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30>=POWER(1024,2),SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30<POWER(1024,3)),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30/POWER(1024,2)),1)&\" Mb\"),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30/POWER(1024,3)),1)&\" Gb\"))))".format(
                              days=processor.days,
                              threshold=threshold),
                          summary_values)

    summary.write_formula(6, 1,
                          "=ROUNDUP(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!B:B)/{days}*30,0)".format(
                              days=processor.days,
                              threshold=threshold),
                          summary_values)

    # Total message volume data
    summary.write_formula(8, 1,
                          "=IF(SUM('Envelope Senders'!E:E)<1024,SUM('Envelope Senders'!E:E)&\" bytes\",IF(AND(SUM('Envelope Senders'!E:E)>=1024,SUM('Envelope Senders'!E:E)<POWER(1024,2)),(ROUND((SUM('Envelope Senders'!E:E)/1024),1)&\" Kb\"),IF(AND(SUM('Envelope Senders'!E:E)>=POWER(1024,2),SUM('Envelope Senders'!E:E)<POWER(1024,3)),(ROUND((SUM('Envelope Senders'!E:E)/POWER(1024,2)),1)&\" Mb\"),(ROUND((SUM('Envelope Senders'!E:E)/POWER(1024,3)),1)&\" Gb\"))))",
                          summary_values)
    summary.write_formula(9, 1, "=SUM('Envelope Senders'!B:B)", summary_values)

    summary.write_formula(10, 1,
                          "=ROUNDUP((SUM('Envelope Senders'!E:E)/SUM('Envelope Senders'!B:B))/1024,0)&\" Kb\"".format(
                              threshold=threshold),
                          summary_values)

    summary.write_formula(11, 1,
                          "=ROUNDUP(SUM('Envelope Senders'!B:B)/{days}/8,0)".format(
                              days=processor.days,
                              threshold=threshold),
                          summary_values)
    summary.autofit()

    sender_sheet = workbook.add_worksheet("Envelope Senders")
    write_data_to_sheet(sender_sheet, processor.sender_data,
                        ['Sender', 'Messages', 'Size', 'Messages Per Day', 'Total Bytes'],
                        processor.days, cell_format)

    from_sheet = workbook.add_worksheet("Header From")
    write_data_to_sheet(from_sheet, processor.from_data,
                        ['From', 'Messages', 'Size', 'Messages Per Day', 'Total Bytes'],
                        processor.days, cell_format)

    return_sheet = workbook.add_worksheet("Return Path")
    write_data_to_sheet(return_sheet, processor.return_data,
                        ['Return Path', 'Messages', 'Size', 'Messages Per Day', 'Total Bytes'],
                        processor.days, cell_format)

    mid_sheet = workbook.add_worksheet("Message ID")
    write_data_to_sheet(mid_sheet, processor.mid_data,
                        ['Sender', 'Message ID Host', 'Message ID Domain', 'Messages', 'Size', 'Messages Per Day',
                         'Total Bytes'], processor.days, cell_format)

    sender_from_sheet = workbook.add_worksheet("Sender + From (Alignment)")
    write_data_to_sheet(sender_from_sheet, processor.sender_from_data,
                        ['Sender', 'From', 'Messages', 'Size', 'Messages Per Day', 'Total Bytes'],
                        processor.days, cell_format)

    workbook.close()
