import argparse
import csv
import datetime
import os
import sys
import re
from glob import glob
from typing import List

import xlsxwriter

# EXCLUDE_DOMAINS = r"^({})".format('|'.join(list(PFPT_DOMAINS)))

PROOFPOINT_DOMAIN_EXCLUSIONS = ['ppops.net', 'pphosted.com', 'knowledgefront.com']


def is_valid_domain_syntax(domain_name):
    pattern = r"(?!-)[A-Za-z0-9-]{1,63}(?<!-)(\.[A-Za-z]{2,63}){1,2}$"
    if not re.match(pattern, domain_name):
        raise argparse.ArgumentTypeError(f"Invalid domain name syntax: {domain_name}")
    return domain_name


def escape_regex_specials(literal_str):
    escape_chars = [".", "*", "+"]
    escaped_text = ""
    for char in literal_str:
        if char in escape_chars:
            escaped_text += "\\" + char
        else:
            escaped_text += char
    return escaped_text


def build_or_regex_string(strings: List):
    return r"({})".format('|'.join(strings))


def average(list: List) -> float:
    return sum(list) / len(list)


def pint(number):
    if int(number) < 0:
        raise argparse.ArgumentTypeError("'{}' is invalid, integer value must be >= 0".format(number))
    return number


def main():
    if len(sys.argv) == 1:
        print("""usage: senderstats [-h] -i SearchExport [SearchExport ...]
                   [--from-field FromField] [--sender-field SenderField]
                   [--msg-size SizeField] [--date-field DateField]
                   [--date-format DateFormat] -o <xlsx> [-t THRESHOLD]""")
        exit(1)

    parser = argparse.ArgumentParser(prog="senderstats",
                                     description="""This tool helps identify the top senders based on smart search outbound message exports.""",
                                     formatter_class=lambda prog: argparse.HelpFormatter(prog, max_help_position=80))

    parser.add_argument('-i', '--input', metavar='<file>', dest="input_files",
                        nargs='+', type=str, required=True,
                        help='Smart search files to read.')

    parser.add_argument('--from-field', metavar='FromField', dest="from_field",
                        type=str, required=False, default='Header_From',
                        help='CSV field of the header From: address. (default=Header_From)')

    parser.add_argument('--sender-field', metavar='SenderField', dest="sender_field",
                        type=str, required=False, default='Sender',
                        help='CSV field of the From: address. (default=Message_Size)')

    parser.add_argument('--msg-size', metavar='SizeField', dest="size_field",
                        type=str, required=False, default='Message_Size',
                        help='CSV field of message size. (default=Message_Size)')

    parser.add_argument('--date-field', metavar='DateField', dest="date_field",
                        type=str, required=False, default='Date',
                        help='CSV field of message date. (default=Date)')

    parser.add_argument('--date-format', metavar='DateFormat', dest="date_format",
                        type=str, required=False, default="%Y-%m-%dT%H:%M:%S.%f%z",
                        help="Date format used to parse the timestamps. (default=%%Y-%%m-%%dT%%H:%%M:%%S.%%f%%z)")

    # parser.add_argument('--filtered-domains', default=[], metavar='<domain>', dest="filtered_domains",
    #                     nargs='+', type=str, help='Restrict domains to a set of domains.')

    parser.add_argument('--excluded-domains', default=[], metavar='<domain>', dest="excluded_domains",
                        nargs='+', type=is_valid_domain_syntax, help='Restrict domains to a set of domains.')

    parser.add_argument('-o', '--output', metavar='<xlsx>', dest="output_file", type=str, required=True,
                        help='Output file')

    parser.add_argument('-t', '--threshold', dest="threshold", type=pint, required=False,
                        help='Integer representing number of messages per day to be considered application traffic. (default=100)',
                        default=100)

    args = parser.parse_args()

    # Handle wildcard expansion
    file_names = []
    for f in args.input_files:
        file_names += glob(f)

    # Remove duplicates
    file_names = set(file_names)

    # Remove all non files resulting from globbing
    file_names = [file for file in file_names if os.path.isfile(file)]

    print("Files to be processed:")
    for files in file_names:
        print(files)
    print()

    print("Domains excluded from processing:")
    for skip in PROOFPOINT_DOMAIN_EXCLUSIONS + args.excluded_domains:
        print(skip)
    print()

    # Remove duplicate domain entries from the exclusion list
    args.excluded_domains = list(
        {escape_regex_specials(domain.casefold()) for domain in PROOFPOINT_DOMAIN_EXCLUSIONS + args.excluded_domains})

    exclude_pattern = r'(\.|@)' + build_or_regex_string(args.excluded_domains)

    exclude_pattern = re.compile(exclude_pattern, flags=re.IGNORECASE)

    total = 0
    skipped = 0
    dates = {}
    sender_data = {}
    from_data = {}
    sender_from_data = {}

    for f in file_names:
        print("Processing: ", f)
        with open(f, 'r', encoding='utf-8-sig') as input_file:
            reader = csv.DictReader(input_file)
            for line in reader:
                total += 1
                env_sender = line[args.sender_field].casefold().strip()
                header_from = line[args.from_field].casefold().strip()
                # Skip empty and domain patterns
                if exclude_pattern.search(env_sender) or not env_sender:
                    skipped += 1
                    continue

                # Determine distinct dates of data, and count number of messages on that day
                date = datetime.datetime.strptime(line[args.date_field], args.date_format)
                if date.strftime('%Y-%m-%d') not in dates:
                    dates[date.strftime('%Y-%m-%d')] = 0

                dates[date.strftime('%Y-%m-%d')] += 1

                message_size = line[args.size_field]

                # Make sure cast to int is valid, else 0
                if message_size.isdigit():
                    message_size = int(message_size)
                else:
                    message_size = 0

                if env_sender not in sender_data:
                    sender_data[env_sender] = []
                sender_data[env_sender].append(message_size)

                if header_from not in from_data:
                    from_data[header_from] = []
                from_data[header_from].append(message_size)

                # This is useful to identify if senders are aligned properly
                sender_header_index = (env_sender, header_from)
                if sender_header_index not in sender_from_data:
                    sender_from_data[sender_header_index] = []
                sender_from_data[sender_header_index].append(message_size)

    days = len(dates)

    workbook = xlsxwriter.Workbook(args.output_file)

    # Formatting Data
    summary_field = workbook.add_format()
    summary_field.set_bold()
    summary_field.set_align('right')
    summary_values = workbook.add_format()
    summary_values.set_align("right")

    cell_format = workbook.add_format()
    cell_format.set_bold()

    summary = workbook.add_worksheet("Summary")
    summary.write(0, 0, "Estimated App Data ({} days)".format(days), summary_field)
    summary.write(1, 0, "Estimated App Messages ({} days)".format(days), summary_field)
    summary.write(2, 0, "Estimated App Average Message Size ({} days)".format(days), summary_field)
    summary.write(3, 0, "Estimated App Peak Hourly Volume ({} days)".format(days), summary_field)

    summary.write(5, 0, "Estimated Monthly App Data", summary_field)
    summary.write(6, 0, "Estimated Monthly App Messages", summary_field)

    summary.write(8, 0, "Total Outbound Data", summary_field)
    summary.write(9, 0, "Total Messages Message", summary_field)
    summary.write(10, 0, "Total Average Message Size", summary_field)
    summary.write(11, 0, "Total Peak Hourly Volume", summary_field)

    sender_sheet = workbook.add_worksheet("Envelope Senders")
    sender_sheet.write(0, 0, "Sender", cell_format)
    sender_sheet.write(0, 1, "Messages", cell_format)
    sender_sheet.write(0, 2, "Size", cell_format)
    sender_sheet.write(0, 3, "Messages Per Day", cell_format)
    sender_sheet.write(0, 4, "Total Bytes", cell_format)

    from_sheet = workbook.add_worksheet("Header From")
    from_sheet.write(0, 0, "From", cell_format)
    from_sheet.write(0, 1, "Messages", cell_format)
    from_sheet.write(0, 2, "Size", cell_format)
    from_sheet.write(0, 3, "Messages Per Day", cell_format)
    from_sheet.write(0, 4, "Total Bytes", cell_format)

    sender_from_sheet = workbook.add_worksheet("Sender + From (Alignment)")
    sender_from_sheet.write(0, 0, "Sender", cell_format)
    sender_from_sheet.write(0, 1, "From", cell_format)
    sender_from_sheet.write(0, 2, "Messages", cell_format)
    sender_from_sheet.write(0, 3, "Size", cell_format)
    sender_from_sheet.write(0, 4, "Messages Per Day", cell_format)
    sender_from_sheet.write(0, 5, "Total Bytes", cell_format)

    # Total summary of App data
    summary.write_formula(0, 1,
                          "=IF(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)<1024,SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)&\" bytes\",IF(AND(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)>=1024,SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)<POWER(1024,2)),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/1024),1)&\" Kb\"),IF(AND(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)>=POWER(1024,2),SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)<POWER(1024,3)),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/POWER(1024,2)),1)&\" Mb\"),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/POWER(1024,3)),1)&\" Gb\"))))".format(
                              threshold=args.threshold),
                          summary_values)

    summary.write_formula(1, 1, "=SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!B:B)".format(
        threshold=args.threshold),
                          summary_values)

    summary.write_formula(2, 1,
                          "=ROUNDUP((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!B:B))/1024,0)&\" Kb\"".format(
                              threshold=args.threshold),
                          summary_values)

    summary.write_formula(3, 1,
                          "=ROUNDUP(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!B:B)/{days}/8,0)".format(
                              days=days,
                              threshold=args.threshold),
                          summary_values)

    # 30 day calculation divide total days * 30
    summary.write_formula(5, 1,
                          "=IF(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30<1024,SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30&\" bytes\",IF(AND(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30>=1024,SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30<POWER(1024,2)),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30/1024),1)&\" Kb\"),IF(AND(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30>=POWER(1024,2),SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30<POWER(1024,3)),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30/POWER(1024,2)),1)&\" Mb\"),(ROUND((SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!E:E)/{days}*30/POWER(1024,3)),1)&\" Gb\"))))".format(
                              days=days,
                              threshold=args.threshold),
                          summary_values)

    summary.write_formula(6, 1,
                          "=ROUNDUP(SUMIF('Envelope Senders'!D:D,\">={threshold}\",'Envelope Senders'!B:B)/{days}*30,0)".format(
                              days=days,
                              threshold=args.threshold),
                          summary_values)

    # Total message volume data
    summary.write_formula(8, 1,
                          "=IF(SUM('Envelope Senders'!E:E)<1024,SUM('Envelope Senders'!E:E)&\" bytes\",IF(AND(SUM('Envelope Senders'!E:E)>=1024,SUM('Envelope Senders'!E:E)<POWER(1024,2)),(ROUND((SUM('Envelope Senders'!E:E)/1024),1)&\" Kb\"),IF(AND(SUM('Envelope Senders'!E:E)>=POWER(1024,2),SUM('Envelope Senders'!E:E)<POWER(1024,3)),(ROUND((SUM('Envelope Senders'!E:E)/POWER(1024,2)),1)&\" Mb\"),(ROUND((SUM('Envelope Senders'!E:E)/POWER(1024,3)),1)&\" Gb\"))))",
                          summary_values)
    summary.write_formula(9, 1, "=SUM('Envelope Senders'!B:B)", summary_values)

    summary.write_formula(10, 1,
                          "=ROUNDUP((SUM('Envelope Senders'!E:E)/SUM('Envelope Senders'!B:B))/1024,0)&\" Kb\"".format(
                              threshold=args.threshold),
                          summary_values)

    summary.write_formula(11, 1,
                          "=ROUNDUP(SUM('Envelope Senders'!B:B)/{days}/8,0)".format(
                              days=days,
                              threshold=args.threshold),
                          summary_values)

    line = 1
    # Write data to envelope worksheet
    for k, v in sender_data.items():
        sender_sheet.write(line, 0, k)
        sender_sheet.write(line, 1, len(v))
        sender_sheet.write(line, 2, average(v))
        sender_sheet.write_formula(line, 3, "=B{line}/{days}".format(line=line + 1, days=days))
        sender_sheet.write_formula(line, 4, "=B{line}*C{line}".format(line=line + 1))
        line += 1

    # Write data to header from worksheet
    line = 1
    for k, v in from_data.items():
        from_sheet.write(line, 0, k)
        from_sheet.write(line, 1, len(v))
        from_sheet.write(line, 2, average(v))
        from_sheet.write_formula(line, 3, "=B{line}/{days}".format(line=line + 1, days=days))
        from_sheet.write_formula(line, 4, "=B{line}*C{line}".format(line=line + 1))
        line += 1

    line = 1
    # Write data to envelope worksheet
    for k, v in sender_from_data.items():
        sender_from_sheet.write(line, 0, k[0])
        sender_from_sheet.write(line, 1, k[1])
        sender_from_sheet.write(line, 2, len(v))
        sender_from_sheet.write(line, 3, average(v))
        sender_from_sheet.write_formula(line, 4, "=C{line}/{days}".format(line=line + 1, days=days))
        sender_from_sheet.write_formula(line, 5, "=C{line}*D{line}".format(line=line + 1))
        line += 1

    workbook.close()

    print("File Processing Summary")
    print("Total Records: ", total)
    print("Skipped Records: ", skipped)
    print("")

    print("Records by Day")

    for d in sorted(dates.keys()):
        print("{}:".format(d),dates[d])

    print("Please see report: {}".format(args.output_file))


# Main entry point of program
if __name__ == '__main__':
    main()
