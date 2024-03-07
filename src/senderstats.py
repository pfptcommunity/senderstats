import argparse
import csv
import datetime
import os
import sys
import re
from collections import defaultdict
from glob import glob
import tldextract
import xlsxwriter

PROOFPOINT_DOMAIN_EXCLUSIONS = ['ppops.net', 'pphosted.com', 'knowledgefront.com']

# Precompiled Regex Matcher (valid email address) parse group 2
email_address_re = re.compile(
    r'(<?\s*([a-zA-Z0-9.!#$%&’*+\/=?^_`{|}~-]+@([a-z0-9-]+(?:\.[a-z0-9-]+)*)\s*)>?\s*(?:;|$))', re.IGNORECASE)

# Precompiled Regex Matcher (valid domain)
valid_domain_re = re.compile(r"(?!-)[a-z0-9-]{1,63}(?<!-)(\.[a-z]{2,63}){1,2}$", re.IGNORECASE)

# Precompiled Regex for bounce attack prevention (PRVS) there prvs and msprvs1 (not much info on msprvs)
prvs_re = re.compile(r'(ms)?prvs\d?=[^=]*=', re.IGNORECASE)

# Precompiled Regex for Sender Rewrite Scheme (SRS)
# <Forwarding Mailbox Username>+SRS=<Hash>=<Timestamp>=<Original Sender Domain>=<Original Sender Username>@
srs_re = re.compile(r'([^+]*)\+?srs\d{0,2}=[^=]+=[^=]+=([^=]+)=([^@]+)@', re.IGNORECASE)


def is_valid_domain_syntax(domain_name: str):
    if not valid_domain_re.match(domain_name):
        raise argparse.ArgumentTypeError(f"Invalid domain name syntax: {domain_name}")
    return domain_name


def is_valid_email_syntax(email: str):
    if not email_address_re.match(email):
        raise argparse.ArgumentTypeError(f"Invalid email address syntax: {email}")
    return email


def validate_xlsx_file(file_path):
    if not file_path.lower().endswith('.xlsx'):
        raise argparse.ArgumentTypeError("File must have a .xlsx extension.")
    return file_path


def strip_display_names(email: str):
    match = email_address_re.search(email)
    if match:
        return match.group(2)
    return email


def get_email_domain(email: str):
    match = email_address_re.search(email)
    if match:
        return match.group(3)
    return email


def get_message_id_host(msgid: str):
    return msgid.strip('<> ').split('@')[-1]


def strip_prvs(email: str):
    return prvs_re.sub('', email)


def convert_srs(email: str):
    match = srs_re.search(email)
    if match:
        return '{}@{}'.format(match.group(2), match.group(1))
    return email


def escape_regex_specials(literal_str: str):
    escape_chars = [".", "*", "+"]
    escaped_text = ""
    for char in literal_str:
        if char in escape_chars:
            escaped_text += "\\" + char
        else:
            escaped_text += char
    return escaped_text


def build_or_regex_string(strings: list):
    return r"({})".format('|'.join(strings))


def average(list: list) -> float:
    return sum(list) / len(list)


def pint(number: str):
    if int(number) < 0:
        raise argparse.ArgumentTypeError("'{}' is invalid, integer value must be >= 0".format(number))
    return number


def main():
    if len(sys.argv) == 1:
        print("""usage: senderstats [-h] -i SearchExport [SearchExport ...] -o <xlsx> [-t THRESHOLD]""")
        exit(1)

    parser = argparse.ArgumentParser(prog="senderstats",
                                     description="""This tool helps identify the top senders based on smart search outbound message exports.""",
                                     formatter_class=lambda prog: argparse.HelpFormatter(prog, max_help_position=80))

    parser.add_argument('-i', '--input', metavar='<file>', dest="input_files",
                        nargs='+', type=str, required=True,
                        help='Smart search files to read.')

    parser.add_argument('--hfrom', metavar='FromField', dest="from_field",
                        type=str, required=False, default='Header_From',
                        help='CSV field of the header From: address. (default=Header_From)')

    parser.add_argument('--mfrom', metavar='SenderField', dest="sender_field",
                        type=str, required=False, default='Sender',
                        help='CSV field of the envelope sender address. (default=Sender)')

    parser.add_argument('--rpath', metavar='ReturnField', dest="return_field",
                        type=str, required=False, default='Header_Return-Path',
                        help='CSV field of the Return-Path: address. (default=Header_Return-Path)')

    parser.add_argument('--mid', metavar='MIDField', dest="mid_field",
                        type=str, required=False, default='Message_ID',
                        help='CSV field of the message ID. (default=Message_ID)')

    parser.add_argument('--size', metavar='SizeField', dest="size_field",
                        type=str, required=False, default='Message_Size',
                        help='CSV field of message size. (default=Message_Size)')

    parser.add_argument('--date', metavar='DateField', dest="date_field",
                        type=str, required=False, default='Date',
                        help='CSV field of message date/time. (default=Date)')

    parser.add_argument('--date-format', metavar='DateFormat', dest="date_format",
                        type=str, required=False, default="%Y-%m-%dT%H:%M:%S.%f%z",
                        help="Date format used to parse the timestamps. (default=%%Y-%%m-%%dT%%H:%%M:%%S.%%f%%z)")

    parser.add_argument('--strip-display-name', action='store_true', dest="no_display",
                        help='Remove display names, address only')

    parser.add_argument('--strip-prvs', action='store_true', dest="strip_prvs",
                        help='Remove bounce attack prevention tag e.g. prvs=tag=sender@domain.com')

    parser.add_argument('--decode-srs', action='store_true', dest="decode_srs",
                        help='Convert SRS forwardmailbox+srs=hash=tt=domain.com=user to user@domain.com')

    parser.add_argument('--no-empty-from', action='store_true', dest="no_empty_from",
                        help='If the header From: is empty the envelope sender address is used')

    parser.add_argument('--show-skip-detail', action='store_true', dest="show_skip_detail",
                        help='Show skipped details')

    parser.add_argument('--excluded-domains', default=[], metavar='<domain>', dest="excluded_domains",
                        nargs='+', type=is_valid_domain_syntax, help='Exclude domains from processing.')

    parser.add_argument('--restrict-domains', default=[], metavar='<domain>', dest="restricted_domains",
                        nargs='+', type=is_valid_domain_syntax, help='Constrain domains for processing.')

    parser.add_argument('--excluded-senders', default=[], metavar='<sender>', dest="excluded_senders",
                        nargs='+', type=is_valid_email_syntax, help='Exclude senders from processing.')

    parser.add_argument('-o', '--output', metavar='<xlsx>', dest="output_file", type=validate_xlsx_file, required=True,
                        help='Output file')

    parser.add_argument('-t', '--threshold', dest="threshold", type=pint, required=False,
                        help='Integer representing number of messages per day to be considered application traffic. (default=100)',
                        default=100)

    args = parser.parse_args()

    file_names = []
    for f in args.input_files:
        file_names += glob(f)

    file_names = set(file_names)
    file_names = [file for file in file_names if os.path.isfile(file)]

    print("Files to be processed:")
    for files in file_names:
        print(files)
    print()

    # Remove duplicates + merge
    args.excluded_domains = sorted(
        list({domain.casefold() for domain in PROOFPOINT_DOMAIN_EXCLUSIONS + args.excluded_domains}))

    # Remove duplicates + merge
    args.restricted_domains = sorted(list({domain.casefold() for domain in args.restricted_domains}))

    # Remove duplicates
    args.excluded_senders = sorted(list({sender.casefold() for sender in args.excluded_senders}))

    print("Domains excluded from processing:")
    for skip in args.excluded_domains:
        print(skip)
    print()

    if args.restricted_domains:
        print("Domains constrained or processing:")
        for skip in args.restricted_domains:
            print(skip)
        print()

    if args.excluded_senders:
        print("Senders excluded from processing:")
        for skip in args.excluded_senders:
            print(skip)
        print()

    args.restricted_domains = list({escape_regex_specials(domain.casefold()) for domain in args.restricted_domains})
    # Pattern used to constrain domains and subdomains
    restricted_domains = r'(\.|@)' + build_or_regex_string(args.restricted_domains)
    restricted_domains = re.compile(restricted_domains, flags=re.IGNORECASE)

    args.excluded_domains = list({escape_regex_specials(domain.casefold()) for domain in args.excluded_domains})
    # Pattern used to exclude domains and subdomains
    exclude_pattern = r'(\.|@)' + build_or_regex_string(args.excluded_domains)
    exclude_pattern = re.compile(exclude_pattern, flags=re.IGNORECASE)

    # Set used to exclude domains and subdomains
    skipped_domain_set = set(args.excluded_senders)

    total = 0
    skipped = 0
    dates = {}
    sender_data = {}
    from_data = {}
    return_data = {}
    sender_from_data = {}
    mid_data = {}
    skipped_senders = defaultdict(int)
    skipped_domains = defaultdict(int)
    for f in file_names:
        print("Processing: ", f)
        with open(f, 'r', encoding='utf-8-sig') as input_file:
            reader = csv.DictReader(input_file)
            for line in reader:
                total += 1
                env_sender = line[args.sender_field].casefold().strip()

                if args.decode_srs:
                    env_sender = convert_srs(env_sender)

                if args.strip_prvs:
                    env_sender = strip_prvs(env_sender)

                # Deal with all the records we don't want to process based on sender.
                # Skip empty and domain patterns
                if exclude_pattern.search(env_sender) or not env_sender:
                    skipped += 1
                    domain = get_email_domain(env_sender)
                    if not env_sender:
                        domain = '(Empty)'
                    skipped_domains[domain] += 1
                    continue

                # Skip empty and domain patterns
                if not restricted_domains.search(env_sender):
                    skipped += 1
                    domain = get_email_domain(env_sender)
                    skipped_domains[domain] += 1
                    continue

                # Skip those domains not part of the processing constraint
                if env_sender in skipped_domain_set:
                    skipped += 1
                    skipped_senders[env_sender] += 1
                    continue

                header_from = line[args.from_field].casefold().strip()

                return_path = line[args.return_field].casefold().strip()

                if args.decode_srs:
                    return_path = strip_prvs(return_path)

                if args.strip_prvs:
                    return_path = convert_srs(return_path)

                # Message ID is unique but often the sending host behind the @ symbol is unique to the application
                message_id = line[args.mid_field].casefold().strip()
                message_id_domain = get_message_id_host(message_id)
                message_id_domain_extract = tldextract.extract(message_id_domain)
                message_id_host = message_id_domain_extract.subdomain
                message_id_domain = message_id_domain_extract.domain
                message_id_domain_suffix = message_id_domain_extract.suffix

                # If header from is empty, we will use env_sender
                if args.no_empty_from and not header_from:
                    header_from = env_sender

                # Add domain suffix to TLD
                if message_id_domain_suffix:
                    message_id_domain += '.' + message_id_domain_suffix

                if not message_id_host and not message_id_domain_suffix:
                    message_id_host = message_id_domain
                    message_id_domain = ''

                if args.no_display:
                    header_from = strip_display_names(header_from)

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

                sender_data.setdefault(env_sender, []).append(message_size)
                from_data.setdefault(header_from, []).append(message_size)
                return_data.setdefault(return_path, []).append(message_size)

                # Fat index for binding commonality
                mid_host_domain_index = (header_from, message_id_host, message_id_domain)
                mid_data.setdefault(mid_host_domain_index, []).append(message_size)

                sender_header_index = (env_sender, header_from)
                sender_from_data.setdefault(sender_header_index, []).append(message_size)

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

    return_sheet = workbook.add_worksheet("Return Path")
    return_sheet.write(0, 0, "Return Path", cell_format)
    return_sheet.write(0, 1, "Messages", cell_format)
    return_sheet.write(0, 2, "Size", cell_format)
    return_sheet.write(0, 3, "Messages Per Day", cell_format)
    return_sheet.write(0, 4, "Total Bytes", cell_format)

    mid_sheet = workbook.add_worksheet("Message ID")
    mid_sheet.write(0, 0, "Sender", cell_format)
    mid_sheet.write(0, 1, "Message ID Host", cell_format)
    mid_sheet.write(0, 2, "Message ID Domain", cell_format)
    mid_sheet.write(0, 3, "Messages", cell_format)
    mid_sheet.write(0, 4, "Size", cell_format)
    mid_sheet.write(0, 5, "Messages Per Day", cell_format)
    mid_sheet.write(0, 6, "Total Bytes", cell_format)

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

    # Write data to return from worksheet
    line = 1
    for k, v in return_data.items():
        return_sheet.write(line, 0, k)
        return_sheet.write(line, 1, len(v))
        return_sheet.write(line, 2, average(v))
        return_sheet.write_formula(line, 3, "=B{line}/{days}".format(line=line + 1, days=days))
        return_sheet.write_formula(line, 4, "=B{line}*C{line}".format(line=line + 1))
        line += 1

    # Write data to return from worksheet
    line = 1
    for k, v in mid_data.items():
        mid_sheet.write(line, 0, k[0])
        mid_sheet.write(line, 1, k[1])
        mid_sheet.write(line, 2, k[2])
        mid_sheet.write(line, 3, len(v))
        mid_sheet.write(line, 4, average(v))
        mid_sheet.write_formula(line, 5, "=D{line}/{days}".format(line=line + 1, days=days))
        mid_sheet.write_formula(line, 6, "=D{line}*E{line}".format(line=line + 1))
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

    print()
    print("File Processing Summary")
    print("Total Records: ", total)
    print("Skipped Records: ", skipped)
    print()

    if args.show_skip_detail:
        print("Skipped Domain Details")
        for k, v in skipped_domains.items():
            print("Skipped {}:".format(k), v)
        print()

        print("Skipped Sender Details")
        for k, v in skipped_senders.items():
            print("Skipped {}:".format(k), v)
        print()

    if dates:
        print("Records by Day")
        for d in sorted(dates.keys()):
            print("{}:".format(d), dates[d])
        print()

    print("Please see report: {}".format(args.output_file))


# Main entry point of program
if __name__ == '__main__':
    main()
