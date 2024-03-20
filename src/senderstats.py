import argparse
import os
import sys
from glob import glob
from pprint import pprint

from constants import DEFAULT_THRESHOLD, DEFAULT_DATE_FORMAT, PROOFPOINT_DOMAIN_EXCLUSIONS
from MessageDataReport import MessageDataReport
from MessageDataProcessor import MessageDataProcessor
from utils import print_summary, compile_domains_pattern, print_list_with_title, average
from validators import is_valid_domain_syntax, is_valid_email_syntax, validate_xlsx_file


def parse_arguments():
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
                        type=str, required=False, default=DEFAULT_DATE_FORMAT,
                        help="Date format used to parse the timestamps. (default={})".format(
                            DEFAULT_DATE_FORMAT.replace('%', '%%')))

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

    parser.add_argument('-t', '--threshold', dest="threshold", type=int, required=False,
                        help='Integer representing number of messages per day to be considered application traffic. (default=100)',
                        default=DEFAULT_THRESHOLD)

    if len(sys.argv) == 1:
        parser.print_usage()  # Print usage information if no arguments are passed
        sys.exit(1)

    return parser.parse_args()


def main():
    args = parse_arguments()

    # Process files and expand wildcards
    file_names = []
    for f in args.input_files:
        file_names += glob(f)

    # Remove duplicates after wildcard expansion
    file_names = set(file_names)

    # Validate files exist
    file_names = [file for file in file_names if os.path.isfile(file)]

    # Remove duplicate sender entries
    args.excluded_senders = sorted(list({sender.casefold() for sender in args.excluded_senders}))

    # Merge domain exclusions and remove duplicates
    args.excluded_domains = sorted(
        list({domain.casefold() for domain in PROOFPOINT_DOMAIN_EXCLUSIONS + args.excluded_domains}))

    # Remove duplicate restricted domains
    args.restricted_domains = sorted(list({domain.casefold() for domain in args.restricted_domains}))

    print_list_with_title("Files to be processed:", file_names)
    print_list_with_title("Senders excluded from processing:", args.excluded_senders)
    print_list_with_title("Domains excluded from processing:", args.excluded_domains)
    print_list_with_title("Domains constrained or processing:", args.restricted_domains)

    # Log processor object
    data_processor = MessageDataProcessor()
    data_processor.skipped_domain_set = set(args.excluded_senders)
    data_processor.excluded_domains_pattern = compile_domains_pattern(args.excluded_domains)
    data_processor.restricted_domains_pattern = compile_domains_pattern(args.restricted_domains)
    data_processor.sender_field = args.sender_field
    data_processor.from_field = args.from_field
    data_processor.return_field = args.return_field
    data_processor.mid_field = args.mid_field
    data_processor.size_field = args.size_field
    data_processor.date_field = args.date_field
    data_processor.date_format = args.date_format

    f_current = 1
    f_total = len(file_names)
    for f in file_names:
        print("Processing:", f, f'({f_current} of {f_total})')
        data_processor.process_file(f)
        f_current += 1

    print()
    print("\nTotal records processed:", data_processor.total)
    print_summary("Skipped due to empty sender", data_processor.empty_senders)
    print_summary("Excluded by excluded senders list", data_processor.excluded_senders, args.show_skip_detail)
    print_summary("Excluded by excluded domains list", data_processor.excluded_domains, args.show_skip_detail)
    print_summary("Excluded by restricted domains list", data_processor.restricted_domains, args.show_skip_detail)
    print()

    if data_processor.dates:
        print("Records by Day")
        for d in sorted(data_processor.dates.keys()):
            print("{}:".format(d), data_processor.dates[d])
        print()

    data_report = MessageDataReport(args.output_file, data_processor, args.threshold)
    data_report.generate_report()

    print("Please see report: {}".format(args.output_file))


if __name__ == '__main__':
    main()
