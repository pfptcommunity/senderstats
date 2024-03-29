import os
import sys
from glob import glob
from senderstats.common.constants import DEFAULT_THRESHOLD, DEFAULT_DOMAIN_EXCLUSIONS
from senderstats.common.utils import *
from senderstats.common.validators import *
from senderstats.lib.MessageDataProcessor import *
from senderstats.lib.MessageDataReport import MessageDataReport


def parse_arguments():
    parser = argparse.ArgumentParser(prog="senderstats", add_help=False,
                                     description="""This tool helps identify the top senders based on smart search outbound message exports.""",
                                     formatter_class=lambda prog: argparse.HelpFormatter(prog, max_help_position=80))

    required_group = parser.add_argument_group('Required arguments (optional)')
    field_group = parser.add_argument_group('Field mapping arguments (optional)')
    reporting_group = parser.add_argument_group('Reporting control arguments (optional)')
    parser_group = parser.add_argument_group('Parsing behavior arguments (optional)')
    output_group = parser.add_argument_group('Extended processing controls (optional)')
    usage = parser.add_argument_group('Usage')
    # Manually add the help option to the new group
    usage.add_argument('-h', '--help', action='help', default=argparse.SUPPRESS,
                       help='Show this help message and exit')

    required_group.add_argument('-i', '--input', metavar='<file>', dest="input_files",
                                nargs='+', type=str, required=True,
                                help='Smart search files to read.')

    required_group.add_argument('-o', '--output', metavar='<xlsx>', dest="output_file",
                                type=validate_xlsx_file, required=True,
                                help='Output file')

    field_group.add_argument('--mfrom', metavar='MFrom', dest="mfrom_field",
                             type=str, required=False,
                             help=f'CSV field of the envelope sender address. (default={DEFAULT_MFROM_FIELD})')

    field_group.add_argument('--hfrom', metavar='HFrom', dest="hfrom_field",
                             type=str, required=False,
                             help=f'CSV field of the header From: address. (default={DEFAULT_HFROM_FIELD})')

    field_group.add_argument('--rpath', metavar='RPath', dest="rpath_field",
                             type=str, required=False,
                             help=f'CSV field of the Return-Path: address. (default={DEFAULT_RPATH_FIELD})')

    field_group.add_argument('--msgid', metavar='MsgID', dest="msgid_field",
                             type=str, required=False,
                             help=f'CSV field of the message ID. (default={DEFAULT_MSGID_FIELD})')

    field_group.add_argument('--size', metavar='MsgSz', dest="msgsz_field",
                             type=str, required=False,
                             help=f'CSV field of message size. (default={DEFAULT_MSGSZ_FIELD})')

    field_group.add_argument('--date', metavar='Date', dest="date_field",
                             type=str, required=False,
                             help=f'CSV field of message date/time. (default={DEFAULT_DATE_FIELD})')

    reporting_group.add_argument('--gen-hfrom', action='store_true', dest="gen_hfrom",
                                 help='Generate report showing the header From: data for messages being sent.')

    reporting_group.add_argument('--gen-rpath', action='store_true', dest="gen_rpath",
                                 help='Generate report showing return path for messages being sent.')

    reporting_group.add_argument('--gen-alignment', action='store_true', dest="gen_alignment",
                                 help='Generate report showing envelope sender and header From: alignment')

    reporting_group.add_argument('--gen-msgid', action='store_true', dest="gen_msgid",
                                 help='Generate report showing parsed Message ID. Helps determine the sending system')

    reporting_group.add_argument('-t', '--threshold', dest="threshold", metavar='N', type=int, required=False,
                                 help=f'Adjust summary report threshold for messages per day to be considered application traffic. (default={DEFAULT_THRESHOLD})',
                                 default=DEFAULT_THRESHOLD)

    parser_group.add_argument('--no-display-name', action='store_true', dest="no_display",
                              help='Remove display and use address only. Converts \'Display Name <user@domain.com>\' to \'user@domain.com\'')

    parser_group.add_argument('--remove-prvs', action='store_true', dest="remove_prvs",
                              help='Remove return path verification strings e.g. prvs=tag=sender@domain.com')

    parser_group.add_argument('--decode-srs', action='store_true', dest="decode_srs",
                              help='Convert sender rewrite scheme, forwardmailbox+srs=hash=tt=domain.com=user to user@domain.com')

    parser_group.add_argument('--no-empty-hfrom', action='store_true', dest="no_empty_hfrom",
                              help='If the header From: is empty the envelope sender address is used')

    parser_group.add_argument('--excluded-domains', default=[], metavar='<domain>', dest="excluded_domains",
                              nargs='+', type=is_valid_domain_syntax, help='Exclude domains from processing.')

    parser_group.add_argument('--restrict-domains', default=[], metavar='<domain>', dest="restricted_domains",
                              nargs='+', type=is_valid_domain_syntax, help='Constrain domains for processing.')

    parser_group.add_argument('--excluded-senders', default=[], metavar='<sender>', dest="excluded_senders",
                              nargs='+', type=is_valid_email_syntax, help='Exclude senders from processing.')

    parser_group.add_argument('--date-format', metavar='DateFmt', dest="date_format",
                              type=str, required=False,
                              help=f'Date format used to parse the timestamps. (default={DEFAULT_DATE_FORMAT.replace("%", "%%")})')

    output_group.add_argument('--show-skip-detail', action='store_true', dest="show_skip_detail",
                              help='Show skipped details')

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
        list({domain.casefold() for domain in DEFAULT_DOMAIN_EXCLUSIONS + args.excluded_domains}))

    # Remove duplicate restricted domains
    args.restricted_domains = sorted(list({domain.casefold() for domain in args.restricted_domains}))

    print_list_with_title("Files to be processed:", file_names)
    print_list_with_title("Senders excluded from processing:", args.excluded_senders)
    print_list_with_title("Domains excluded from processing:", args.excluded_domains)
    print_list_with_title("Domains constrained or processing:", args.restricted_domains)

    # Log processor object (find a cleaner way to apply these settings)
    data_processor = MessageDataProcessor(args.excluded_senders, args.excluded_domains, args.restricted_domains)

    # Configure fields
    if args.hfrom_field:
        data_processor.set_hfrom_field(args.hfrom_field)

    if args.mfrom_field:
        data_processor.set_hfrom_field(args.mfrom_field)

    if args.rpath_field:
        data_processor.set_rpath_field(args.rpath_field)

    if args.msgid_field:
        data_processor.set_msgid_field(args.msgid_field)

    if args.msgsz_field:
        data_processor.set_msgsz_field(args.msgsz_field)

    if args.date_field:
        data_processor.set_date_field(args.date_field)

    if args.date_format:
        data_processor.set_date_format = args.date_format

    # Set processing flags
    if args.remove_prvs:
        data_processor.set_opt_remove_prvs(args.remove_prvs)

    if args.decode_srs:
        data_processor.set_opt_decode_srs(args.decode_srs)

    if args.no_empty_hfrom:
        data_processor.set_opt_empty_from(args.no_empty_hfrom)

    if args.no_display:
        data_processor.set_opt_no_display(args.no_display)

    if args.gen_hfrom:
        data_processor.set_opt_gen_hfrom(args.gen_hfrom)

    if args.gen_rpath:
        data_processor.set_opt_gen_rpath(args.gen_rpath)

    if args.gen_alignment:
        data_processor.set_opt_gen_alignment(args.gen_alignment)

    if args.gen_msgid:
        data_processor.set_opt_gen_msgid(args.gen_msgid)

    f_current = 1
    f_total = len(file_names)
    for f in file_names:
        print("Processing:", f, f'({f_current} of {f_total})')
        data_processor.process_file(f)
        f_current += 1

    print()
    print("\nTotal records processed:", data_processor.get_total_processed_count())
    print_summary("Skipped due to empty sender", data_processor.get_empty_sender_count())
    print_summary("Excluded by excluded senders list", data_processor.get_excluded_sender_count(),
                  args.show_skip_detail)
    print_summary("Excluded by excluded domains list", data_processor.get_excluded_domain_count(),
                  args.show_skip_detail)
    print_summary("Excluded by restricted domains list", data_processor.get_excluded_domain_count(),
                  args.show_skip_detail)
    print()

    date_counts = data_processor.get_date_counter()
    if date_counts:
        print("Records by Day")
        for d in sorted(date_counts.keys()):
            print("{}:".format(d), date_counts[d])
        print()

    data_report = MessageDataReport(args.output_file, data_processor, args.threshold)
    data_report.generate_report()
    data_report.close()

    print("Please see report: {}".format(args.output_file))


if __name__ == '__main__':
    main()
