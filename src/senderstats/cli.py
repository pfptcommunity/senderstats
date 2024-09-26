import csv
import os
import sys
from glob import glob

from common.constants import DEFAULT_MFROM_FIELD, DEFAULT_HFROM_FIELD, DEFAULT_RPATH_FIELD, DEFAULT_MSGSZ_FIELD, \
    DEFAULT_MSGID_FIELD, DEFAULT_SUBJECT_FIELD, DEFAULT_DATE_FIELD, DEFAULT_THRESHOLD, DEFAULT_DOMAIN_EXCLUSIONS, \
    DEFAULT_DATE_FORMAT
from data.Mapper import Mapper
from data.MessageData import MessageData
from data.filters import *
from data.processors import *
from data.transformers import *
from senderstats.common.utils import *
from senderstats.common.validators import *


def parse_arguments():
    parser = argparse.ArgumentParser(prog="senderstats", add_help=False,
                                     description="""This tool helps identify the top senders based on smart search outbound message exports.""",
                                     formatter_class=lambda prog: argparse.HelpFormatter(prog, max_help_position=80))

    required_group = parser.add_argument_group('Input / Output arguments (required)')
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

    field_group.add_argument('--subject', metavar='Subject', dest="subject_field",
                             type=str, required=False,
                             help=f'CSV field of the Subject, only used if --sample-subject is specified. (default={DEFAULT_SUBJECT_FIELD})')

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

    parser_group.add_argument('--sample-subject', action='store_true', dest="sample_subject",
                              help='Enable probabilistic random sampling of subject lines found during processing')

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

    default_field_mappings = {
        'mfrom': DEFAULT_MFROM_FIELD,
        'hfrom': DEFAULT_HFROM_FIELD,
        'rpath': DEFAULT_RPATH_FIELD,
        'msgsz': DEFAULT_MSGSZ_FIELD,
        'msgid': DEFAULT_MSGID_FIELD,
        'subject': DEFAULT_SUBJECT_FIELD,
        'date': DEFAULT_DATE_FIELD
    }

    # Configure fields
    field_mapper = Mapper(default_field_mappings)
    if args.mfrom_field:
        field_mapper.add_mapping('mfrom', args.mfrom_field)

    if args.hfrom_field:
        field_mapper.add_mapping('hfrom', args.hfrom_field)

    if args.rpath_field:
        field_mapper.add_mapping('rpath', args.rpath_field)

    if args.msgid_field:
        field_mapper.add_mapping('msgid', args.msgid_field)

    if args.msgsz_field:
        field_mapper.add_mapping('msgsz', args.msgsz_field)

    if args.subject_field:
        field_mapper.add_mapping('subject', args.subject_field)

    if args.date_field:
        field_mapper.add_mapping('date', args.date_field)

    # Define filters, they may not all be used
    exclude_empty_sender_filter = ExcludeEmptySenderFilter()
    exclude_domain_filter = ExcludeDomainFilter(args.excluded_domains)
    exclude_senders_filter = ExcludeSenderFilter(args.excluded_senders)
    restrict_senders_filter = RestrictDomainFilter(args.restricted_domains)

    # Define transformers, they may not all be used
    mfrom_transform = MFromTransform(args.decode_srs, args.remove_prvs)
    hfrom_transform = HFromTransform(args.no_display, args.no_empty_hfrom)
    msgid_transform = MIDTransform()
    rpath_transform = RPathTransform(args.decode_srs, args.remove_prvs)

    # Define processors, they may not all be used
    mfrom_processor = MFromProcessor(args.sample_subject)
    hfrom_processor = HFromProcessor(args.sample_subject)
    msgid_processor = MIDProcessor(args.sample_subject)
    rpath_processor = RPathProcessor(args.sample_subject)
    align_processor = AlignmentProcessor(args.sample_subject)

    # Build the filtering minimum is always envelope senders
    pipeline = (exclude_empty_sender_filter
                .set_next(mfrom_transform)
                .set_next(exclude_domain_filter)
                .set_next(exclude_senders_filter)
                .set_next(restrict_senders_filter)
                .set_next(mfrom_processor))

    # Setup other processors
    if args.gen_hfrom or args.gen_alignment:
        pipeline.set_next(hfrom_transform)

    if args.gen_hfrom:
        pipeline.set_next(hfrom_processor)

    if args.gen_alignment:
        pipeline.set_next(align_processor)

    if args.gen_rpath:
        pipeline.set_next(rpath_transform)
        pipeline.set_next(rpath_processor)

    if args.gen_msgid:
        pipeline.set_next(msgid_transform)
        pipeline.set_next(msgid_processor)

    message_data = MessageData(field_mapper)

    f_current = 1
    f_total = len(file_names)
    for input_file in file_names:
        print("Processing:", f, f'({f_current} of {f_total})')
        with (open(input_file, 'r', encoding='utf-8-sig') as input_file):
            reader = csv.reader(input_file)
            headers = next(reader)  # Read the first line which contains the headers
            field_mapper.configure(headers)
            for csv_line in reader:
                message_data.load(csv_line)
                pipeline.handle(message_data)

        f_current += 1

    # print()
    # print("\nTotal records processed:", data_processor.get_total_processed_count())
    # print_summary("Skipped due to empty sender", data_processor.get_empty_sender_count())
    # print_summary("Excluded by excluded senders list", data_processor.get_excluded_sender_count(),
    #               args.show_skip_detail)
    # print_summary("Excluded by excluded domains list", data_processor.get_excluded_domain_count(),
    #               args.show_skip_detail)
    # print_summary("Excluded by restricted domains list", data_processor.get_excluded_domain_count(),
    #               args.show_skip_detail)
    # print()
    #
    # date_counts = data_processor.get_date_counter()
    # if date_counts:
    #     print("Records by Day")
    #     for d in sorted(date_counts.keys()):
    #         print("{}:".format(d), date_counts[d])
    #     print()
    #
    # data_report = MessageDataReport(args.output_file, data_processor, args.threshold)
    # data_report.generate_report()
    # data_report.close()
    #
    # print("Please see report: {}".format(args.output_file))


if __name__ == '__main__':
    main()
