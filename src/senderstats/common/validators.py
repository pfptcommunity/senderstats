import argparse
import sys

import regex as re

from .defaults import *
from .regex_patterns import EMAIL_ADDRESS_REGEX, VALID_DOMAIN_REGEX


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

    field_group.add_argument('--rcpts', metavar='Rcpts', dest="rcpts_field",
                             type=str, required=False,
                             help=f'CSV field of the header recipient addresses. (default={DEFAULT_RCPTS_FIELD})')

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

    # reporting_group.add_argument('-t', '--threshold', dest="threshold", metavar='N', type=int, required=False,
    #                              help=f'Adjust summary report threshold for messages per day to be considered application traffic. (default={DEFAULT_THRESHOLD})',
    #                              default=DEFAULT_THRESHOLD)

    parser_group.add_argument('--expand-recipients', action='store_true', dest="expand_recipients",
                              help='Expand recipients counts messages by destination. E.g. 1 message going to 3 people, is 3 messages sent.')

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
                              help=f'Date format used to parse the timestamps. (default={DEFAULT_DATE_FORMAT.replace("%", "%%")})',
                              default=DEFAULT_DATE_FORMAT)

    # output_group.add_argument('--show-skip-detail', action='store_true', dest="show_skip_detail",
    #                           help='Show skipped details')

    if len(sys.argv) == 1:
        parser.print_usage()  # Print usage information if no arguments are passed
        sys.exit(1)

    return parser.parse_args()


def is_valid_domain_syntax(domain_name: str):
    """
    Validates if the provided domain name follows the expected syntax.

    :param domain_name: Domain name to validate.
    :return: The domain name if valid.
    :raises: argparse.ArgumentTypeError if the domain name syntax is invalid.
    """
    if not re.match(VALID_DOMAIN_REGEX, domain_name, re.IGNORECASE):
        raise argparse.ArgumentTypeError(f"Invalid domain name syntax: {domain_name}")
    return domain_name


def is_valid_email_syntax(email: str):
    """
    Validates if the provided email follows the expected syntax.

    :param email: Email address to validate.
    :return: The email address if valid.
    :raises: argparse.ArgumentTypeError if the email syntax is invalid.
    """
    if not re.match(EMAIL_ADDRESS_REGEX, email, re.IGNORECASE):
        raise argparse.ArgumentTypeError(f"Invalid email address syntax: {email}")
    return email


def validate_xlsx_file(file_path):
    """
    Validates if the provided file path ends with a .xlsx extension.

    :param file_path: Path to the file to validate.
    :return: The file path if valid.
    :raises: argparse.ArgumentTypeError if the file does not end with .xlsx.
    """
    if not file_path.lower().endswith('.xlsx'):
        raise argparse.ArgumentTypeError("File must have a .xlsx extension.")
    return file_path
