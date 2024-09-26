import argparse
import sys
from senderstats.common.utils import print_list_with_title
from senderstats.common.validators import parse_arguments
from senderstats.processing import process_input_files, process_exclusions, configure_field_mapper, build_pipeline, process_files

def main():
    args = parse_arguments()
    file_names = process_input_files(args.input_files)
    args = process_exclusions(args)

    print_list_with_title("Files to be processed:", file_names)
    print_list_with_title("Senders excluded from processing:", args.excluded_senders)
    print_list_with_title("Domains excluded from processing:", args.excluded_domains)
    print_list_with_title("Domains constrained or processing:", args.restricted_domains)

    field_mapper = configure_field_mapper(args)
    pipeline = build_pipeline(args)
    process_files(file_names, field_mapper, pipeline)

if __name__ == "__main__":
    main()