from common.defaults import DEFAULT_DATE_FORMAT
from data.processors.DateProcessor import DateProcessor
from report.MessageDataReport import MessageDataReport
from senderstats.common.utils import print_list_with_title
from senderstats.common.validators import parse_arguments
from senderstats.processing import process_input_files, process_exclusions, configure_field_mapper, build_pipeline, \
    process_files, get_processors


def main():
    args = parse_arguments()
    file_names = process_input_files(args.input_files)
    args = process_exclusions(args)

    print_list_with_title("Files to be processed:", file_names)
    print_list_with_title("Senders excluded from processing:", args.excluded_senders)
    print_list_with_title("Domains excluded from processing:", args.excluded_domains)
    print_list_with_title("Domains constrained or processing:", args.restricted_domains)

    field_mapper = configure_field_mapper(args)
    pipeline = build_pipeline(args, field_mapper)

    # Add to calculate date metrics
    date_processor = DateProcessor(DEFAULT_DATE_FORMAT)
    pipeline.set_next(date_processor)

    process_files(file_names, field_mapper, pipeline)
    processor_list = get_processors(pipeline)
    report = MessageDataReport(args.output_file, args.threshold, len(date_processor.get_date_counter()))

    report.create_sizing_summary()

    for processor in processor_list:
        report.create_summary(processor)

    report.create_hourly_summary(date_processor)

    report.close()


if __name__ == "__main__":
    main()
