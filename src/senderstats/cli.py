from senderstats.common.defaults import DEFAULT_THRESHOLD
from senderstats.common.validators import parse_arguments
from senderstats.processing import PipelineProcessor
from senderstats.report.MessageDataReport import MessageDataReport


def main():
    args = parse_arguments()
    processor = PipelineProcessor(args)
    processor.exclusion_summary()
    processor.process_files()

    report = MessageDataReport(args.output_file, DEFAULT_THRESHOLD, processor.get_date_count())
    report.create_sizing_summary()

    for proc in processor.get_processors():
        report.create_summary(proc)

    report.create_hourly_summary(processor.get_date_processor())

    report.close()


if __name__ == "__main__":
    main()
