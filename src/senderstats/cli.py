import asyncio

from senderstats.cli_args import parse_arguments
from senderstats.common.Config import Config
from senderstats.common.utils import print_list_with_title
from senderstats.processing.DataSourceManager import DataSourceManager
from senderstats.processing.DataSourceManager import SourceType
from senderstats.processing.PipelineProcessor import PipelineProcessor
from senderstats.reporting.PipelineProcessorReport import PipelineProcessorReport


def main():
    # Config object stores all arguments parsed
    config = Config(parse_arguments())

    if config.source_type == SourceType.CSV:
        print_list_with_title("Files to be processed:", config.input_files)
    print_list_with_title("IPs excluded from processing:", config.exclude_ips)
    print_list_with_title("Senders excluded from processing:", config.exclude_senders)
    print_list_with_title("Domains excluded from processing:", config.exclude_domains)
    print_list_with_title("Domains constrained for processing:", config.restrict_domains)

    data_source_manager = DataSourceManager(config)

    processor = PipelineProcessor(config, data_source_manager)

    asyncio.run(processor.process_data())

    processor.filter_summary()

    report = PipelineProcessorReport(config.output_file, processor)
    report.generate()
    report.close()


if __name__ == "__main__":
    main()
