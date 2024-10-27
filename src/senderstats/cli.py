import asyncio

from processing.PipelineManager import PipelineManager
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

    # This will create a CSV data source or WebSocket for PoD Log API
    data_source_manager = DataSourceManager(config)

    # Pipeline manager builds the correct filters and processing depending on the report options
    pipeline_manager = PipelineManager(config)

    processor = PipelineProcessor(data_source_manager, pipeline_manager)

    asyncio.run(processor.process_data())

    # Display filtering statistics
    pipeline_manager.get_filter_manager().display_summary()

    report = PipelineProcessorReport(config.output_file, pipeline_manager)
    report.generate()
    report.close()


if __name__ == "__main__":
    main()
