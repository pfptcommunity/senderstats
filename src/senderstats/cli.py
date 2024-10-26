import asyncio

from senderstats.processing.DataSourceManager import DataSourceManager
from senderstats.cli_args import parse_arguments
from senderstats.common.Config import Config
from senderstats.processing.PipelineProcessor import PipelineProcessor
from senderstats.reporting.PipelineProcessorReport import PipelineProcessorReport


def main():
    args = parse_arguments()
    # Config object stores all arguments parsed
    config = Config(args)
    data_source_manager = DataSourceManager(config)
    processor = PipelineProcessor(config, data_source_manager)
    processor.exclusion_summary()
    asyncio.run(processor.process_data())
    processor.filter_summary()

    report = PipelineProcessorReport(args.output_file, processor)
    report.generate()
    report.close()


if __name__ == "__main__":
    main()
