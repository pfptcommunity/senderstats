from senderstats.cli_args import parse_arguments
from senderstats.processing.config_manager import ConfigManager
from senderstats.processing.data_source_manager import DataSourceManager
from senderstats.processing.pipeline_manager import PipelineManager
from senderstats.processing.pipeline_processor import PipelineProcessor
from senderstats.reporting.pipeline_processor_report import PipelineProcessorReport


def main():
    # Config object stores all arguments parsed
    config = ConfigManager(parse_arguments())

    config.display_filter_criteria()

    # This will create a CSV data source or WebSocket for PoD Log API
    data_source_manager = DataSourceManager(config)

    # Pipeline manager builds the correct filters and processing depending on the report options
    pipeline_manager = PipelineManager(config)

    processor = PipelineProcessor(data_source_manager, pipeline_manager)

    processor.process_data()

    # Display filtering statistics
    pipeline_manager.get_filter_manager().display_summary()

    report = PipelineProcessorReport(config.output_file, pipeline_manager)
    report.generate()
    report.close()


if __name__ == "__main__":
    main()
