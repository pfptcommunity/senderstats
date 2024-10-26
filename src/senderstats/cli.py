import asyncio

from senderstats.common.validators import parse_arguments
from senderstats.processing.PipelineProcessor import PipelineProcessor
from senderstats.reporting.PipelineProcessorReport import PipelineProcessorReport


def main():
    args = parse_arguments()
    processor = PipelineProcessor(args)
    processor.exclusion_summary()
    asyncio.run(processor.process_data())
    processor.filter_summary()

    report = PipelineProcessorReport(args.output_file, processor)
    report.generate()
    report.close()


if __name__ == "__main__":
    main()
