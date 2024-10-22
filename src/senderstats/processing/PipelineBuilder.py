from senderstats.processing.FilterManager import FilterManager
from senderstats.processing.ProcessorManager import ProcessorManager
from senderstats.processing.TransformManager import TransformManager


class PipelineBuilder:
    def __init__(self, transform_manager: TransformManager, filter_manager: FilterManager,
                 processor_manager: ProcessorManager):
        self.transform_manager = transform_manager
        self.filter_manager = filter_manager
        self.processor_manager = processor_manager

    def build_pipeline(self, args):
        pipeline = (self.transform_manager.csv_to_message_data_transform
                    .set_next(self.filter_manager.exclude_empty_sender_filter)
                    .set_next(self.transform_manager.mfrom_transform))

        if args.excluded_domains:
            pipeline.set_next(self.filter_manager.exclude_domain_filter)

        if args.excluded_senders:
            pipeline.set_next(self.filter_manager.exclude_senders_filter)

        if args.restricted_domains:
            pipeline.set_next(self.filter_manager.restrict_senders_filter)

        pipeline.set_next(self.transform_manager.date_transform)
        pipeline.set_next(self.processor_manager.mfrom_processor)

        if args.gen_hfrom or args.gen_alignment:
            pipeline.set_next(self.transform_manager.hfrom_transform)
        if args.gen_hfrom:
            pipeline.set_next(self.processor_manager.hfrom_processor)
        if args.gen_rpath:
            pipeline.set_next(self.transform_manager.rpath_transform)
            pipeline.set_next(self.processor_manager.rpath_processor)
        if args.gen_msgid:
            pipeline.set_next(self.transform_manager.msgid_transform)
            pipeline.set_next(self.processor_manager.msgid_processor)
        if args.gen_alignment:
            pipeline.set_next(self.processor_manager.align_processor)

        pipeline.set_next(self.processor_manager.date_processor)

        return pipeline
