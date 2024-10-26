from senderstats.common.Config import Config
from senderstats.processing.FilterManager import FilterManager
from senderstats.processing.ProcessorManager import ProcessorManager
from senderstats.processing.TransformManager import TransformManager


class PipelineBuilder:
    def __init__(self, transform_manager: TransformManager, filter_manager: FilterManager,
                 processor_manager: ProcessorManager):
        self.transform_manager = transform_manager
        self.filter_manager = filter_manager
        self.processor_manager = processor_manager

    def build_pipeline(self, config: Config):
        pipeline = (self.filter_manager.exclude_empty_sender_filter
                    .set_next(self.filter_manager.exclude_invalid_size_filter)
                    .set_next(self.transform_manager.mfrom_transform))

        if config.exclude_ips:
            pipeline.set_next(self.filter_manager.exclude_ip_filter)

        if config.exclude_domains:
            pipeline.set_next(self.filter_manager.exclude_domain_filter)

        if config.exclude_senders:
            pipeline.set_next(self.filter_manager.exclude_senders_filter)

        if config.restrict_domains:
            pipeline.set_next(self.filter_manager.restrict_senders_filter)

        pipeline.set_next(self.transform_manager.date_transform)
        pipeline.set_next(self.processor_manager.mfrom_processor)

        if config.gen_hfrom or config.gen_alignment:
            pipeline.set_next(self.transform_manager.hfrom_transform)
        if config.gen_hfrom:
            pipeline.set_next(self.processor_manager.hfrom_processor)
        if config.gen_rpath:
            pipeline.set_next(self.transform_manager.rpath_transform)
            pipeline.set_next(self.processor_manager.rpath_processor)
        if config.gen_msgid:
            pipeline.set_next(self.transform_manager.msgid_transform)
            pipeline.set_next(self.processor_manager.msgid_processor)
        if config.gen_alignment:
            pipeline.set_next(self.processor_manager.align_processor)

        pipeline.set_next(self.processor_manager.date_processor)

        return pipeline
