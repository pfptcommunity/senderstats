from senderstats.interfaces import Filter, Processor, Transform
from senderstats.processing.config_manager import ConfigManager
from senderstats.processing.filter_manager import FilterManager
from senderstats.processing.processor_manager import ProcessorManager
from senderstats.processing.transform_manager import TransformManager


class PipelineManager:
    def __init__(self, config: ConfigManager):
        self.__filter_manager = FilterManager(config)
        self.__transform_manager = TransformManager(config)
        self.__processor_manager = ProcessorManager(config)

        pipeline = (self.__filter_manager.exclude_empty_sender_filter
                    .set_next(self.__filter_manager.exclude_invalid_size_filter)
                    .set_next(self.__transform_manager.mfrom_transform))

        if config.exclude_ips:
            pipeline.set_next(self.__filter_manager.exclude_ip_filter)

        if config.exclude_domains:
            pipeline.set_next(self.__filter_manager.exclude_domain_filter)

        if config.exclude_senders:
            pipeline.set_next(self.__filter_manager.exclude_senders_filter)

        if config.restrict_domains:
            pipeline.set_next(self.__filter_manager.restrict_senders_filter)

        pipeline.set_next(self.__transform_manager.date_transform)
        pipeline.set_next(self.__processor_manager.mfrom_processor)

        if config.gen_hfrom or config.gen_alignment:
            pipeline.set_next(self.__transform_manager.hfrom_transform)
        if config.gen_hfrom:
            pipeline.set_next(self.__processor_manager.hfrom_processor)
        if config.gen_rpath:
            pipeline.set_next(self.__transform_manager.rpath_transform)
            pipeline.set_next(self.__processor_manager.rpath_processor)
        if config.gen_msgid:
            pipeline.set_next(self.__transform_manager.msgid_transform)
            pipeline.set_next(self.__processor_manager.msgid_processor)
        if config.gen_alignment:
            pipeline.set_next(self.__processor_manager.align_processor)

        pipeline.set_next(self.__processor_manager.date_processor)

        self.__pipeline = pipeline

    def get_pipeline(self):
        return self.__pipeline

    def get_filter_manager(self):
        return self.__filter_manager

    def get_processor_manager(self):
        return self.__processor_manager

    def get_transform_manager(self):
        return self.__transform_manager

    def get_active_processors(self) -> list:
        processors = []
        current = self.__pipeline
        while current is not None:
            if isinstance(current, Processor):
                processors.append(current)
            current = current.get_next()
        return processors

    def get_active_filters(self) -> list:
        processors = []
        current = self.__pipeline
        while current is not None:
            if isinstance(current, Filter):
                processors.append(current)
            current = current.get_next()
        return processors

    def get_active_transforms(self) -> list:
        processors = []
        current = self.__pipeline
        while current is not None:
            if isinstance(current, Transform):
                processors.append(current)
            current = current.get_next()
        return processors
