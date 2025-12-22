from senderstats.core.processors import *
from senderstats.processing.config_manager import ConfigManager


class ProcessorManager:
    def __init__(self, config: ConfigManager):
        self.mfrom_processor = MFromProcessor(config.sample_subject, config.with_probability, config.expand_recipients, debug=config.debug)
        self.hfrom_processor = HFromProcessor(config.sample_subject, config.with_probability, config.expand_recipients, debug=config.debug)
        self.msgid_processor = MIDProcessor(config.sample_subject, config.with_probability, config.expand_recipients, debug=config.debug)
        self.rpath_processor = RPathProcessor(config.sample_subject, config.with_probability, config.expand_recipients, debug=config.debug)
        self.align_processor = AlignmentProcessor(config.sample_subject, config.with_probability, config.expand_recipients, debug=config.debug)
        self.date_processor = DateProcessor(config.expand_recipients)
