from senderstats.common.Config import Config
from senderstats.core.processors import *


class ProcessorManager:
    def __init__(self, config: Config):
        self.mfrom_processor = MFromProcessor(config.sample_subject, config.expand_recipients)
        self.hfrom_processor = HFromProcessor(config.sample_subject, config.expand_recipients)
        self.msgid_processor = MIDProcessor(config.sample_subject, config.expand_recipients)
        self.rpath_processor = RPathProcessor(config.sample_subject, config.expand_recipients)
        self.align_processor = AlignmentProcessor(config.sample_subject, config.expand_recipients)
        self.date_processor = DateProcessor(config.expand_recipients)
