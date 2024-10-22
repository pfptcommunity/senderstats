from senderstats.core.processors import *


class ProcessorManager:
    def __init__(self, args):
        self.mfrom_processor = MFromProcessor(args.sample_subject, args.expand_recipients)
        self.hfrom_processor = HFromProcessor(args.sample_subject, args.expand_recipients)
        self.msgid_processor = MIDProcessor(args.sample_subject, args.expand_recipients)
        self.rpath_processor = RPathProcessor(args.sample_subject, args.expand_recipients)
        self.align_processor = AlignmentProcessor(args.sample_subject, args.expand_recipients)
        self.date_processor = DateProcessor(args.expand_recipients)
