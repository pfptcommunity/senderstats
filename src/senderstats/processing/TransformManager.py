from senderstats.common.Config import Config
from senderstats.core.transformers import *


class TransformManager:
    def __init__(self, config: Config):
        self.date_transform = DateTransform(config.date_format)
        self.mfrom_transform = MFromTransform(config.decode_srs, config.remove_prvs)
        self.hfrom_transform = HFromTransform(config.no_display_name, config.no_empty_hfrom)
        self.msgid_transform = MIDTransform()
        self.rpath_transform = RPathTransform(config.decode_srs, config.remove_prvs)
