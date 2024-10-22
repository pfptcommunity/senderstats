from senderstats.core.transformers import *
from senderstats.processing.MapperManager import MapperManager


class TransformManager:
    def __init__(self, args, mapper_manager: MapperManager):
        self.csv_to_message_data_transform = MessageDataTransform(mapper_manager.field_mapper)
        self.date_transform = DateTransform(args.date_format)
        self.mfrom_transform = MFromTransform(args.decode_srs, args.remove_prvs)
        self.hfrom_transform = HFromTransform(args.no_display, args.no_empty_hfrom)
        self.msgid_transform = MIDTransform()
        self.rpath_transform = RPathTransform(args.decode_srs, args.remove_prvs)
