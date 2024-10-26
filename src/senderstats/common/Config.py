import os
from glob import glob
from typing import List

from common.defaults import DEFAULT_DOMAIN_EXCLUSIONS


class Config:
    def __init__(self, args):
        # Data source configurations
        self.source_type = args.source_type
        self.input_files = Config.__prepare_input_files(args.input_files)
        self.token = args.token
        self.cluster_id = args.cluster_id

        # Output configurations
        self.output_file = args.output_file

        # Field mapping configurations
        self.ip_field = args.ip_field
        self.mfrom_field = args.mfrom_field
        self.hfrom_field = args.hfrom_field
        self.rcpts_field = args.rcpts_field
        self.rpath_field = args.rpath_field
        self.msgid_field = args.msgid_field
        self.subject_field = args.subject_field
        self.msgsz_field = args.msgsz_field
        self.date_field = args.date_field

        # Processing options
        self.gen_hfrom = args.gen_hfrom
        self.gen_rpath = args.gen_rpath
        self.gen_alignment = args.gen_alignment
        self.gen_msgid = args.gen_msgid
        self.expand_recipients = args.expand_recipients
        self.no_display_name = args.no_display
        self.remove_prvs = args.remove_prvs
        self.decode_srs = args.decode_srs
        self.no_empty_hfrom = args.no_empty_hfrom
        self.sample_subject = args.sample_subject
        self.exclude_ips = Config.__prepare_exclusions(args.exclude_ips)
        if args.no_default_exclude_domains:
            self.exclude_domains = Config.__prepare_exclusions(args.exclude_domains)
        else:
            self.exclude_domains = Config.__prepare_exclusions(DEFAULT_DOMAIN_EXCLUSIONS + args.exclude_domains)
        self.restrict_domains = Config.__prepare_exclusions(args.restrict_domains)
        self.exclude_senders = Config.__prepare_exclusions(args.exclude_senders)
        self.date_format = args.date_format
        self.no_default_exclude_domains = args.no_default_exclude_domains

    @staticmethod
    def __prepare_input_files(input_files: List[str]):
        file_names = []
        for f in input_files:
            file_names += glob(f)
        file_names = set(file_names)
        return [file for file in file_names if os.path.isfile(file)]

    @staticmethod
    def __prepare_exclusions(exclusions: List[str]):
        return sorted(list({item.casefold() for item in exclusions}))
