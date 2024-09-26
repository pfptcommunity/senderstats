from typing import List

from data import Mapper


class MessageData:
    def __init__(self, field_mapper: Mapper):
        """Initialize the DataRecord object with default values."""
        self.field_mapper = field_mapper
        self.message_size = 0
        self.mfrom = ''
        self.hfrom = ''
        self.rpath = ''
        self.rcpts = []
        self.msgid = ''
        self.msgid_domain = ''
        self.msgid_host = ''
        self.subject = ''
        self.date = ''

    def load(self, row: List[str]):
        """Set or reset values based on the new row."""
        self.message_size = int(self.field_mapper.get_field(row, 'msgsz')) if self.field_mapper.get_field(row,
                                                                                                          'msgsz').isdigit() else 0
        self.mfrom = self.field_mapper.get_field(row, 'mfrom').casefold().strip()
        self.hfrom = self.field_mapper.get_field(row, 'hfrom').casefold().strip()
        self.rpath = self.field_mapper.get_field(row, 'rpath').casefold().strip()
        self.rcpts = self.field_mapper.get_field(row, 'rcpts').casefold().strip().split(',')
        self.msgid = self.field_mapper.get_field(row, 'msgid').casefold().strip('<>[] ')
        self.msgid_domain = ''
        self.msgid_host = ''
        self.subject = self.field_mapper.get_field(row, 'subject').strip()
        self.date = self.field_mapper.get_field(row, 'date').strip()
