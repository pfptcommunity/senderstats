class FormatManager:
    def __init__(self, workbook):
        self.workbook = workbook
        self.header_format = self.create_format({'bold': True})
        self.summary_format = self.create_format({'bold': True, 'align': 'right', 'hidden': True})
        self.summary_highlight_format = self.create_format(
            {'bold': True, 'align': 'right', 'hidden': True, 'bg_color': '#FFFF00'})
        self.summary_values_format = self.create_format({'align': 'right', 'hidden': True})
        self.summary_highlight_values_format = self.create_format(
            {'align': 'right', 'hidden': True, 'bg_color': '#FFFF00'})
        self.field_values_format = self.create_format({'align': 'right', 'locked': False, 'hidden': True})
        self.data_cell_format = self.create_format({'valign': 'top', 'text_wrap': True})

    def create_format(self, properties):
        return self.workbook.add_format(properties)
