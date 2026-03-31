class FormatManager:
    def __init__(self, workbook):
        self.workbook = workbook

        # --- Generic ---
        self.header_format = self.create_format({
            "bold": True,
            "bottom": 1,
        })

        self.data_cell_format = self.create_format({
            "valign": "top",
            "text_wrap": True,
        })

        # --- Dashboard / Summary styles ---
        self.grouped_header_format = self.create_format({
            "bold": True,
            "align": "center",
            "valign": "vcenter",
            "text_wrap": False,
            "bg_color": "#D9E1F2",
            "bottom": 1,
            "hidden": True,     # optional (just affects formula bar)
        })

        self.summary_label_format = self.create_format({
            "bold": True,
            "align": "left",
            "valign": "vcenter",
            "hidden": True,     # optional
        })

        self.summary_value_format = self.create_format({
            "align": "right",
            "valign": "vcenter",
            "hidden": True,     # IMPORTANT: hides formulas when sheet is protected
        })

        self.summary_highlight_label_format = self.create_format({
            "bold": True,
            "align": "left",
            "valign": "vcenter",
            "bg_color": "#FFF59D",
            "hidden": True,     # optional
        })

        self.summary_highlight_value_format = self.create_format({
            "align": "right",
            "valign": "vcenter",
            "bg_color": "#FFF59D",
            "hidden": True,     # IMPORTANT
        })

        # Editable input cells (threshold + dropdown) - NOT hidden
        self.input_value_format = self.create_format({
            "align": "right",
            "valign": "vcenter",
            "locked": False,
            "bg_color": "#FFF2CC",
            "border": 1,
        })

    def create_format(self, properties):
        return self.workbook.add_format(properties)
