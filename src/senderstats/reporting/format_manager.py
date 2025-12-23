class FormatManager:
    def __init__(self, workbook):
        self.workbook = workbook

        # --- Generic ---
        self.header_format = self.create_format({
            "bold": True,
            "bottom": 1,
        })

        # Table/report body cells
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
        })

        # Labels (make these LEFT aligned so the sheet reads naturally)
        self.summary_label_format = self.create_format({
            "bold": True,
            "align": "left",
            "valign": "vcenter",
        })

        # Values (right aligned)
        self.summary_value_format = self.create_format({
            "align": "right",
            "valign": "vcenter",
        })

        # Highlight label/value rows (your yellow rows)
        self.summary_highlight_label_format = self.create_format({
            "bold": True,
            "align": "left",
            "valign": "vcenter",
            "bg_color": "#FFF59D",   # softer yellow than pure #FFFF00
        })
        self.summary_highlight_value_format = self.create_format({
            "align": "right",
            "valign": "vcenter",
            "bg_color": "#FFF59D",
        })

        # Editable input cells (threshold + dropdown)
        self.input_value_format = self.create_format({
            "align": "right",
            "valign": "vcenter",
            "locked": False,
            "bg_color": "#FFF2CC",   # Excel-ish input yellow
            "border": 1,
        })

        # Slightly muted helper text if you ever want it
        self.helper_text_format = self.create_format({
            "align": "left",
            "valign": "vcenter",
            "font_color": "#666666",
        })

    def create_format(self, properties):
        return self.workbook.add_format(properties)
