class MessageData:
    def __init__(self):
        self.fields = {}

    def set_field(self, field_name: str, value):
        self.fields[field_name] = value

    def get_field(self, field_name: str):
        return self.fields.get(field_name, '')

    def __getattr__(self, name):
        if name in self.fields:
            return self.fields[name]
        raise AttributeError(f"'MessageData' object has no attribute '{name}'")

    def __setattr__(self, name, value):
        if name == 'fields':
            super().__setattr__(name, value)
        else:
            self.fields[name] = value

    def __repr__(self):
        return f"MessageData(fields={self.fields})"
