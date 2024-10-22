import csv


class CSVProcessor:
    def __init__(self, mapper_manager):
        self.mapper_manager = mapper_manager

    def process(self, input_file, pipeline):
        try:
            with open(input_file, 'r', encoding='utf-8-sig') as file:
                reader = csv.reader(file)
                headers = next(reader)
                self.mapper_manager.field_mapper.reindex(headers)
                for csv_line in reader:
                    pipeline.handle(csv_line)
        except Exception as e:
            print(f"Error processing file {input_file}: {e}")
