from random import random
from typing import Dict
from lib.DataProcessingStatus import DataProcessingStatus
from lib.DataRecord import DataRecord


class DataProcessor:

    def process(self, data_record: DataRecord) -> DataProcessingStatus:
        """General data processing method to be overridden."""
        raise NotImplementedError("This method should be overridden by subclasses.")

    def update_message_size_and_subjects(self, data: Dict, message_size: int, subject: str):
        # Ensure the message_size list exists and append the new message size
        data.setdefault("message_size", []).append(message_size)

        #if not self.__opt_sample_subject:
        #    return

        data.setdefault("subjects", [])

        # Avoid storing empty subject lines
        if not subject:
            return

        # Calculate probability based on the number of processed records
        probability = 1 / len(data['message_size'])

        # Ensure at least one subject is added if subjects array is empty
        if not data['subjects'] or random() < probability:
            data['subjects'].append(subject)
