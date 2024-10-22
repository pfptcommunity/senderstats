import os
from glob import glob


class InputFileManager:
    def __init__(self, args):
        self.input_files = self.__process_input_files(args.input_files)

    def __process_input_files(self, input_files):
        file_names = []
        for f in input_files:
            file_names += glob(f)
        file_names = set(file_names)
        return [file for file in file_names if os.path.isfile(file)]
