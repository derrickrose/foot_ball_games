import os
from typing import List

SEPARATOR = os.path.sep


class Env:
    def __init__(self, input_path: str, output_path: str, selected_country: str, extension: str, year: str):
        self.input_path = input_path if input_path != "local" else f"..{SEPARATOR}files{SEPARATOR}foot-ball-games.csv"
        self.output_path = output_path if output_path != "local" else f"..{SEPARATOR}files{SEPARATOR}"
        self.selected_country = selected_country
        self.extension = extension
        self.year = year


def from_spark_submit(args: List[str]) -> Env:
    env = Env(args[1], args[2], args[3], args[4], args[5])
    return env
