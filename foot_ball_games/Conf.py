from typing import List


class Env:
    def __init__(self, selected_country: str, extension: str, year: str):
        self.selected_country = selected_country
        self.extension = extension
        self.year = year


def from_spark_submit(args: List[str]) -> Env:
    env = Env(args[1], args[2], args[3])
    return env
