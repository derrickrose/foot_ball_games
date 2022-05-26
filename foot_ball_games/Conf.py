from typing import List


class Env:
    def __init__(self, selected_country: str, year: str, extension: str):
        self.selected_country = selected_country
        self.year = year
        self.extension = extension


def from_spark_submit(args: List[str]) -> Env:
    env = Env(args[1], args[2], args[3])
    return env
