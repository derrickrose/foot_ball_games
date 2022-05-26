from typing import List


class Configuration(object):
    def __int__(self, **kwargs):
        self.selected_country = kwargs.get("selected_country")
        self.year = kwargs.get("year")
        self.extension = kwargs.get("extension")


def from_spark_submit(args: List[str]) -> Configuration:
    return Configuration(
        **{"selected_country": args[1],
           "year": args[2],
           "extension": args[3]}
    )
