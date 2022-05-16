from pyspark import Row


class InputMapper(object):

    @staticmethod
    def date(row: Row) -> str:
        row.__getattribute__("date")

    @staticmethod
    def home_team(row: Row) -> str:
        row.__getattribute__("home_team")

    @staticmethod
    def away_team(row: Row) -> str:
        row.__getattribute__("away_team")

    @staticmethod
    def home_score(row: Row) -> int:
        row.__getattribute__("home_score")

    @staticmethod
    def away_score(row: Row) -> int:
        row.__getattribute__("away_score")

    @staticmethod
    def tournament(row: Row) -> str:
        row.__getattribute__("tournament")

    @staticmethod
    def country(row: Row) -> str:
        row.__getattribute__("country")

    @staticmethod
    def neutral(row: Row) -> bool:
        row.__getattribute__("neutral")
