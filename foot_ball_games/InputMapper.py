from pyspark import Row


def get_date(row: Row) -> str:
    row.__getattribute__("date")


def get_home_team(row: Row) -> str:
    row.__getattribute__("home_team")


def get_away_team(row: Row) -> str:
    row.__getattribute__("away_team")


def get_home_score(row: Row) -> int:
    row.__getattribute__("home_score")


def get_away_score(row: Row) -> int:
    row.__getattribute__("away_score")


def get_tournament(row: Row) -> str:
    row.__getattribute__("tournament")


def get_city(row: Row) -> str:
    row.__getattribute__("city")


def get_country(row: Row) -> str:
    row.__getattribute__("country")


def get_neutral(row: Row) -> bool:
    row.__getattribute__("neutral")
