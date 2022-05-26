from pyspark import Row


def get_date(row: Row) -> str:
    return row.__getitem__("date")


def get_home_team(row: Row) -> str:
    return row.__getitem__("home_team")


def get_away_team(row: Row) -> str:
    return row.__getitem__("away_team")


def get_home_score(row: Row) -> int:
    return row.__getitem__("home_score")


def get_away_score(row: Row) -> int:
    return row.__getitem__("away_score")


def get_tournament(row: Row) -> str:
    return row.__getitem__("tournament")


def get_city(row: Row) -> str:
    return row.__getitem__("city")


def get_country(row: Row) -> str:
    return row.__getitem__("country")


def get_neutral(row: Row) -> bool:
    return row.__getitem__("neutral")
