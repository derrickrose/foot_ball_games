def filter_by_country(country: str) -> str:
    return f""" home_team == '{country}' OR away_team == '{country}' """


def filter_by_year(year: str) -> str:
    return f""" date > '{int(year) - 1}' AND date < '{int(year) + 1}' """


def filter_by_year_between(min_year: str, max_year: str) -> str:
    return f""" date between '{min_year}' AND '{max_year}' """
