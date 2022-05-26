from typing import Tuple
from InputMapper import get_date, get_home_team, get_away_team, get_home_score, get_away_score, get_city, \
    get_neutral, get_tournament, get_country

from pyspark import Row


def get_opponent(row: Row, selected_country: str) -> str:
    return get_home_team(row) if get_away_team(row) == selected_country else get_away_team(row)


def play_at_home(row: Row, selected_country: str) -> bool:
    return get_home_team(row) == selected_country


def get_team_score(row: Row, selected_country: str) -> str:
    return get_home_score(row) if play_at_home(row, selected_country) else get_away_score(row)


def get_opponent_score(row: Row, country: str) -> str:
    return get_away_score(row) if play_at_home(row, country) else get_home_score(row)


def to_output_game_result(row: Row, selected_country: str) -> Tuple:
    date = get_date(row)
    team_score = get_team_score(row, selected_country)
    team = selected_country
    opponent = get_opponent(row, selected_country)
    opponent_score = get_opponent_score(row, selected_country)
    at_home = play_at_home(row, selected_country)
    tournament = get_tournament(row)
    city = get_city(row)
    country = get_country(row)
    is_neutral = get_neutral(row)
    return (
        date,
        team_score,
        team,
        opponent,
        opponent_score,
        at_home,
        tournament,
        city,
        country,
        is_neutral
    )
