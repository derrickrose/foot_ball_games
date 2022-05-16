from pyspark.sql.types import StructType, IntegerType, StringType, BooleanType, StructField, DataType


class FootBallGame(object):

    @staticmethod
    def int_field(field_name: str, nullable: bool) -> StructField:
        return StructField(field_name, IntegerType(), nullable)

    @staticmethod
    def string_field(field_name: str, nullable: bool) -> StructField:
        return StructField(field_name, StringType(), nullable)

    @staticmethod
    def bool_field(field_name: bool, nullable: bool) -> StructField:
        return StructField(field_name, BooleanType(), nullable)

    _date: str = string_field("date", False)
    _home_team: str = string_field("home_team", False)
    _away_team: str = string_field("away_team", False)
    _home_score: int = int_field("home_score", False)
    _away_score: int = int_field("away_score", False)
    _tournament: str = string_field("tournament", False)
    _city: str = string_field("city", False)
    _country: str = string_field("country", False)
    _neutral: bool = bool_field("neutral", False)

    foot_ball_game = StructType(
        [_date, _home_team, _away_team, _home_score, _away_score, _tournament, _city, _country, _neutral])
