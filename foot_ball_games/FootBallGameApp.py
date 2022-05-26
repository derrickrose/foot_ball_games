import os.path

from pyspark.sql import SparkSession, DataFrame

import Conf
import Schemas
from Conf import from_spark_submit

from Filters import filter_by_country, filter_by_year
from OutputMapper import to_output_game_result

SEPARATOR = os.path.sep

if __name__ == "__main__":
    conf: Conf.Configuration = from_spark_submit()

    spark_session: SparkSession = SparkSession.builder.appName("foot-ball-games") \
        .master("local") \
        .getOrCreate()

    foot_ball_games_DF: DataFrame = spark_session.read.schema(Schemas.FootBallGame.schema) \
        .option("mode", "permissive") \
        .option("header", True).csv(f"..{SEPARATOR}files{SEPARATOR}foot-ball-games.csv")

    selected_country_games_DF: DataFrame = foot_ball_games_DF.filter(filter_by_country(conf.selected_country)) \
        .filter(filter_by_year(conf.year)).rdd.map(lambda row: to_output_game_result(row, conf.selected_country)) \
        .toDF(Schemas.OutputCountryGame.schema_list)

    selected_country_games_DF.write.mode("overwrite").json(f"..{SEPARATOR}files{SEPARATOR}result{SEPARATOR}")
