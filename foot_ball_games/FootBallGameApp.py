import os.path

from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame

import Schemas

from Filters import filter_by_country, filter_by_year
from Mappers import to_output_game_result
from Schemas import OutputCountryGame

SEPARATOR = os.path.sep

if __name__ == "__main__":
    spark_session = SparkSession.builder.appName("foot-ball-games") \
        .master("local") \
        .getOrCreate()

    foot_ball_games_DF = spark_session.read.schema(Schemas.FootBallGame.schema) \
        .option("mode", "permissive") \
        .option("header", True).csv(f"..{SEPARATOR}files{SEPARATOR}foot-ball-games.csv")

    country = "Morocco"
    morocco_games_DF = foot_ball_games_DF.filter(filter_by_country(country)).filter(
        filter_by_year("2000"))

    rdd1: RDD = morocco_games_DF.rdd.map(
        lambda x: to_output_game_result(x)
    )

    # test_df: DataFrame = morocco_games_DF.rdd.map(
    #     lambda x: x
    # ).toDF(Schemas.FootBallGame.schema)

    # test_df.show()

    #
    # output_morocco_games = morocco_games_DF.rdd.map(
    #     lambda row: to_output_game_result(row, country)
    # ).toDF(OutputCountryGame.schema_list)

    morocco_games_DF.show()
    # output_morocco_games.show()
