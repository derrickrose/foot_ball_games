import os.path

from pyspark.sql import SparkSession, DataFrame

import Schemas

from Filters import filter_by_country, filter_by_year
from OutputMapper import to_output_game_result

SEPARATOR = os.path.sep

if __name__ == "__main__":
    spark_session = SparkSession.builder.appName("foot-ball-games") \
        .master("local") \
        .getOrCreate()

    foot_ball_games_DF = spark_session.read.schema(Schemas.FootBallGame.schema) \
        .option("mode", "permissive") \
        .option("header", True).csv(f"..{SEPARATOR}files{SEPARATOR}foot-ball-games.csv")

    country = "Morocco"
    morocco_games_DF: DataFrame = foot_ball_games_DF.filter(filter_by_country(country)) \
        .filter(filter_by_year("2000")).rdd.map(lambda row: to_output_game_result(row, country)) \
        .toDF(Schemas.OutputCountryGame.schema_list)

    morocco_games_DF.write.mode("overwrite").json(f"..{SEPARATOR}files{SEPARATOR}result{SEPARATOR}")
