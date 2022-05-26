import os.path

from pyspark.sql import SparkSession

import Schemas

SEPARATOR = os.path.sep

if __name__ == "__main__":
    spark_session = SparkSession.builder.appName("foot-ball-games") \
        .master("local") \
        .getOrCreate()

    foot_ball_games_DF = spark_session.read.schema(Schemas.FootBallGame.schema) \
        .option("mode", "permissive") \
        .option("header", True).csv(f"..{SEPARATOR}files{SEPARATOR}foot-ball-games.csv")

    foot_ball_games_DF.show()
