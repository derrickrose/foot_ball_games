import os.path
import sys

from pyspark.sql import SparkSession, DataFrame

import Schemas
from Conf import from_spark_submit, SEPARATOR

from Filters import filter_by_country, filter_by_year
from OutputMapper import to_output_game_result

if __name__ == "__main__":
    test = ["0", "local", "local", "Mexico", "MX", "2000"]
    conf = from_spark_submit(test)
    # conf = from_spark_submit(sys.argv)

    spark_session: SparkSession = SparkSession.builder.appName("foot-ball-games") \
        .master("local") \
        .getOrCreate()

    foot_ball_games_DF: DataFrame = spark_session.read.schema(Schemas.FootBallGame.schema) \
        .option("mode", "permissive") \
        .option("header", True).csv(conf.input_path)

    selected_country_games_DF: DataFrame = foot_ball_games_DF.filter(filter_by_country(conf.selected_country)) \
        .filter(filter_by_year(conf.year)).rdd.map(lambda row: to_output_game_result(row, conf.selected_country)) \
        .toDF(Schemas.OutputCountryGame.schema_list)

    selected_country_games_DF.write.mode("overwrite").json(
        f"{conf.output_path}result{SEPARATOR}{conf.extension}{SEPARATOR}{conf.year}")
