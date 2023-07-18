"""starting Spark Session"""

from pyspark.sql import SparkSession


def get_spark_builder(app_name="default_name"):
    sparkBuilder = (
        SparkSession.builder.config("spark.ui.showConsoleProgress", "false").config(
            "spark.sql.autobroadcastjointhreshold", -1
        )
        # .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        # .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # https://sparkbyexamples.com/spark/spark-adaptive-query-execution/ for Spark 3
        .appName(app_name)
    )
    return sparkBuilder

    # sc = spark.sparkContext

    # sc.setLogLevel("ERROR") # moved to the main process
