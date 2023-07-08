"""starting Spark Session"""

from pyspark.sql import SparkSession
from pass_spark_config import read_spark_config

custom_config = read_spark_config()

spark = (
    SparkSession.builder
    .config("spark.ui.showConsoleProgress", "false")
    .config('spark.sql.autobroadcastjointhreshold', -1)
    # .config("spark.sql.catalogImplementation", "hive")
    # https://sparkbyexamples.com/spark/spark-adaptive-query-execution/ for Spark 3
    .appName(custom_config['app_name']).getOrCreate()
)

sc = spark.sparkContext

sc.setLogLevel("ERROR")
