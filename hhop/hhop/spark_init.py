"""starting Spark Session"""

from pyspark.sql import SparkSession
from hhop.hhop.pass_spark_config import read_spark_config

custom_config = {}
try:
    custom_config = read_spark_config(rename_file=False, remove_dir=False)
except FileNotFoundError:
    print("temp config file not found, using defaults")

spark = (
    SparkSession.builder.config("spark.ui.showConsoleProgress", "false")
    .config("spark.sql.autobroadcastjointhreshold", -1)
    # .config("spark.sql.catalogImplementation", "hive")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # https://sparkbyexamples.com/spark/spark-adaptive-query-execution/ for Spark 3
    .appName(custom_config.get("app_name", "defaultname"))
    .getOrCreate()
)

sc = spark.sparkContext

sc.setLogLevel("ERROR")
