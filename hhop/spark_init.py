"""starting Spark Session"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as F
import pyspark
import pandas as pd
from pyspark.sql.window import Window as W

spark = (
    SparkSession.builder.config("spark.sql.catalogImplementation", "hive")
    # https://sparkbyexamples.com/spark/spark-adaptive-query-execution/ for Spark 3
    .appName("app").getOrCreate()
)

sc = spark.sparkContext

sc.setLogLevel("ERROR")
