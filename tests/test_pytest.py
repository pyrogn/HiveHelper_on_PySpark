import os
import sys

# do not know how to make it the Python way
sys.path.append(os.path.join(os.path.dirname(__file__), os.pardir, "hhop_lib"))

from pyspark.sql.functions import col
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window as W

custom_spark_params = {
    "app_name": "custom_app_name123",
}
print(os.getcwd(), __name__)


# import pass_spark_config
from pass_spark_config import write_spark_config

write_spark_config(custom_spark_params)

from hhop import (
    DFExtender,
    SchemaManager,
    TablePartitionDescriber,
)  # main classes
from funs import (
    read_table,
    write_table,
    write_read_table,
    union_all,
    deduplicate_df,
)  # useful functions
from spark_init import spark
from exceptions import HhopException


def test_start_spark():
    print(123412341234123412341234)
    print(os.path.join(os.path.dirname(__file__), os.pardir))
    print(os.getcwd())
    assert spark.sql("select 1 limit 1").collect()[0][0] == 1


def test_start_spark2():
    print(123412341234123412341234)
    print(os.path.join(os.path.dirname(__file__), os.pardir))
    print(os.getcwd())
    assert spark.sql("select 2 limit 1").collect()[0][0] == 2
